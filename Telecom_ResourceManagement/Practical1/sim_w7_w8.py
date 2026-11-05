#!/usr/bin/env python3
"""
ICS 2403 - Milestone 7 (Fault Tolerance and Recovery) and
Milestone 8 (Distributed System Models) - simulation harness.

Standard library only. Every random draw comes from an explicit seed, so
re-running with the same arguments reproduces every number in the report.

    python3 sim_w7_w8.py --out results --seeds 8 --m8-seeds 5

Milestone 7: a time-stepped model (1 s tick) of a session-routing service
replicated on n nodes. Faults are injected stochastically (or by script),
failures are detected by missed heartbeats, a new leader is elected, and
service is restored. Outputs availability, MTTR, false failovers and
split-brain exposure.

Milestone 8: an event-driven model of five distributed-system models
(host-based, workstation with migration, processor pool, server-based,
integrated) under one workload with common random numbers, plus a node
failure experiment.

All fault rates and service times are accelerated relative to production
systems; the results are model-based comparisons, not measurements of a
real network.
"""
import argparse
import bisect
import heapq
import json
import math
import os
import platform
import random
import statistics
import sys
from dataclasses import dataclass, field, asdict, replace

# Two-sided 95% t critical values, indexed by degrees of freedom.
T_CRIT = {1: 12.706, 2: 4.303, 3: 3.182, 4: 2.776, 5: 2.571,
          6: 2.447, 7: 2.365, 8: 2.306, 9: 2.262}


def ci95(xs):
    """Mean and 95% half-width across seeds."""
    n = len(xs)
    if n == 0:
        return (float("nan"), float("nan"))
    if n == 1:
        return (xs[0], float("nan"))
    return (statistics.fmean(xs),
            T_CRIT.get(n - 1, 1.96) * statistics.stdev(xs) / math.sqrt(n))


def fm(pair, scale=1.0, digits=2, unit=""):
    m, h = pair
    if math.isnan(m):
        return "n/a"
    if math.isnan(h):
        return f"{m * scale:.{digits}f}{unit}"
    return f"{m * scale:.{digits}f} ± {h * scale:.{digits}f}{unit}"


# =========================================================================
# MILESTONE 7 - fault injection, detection, failover and recovery
# =========================================================================

@dataclass
class FaultMix:
    crash: float = 0.40       # fail-stop; repaired after Exp(mttr_crash_s)
    hang: float = 0.20        # service failure; health checks fail; Exp(mttr_hang_s)
    partition: float = 0.25   # communication failure; unreachable for U(partition_s)
    omission: float = 0.15    # omission failure; heartbeats dropped at p_omit for U(omission_s)


@dataclass
class M7Config:
    name: str
    n_replicas: int
    quorum: bool              # True: serving and election need a majority
    mtbf_s: float = 4000.0    # mean time between faults per node
    hb_k: int = 3             # missed heartbeats before a node is suspected
    horizon_s: float = 40_000.0
    p_drop_base: float = 0.01  # background heartbeat loss
    p_omit: float = 0.5        # heartbeat loss during an omission burst
    mttr_crash_s: float = 120.0
    mttr_hang_s: float = 60.0
    partition_s: tuple = (10.0, 120.0)
    omission_s: tuple = (5.0, 30.0)
    d_elect_s: float = 2.0     # leader election / promotion time
    d_sync_s: float = 5.0      # catch-up time before a repaired node may serve or vote
    d_restart_s: float = 10.0  # single-node restart delay (n = 1 only)
    mix: FaultMix = field(default_factory=FaultMix)


def run_m7(cfg, seed, scripted=(), trace=False):
    """
    scripted: iterable of (step_second, node, kind, duration_s) injected
    deterministically. trace=True returns an event log.
    """
    rng = random.Random(seed)
    n = cfg.n_replicas
    maj = n // 2 + 1
    steps = int(cfg.horizon_s)
    lam = 1.0 / cfg.mtbf_s  # per node per second (dt = 1 s)

    cum, acc = [], 0.0
    for kind, p in (("crash", cfg.mix.crash), ("hang", cfg.mix.hang),
                    ("partition", cfg.mix.partition), ("omission", cfg.mix.omission)):
        acc += p
        cum.append((acc, kind))

    up = [True] * n
    hung = [False] * n
    part_until = [0.0] * n
    omit_until = [0.0] * n
    repair_at = [None] * n
    eligible_at = [0.0] * n
    missed = [0] * n
    suspected = [False] * n
    leader = 0
    leader_ready = 0.0
    prev_leader = None         # deposed leader that may still accept local writes
    serving_prev = True
    down_since = None
    outages = []
    faults = 0
    fault_counts = {k: 0 for _, k in cum}
    false_failovers = 0
    split_brain = 0
    serving_ticks = 0
    log = []

    def note(t, msg):
        if trace:
            log.append((t, msg))

    def inject(i, kind, t, duration=None):
        nonlocal faults
        faults += 1
        fault_counts[kind] += 1
        if kind == "crash":
            up[i] = False
            repair_at[i] = t + (duration if duration is not None
                                else rng.expovariate(1 / cfg.mttr_crash_s))
        elif kind == "hang":
            hung[i] = True
            repair_at[i] = t + (duration if duration is not None
                                else rng.expovariate(1 / cfg.mttr_hang_s))
        elif kind == "partition":
            part_until[i] = t + (duration if duration is not None
                                 else rng.uniform(*cfg.partition_s))
        else:
            omit_until[i] = t + (duration if duration is not None
                                 else rng.uniform(*cfg.omission_s))
        note(t, f"FAULT {kind} injected on node {i}")

    for step in range(steps):
        t = float(step)

        # 1. Fault injection (scripted first, then stochastic).
        for (st, node, kind, dur) in scripted:
            if int(st) == step:
                inject(node, kind, t, dur)
        for i in range(n):
            if (up[i] and not hung[i] and part_until[i] <= t
                    and omit_until[i] <= t and rng.random() < lam):
                u = rng.random()
                kind = next((k for c, k in cum if u < c), "omission")
                inject(i, kind, t)

        # 2. Repairs (crashed or hung nodes return; they must catch up first).
        for i in range(n):
            if repair_at[i] is not None and t >= repair_at[i]:
                up[i] = True
                hung[i] = False
                repair_at[i] = None
                eligible_at[i] = t + cfg.d_sync_s
                note(t, f"REPAIR node {i}")

        # 3. Heartbeats and failure detection.
        for i in range(n):
            p_loss = cfg.p_omit if omit_until[i] > t else cfg.p_drop_base
            reachable = up[i] and not hung[i] and part_until[i] <= t
            delivered = reachable and rng.random() >= p_loss
            missed[i] = 0 if delivered else missed[i] + 1
            was = suspected[i]
            suspected[i] = missed[i] >= cfg.hb_k
            if suspected[i] and not was:
                note(t, f"SUSPECT node {i} ({missed[i]} missed heartbeats)")

        # 4. Who can serve or vote right now (ground truth).
        ok = [up[i] and not hung[i] and part_until[i] <= t and t >= eligible_at[i]
              for i in range(n)]
        n_ok = sum(ok)

        # 5. Leader management (the monitor's view: suspicion, not ground truth).
        cand = [i for i in range(n) if ok[i] and not suspected[i]]
        if leader is not None and suspected[leader]:
            if up[leader] and not hung[leader] and part_until[leader] <= t:
                false_failovers += 1          # healthy leader deposed by omission
            prev_leader = leader
            leader = None
            note(t, "leader suspected; failover started")
        if leader is None and cand and (not cfg.quorum or len(cand) >= maj):
            leader = cand[0]
            leader_ready = t + (cfg.d_restart_s if n == 1 else cfg.d_elect_s)
            note(t, f"LEADER -> node {leader}, serving at t+{leader_ready - t:.0f}s")

        # The deposed leader is no longer a writer once it is down, hung,
        # or reachable and back in the membership.
        if prev_leader is not None and (
                not up[prev_leader] or hung[prev_leader]
                or (part_until[prev_leader] <= t and not suspected[prev_leader])):
            prev_leader = None
        # Without quorum fencing, a deposed leader that is still up keeps
        # accepting local writes: count those exposure seconds.
        if (prev_leader is not None and leader is not None
                and leader != prev_leader and not cfg.quorum):
            split_brain += 1

        # 6. Service state.
        has_quorum = (not cfg.quorum) or n_ok >= maj
        serving = (leader is not None and ok[leader]
                   and t >= leader_ready and has_quorum)
        if serving:
            serving_ticks += 1
        if serving != serving_prev:
            note(t, "SERVICE RESTORED" if serving else "SERVICE DOWN")
            serving_prev = serving
        if not serving and down_since is None:
            down_since = t
        elif serving and down_since is not None:
            outages.append(t - down_since)
            down_since = None

    if down_since is not None:
        outages.append(steps - down_since)

    avail = serving_ticks / steps
    return {
        "availability": avail,
        "downtime_min_per_year": (1.0 - avail) * 525_600,
        "outages": len(outages),
        "mttr_s": statistics.fmean(outages) if outages else 0.0,
        "max_outage_s": max(outages) if outages else 0.0,
        "faults": faults,
        "fault_counts": fault_counts,
        "false_failovers": false_failovers,
        "split_brain_s": split_brain,
        "log": log,
    }


def m7_cell(cfg, seeds):
    runs = [run_m7(cfg, s) for s in seeds]
    mttrs = [r["mttr_s"] for r in runs if r["outages"] > 0]
    return {
        "config": cfg.name,
        "n_replicas": cfg.n_replicas,
        "quorum": cfg.quorum,
        "mtbf_s": cfg.mtbf_s,
        "hb_k": cfg.hb_k,
        "seeds": len(seeds),
        "availability": ci95([r["availability"] for r in runs]),
        "downtime_min_per_year": ci95([r["downtime_min_per_year"] for r in runs]),
        "outages_per_run": ci95([r["outages"] for r in runs]),
        "mttr_s": ci95(mttrs) if mttrs else (float("nan"), float("nan")),
        "max_outage_s": ci95([r["max_outage_s"] for r in runs]),
        "faults_per_run": ci95([r["faults"] for r in runs]),
        "false_failovers_per_run": ci95([r["false_failovers"] for r in runs]),
        "split_brain_s_per_run": ci95([r["split_brain_s"] for r in runs]),
    }


def run_milestone7(seeds):
    configs = [
        M7Config("A: single node + restart", 1, False),
        M7Config("B: primary-backup (n=2)", 2, False),
        M7Config("C: majority quorum (n=3)", 3, True),
        M7Config("D: majority quorum (n=5)", 5, True),
    ]
    out = {"baseline": [], "mtbf_sweep": [], "heartbeat_sweep": [], "scripted": {}}

    # Baseline: four replication configurations at MTBF 4000 s, hb_k = 3.
    for cfg in configs:
        out["baseline"].append(m7_cell(cfg, seeds))

    # Fault-rate sweep (failure-driven stress test) for A, B, C.
    for cfg in configs[:3]:
        for mtbf in (8000.0, 2000.0, 1000.0):
            out["mtbf_sweep"].append(m7_cell(replace(cfg, mtbf_s=mtbf), seeds))

    # Detection-timeout sweep on the quorum configuration C.
    for k in (1, 2, 5):
        out["heartbeat_sweep"].append(m7_cell(replace(configs[2], hb_k=k), seeds))

    # Scripted, deterministic failure timelines (Part E: failure -> detection
    # -> recovery -> restoration). Random faults disabled for these runs.
    scenarios = {
        "leader crash at t=100 s (repair 120 s)": [(100, 0, "crash", 120.0)],
        "leader partitioned at t=100 s (60 s)": [(100, 0, "partition", 60.0)],
    }
    for label, script in scenarios.items():
        out["scripted"][label] = {}
        for cfg in configs:
            quiet = replace(cfg, mtbf_s=1e12)
            r = run_m7(quiet, seed=1, scripted=script, trace=True)
            out["scripted"][label][cfg.name] = {
                "outages": r["outages"],
                "max_outage_s": r["max_outage_s"],
                "split_brain_s": r["split_brain_s"],
                "events": [(t, m) for (t, m) in r["log"] if 95 <= t <= 300],
            }
    return out


# =========================================================================
# MILESTONE 8 - distributed system models under one workload
# =========================================================================

MU = 100.0             # requests/s per processor (mean service time 10 ms)
RHO = 0.7              # offered load per processor
HOT_FACTOR = 1.2       # node 0 receives 1.2x the average share of attachments (hot spot)
CRIT_FRAC = 0.4        # latency-critical share of requests
SLA_MS = 50.0
T_MIG_S = 0.005        # workstation: migrate if home queue wait exceeds 5 ms
T_SPILL_S = 0.005      # integrated edge: spill to pool if wait exceeds 5 ms
MIG_COST_S = 0.003     # cost of migrating a request to another workstation
RETRY_S = 0.002        # server-based: client retry penalty after a failed server
RTT_S = {"host": 0.0, "workstation": 0.0, "pool": 0.001,
         "server": 0.002, "integrated": 0.001}
MODELS = ["host", "workstation", "pool", "server", "integrated", "integrated_cm"]
MODEL_LABEL = {
    "host": "Host-based",
    "workstation": "Workstation (+migration)",
    "pool": "Processor pool",
    "server": "Server-based",
    "integrated": "Integrated (50/50 split)",
    "integrated_cm": "Integrated (capacity-matched)",
}


def hot_cdf(k, hot=None):
    """Node 0 gets hot/k of attachments (hot = HOT_FACTOR); the rest split evenly.
    Stable for any k because node 0's utilization is hot * rho."""
    hot = HOT_FACTOR if hot is None else hot
    w = [hot / k] + [(1.0 - hot / k) / (k - 1)] * (k - 1)
    acc, cdf = 0.0, []
    for x in w:
        acc += x
        cdf.append(acc)
    return cdf


def run_m8(model, n, seed, rho=RHO, n_req=60_000, fail=False):
    """Event-driven FCFS model. Common random numbers across models for a given seed."""
    arr_rng = random.Random(seed * 10 + 1)
    cls_rng = random.Random(seed * 10 + 2)
    home_rng = random.Random(seed * 10 + 3)
    svc_rng = random.Random(seed * 10 + 4)

    lam = rho * n * MU
    arrivals, t = [], 0.0
    for _ in range(n_req):
        t += arr_rng.expovariate(lam)
        arrivals.append(t)
    H = arrivals[-1]
    services = [svc_rng.expovariate(MU) for _ in range(n_req)]
    crit = [cls_rng.random() < CRIT_FRAC for _ in range(n_req)]
    us = [home_rng.random() for _ in range(n_req)]

    S = n // 4            # server-based: number of servers (4 processors each)
    if model in ("host", "workstation"):
        stations = [[0.0] for _ in range(n)]
        cdf = hot_cdf(n)
    elif model == "pool":
        stations = [[0.0] * n]
        cdf = None
    elif model == "server":
        stations = [[0.0] * 4 for _ in range(S)]
        cdf = None
    else:  # integrated variants: E single-processor edge hosts + pool of n-E
        # 50/50 split: half the processors at the edge.
        # capacity-matched: edge processors = critical share of the processors.
        E = n // 2 if model == "integrated" else max(1, round(CRIT_FRAC * n))
        stations = [[0.0] for _ in range(E)] + [[0.0] * (n - E)]
        cdf = hot_cdf(E)
    POOL = len(stations) - 1  # only meaningful for integrated

    def home_of(i):
        if model == "server":
            return min(int(us[i] * S), S - 1)
        return min(bisect.bisect_right(cdf, us[i]), len(cdf) - 1)

    # Node 0 fails for the middle 20% of the run (fixed victim across models).
    t_f, t_r = (0.4 * H, 0.6 * H) if fail else (math.inf, math.inf)

    def alive(st, a):
        return not (fail and model != "pool" and st == 0 and t_f <= a < t_r)

    busy = 0.0
    lat = []
    lost = 0
    msgs = 0
    win_total = win_ok = 0
    pool_removed = False

    for i in range(n_req):
        a = arrivals[i]
        svc = services[i]
        in_win = fail and t_f <= a < t_r
        if in_win:
            win_total += 1
        # Pool failure: one processor is unavailable until repair.
        if fail and model == "pool" and not pool_removed and a >= t_f:
            heapq.heapreplace(stations[0], t_r)
            pool_removed = True

        st = None
        extra = 0.0
        m = 0

        if model == "host":
            h = home_of(i)
            if alive(h, a):
                st = h

        elif model == "workstation":
            h = home_of(i)
            if alive(h, a) and max(0.0, stations[h][0] - a) <= T_MIG_S:
                st = h
            else:
                best = math.inf
                for s in range(n):
                    if alive(s, a) and stations[s][0] < best:
                        best, st = stations[s][0], s
                if st is not None and st != h:
                    extra += MIG_COST_S
                    m += 2

        elif model == "pool":
            st = 0
            extra += RTT_S["pool"]
            m += 2

        elif model == "server":
            s = home_of(i)
            m += 2
            extra += RTT_S["server"]
            if not alive(s, a):
                m += 2
                extra += RETRY_S
                best = math.inf
                s = None
                for x in range(S):
                    if alive(x, a) and stations[x][0] < best:
                        best, s = stations[x][0], x
            st = s

        else:  # integrated
            if crit[i]:
                h = home_of(i)
                if alive(h, a) and max(0.0, stations[h][0] - a) <= T_SPILL_S:
                    st = h
                else:
                    st = POOL
                    extra += RTT_S["integrated"]
                    m += 2
            else:
                st = POOL
                extra += RTT_S["integrated"]
                m += 2

        if st is None:
            lost += 1
            continue

        q = stations[st]
        start = q[0] if q[0] > a else a
        fin = start + svc
        heapq.heapreplace(q, fin)
        busy += svc
        lat.append((fin - a + extra) * 1000.0)
        msgs += m
        if in_win:
            win_ok += 1

    lat_sorted = sorted(lat)

    def pct(p):
        if not lat_sorted:
            return math.inf
        return lat_sorted[min(len(lat_sorted) - 1, int(p * len(lat_sorted)))]

    return {
        "model": model,
        "n": n,
        "rho": rho,
        "fail": fail,
        "mean_ms": statistics.fmean(lat) if lat else math.inf,
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        "throughput_rps": len(lat) / H,
        "utilization": busy / (n * H),
        "success_rate": len(lat) / n_req,
        "sla_violation_rate": (sum(1 for x in lat if x > SLA_MS) + lost) / n_req,
        "window_success_rate": (win_ok / win_total) if (fail and win_total) else None,
        "processors": sum(len(q) for q in stations),
        "msgs_per_req": msgs / n_req,
    }


def m8_cell(model, n, seeds, rho=RHO, fail=False):
    runs = [run_m8(model, n, s, rho=rho, fail=fail) for s in seeds]
    keys = ["mean_ms", "p95_ms", "p99_ms", "throughput_rps", "utilization",
            "success_rate", "sla_violation_rate", "msgs_per_req"]
    row = {"model": model, "label": MODEL_LABEL[model], "n": n, "rho": rho,
           "fail": fail, "seeds": len(seeds), "processors": runs[0]["processors"]}
    for k in keys:
        row[k] = ci95([r[k] for r in runs])
    if fail:
        row["window_success_rate"] = ci95([r["window_success_rate"] for r in runs])
    return row


def run_milestone8(seeds):
    out = {"scalability": [], "load_sweep": [], "failure": [], "cost": []}
    for n in (4, 8, 16, 32, 64):
        for model in MODELS:
            out["scalability"].append(m8_cell(model, n, seeds, rho=RHO))
    for rho in (0.3, 0.5, 0.7, 0.85):
        for model in MODELS:
            out["load_sweep"].append(m8_cell(model, 16, seeds, rho=rho))
    for model in MODELS:
        out["failure"].append(m8_cell(model, 16, seeds, rho=RHO, fail=True))
    for model in MODELS:
        out["cost"].append(next(r for r in out["scalability"]
                                if r["model"] == model and r["n"] == 16))
    return out


# =========================================================================
# Reporting
# =========================================================================

def md_m7(res):
    lines = []
    lines.append("## M7 baseline (MTBF 4000 s, hb_k = 3, 95% CI over seeds)\n")
    lines.append("| Configuration | Availability | Downtime (min/yr) | Outages/run | MTTR (s) | Max outage (s) | Faults/run | False failovers/run | Split-brain s/run |")
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for c in res["baseline"]:
        lines.append(f"| {c['config']} | {fm(c['availability'], 100, 3, '%')} | "
                     f"{fm(c['downtime_min_per_year'], 1, 0)} | {fm(c['outages_per_run'], 1, 1)} | "
                     f"{fm(c['mttr_s'], 1, 1)} | {fm(c['max_outage_s'], 1, 0)} | "
                     f"{fm(c['faults_per_run'], 1, 1)} | {fm(c['false_failovers_per_run'], 1, 1)} | "
                     f"{fm(c['split_brain_s_per_run'], 1, 0)} |")
    lines.append("\n## M7 fault-rate sweep (failure-driven stress test)\n")
    lines.append("| Configuration | MTBF (s) | Availability | Outages/run | MTTR (s) | Faults/run |")
    lines.append("|---|---|---|---|---|---|")
    for c in res["mtbf_sweep"]:
        lines.append(f"| {c['config']} | {c['mtbf_s']:.0f} | {fm(c['availability'], 100, 3, '%')} | "
                     f"{fm(c['outages_per_run'], 1, 1)} | {fm(c['mttr_s'], 1, 1)} | {fm(c['faults_per_run'], 1, 1)} |")
    lines.append("\n## M7 detection-timeout sweep (quorum n=3)\n")
    lines.append("| hb_k (missed heartbeats) | Detection time (s) | Availability | MTTR (s) | False failovers/run |")
    lines.append("|---|---|---|---|---|")
    base = res["baseline"][2]
    rows = [(1, None)] + [(c["hb_k"], c) for c in res["heartbeat_sweep"]]
    for k, c in sorted([(1, res["heartbeat_sweep"][0]), (2, res["heartbeat_sweep"][1]),
                        (3, base), (5, res["heartbeat_sweep"][2])], key=lambda x: x[0]):
        lines.append(f"| {k} | {k} | {fm(c['availability'], 100, 3, '%')} | "
                     f"{fm(c['mttr_s'], 1, 1)} | {fm(c['false_failovers_per_run'], 1, 1)} |")
    lines.append("\n## M7 scripted failure timelines (seed 1, random faults off)\n")
    for label, per_cfg in res["scripted"].items():
        lines.append(f"### Scenario: {label}\n")
        lines.append("| Configuration | Outages | Longest outage (s) | Split-brain s |")
        lines.append("|---|---|---|---|")
        for name, d in per_cfg.items():
            lines.append(f"| {name} | {d['outages']} | {d['max_outage_s']:.0f} | {d['split_brain_s']} |")
        lines.append("")
        lines.append("Event timeline for configuration C (quorum, n=3):\n")
        lines.append("```")
        for t, msg in per_cfg["C: majority quorum (n=3)"]["events"]:
            lines.append(f"t={t:6.0f}s  {msg}")
        lines.append("```\n")
    return "\n".join(lines)


def md_m8(res):
    L = []
    L.append("## M8 scalability (offered load 0.7 per processor, 95% CI over seeds)\n")
    L.append("| Nodes | Model | Mean (ms) | p95 (ms) | p99 (ms) | Utilization | Throughput (req/s) | SLA (50 ms) violations |")
    L.append("|---|---|---|---|---|---|---|---|")
    for r in res["scalability"]:
        L.append(f"| {r['n']} | {r['label']} | {fm(r['mean_ms'], 1, 1)} | {fm(r['p95_ms'], 1, 1)} | "
                 f"{fm(r['p99_ms'], 1, 1)} | {fm(r['utilization'], 100, 1, '%')} | "
                 f"{fm(r['throughput_rps'], 1, 0)} | {fm(r['sla_violation_rate'], 100, 2, '%')} |")
    L.append("\n## M8 load sweep (16 nodes)\n")
    L.append("| Offered load | Model | Mean (ms) | p95 (ms) | SLA violations |")
    L.append("|---|---|---|---|---|")
    for r in res["load_sweep"]:
        L.append(f"| {r['rho']:.2f} | {r['label']} | {fm(r['mean_ms'], 1, 1)} | {fm(r['p95_ms'], 1, 1)} | "
                 f"{fm(r['sla_violation_rate'], 100, 2, '%')} |")
    L.append("\n## M8 reliability: node 0 fails for the middle 20% of the run (16 nodes, load 0.7)\n")
    L.append("| Model | Overall success | Success during failure window | p95 (ms) | SLA violations |")
    L.append("|---|---|---|---|---|")
    for r in res["failure"]:
        L.append(f"| {r['label']} | {fm(r['success_rate'], 100, 2, '%')} | "
                 f"{fm(r['window_success_rate'], 100, 2, '%')} | {fm(r['p95_ms'], 1, 1)} | "
                 f"{fm(r['sla_violation_rate'], 100, 2, '%')} |")
    L.append("\n## M8 cost proxies (16 nodes, load 0.7)\n")
    L.append("| Model | Processors | Network messages per request |")
    L.append("|---|---|---|")
    for r in res["cost"]:
        L.append(f"| {r['label']} | {r['processors']} | {fm(r['msgs_per_req'], 1, 2)} |")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default="results")
    ap.add_argument("--seeds", type=int, default=8, help="seeds for Milestone 7")
    ap.add_argument("--m8-seeds", type=int, default=5, help="seeds for Milestone 8")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)

    m7_seeds = list(range(1, args.seeds + 1))
    m8_seeds = list(range(1, args.m8_seeds + 1))

    meta = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "m7_seeds": m7_seeds,
        "m8_seeds": m8_seeds,
        "m7_horizon_s": M7Config("x", 1, False).horizon_s,
        "m7_tick_s": 1.0,
        "m7_default_params": {k: v for k, v in asdict(M7Config("x", 1, False)).items()
                              if k not in ("name", "n_replicas", "quorum", "mix")},
        "m7_fault_mix": asdict(FaultMix()),
        "m8_requests_per_run": 60_000,
        "m8_params": {"MU": MU, "RHO": RHO, "HOT_FACTOR": HOT_FACTOR, "CRIT_FRAC": CRIT_FRAC,
                      "SLA_MS": SLA_MS, "T_MIG_S": T_MIG_S, "T_SPILL_S": T_SPILL_S,
                      "MIG_COST_S": MIG_COST_S, "RETRY_S": RETRY_S, "RTT_S": RTT_S},
    }

    print("Running Milestone 7 ...", flush=True)
    r7 = run_milestone7(m7_seeds)
    print("Running Milestone 8 ...", flush=True)
    r8 = run_milestone8(m8_seeds)

    with open(os.path.join(args.out, "results.json"), "w") as f:
        json.dump({"meta": meta, "m7": r7, "m8": r8}, f, indent=2, default=str)
    with open(os.path.join(args.out, "m7_tables.md"), "w") as f:
        f.write(md_m7(r7))
    with open(os.path.join(args.out, "m8_tables.md"), "w") as f:
        f.write(md_m8(r8))
    print(md_m7(r7))
    print()
    print(md_m8(r8))


if __name__ == "__main__":
    main()

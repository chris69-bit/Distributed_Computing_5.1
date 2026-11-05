"""Milestone 2 experiment: distributed processing and performance.

For every (placement policy, offered load) pair this script:
  1. starts the manager and the three sites as separate OS processes (fresh each run)
  2. asks the manager to admit a fixed set of 10 service instances (control plane)
  3. sends a seeded Poisson stream of traffic TASKs straight to the sites that host
     those instances (data plane), at the chosen offered load (tasks per second)
  4. measures, for the tasks of the measurement window:
       throughput   = completed tasks / window length
       latency      = T_response - T_request   (+ the modelled link delay, see below)
       jitter       = mean |latency_i - latency_(i-1)| between consecutive tasks of a flow
       packet loss  = (dropped + timed out + failed) / sent
       CPU, memory  = sampled every second on every node with psutil
  5. writes one CSV row per run and one raw CSV per run (every task)

Link delay: all nodes usually run on one machine, where the real network delay is
almost zero. The configured link latency (e.g. edge-A -> core-1 = 5 ms one way) is added
to every task's measured time as 2 x one-way delay. This is a model, stated as such.

    python perf_experiment.py                         # full sweep from config.json
    python perf_experiment.py --policies edge_first --loads 200 --measure 10   # one quick run
    python analyze.py results/<folder>                # charts and tables
"""
import argparse
import asyncio
import csv
import json
import math
import os
import random
import statistics
import subprocess
import sys
import time

from run_demo import launch                       # S1 launcher, reused unchanged
from telerm.monitor import ResourceMonitor
from telerm.protocol import STATS, Channel, call, mono, now

ROOT = os.path.dirname(os.path.abspath(__file__))


def pct(xs, p):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def jain(xs):
    xs = [x for x in xs if x is not None]
    if not xs or sum(xs) == 0:
        return None
    return sum(xs) ** 2 / (len(xs) * sum(x * x for x in xs))


async def wait_for_sites(cfg):
    m = cfg["manager"]
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            st = (await call(m["host"], m["port"], {"type": "STATUS"}))["status"]
            if len(st["sites"]) == len(cfg["sites"]):
                return
        except Exception:
            pass
        await asyncio.sleep(0.3)
    raise RuntimeError("sites did not register in time - check the logs folder")


async def admit_instances(cfg, hold_s):
    """Control plane: ask the manager to place each instance. Returns the accepted ones."""
    m, perf, svcs = cfg["manager"], cfg["perf"], cfg["services"]
    out = []
    for spec in perf["instances"]:
        s = svcs[spec["service"]]
        req = {"service": spec["service"], "ingress": spec["ingress"],
               "demand": {"cpu": s["cpu"], "mem": s["mem"], "bw": s["bw"]},
               "max_latency_ms": s["max_latency_ms"], "holding_s": hold_s,
               "work_kb": perf["task_work_kb"][spec["service"]],
               "backlogged": False, "queue_limit": perf["queue_limit"]}
        r = await call(m["host"], m["port"], {"type": "REQUEST", "request": req})
        if r.get("status") == "ACCEPTED":
            out.append({"sid": r["sid"], "service": spec["service"], "ingress": spec["ingress"],
                        "site": r["site"], "address": r["address"], "link_ms": r["latency_ms"],
                        "cpu": s["cpu"], "work_kb": req["work_kb"]})
        else:
            print(f"    ! {spec['service']} from {spec['ingress']} was BLOCKED ({r.get('reason')})")
    return out


async def run_traffic(insts, rate, warmup, measure, timeout, seed):
    """Data plane: Poisson task arrivals, sent directly to the hosting sites."""
    rng = random.Random(seed)
    weights = [i["cpu"] / i["work_kb"] for i in insts]       # CPU load proportional to booking
    channels = {}
    for addr in sorted({i["address"] for i in insts}):
        h, p = addr.rsplit(":", 1)
        ch = Channel(h, int(p))
        await ch.connect()
        channels[addr] = ch

    records, pending = [], set()
    seqs = {i["sid"]: 0 for i in insts}
    t0 = mono()
    w0, w1 = t0 + warmup, t0 + warmup + measure

    async def one_task(inst, seq):
        rec = {"sid": inst["sid"], "service": inst["service"], "site": inst["site"], "seq": seq,
               "link_ms": inst["link_ms"], "t_req": mono()}
        try:
            r = await channels[inst["address"]].request(
                {"type": "TASK", "sid": inst["sid"], "seq": seq, "work_kb": inst["work_kb"]}, timeout=timeout)
            rec["t_resp"] = mono()
            rec["status"] = r.get("status", "DONE" if r.get("ok") else "FAILED")
            rec["queue_ms"], rec["exec_ms"] = r.get("queue_ms"), r.get("exec_ms")
        except asyncio.TimeoutError:
            rec["t_resp"], rec["status"] = None, "TIMEOUT"
        except Exception as exc:
            rec["t_resp"], rec["status"] = None, f"ERROR"
            rec["error"] = repr(exc)[:80]
        records.append(rec)

    next_t = t0
    while True:
        next_t += rng.expovariate(rate)
        if next_t > w1:
            break
        delay = next_t - mono()
        if delay > 0:
            await asyncio.sleep(delay)
        inst = rng.choices(insts, weights)[0]
        seqs[inst["sid"]] += 1
        t = asyncio.create_task(one_task(inst, seqs[inst["sid"]]))
        pending.add(t)
        t.add_done_callback(pending.discard)
    if pending:
        await asyncio.wait(pending, timeout=timeout + 1)
    for ch in channels.values():
        await ch.close()
    return records, t0, w0, w1


def compute_metrics(records, insts, w0, w1, t0_wall, t0_mono, timeseries, policy, rate, rep, gen_cpu):
    D = w1 - w0
    win = [r for r in records if w0 <= r["t_req"] < w1]                    # tasks SENT in the window
    for r in records:
        if r["t_resp"] is not None and r["status"] == "DONE":
            r["measured_ms"] = (r["t_resp"] - r["t_req"]) * 1000
            r["e2e_ms"] = r["measured_ms"] + 2 * r["link_ms"]                # + modelled link delay
            r["comm_ms"] = max(0.0, r["measured_ms"] - (r["queue_ms"] or 0) - (r["exec_ms"] or 0))
    done = [r for r in win if r["status"] == "DONE"]
    lost = [r for r in win if r["status"] != "DONE"]
    completed_in_window = sum(1 for r in records if r["status"] == "DONE" and w0 <= r["t_resp"] < w1)
    lat = [r["e2e_ms"] for r in done]

    def jitter(rs):                                           # per flow (instance), in send order
        js = []
        by = {}
        for r in sorted(rs, key=lambda r: r["t_req"]):
            by.setdefault(r["sid"], []).append(r["e2e_ms"])
        for xs in by.values():
            js += [abs(a - b) for a, b in zip(xs[1:], xs[:-1])]
        return mean(js)

    # resource samples inside the window (manager timeseries uses wall-clock time)
    ws, we = t0_wall + (w0 - t0_mono), t0_wall + (w1 - t0_mono)
    samples = [s for s in timeseries if ws <= s["wall"] <= we + 0.5]
    site_ids = sorted({i["site"] for i in insts} | {sid for s in samples for sid in s["sites"]})
    row = {"policy": policy, "offered_load": rate, "rep": rep, "window_s": round(D, 2),
           "sent": len(win), "completed": len(done), "lost": len(lost),
           "dropped": sum(1 for r in lost if r["status"] == "DROPPED"),
           "timeouts": sum(1 for r in lost if r["status"] == "TIMEOUT"),
           "achieved_offered": round(len(win) / D, 1),
           "throughput": round(completed_in_window / D, 1),
           "loss_pct": round(100 * len(lost) / len(win), 2) if win else None,
           "lat_mean_ms": round(mean(lat), 2) if lat else None,
           "lat_p50_ms": round(pct(lat, .5), 2) if lat else None,
           "lat_p95_ms": round(pct(lat, .95), 2) if lat else None,
           "lat_p99_ms": round(pct(lat, .99), 2) if lat else None,
           "jitter_ms": round(jitter(done), 2) if done else None,
           "queue_ms": round(mean([r["queue_ms"] for r in done]), 2) if done else None,
           "exec_ms": round(mean([r["exec_ms"] for r in done]), 2) if done else None,
           "comm_ms": round(mean([r["comm_ms"] for r in done]), 2) if done else None,
           "link_ms": round(mean([2 * r["link_ms"] for r in done]), 2) if done else None,
           "gen_cpu_pct": gen_cpu,
           "mgr_cpu_pct": round(mean([s["manager"]["cpu_pct"] for s in samples]) or 0, 1),
           "sys_cpu_pct": round(mean([s["manager"]["sys_cpu_pct"] for s in samples]) or 0, 1),
           "sys_mem_pct": round(mean([s["manager"]["sys_mem_pct"] for s in samples]) or 0, 1)}
    util = []
    for sid in site_ids:
        meas = [s["sites"][sid]["measured"] for s in samples if sid in s["sites"] and s["sites"][sid]["measured"]]
        sd = [r for r in done if r["site"] == sid]
        sw = [r for r in win if r["site"] == sid]
        u = mean([m["slot_util"] for m in meas])
        util.append(u)
        row.update({
            f"{sid}_instances": sum(1 for i in insts if i["site"] == sid),
            f"{sid}_tasks_share_pct": round(100 * len(sw) / len(win), 1) if win else None,
            f"{sid}_throughput": round(sum(1 for r in records if r["site"] == sid and r["status"] == "DONE"
                                           and w0 <= r["t_resp"] < w1) / D, 1),
            f"{sid}_lat_mean_ms": round(mean([r["e2e_ms"] for r in sd]), 2) if sd else None,
            f"{sid}_queue_ms": round(mean([r["queue_ms"] for r in sd]), 2) if sd else None,
            f"{sid}_exec_ms": round(mean([r["exec_ms"] for r in sd]), 2) if sd else None,
            f"{sid}_loss_pct": round(100 * sum(1 for r in sw if r["status"] != "DONE") / len(sw), 2) if sw else None,
            f"{sid}_worker_busy_pct": round(100 * u, 1) if u is not None else None,
            f"{sid}_cpu_pct": round(mean([m["cpu_pct"] for m in meas]) or 0, 1),
            f"{sid}_cpu_peak_pct": round(max([m["cpu_pct"] for m in meas] or [0]), 1),
            f"{sid}_mem_mb": round(mean([m["rss_mb"] for m in meas]) or 0, 1),
            f"{sid}_queued_mean": round(mean([m["queued"] for m in meas]) or 0, 1)})
    row["balance_jain"] = round(jain(util), 3) if jain(util) is not None else None
    return row


async def one_run(cfg, policy, rate, rep, out_dir, no_launch=False):
    perf = cfg["perf"]
    run_dir = os.path.join(out_dir, f"{policy}_{rate}_r{rep}")
    os.makedirs(run_dir, exist_ok=True)
    procs = {} if no_launch else launch(cfg, os.path.join(out_dir, "config_used.json"), policy, run_dir)
    m = cfg["manager"]
    insts = []
    try:
        await wait_for_sites(cfg)
        hold = perf["warmup_s"] + perf["measure_s"] + perf["drain_s"] + 60
        insts = await admit_instances(cfg, hold)
        await asyncio.sleep(1.2)                                       # let the first samples arrive
        gen_mon = ResourceMonitor(include_children=False)              # the generator itself
        t0_wall, t0_mono = now(), mono()
        seed = cfg["seed"] * 1000 + rate * 10 + rep
        records, t0, w0, w1 = await run_traffic(insts, rate, perf["warmup_s"], perf["measure_s"],
                                                perf["task_timeout_s"], seed)
        gen_cpu = gen_mon.sample()["cpu_pct"]
        await asyncio.sleep(perf["drain_s"])
        st = (await call(m["host"], m["port"], {"type": "STATUS", "full": True}, timeout=10))["status"]
        row = compute_metrics(records, insts, w0, w1, t0_wall, t0_mono, st["timeseries"],
                              policy, rate, rep, gen_cpu)
        with open(os.path.join(run_dir, "tasks.csv"), "w", newline="") as f:
            keys = ["sid", "service", "site", "seq", "status", "t_req", "t_resp", "measured_ms", "e2e_ms",
                    "queue_ms", "exec_ms", "comm_ms", "link_ms"]
            w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            w.writeheader()
            for r in sorted(records, key=lambda r: r["t_req"]):
                w.writerow({**r, "t_req": round(r["t_req"] - t0, 5),
                            "t_resp": round(r["t_resp"] - t0, 5) if r.get("t_resp") else None})
        json.dump({"instances": insts, "placement": {i["sid"]: i["site"] for i in insts},
                   "timeseries": st["timeseries"], "manager_stats": st["stats"]},
                  open(os.path.join(run_dir, "run.json"), "w"), indent=1, default=str)
        return row, insts
    finally:
        if no_launch:                       # nodes keep running: end this run's instances instead
            for i in insts:
                h, p = i["address"].rsplit(":", 1)
                try:
                    await call(h, int(p), {"type": "RELEASE", "sid": i["sid"]}, timeout=2)
                except Exception:
                    pass
            await asyncio.sleep(1.5)
        else:
            for s in cfg["sites"]:
                try:
                    await call(s["host"], s["port"], {"type": "SHUTDOWN"}, timeout=1)
                except Exception:
                    pass
            try:
                await call(m["host"], m["port"], {"type": "SHUTDOWN"}, timeout=1)
            except Exception:
                pass
        for p in procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(ROOT, "config.json"))
    ap.add_argument("--policies", nargs="+")
    ap.add_argument("--loads", nargs="+", type=int)
    ap.add_argument("--reps", type=int)
    ap.add_argument("--measure", type=float)
    ap.add_argument("--out")
    ap.add_argument("--no-launch", action="store_true",
                    help="nodes are already running (e.g. on several machines); the placement policy is "
                         "whatever the manager was started with")
    a = ap.parse_args()
    cfg = json.load(open(a.config))
    perf = cfg["perf"]
    if a.policies: perf["policies"] = a.policies
    if a.loads: perf["offered_loads"] = a.loads
    if a.reps: perf["repetitions"] = a.reps
    if a.measure: perf["measure_s"] = a.measure
    out = a.out or os.path.join(ROOT, "results", f"{perf['experiment']}_{time.strftime('%Y%m%d-%H%M%S')}")
    os.makedirs(out, exist_ok=True)
    try:
        import psutil
        hw = {"cpu_logical": psutil.cpu_count(), "cpu_physical": psutil.cpu_count(logical=False),
              "ram_gb": round(psutil.virtual_memory().total / 2**30, 1), "psutil": psutil.__version__}
    except ImportError:
        hw = {"cpu_logical": os.cpu_count()}
    import platform
    env = {"python": sys.version.split()[0], "platform": platform.platform(), "processor": platform.processor(), **hw}
    json.dump({**cfg, "environment": env}, open(os.path.join(out, "config_used.json"), "w"), indent=2)
    print(f"Environment: {env}")

    rows, n = [], len(perf["policies"]) * len(perf["offered_loads"]) * perf["repetitions"]
    k = 0
    for rep in range(1, perf["repetitions"] + 1):
        for policy in perf["policies"]:
            for rate in perf["offered_loads"]:
                k += 1
                print(f"[{k}/{n}] policy={policy:<12} offered={rate:>4} tasks/s  rep={rep} ...", flush=True)
                row, insts = asyncio.run(one_run(cfg, policy, rate, rep, out, a.no_launch))
                if k == 1 or rate == perf["offered_loads"][0]:
                    print("    placement: " + ", ".join(f"{i['service']}@{i['ingress']}->{i['site']}" for i in insts))
                print(f"    throughput {row['throughput']:>6} /s   latency mean {row['lat_mean_ms']} ms "
                      f"p95 {row['lat_p95_ms']} ms   jitter {row['jitter_ms']} ms   loss {row['loss_pct']}%   "
                      f"machine CPU {row['sys_cpu_pct']}%", flush=True)
                rows.append(row)
                keys = sorted({k2 for r in rows for k2 in r}, key=lambda c: (c not in rows[0], list(rows[0]).index(c) if c in rows[0] else 0, c))
                with open(os.path.join(out, "runs.csv"), "w", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=keys)
                    w.writeheader()
                    w.writerows(rows)
    print(f"\nResults saved to {out}\nNext: python analyze.py {out}")


if __name__ == "__main__":
    main()

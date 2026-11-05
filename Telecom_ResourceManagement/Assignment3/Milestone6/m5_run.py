"""Milestone 5 experiments: distributed transactions (2PC, 3PC) on the TeleRM cluster.

  python m5_run.py latency      E1 transaction latency and message cost: protocol x participants x delay
  python m5_run.py throughput   E2 throughput and abort ratio vs concurrency (and contention)
  python m5_run.py failures     E3 failure matrix: crash points x protocol/coordinator design
  python m5_run.py chaos        E4 consistency under random crashes: 2PC+Raft vs no-transaction baseline
  python m5_run.py avail        E5 availability and recovery time when the coordinator dies under load
  python m5_run.py smoke        one quick transaction of each protocol

Every run starts a fresh local cluster (m5_cluster.py): registry, N coordinator replicas, K sites,
all separate OS processes. Crashes are SIGKILL or a named crash point inside the protocol code.
"""
import argparse
import asyncio
import csv
import json
import os
import random
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from m5_cluster import Cluster5, SITE_BASE, mean, parts_of, pct, rnd  # noqa: E402
from telerm.protocol import call, mono  # noqa: E402


def append_row(path, row):
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


# ============================================================================ smoke
async def smoke(a):
    cl = Cluster5(os.path.join(a.out, "smoke"), n_orch=3, n_sites=3)
    await cl.start()
    try:
        for proto in ("naive", "2pc", "3pc"):
            r = await cl.txn(parts_of(3, holding=1.0, sites=cl.site_names), proto=proto)
            print(proto, r.get("status"), r.get("txn"), "total_ms", r.get("total_ms"), "msgs", r.get("msgs"))
        await cl.settle()
        v, _, _ = await cl.check()
        print("invariants", v)
    finally:
        await cl.stop()


# ============================================================================ E1 latency
async def e1_one(a, proto, k, delay, rep, fsync=False, n=80):
    tag = f"{proto}_k{k}_d{delay}{'_fsync' if fsync else ''}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "e1", tag), n_orch=3, n_sites=max(k, 1), delay_ms=delay, fsync=fsync)
    await cl.start()
    try:
        await asyncio.sleep(0.5)
        for _ in range(5):                                        # warm-up
            await cl.txn(parts_of(k, holding=0.2, sites=cl.site_names, cpu=50), proto=proto)
        rows = []
        for i in range(n):
            r = await cl.txn(parts_of(k, holding=0.2, sites=cl.site_names, cpu=50), proto=proto)
            if r.get("ok"):
                rows.append(r)
        await cl.settle(10, need_done=False)
        _, lst = await cl.leader()
        p2 = list((lst or {}).get("phase2_ms", {}).values())
        tot = [r["total_ms"] for r in rows]
        out = {"proto": proto, "k": k, "one_way_delay_ms": delay, "fsync": int(fsync), "rep": rep, "n": len(rows),
               "lat_mean_ms": rnd(mean(tot)), "lat_p50_ms": rnd(pct(tot, .5)), "lat_p95_ms": rnd(pct(tot, .95)),
               "plan_ms": rnd(mean([r.get("plan_ms") for r in rows])), "begin_ms": rnd(mean([r.get("begin_ms") for r in rows])),
               "prepare_ms": rnd(mean([r.get("prepare_ms") for r in rows])),
               "precommit_ms": rnd(mean([r.get("precommit_ms") for r in rows])),
               "decide_ms": rnd(mean([r.get("decide_ms") for r in rows])), "phase2_ms": rnd(mean(p2)),
               "msgs": mean([r.get("msgs") for r in rows]), "raft_entries": mean([r.get("raft_entries") for r in rows])}
        append_row(os.path.join(a.out, "e1_latency.csv"), out)
        print(out)
    finally:
        await cl.stop()


async def e1(a):
    for rep in range(a.reps):
        for delay in (0, 5):
            for k in (1, 2, 3, 4):
                for proto in ("naive", "2pc", "3pc"):
                    await e1_one(a, proto, k, delay, rep)
        await e1_one(a, "2pc", 3, 0, rep, fsync=True)
        await e1_one(a, "3pc", 3, 0, rep, fsync=True)


# ============================================================================ E2 throughput
async def e2_one(a, proto, clients, tight, rep, dur=6.0):
    tag = f"{proto}_c{clients}_{'tight' if tight else 'loose'}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "e2", tag), n_orch=3, n_sites=3, delay_ms=2.0,
                  site_cpu=2000 if tight else 40000)
    await cl.start()
    try:
        await asyncio.sleep(0.5)
        stop_at = mono() + dur
        res = []
        rnd_ = random.Random(a.seed + rep)

        async def worker(w):
            while mono() < stop_at:
                parts = parts_of(3, holding=0.6, sites=cl.site_names, cpu=200)
                t0 = mono()
                try:
                    r = await cl.txn(parts, proto=proto, timeout=5)
                except Exception as exc:
                    r = {"status": "ERROR"}
                res.append((mono() - t0, r.get("status"), r.get("reason")))
                await asyncio.sleep(rnd_.random() * 0.002)
        t_start = mono()
        await asyncio.gather(*[worker(w) for w in range(clients)])
        el = mono() - t_start
        c = [x for x in res if x[1] == "COMMITTED"]
        ab = [x for x in res if x[1] == "ABORTED"]
        bl = [x for x in res if x[1] == "BLOCKED"]
        await cl.settle(15)
        v, _, lst = await cl.check()
        out = {"proto": proto, "clients": clients, "tight": int(tight), "rep": rep, "offered": len(res),
               "committed": len(c), "aborted": len(ab), "blocked_at_plan": len(bl),
               "throughput_per_s": rnd(len(c) / el), "commit_ratio": rnd(len(c) / max(1, len(res)), 3),
               "lat_mean_ms": rnd(mean([x[0] * 1000 for x in c])), "lat_p95_ms": rnd(pct([x[0] * 1000 for x in c], .95)),
               "abort_reasons": json.dumps({r: sum(1 for x in ab if x[2] == r) for r in {x[2] for x in ab}}),
               **{f"inv_{k}": v[k] for k in ("atomicity", "over_capacity", "ledger_mismatch", "stuck_prepared")}}
        append_row(os.path.join(a.out, "e2_throughput.csv"), out)
        print(out)
    finally:
        await cl.stop()


async def e2(a):
    for rep in range(a.reps):
        for tight in (False, True):
            for clients in (1, 2, 4, 8, 16, 32):
                for proto in ("2pc", "3pc"):
                    await e2_one(a, proto, clients, tight, rep)
                if not tight and clients <= 8:
                    pass


# ============================================================================ E3 failures
SCENARIOS = {
    # name: (who, fault point, restart after s)
    "P_before_vote":       ("site1", "p_before_vote"),
    "P_after_vote_logged": ("site1", "p_after_prepared_log"),
    "P_after_commit_log":  ("site1", "p_after_commit_log"),
    "C_after_begin":       ("coord", "c_after_begin"),
    "C_after_votes":       ("coord", "c_after_votes"),
    "C_after_decide":      ("coord", "c_after_decide"),
    "C_mid_phase2":        ("coord", "c_mid_phase2"),
    "C_after_1st_precommit": ("coord", "c_after_first_precommit"),
}
CONFIGS = {"2PC+Raft(3)": ("2pc", 3), "2PC single coordinator": ("2pc", 1), "3PC single coordinator": ("3pc", 1)}


async def e3_one(a, cfg, scen, rep):
    proto, n_orch = CONFIGS[cfg]
    who, point = SCENARIOS[scen]
    if point == "c_after_first_precommit" and proto != "3pc":
        return
    tag = f"{cfg.replace(' ', '_').replace('(', '').replace(')', '').replace('+', '_')}_{scen}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "e3", tag), n_orch=n_orch, n_sites=3, delay_ms=2.0, indoubt_s=1.0)
    await cl.start()
    down_s = 6.0 if who == "coord" else 3.0
    try:
        await asyncio.sleep(0.5)
        target = (await cl.leader())[0] if who == "coord" else who
        await cl.fault(target, point=point)
        t0 = mono()
        task = asyncio.create_task(cl.txn(parts_of(3, holding=3.0, sites=cl.site_names, cpu=500), proto=proto,
                                          req_id=f"R-{tag}", timeout=2.5, retry_s=25))
        while cl.alive(target) and mono() - t0 < 8:
            await asyncio.sleep(0.01)
        crashed = not cl.alive(target)
        t_crash = mono()

        async def sites_free_time():
            ok = await cl.settle(40, need_done=False)
            return mono() if ok else None
        watcher = asyncio.create_task(sites_free_time())          # when does every site leave PREPARED?
        restarted = False
        if crashed and (n_orch == 1 or who != "coord"):       # a replicated coordinator is NOT restarted
            await asyncio.sleep(down_s)
            cl.restart(target)
            restarted = True
        t_sites = await watcher
        sites_free = t_sites is not None
        settled = await cl.settle(40)
        t_settle = mono()
        try:
            r = await asyncio.wait_for(task, 20)
        except Exception as exc:
            r = {"status": "ERROR", "reason": repr(exc)[:60]}
        # when did every site leave the in-doubt state? (taken from the sites' own records)
        v, sites, lst = await cl.check()
        indoubt = [x for s in sites.values() if s for x in s["indoubt_log"]]
        held = [x for s in sites.values() if s for x in s["held_log"]]
        blocked_cpu_s = sum(x["held_s"] * x["cpu"] for x in held if x["held_s"] > 0.5)   # beyond a normal protocol run
        final = None
        if lst:
            final = next(iter(lst["txns"].values()), {}).get("state")
        out = {"config": cfg, "scenario": scen, "rep": rep, "crashed": int(crashed), "restarted": int(restarted),
               "client_status": r.get("status"), "final_state": final, "settled": int(settled),
               "sites_free_s": rnd(t_sites - t_crash, 2) if crashed and sites_free else None,
               "resolve_s": rnd(t_settle - t_crash, 2) if crashed else None,
               "max_held_s": rnd(max([x["held_s"] for x in held], default=0), 2),
               "blocked_cpu_s": rnd(blocked_cpu_s, 1), "resolved_by": ",".join(sorted({x["how"] for x in indoubt})),
               "atomicity_viol": v["atomicity"], "coord_part_mismatch": v["coord_participant_mismatch"],
               "stuck_prepared": v["stuck_prepared"], "over_capacity": v["over_capacity"],
               "ledger_mismatch": v["ledger_mismatch"], "protocol_viol": v["protocol_violations"]}
        append_row(os.path.join(a.out, "e3_failures.csv"), out)
        print(out)
    finally:
        await cl.stop()


async def e3(a):
    for rep in range(a.reps):
        for cfg in CONFIGS:
            for scen in SCENARIOS:
                await e3_one(a, cfg, scen, rep)


# ============================================================================ E4 chaos
def naive_partial_chains(wal_dir):
    """Chains installed by the no-transaction baseline that exist only in part (an orphan)."""
    groups = {}
    for fn in os.listdir(wal_dir):
        undone = set()
        recs = [json.loads(l) for l in open(os.path.join(wal_dir, fn)) if l.strip().startswith("{")]
        for r in recs:
            if r["r"] == "DIRECT_UNDO":
                undone.add(r["sid"])
        for r in recs:
            if r["r"] == "DIRECT" and r["sid"] not in undone:
                txn = r["sid"].split(".")[0]
                g = groups.setdefault(txn, {"k": (r.get("part") or {}).get("k", 0), "n": 0})
                g["n"] += 1
    partial = [t for t, g in groups.items() if g["k"] and g["n"] < g["k"]]
    return len(groups), len(partial)


async def e4_one(a, proto, n_orch, rep, dur=24.0):
    tag = f"{proto}_n{n_orch}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "e4", tag), n_orch=n_orch, n_sites=4, delay_ms=2.0, indoubt_s=1.0, site_cpu=6000)
    await cl.start()
    rng = random.Random(a.seed + rep)
    try:
        await asyncio.sleep(0.5)
        stop_at = mono() + dur
        res = []

        async def worker(w):
            n = 0
            while mono() < stop_at:
                n += 1
                k = rng.choice([2, 3, 4])
                sites = rng.sample(cl.site_names, k)
                try:
                    r = await cl.txn(parts_of(k, holding=1.0, sites=sites, cpu=300), proto=proto,
                                     req_id=f"w{w}-{n}", timeout=3.0, retry_s=8 if proto != "naive" else 0)
                except Exception:
                    r = {"status": "ERROR"}
                res.append(r.get("status"))
                await asyncio.sleep(0.05)

        async def chaos():
            kills = 0
            await asyncio.sleep(2.0)
            while mono() < stop_at - 4:
                victim = rng.choice(cl.site_names + (["leader"] * 2))
                if victim == "leader":
                    victim = (await cl.leader())[0]
                    if victim is None:
                        await asyncio.sleep(0.5)
                        continue
                cl.kill(victim)
                kills += 1
                await asyncio.sleep(rng.uniform(1.0, 2.0))
                cl.restart(victim)
                await asyncio.sleep(rng.uniform(1.5, 3.0))
            return kills
        out_c = await asyncio.gather(chaos(), *[worker(w) for w in range(3)])
        kills = out_c[0]
        # make sure everything is up again, then let things settle
        for n in cl.ids + cl.site_names:
            if not cl.alive(n):
                cl.restart(n)
        await asyncio.sleep(3.0)
        settled = await cl.settle(40, need_done=(proto != "naive"))
        await asyncio.sleep(1.5)
        v, sites, lst = await cl.check()
        row = {"proto": proto, "orch_replicas": n_orch, "rep": rep, "kills": kills, "requests": len(res),
               "committed": sum(1 for s in res if s == "COMMITTED"), "aborted": sum(1 for s in res if s == "ABORTED"),
               "blocked": sum(1 for s in res if s == "BLOCKED"), "client_errors": sum(1 for s in res if s in ("ERROR", None)),
               "settled": int(settled), "atomicity_viol": v["atomicity"], "coord_part_mismatch": v["coord_participant_mismatch"],
               "over_capacity": v["over_capacity"], "ledger_mismatch": v["ledger_mismatch"],
               "stuck_prepared": v["stuck_prepared"], "protocol_viol": v["protocol_violations"], "txns_in_log": v["txns"]}
        wal = os.path.join(cl.run_dir, "wal")
        chains, partial = naive_partial_chains(wal)
        row["naive_chains"], row["partial_chains"] = chains, partial
        append_row(os.path.join(a.out, "e4_chaos.csv"), row)
        print(row)
    finally:
        await cl.stop()


async def e4(a):
    for rep in range(a.reps):
        await e4_one(a, "2pc", 3, rep)
        await e4_one(a, "3pc", 3, rep)
        await e4_one(a, "naive", 3, rep)


# ============================================================================ E5 availability
async def e5_one(a, cfg, rep, dur=22.0, t_kill=6.0, down_s=6.0):
    proto, n_orch = CONFIGS[cfg]
    tag = f"{cfg.replace(' ', '_').replace('(', '').replace(')', '').replace('+', '_')}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "e5", tag), n_orch=n_orch, n_sites=3, delay_ms=2.0, indoubt_s=1.0, site_cpu=40000)
    await cl.start()
    try:
        await asyncio.sleep(0.5)
        t0 = mono()
        stop_at = t0 + dur
        events = []                                         # (t_done, status)

        async def worker(w):
            n = 0
            while mono() < stop_at:
                n += 1
                try:
                    r = await cl.txn(parts_of(3, holding=0.5, sites=cl.site_names, cpu=100), proto=proto,
                                     req_id=f"e5-{w}-{n}", timeout=1.0, retry_s=0)
                    events.append((mono() - t0, r.get("status")))
                except Exception:
                    events.append((mono() - t0, "ERROR"))
                    await asyncio.sleep(0.05)
                await asyncio.sleep(0.01)

        async def killer():
            await asyncio.sleep(t_kill)
            lid = (await cl.leader())[0]
            cl.kill(lid)
            tk = mono() - t0
            if n_orch == 1:
                await asyncio.sleep(down_s)
                cl.restart(lid)
            return tk
        res = await asyncio.gather(killer(), *[worker(w) for w in range(4)])
        tk = res[0]
        await cl.settle(30)
        ok_t = sorted(t for t, s in events if s == "COMMITTED")
        bins = [0] * int(dur / 0.5)
        for t in ok_t:
            b = int(t / 0.5)
            if b < len(bins):
                bins[b] += 1
        avail = sum(1 for b in bins if b > 0) / len(bins)
        after = [t for t in ok_t if t > tk]
        before = [t for t in ok_t if t <= tk]
        gap = (after[0] - tk) if after else None
        v, sites, lst = await cl.check()
        held = [x for s in sites.values() if s for x in s["held_log"]]
        row = {"config": cfg, "rep": rep, "committed": len(ok_t), "errors": sum(1 for _, s in events if s in ("ERROR", None)),
               "availability": rnd(avail, 3), "mttr_s": rnd(gap, 2), "tput_before_per_s": rnd(len(before) / tk, 1),
               "max_held_s": rnd(max([x["held_s"] for x in held], default=0), 2),
               "blocked_cpu_s": rnd(sum(x["held_s"] * x["cpu"] for x in held if x["held_s"] > 0.5), 1),
               "atomicity_viol": v["atomicity"], "stuck_prepared": v["stuck_prepared"], "bins": json.dumps(bins),
               "t_kill": rnd(tk, 2)}
        append_row(os.path.join(a.out, "e5_availability.csv"), row)
        print({k: v for k, v in row.items() if k != "bins"})
    finally:
        await cl.stop()


async def e5(a):
    for rep in range(a.reps):
        for cfg in CONFIGS:
            await e5_one(a, cfg, rep)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp", choices=["smoke", "latency", "throughput", "failures", "chaos", "avail"])
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--out", default="results/M5")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    fn = {"smoke": smoke, "latency": e1, "throughput": e2, "failures": e3, "chaos": e4, "avail": e5}[a.exp]
    asyncio.run(fn(a))


if __name__ == "__main__":
    main()

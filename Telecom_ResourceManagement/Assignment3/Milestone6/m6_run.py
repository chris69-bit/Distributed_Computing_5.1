"""Milestone 6 experiments: concurrency, mutual exclusion, deadlock, starvation (TeleRM S6).

  python m6_run.py race        X1 race condition on the site ledger: unsafe vs locked vs atomic
  python m6_run.py deadlock    X2 a real distributed deadlock and five ways of dealing with it
  python m6_run.py workload    X3 deadlock strategies under a random multi-site workload (utilisation!)
  python m6_run.py workload1   X3b the same with ONE slot per site (every site is a lock): many deadlocks
  python m6_run.py starvation  X4 a large request vs a stream of small ones: greedy, fifo, aging
  python m6_run.py detector    X5 cost of detection: algorithm scaling, interval, false positives
  python m6_run.py smoke       one deadlock, detected and resolved
"""
import argparse
import asyncio
import csv
import json
import os
import random
import shutil
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from m5_cluster import Cluster5, mean, parts_of, pct, rnd  # noqa: E402
from m5_run import append_row  # noqa: E402
from telerm.deadlock import assemble, detect, find_cycle, wait_for_graph  # noqa: E402
from telerm.protocol import call, mono  # noqa: E402
from telerm.resources import Resources  # noqa: E402
from telerm.txn import Participant  # noqa: E402

STRATEGIES = ["nowait", "timeout", "detect", "waitdie", "ordered"]


# ============================================================================ X1 race
async def x1_one(a, race, c, rep):
    d = tempfile.mkdtemp(prefix="x1_")
    try:
        part = Participant("s0", Resources(4000, 1e6, 1e6), d, race=race)
        pt = [{"i": 0, "service": "x", "ingress": "i", "demand": {"cpu": 100, "mem": 1, "bw": 1}, "holding_s": 5}]
        msgs = [{"type": "PREPARE", "txn": f"T{i + 1:05d}", "proto": "2pc", "parts": pt, "participants": {},
                 "cc": "nowait"} for i in range(c)]
        t0 = mono()
        rs = await asyncio.gather(*[part.handle(m) for m in msgs])
        dt = mono() - t0
        yes = sum(1 for r in rs if r.get("vote") == "YES")
        alloc = part.ledger.allocated.cpu
        row = {"race": race, "concurrent_requests": c, "rep": rep, "capacity_cpu": 4000, "max_grantable": 40,
               "granted": yes, "allocated_cpu": alloc, "over_commit_cpu": max(0.0, alloc - 4000),
               "over_commit_pct": rnd(100 * max(0.0, alloc - 4000) / 4000, 1), "elapsed_ms": rnd(dt * 1000, 2),
               "ops_per_s": rnd(c / dt, 0)}
        append_row(os.path.join(a.out, "x1_race.csv"), row)
        print(row)
    finally:
        shutil.rmtree(d, ignore_errors=True)


async def x1(a):
    for rep in range(a.reps):
        for race in ("unsafe", "locked", "atomic"):
            for c in (10, 20, 40, 80, 160, 320):
                await x1_one(a, race, c, rep)


# ============================================================================ X2 deadlock scenario
def crossing_txns(cl):
    """T1 takes site0 first and site1 150 ms later; T2 does the opposite. Each needs 600 of 1000 cpu."""
    s0, s1 = cl.site_names[0], cl.site_names[1]

    def part(i, site, delay):
        return {"service": f"svc{i}", "ingress": "core-1", "demand": {"cpu": 600, "mem": 1, "bw": 1},
                "max_latency_ms": 100, "holding_s": 1.0, "site": site, "force": True, "debug_delay_ms": delay}
    t1 = [part(0, s0, 0), part(1, s1, 150)]
    t2 = [part(0, s1, 0), part(1, s0, 150)]
    return t1, t2


async def x2_one(a, strat, rep, detect_ms=200, lock_wait=3.0, snapshot=False, tag_extra=""):
    tag = f"{strat}{tag_extra}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "x2", tag), n_orch=3, n_sites=2, site_cpu=1000, delay_ms=1.0,
                  detect_ms=detect_ms if strat == "detect" else 0, stable_rounds=2)
    await cl.start()
    try:
        await asyncio.sleep(0.5)
        t1, t2 = crossing_txns(cl)
        cc = strat
        t0 = mono()
        r1 = asyncio.create_task(cl.txn(t1, cc=cc, lock_wait_s=lock_wait, timeout=lock_wait + 6))
        await asyncio.sleep(0.01)                           # T1 is older (smaller timestamp)
        r2 = asyncio.create_task(cl.txn(t2, cc=cc, lock_wait_s=lock_wait, timeout=lock_wait + 6))
        snap = None
        if snapshot:
            await asyncio.sleep(0.6)
            snap = []
            for k in range(2):
                snap.append((await call("127.0.0.1", 8920 + k, {"type": "LOCKSTATE"}, timeout=2)))
        res = await asyncio.gather(r1, r2)
        t_both = mono() - t0
        await cl.settle(15)
        v, sites, lst = await cl.check()
        held = [x for s in sites.values() if s for x in s["held_log"]]
        wasted = sum(x["held_s"] * x["cpu"] for x in held if x["outcome"] == "ABORT")
        det = (lst or {}).get("detector", {})
        row = {"strategy": strat, "rep": rep, "detect_ms": detect_ms if strat == "detect" else None,
               "T1": res[0].get("status"), "T1_reason": res[0].get("reason"), "T2": res[1].get("status"),
               "T2_reason": res[1].get("reason"), "committed": sum(1 for r in res if r.get("status") == "COMMITTED"),
               "time_to_both_answers_s": rnd(t_both, 2), "wasted_cpu_s": rnd(wasted, 1),
               "deadlocks_detected": det.get("deadlocks", 0), "victims": det.get("victims", 0),
               "detect_resolve_s": rnd(mean(det.get("resolve_s", [])), 3) if det.get("resolve_s") else None,
               "atomicity_viol": v["atomicity"], "over_capacity": v["over_capacity"], "stuck": v["stuck_prepared"]}
        append_row(os.path.join(a.out, "x2_deadlock.csv"), row)
        if snap:
            json.dump(snap, open(os.path.join(a.out, f"x2_snapshot_{strat}.json"), "w"))
        print(row)
    finally:
        await cl.stop()


async def x2(a):
    for rep in range(a.reps):
        for strat in STRATEGIES:
            await x2_one(a, strat, rep, snapshot=(rep == 0 and strat == "timeout"))


async def smoke(a):
    await x2_one(a, "detect", 0, snapshot=True)


# ============================================================================ X3 workload
async def x3_one(a, strat, clients, rep, dur=10.0, slots=2):
    cap = 1000 * slots                                       # slots = how many 1000-mc parts fit on a site
    tag = f"{strat}_c{clients}_r{rep}" + ("" if slots == 2 else f"_s{slots}")
    cl = Cluster5(os.path.join(a.out, "x3", tag), n_orch=3, n_sites=4, site_cpu=cap, delay_ms=1.0,
                  detect_ms=200 if strat == "detect" else 0, stable_rounds=2)
    await cl.start()
    rng = random.Random(a.seed + rep * 100 + clients)
    try:
        await asyncio.sleep(0.5)
        stop_at = mono() + dur
        res, samples = [], []

        async def worker(w):
            n = 0
            while mono() < stop_at:
                n += 1
                k = rng.choice([2, 3])
                sites = rng.sample(cl.site_names, k)
                parts = parts_of(k, holding=0.3, sites=sites, cpu=1000, force=True)
                t0 = mono()
                try:
                    r = await cl.txn(parts, cc=strat, lock_wait_s=1.5, timeout=8)
                except Exception:
                    r = {"status": "ERROR"}
                res.append((mono() - t0, r.get("status"), r.get("reason")))
                await asyncio.sleep(0.005)

        async def sampler():
            while mono() < stop_at:
                row = []
                for k in range(4):
                    try:
                        s = await call("127.0.0.1", 8920 + k, {"type": "LOCKSTATE"}, timeout=1)
                        row.append((cap - s["free"]["cpu"], s["active_cpu"], s["n_waiting"]))
                    except Exception:
                        pass
                if row:
                    samples.append(row)
                await asyncio.sleep(0.25)
        t_start = mono()
        await asyncio.gather(sampler(), *[worker(w) for w in range(clients)])
        el = mono() - t_start
        await cl.settle(20)
        v, sites, lst = await cl.check()
        c = [x for x in res if x[1] == "COMMITTED"]
        ab = [x for x in res if x[1] == "ABORTED"]
        reasons = {}
        for x in ab:
            reasons[x[2]] = reasons.get(x[2], 0) + 1
        locked = mean([sum(r[0] for r in row) / (cap * len(row)) for row in samples])
        useful = mean([sum(r[1] for r in row) / (cap * len(row)) for row in samples])
        waiting = mean([sum(r[2] for r in row) for row in samples])
        det = (lst or {}).get("detector", {})
        waits = [x["wait_s"] for s in sites.values() if s for x in s["wait_log"] if x["outcome"] == "GRANTED"]
        row = {"strategy": strat, "slots_per_site": slots, "clients": clients, "rep": rep, "offered": len(res), "committed": len(c),
               "aborted": len(ab), "other": len(res) - len(c) - len(ab),
               "throughput_per_s": rnd(len(c) / el, 2), "commit_ratio": rnd(len(c) / max(1, len(res)), 3),
               "lat_mean_ms": rnd(mean([x[0] * 1000 for x in c])), "lat_p95_ms": rnd(pct([x[0] * 1000 for x in c], .95)),
               "abort_reasons": json.dumps(reasons), "util_locked": rnd(locked, 3), "util_useful": rnd(useful, 3),
               "util_wasted": rnd((locked or 0) - (useful or 0), 3), "mean_waiting_requests": rnd(waiting, 2),
               "mean_lock_wait_ms": rnd(mean(waits) * 1000 if waits else None), "deadlocks": det.get("deadlocks", 0),
               "victims": det.get("victims", 0), "detector_msgs": det.get("msgs", 0),
               "atomicity_viol": v["atomicity"], "over_capacity": v["over_capacity"], "stuck": v["stuck_prepared"]}
        append_row(os.path.join(a.out, "x3_workload.csv" if slots == 2 else "x3b_workload_1slot.csv"), row)
        print(row)
    finally:
        await cl.stop()


async def x3(a):
    for rep in range(a.reps):
        for clients in (4, 8, 16):
            for strat in STRATEGIES:
                await x3_one(a, strat, clients, rep)


async def x3b(a):
    """One slot per site: every site is a mutual-exclusion lock, so circular waits are frequent."""
    for rep in range(a.reps):
        for clients in (4, 8, 16):
            for strat in STRATEGIES:
                await x3_one(a, strat, clients, rep, slots=1)


# ============================================================================ X4 starvation
async def x4_one(a, disc, rep, dur=16.0):
    tag = f"{disc}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "x4", tag), n_orch=1, n_sites=1, site_cpu=2000, delay_ms=0.5,
                  discipline=disc, aging_s=1.0)
    await cl.start()
    try:
        await asyncio.sleep(0.5)
        stop_at = mono() + dur
        small, large, samples = [], [], []
        site = cl.site_names[0]

        async def small_client(w):
            while mono() < stop_at:
                t0 = mono()
                try:
                    r = await cl.txn(parts_of(1, holding=0.3, sites=[site], cpu=250, force=True), cc="timeout",
                                     lock_wait_s=6.0, timeout=10)
                except Exception:
                    r = {"status": "ERROR"}
                small.append((mono() - t0, r.get("status")))
                await asyncio.sleep(0.002)

        async def large_client():
            await asyncio.sleep(1.0)
            while mono() < stop_at - 1:
                t0 = mono()
                try:
                    r = await cl.txn(parts_of(1, holding=0.5, sites=[site], cpu=1500, force=True), cc="timeout",
                                     lock_wait_s=6.0, timeout=10)
                except Exception:
                    r = {"status": "ERROR"}
                large.append((mono() - t0, r.get("status"), r.get("reason")))
                await asyncio.sleep(0.2)

        async def sampler():
            while mono() < stop_at:
                try:
                    s = await call("127.0.0.1", 8920, {"type": "LOCKSTATE"}, timeout=1)
                    samples.append((2000 - s["free"]["cpu"], s["active_cpu"]))
                except Exception:
                    pass
                await asyncio.sleep(0.1)
        t_start = mono()
        await asyncio.gather(sampler(), large_client(), *[small_client(w) for w in range(8)])
        el = mono() - t_start
        sc = [x for x in small if x[1] == "COMMITTED"]
        lc = [x for x in large if x[1] == "COMMITTED"]
        lt = [x for x in large if x[2] == "lock_timeout"]
        row = {"discipline": disc, "rep": rep, "small_committed": len(sc), "small_per_s": rnd(len(sc) / el, 1),
               "small_lat_mean_ms": rnd(mean([x[0] * 1000 for x in sc])), "small_lat_p95_ms": rnd(pct([x[0] * 1000 for x in sc], .95)),
               "large_attempts": len(large), "large_committed": len(lc), "large_timeouts": len(lt),
               "large_wait_mean_s": rnd(mean([x[0] for x in lc]), 2), "large_wait_max_s": rnd(max([x[0] for x in large], default=None), 2),
               "util_locked": rnd(mean([x[0] / 2000 for x in samples]), 3), "util_running": rnd(mean([x[1] / 2000 for x in samples]), 3)}
        append_row(os.path.join(a.out, "x4_starvation.csv"), row)
        print(row)
    finally:
        await cl.stop()


async def x4(a):
    for rep in range(a.reps):
        for disc in ("greedy", "fifo", "aging"):
            await x4_one(a, disc, rep)


# ============================================================================ X5 detector
def random_state(n, m, mode):
    """n transactions, m sites. mode 'ring': one big deadlock cycle. 'chain': no deadlock, but each one
    can only finish after the next has (the algorithm needs n passes)."""
    free = {(f"s{j}", "cpu"): 0.0 for j in range(m)}
    alloc, req = {}, {}
    for i in range(n):
        a_r, b_r = (f"s{i % m}", "cpu"), (f"s{(i + 1) % m}", "cpu")
        if mode == "ring":
            alloc[f"T{i}"] = {(f"u{i}", "cpu"): 1.0}
            req[f"T{i}"] = {(f"u{(i + 1) % n}", "cpu"): 1.0}
            free[(f"u{i}", "cpu")] = 0.0
        else:                                                 # chain: T_i waits for what T_{i+1} holds; last one is free
            alloc[f"T{i}"] = {(f"u{i}", "cpu"): 1.0}
            free[(f"u{i}", "cpu")] = 0.0
            if i < n - 1:
                req[f"T{i}"] = {(f"u{i + 1}", "cpu"): 1.0}
    return free, alloc, req


async def x5(a):
    # (a) algorithm scaling (in-process)
    for mode in ("ring", "chain"):
        for n in (10, 100, 1000, 3000):
            free, alloc, req = random_state(n, 16, mode)
            ts = []
            for _ in range(3):
                t0 = time.perf_counter()
                d = detect(free, dict(alloc), req)
                ts.append((time.perf_counter() - t0) * 1000)
            row = {"mode": mode, "transactions": n, "deadlocked_found": len(d), "detect_ms": rnd(min(ts), 3)}
            append_row(os.path.join(a.out, "x5_scaling.csv"), row)
            print(row)
    # (b) interval vs time to resolve
    for rep in range(a.reps):
        for ms in (50, 100, 200, 500, 1000):
            await x2_one(a, "detect", rep, detect_ms=ms, tag_extra=f"_i{ms}")
    # (c) false positives on a deadlock-free workload (ordered acquisition) with 1 vs 2 stable rounds
    for rep in range(a.reps):
        for stable in (1, 2):
            await x5_fp(a, stable, rep)


async def x5_fp(a, stable, rep, dur=10.0):
    tag = f"fp_s{stable}_r{rep}"
    cl = Cluster5(os.path.join(a.out, "x5", tag), n_orch=3, n_sites=4, site_cpu=2000, delay_ms=1.0, detect_ms=100,
                  stable_rounds=stable)
    await cl.start()
    rng = random.Random(a.seed + rep)
    try:
        await asyncio.sleep(0.5)
        stop_at = mono() + dur
        res = []

        async def worker(w):
            while mono() < stop_at:
                k = rng.choice([2, 3])
                sites = rng.sample(cl.site_names, k)
                try:
                    r = await cl.txn(parts_of(k, holding=0.3, sites=sites, cpu=1000, force=True), cc="ordered",
                                     lock_wait_s=3.0, timeout=10)
                except Exception:
                    r = {"status": "ERROR"}
                res.append(r.get("status"))
        await asyncio.gather(*[worker(w) for w in range(16)])
        await cl.settle(20)
        _, lst = await cl.leader()
        det = (lst or {}).get("detector", {})
        row = {"stable_rounds": stable, "rep": rep, "txns": len(res), "committed": sum(1 for s in res if s == "COMMITTED"),
               "detector_rounds": det.get("rounds"), "deadlocks_reported": det.get("deadlocks"),
               "victims_aborted": det.get("victims"), "note": "ordered acquisition cannot deadlock, so every victim is a false positive"}
        append_row(os.path.join(a.out, "x5_false_positives.csv"), row)
        print(row)
    finally:
        await cl.stop()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("exp", choices=["smoke", "race", "deadlock", "workload", "workload1", "starvation", "detector"])
    ap.add_argument("--reps", type=int, default=1)
    ap.add_argument("--out", default="results/M6")
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    fn = {"smoke": smoke, "race": x1, "deadlock": x2, "workload": x3, "workload1": x3b, "starvation": x4, "detector": x5}[a.exp]
    asyncio.run(fn(a))


if __name__ == "__main__":
    main()

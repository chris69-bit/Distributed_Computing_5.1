"""Prints the summary tables of results/M6/*.csv used in the report (no charts)."""
import csv
import json
import os
import sys
from collections import defaultdict

OUT = sys.argv[1] if len(sys.argv) > 1 else "results/M6"


def rows(name):
    p = os.path.join(OUT, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def f(x):
    try:
        return float(x)
    except Exception:
        return None


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def sd(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return 0.0
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** .5


def fm(x, d=2):
    return "-" if x is None else f"{x:.{d}f}"


r1 = rows("x1_race.csv")
if r1:
    print("X1 race: mode | concurrent | granted (max 40) | over-commit % | ops/s   (mean of reps)")
    for mode in ("unsafe", "locked", "atomic"):
        for c in sorted({int(r["concurrent_requests"]) for r in r1}):
            sel = [r for r in r1 if r["race"] == mode and int(r["concurrent_requests"]) == c]
            print(f"  {mode:7s} c={c:3d}  granted={fm(mean([f(r['granted']) for r in sel]),0):>4s}  over%={fm(mean([f(r['over_commit_pct']) for r in sel]),0):>4s}  ops/s={fm(mean([f(r['ops_per_s']) for r in sel]),0)}")

r2 = rows("x2_deadlock.csv")
if r2:
    print("\nX2 deadlock scenario (reps):  strategy | T1 / T2 outcome | commits | time to both answers s | wasted cpu.s | deadlocks | resolve s")
    for st in ("nowait", "timeout", "detect", "waitdie", "ordered"):
        sel = [r for r in r2 if r["strategy"] == st and r["detect_ms"] in ("", "200")]
        if not sel:
            continue
        outs = sorted({f"{r['T1']}({r['T1_reason'] or '-'})/{r['T2']}({r['T2_reason'] or '-'})" for r in sel})
        print(f"  {st:8s} n={len(sel)} {outs} commits={fm(mean([f(r['committed']) for r in sel]),1)} t={fm(mean([f(r['time_to_both_answers_s']) for r in sel]))}±{fm(sd([f(r['time_to_both_answers_s']) for r in sel]))} wasted={fm(mean([f(r['wasted_cpu_s']) for r in sel]),0)} dl={fm(mean([f(r['deadlocks_detected']) for r in sel]),1)} resolve={fm(mean([f(r['detect_resolve_s']) for r in sel]),3)} viol={sum(int(r['atomicity_viol'])+int(r['over_capacity'])+int(r['stuck']) for r in sel)}")

r3 = rows("x3_workload.csv")
if r3:
    print("\nX3 workload: strategy | clients | commits/s | commit ratio | p95 ms | lock-wait ms | util locked/useful/wasted | waiting | deadlocks/victims | abort reasons")
    for c in (4, 8, 16):
        for st in ("nowait", "timeout", "detect", "waitdie", "ordered"):
            sel = [r for r in r3 if r["strategy"] == st and int(r["clients"]) == c]
            if not sel:
                continue
            rs = defaultdict(int)
            for r in sel:
                for k, v in json.loads(r["abort_reasons"]).items():
                    rs[k] += v
            print(f"  c={c:2d} {st:8s} {fm(mean([f(r['throughput_per_s']) for r in sel]),1):>5s}/s ratio={fm(mean([f(r['commit_ratio']) for r in sel]))} p95={fm(mean([f(r['lat_p95_ms']) for r in sel]),0)} wait={fm(mean([f(r['mean_lock_wait_ms']) for r in sel]),0)} util={fm(mean([f(r['util_locked']) for r in sel]))}/{fm(mean([f(r['util_useful']) for r in sel]))}/{fm(mean([f(r['util_wasted']) for r in sel]))} waiting={fm(mean([f(r['mean_waiting_requests']) for r in sel]),1)} dl={fm(mean([f(r['deadlocks']) for r in sel]),1)}/{fm(mean([f(r['victims']) for r in sel]),1)} reasons={dict(rs)} viol={sum(int(r['atomicity_viol'])+int(r['over_capacity'])+int(r['stuck']) for r in sel)} n={len(sel)}")

r3b = rows("x3b_workload_1slot.csv")
if r3b:
    print("\nX3b one slot per site: strategy | clients | commits/s | commit ratio | p95 ms | lock-wait ms | util locked/useful/wasted | deadlocks/victims | abort reasons")
    for c in (4, 8, 16):
        for st in ("nowait", "timeout", "detect", "waitdie", "ordered"):
            sel = [r for r in r3b if r["strategy"] == st and int(r["clients"]) == c]
            if not sel:
                continue
            rs = defaultdict(int)
            for r in sel:
                for k, v in json.loads(r["abort_reasons"]).items():
                    rs[k] += v
            print(f"  c={c:2d} {st:8s} {fm(mean([f(r['throughput_per_s']) for r in sel]),1):>5s}/s ratio={fm(mean([f(r['commit_ratio']) for r in sel]))} p95={fm(mean([f(r['lat_p95_ms']) for r in sel]),0)} wait={fm(mean([f(r['mean_lock_wait_ms']) for r in sel]),0)} util={fm(mean([f(r['util_locked']) for r in sel]))}/{fm(mean([f(r['util_useful']) for r in sel]))}/{fm(mean([f(r['util_wasted']) for r in sel]))} dl={fm(mean([f(r['deadlocks']) for r in sel]),1)}/{fm(mean([f(r['victims']) for r in sel]),1)} reasons={dict(rs)} viol={sum(int(r['atomicity_viol'])+int(r['over_capacity'])+int(r['stuck']) for r in sel)} n={len(sel)}")

r4 = rows("x4_starvation.csv")
if r4:
    print("\nX4 starvation: discipline | large attempts/committed/timeouts | large wait mean/max s | small commits/s | small p95 ms | util locked/running")
    for d in ("greedy", "fifo", "aging"):
        sel = [r for r in r4 if r["discipline"] == d]
        if sel:
            print(f"  {d:7s} n={len(sel)} large={fm(mean([f(r['large_attempts']) for r in sel]),1)}/{fm(mean([f(r['large_committed']) for r in sel]),1)}/{fm(mean([f(r['large_timeouts']) for r in sel]),1)} wait={fm(mean([f(r['large_wait_mean_s']) for r in sel]))}/{fm(mean([f(r['large_wait_max_s']) for r in sel]))} small={fm(mean([f(r['small_per_s']) for r in sel]),1)}/s p95={fm(mean([f(r['small_lat_p95_ms']) for r in sel]),0)} util={fm(mean([f(r['util_locked']) for r in sel]),3)}/{fm(mean([f(r['util_running']) for r in sel]),3)}")

r5 = rows("x5_scaling.csv")
if r5:
    print("\nX5a detection algorithm time (ms), 16 sites:")
    for r in r5:
        print(f"  {r['mode']:5s} n={r['transactions']:>5s} deadlocked_found={r['deadlocked_found']:>5s} {r['detect_ms']} ms")
ri = [r for r in r2 if r["strategy"] == "detect" and r["detect_ms"] not in ("", "200")] + [r for r in r2 if r["strategy"] == "detect" and r["detect_ms"] == "200"]
if ri:
    print("\nX5b interval vs resolution (all detect runs of X2 + X5): interval ms | time to both answers s | detect resolve s | n")
    for ms in ("50", "100", "200", "500", "1000"):
        sel = [r for r in ri if r["detect_ms"] == ms]
        if sel:
            print(f"  {ms:>5s} ms  t={fm(mean([f(r['time_to_both_answers_s']) for r in sel]))}  resolve={fm(mean([f(r['detect_resolve_s']) for r in sel]),3)}  n={len(sel)}")
r6 = rows("x5_false_positives.csv")
if r6:
    print("\nX5c false positives on a deadlock-free workload:")
    for st in ("1", "2"):
        sel = [r for r in r6 if r["stable_rounds"] == st]
        if sel:
            print(f"  stable_rounds={st}: txns={sum(int(r['txns']) for r in sel)} rounds={sum(int(r['detector_rounds']) for r in sel)} deadlocks_reported={sum(int(r['deadlocks_reported']) for r in sel)} victims={sum(int(r['victims_aborted']) for r in sel)} n={len(sel)}")

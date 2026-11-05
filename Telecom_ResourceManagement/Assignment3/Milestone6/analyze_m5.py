"""Prints the summary tables of results/M5/*.csv used in the report (no charts)."""
import csv
import json
import os
import sys
from collections import defaultdict


OUT = sys.argv[1] if len(sys.argv) > 1 else "results/M5"
COL = {"naive": "#c0392b", "2pc": "#2e5597", "3pc": "#27ae60"}


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


def group(rs, keys, val):
    g = defaultdict(list)
    for r in rs:
        g[tuple(r[k] for k in keys)].append(f(r[val]))
    return g


# ---------------------------------------------------------------- E1
r1 = [r for r in rows("e1_latency.csv") if r["fsync"] == "0"]
if r1:
    print("\nE1 latency (ms, mean of reps) [delay 0 | delay 5]")
    for pr in ("naive", "2pc", "3pc"):
        for k in ("1", "2", "3", "4"):
            out = []
            for d in ("0", "5"):
                v = [f(r["lat_mean_ms"]) for r in r1 if r["proto"] == pr and r["k"] == k and r["one_way_delay_ms"] == d]
                m = [f(r["msgs"]) for r in r1 if r["proto"] == pr and r["k"] == k and r["one_way_delay_ms"] == d]
                out.append(f"{mean(v):.1f}±{sd(v):.1f}")
            msgs = mean([f(r["msgs"]) for r in r1 if r["proto"] == pr and r["k"] == k])
            p2 = mean([f(r["phase2_ms"]) for r in r1 if r["proto"] == pr and r["k"] == k and r["one_way_delay_ms"] == "5"])
            print(f"  {pr:5s} k={k}  {out[0]:>10s} | {out[1]:>10s}   msgs={msgs}  phase2@5ms={p2}")
    # breakdown at k=3 delay 5
    print("\nE1 breakdown k=3, delay 5 ms:")
    for pr in ("2pc", "3pc"):
        sel = [r for r in r1 if r["proto"] == pr and r["k"] == "3" and r["one_way_delay_ms"] == "5"]
        print(" ", pr, {c: round(mean([f(r[c]) for r in sel]), 2) for c in ("plan_ms", "begin_ms", "prepare_ms", "precommit_ms", "decide_ms", "phase2_ms", "lat_mean_ms") if mean([f(r[c]) for r in sel]) is not None})
    fs = [r for r in rows("e1_latency.csv") if r["fsync"] == "1"]
    if fs:
        print("\nE1 fsync k=3 delay 0:")
        for pr in ("2pc", "3pc"):
            a = [f(r["lat_mean_ms"]) for r in fs if r["proto"] == pr]
            b = [f(r["lat_mean_ms"]) for r in r1 if r["proto"] == pr and r["k"] == "3" and r["one_way_delay_ms"] == "0"]
            print(f"  {pr}: no fsync {mean(b):.2f} ms, fsync {mean(a):.2f} ms")

# ---------------------------------------------------------------- E2
r2 = rows("e2_throughput.csv")
if r2:
    print("\nE2 (mean over reps)  proto tight clients: throughput, commit_ratio, p95, aborted, blocked_at_plan")
    for tight in ("0", "1"):
        for pr in ("2pc", "3pc"):
            for c in sorted({int(r["clients"]) for r in r2}):
                sel = [r for r in r2 if r["proto"] == pr and r["tight"] == tight and int(r["clients"]) == c]
                print(f"  {pr} tight={tight} c={c:2d}: {mean([f(r['throughput_per_s']) for r in sel]):6.1f}/s  ratio={mean([f(r['commit_ratio']) for r in sel]):.2f}  p95={mean([f(r['lat_p95_ms']) for r in sel]):6.1f}  aborted={mean([f(r['aborted']) for r in sel]):.0f} blocked={mean([f(r['blocked_at_plan']) for r in sel]):.0f}  inv={[r['inv_atomicity']+r['inv_over_capacity']+r['inv_ledger_mismatch']+r['inv_stuck_prepared'] for r in sel]}")

# ---------------------------------------------------------------- E3
r3 = rows("e3_failures.csv")
if r3:
    print("\nE3 failure matrix (mean over reps): config | scenario | outcome | sites_free_s | max_held_s | blocked_cpu_s | viol")
    cfgs = ["2PC+Raft(3)", "2PC single coordinator", "3PC single coordinator"]
    scens = []
    for r in r3:
        if r["scenario"] not in scens:
            scens.append(r["scenario"])
    for sc in scens:
        for cf in cfgs:
            sel = [r for r in r3 if r["scenario"] == sc and r["config"] == cf]
            if not sel:
                continue
            outc = sorted({(r["client_status"], r["final_state"]) for r in sel})
            print(f"  {sc:22s} {cf:24s} {outc}  free={mean([f(r['sites_free_s']) for r in sel])}  held={mean([f(r['max_held_s']) for r in sel])}  cpu_s={mean([f(r['blocked_cpu_s']) for r in sel])}  viol={sum(int(r['atomicity_viol'])+int(r['coord_part_mismatch'])+int(r['stuck_prepared'])+int(r['over_capacity'])+int(r['ledger_mismatch'])+int(r['protocol_viol']) for r in sel)}  n={len(sel)}")

# ---------------------------------------------------------------- E4
r4 = rows("e4_chaos.csv")
if r4:
    print("\nE4 chaos (sum over reps): proto | requests committed aborted errors | atomicity viol, coord/part mismatch, over_cap, ledger, stuck, protocol | partial chains / chains")
    for pr in ("2pc", "3pc", "naive"):
        sel = [r for r in r4 if r["proto"] == pr]
        if not sel:
            continue
        S = lambda c: sum(int(r[c]) for r in sel)
        print(f"  {pr:5s} reps={len(sel)} kills={S('kills')} req={S('requests')} commit={S('committed')} abort={S('aborted')} err={S('client_errors')} | {S('atomicity_viol')} {S('coord_part_mismatch')} {S('over_capacity')} {S('ledger_mismatch')} {S('stuck_prepared')} {S('protocol_viol')} | partial={S('partial_chains')} / chains={S('naive_chains')} settled={S('settled')}")

# ---------------------------------------------------------------- E5
r5 = rows("e5_availability.csv")
if r5:
    cfgs = ["2PC+Raft(3)", "2PC single coordinator", "3PC single coordinator"]
    print("\nE5 availability: a 0.5 s window is 'up' if it commits >= 50% of the pre-kill average rate")
    print("  config | availability | MTTR s (first window back to >=50%, minus kill time) | max_held s | blocked_cpu_s | client errors | tput before kill")
    for cf in cfgs:
        av, mt = [], []
        sel = [r for r in r5 if r["config"] == cf]
        for r in sel:
            b = json.loads(r["bins"]); tk = f(r["t_kill"]); kb = int(tk / 0.5)
            pre = sum(b[:kb]) / max(1, kb)
            up = [x >= 0.5 * pre for x in b]
            av.append(sum(up) / len(up))
            rec = next((i for i in range(kb + 1, len(b)) if all(up[i:i + 2])), None)
            mt.append(None if rec is None else rec * 0.5 - tk)
        print(f"  {cf:24s} avail={mean(av):.3f}  MTTR={mean(mt):.2f}±{sd(mt):.2f}  held={mean([f(r['max_held_s']) for r in sel]):.2f}  cpu_s={mean([f(r['blocked_cpu_s']) for r in sel]):.0f}  errors={mean([f(r['errors']) for r in sel]):.0f}  tput_before={mean([f(r['tput_before_per_s']) for r in sel]):.0f}  viol={sum(int(r['atomicity_viol'])+int(r['stuck_prepared']) for r in sel)}")

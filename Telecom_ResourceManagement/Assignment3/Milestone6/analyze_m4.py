"""Milestone 4 analysis: charts + summary tables from the five M4 experiments.

    python analyze_m4.py      # reads results/M4/*.csv, writes results/M4-charts/*.png and results/M4-summary.md

Values are the MEDIAN over repetitions with (min–max) in brackets: on a 2-core machine running
up to 10 processes, single repetitions are noisy and the median is less sensitive to outliers.
"""
import csv
import json
import os
import statistics
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(ROOT, "results", "M4")
OUT = os.path.join(ROOT, "results", "M4-charts")
os.makedirs(OUT, exist_ok=True)

S3C, RAFTC, FSYNC = "#52514e", "#4a3aa7", "#9085d6"
NC = {3: "#2a78d6", 5: "#eb6834"}
WALL, LAMP = "#eb6834", "#1baf7a"
INK, INK2, MUTED, GRID, AXIS, SURF = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": INK, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "figure.facecolor": "white",
    "axes.facecolor": SURF, "legend.frameon": False, "lines.linewidth": 2, "lines.markersize": 5})
md = ["# Milestone 4 results summary\n",
      "Median over repetitions; brackets show min–max across repetitions.\n"]


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def rows(name):
    p = os.path.join(R, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def agg(rs, col):
    vals = [num(r.get(col)) for r in rs]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None, None
    return statistics.median(vals), min(vals), max(vals)


def fmt(t, d=1):
    m, lo, hi = t
    if m is None:
        return "–"
    return f"{m:.{d}f}" if hi - lo < 10 ** -d else f"{m:.{d}f} ({lo:.{d}f}–{hi:.{d}f})"


def save(fig, name):
    fig.savefig(os.path.join(OUT, name), dpi=170, bbox_inches="tight")
    plt.close(fig)


def errbar(ax, x, t, color, marker="o", label=None, **kw):
    m, lo, hi = t
    if m is None:
        return
    ax.errorbar([x], [m], yerr=[[m - lo], [hi - m]], fmt=marker, color=color, ecolor=color, elinewidth=1,
                capsize=3, label=label, **kw)


# ============================================================================ E1
ov = rows("overhead.csv")
if ov:
    g = defaultdict(list)
    for r in ov:
        g[(r["mode"], int(r["replicas"]), int(r["clients"]))].append(r)
    clients = sorted({int(r["clients"]) for r in ov})
    sizes = sorted({int(r["replicas"]) for r in ov if r["mode"] == "Raft"})
    md.append("\n## E1 – Coordination overhead vs number of replicas\n")
    md.append("| Mode | Replicas | Clients | Admission (ms) | ↳ commit (ms) | Admissions/s | Messages per admission "
              "(incl. its release) | Idle messages/s | Leader CPU % | Follower CPU % |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    order = [("S3 single orchestrator", 1)] + [("Raft", n) for n in sizes] + [("Raft+fsync", 3)]
    for c in clients:
        for mode, n in order:
            rs = g.get((mode, n, c), [])
            if not rs:
                continue
            md.append(f"| {mode} | {n} | {c} | {fmt(agg(rs, 'admit_ms_mean'), 2)} | {fmt(agg(rs, 'commit_ms_mean'), 2)} | "
                      f"{fmt(agg(rs, 'throughput_adm_s'))} | {fmt(agg(rs, 'msgs_per_admission'))} | "
                      f"{fmt(agg(rs, 'idle_msgs_per_s'))} | {fmt(agg(rs, 'leader_cpu_pct'))} | "
                      f"{fmt(agg(rs, 'follower_cpu_pct'))} |")

    fig, axs = plt.subplots(1, 3, figsize=(13, 3.6))
    ax = axs[0]
    for c, mk, ls in zip(clients, ("o", "s"), ("-", "--")):
        xs = [n for n in sizes if g.get(("Raft", n, c))]
        ys = [agg(g[("Raft", n, c)], "admit_ms_mean")[0] for n in xs]
        ax.plot(xs, ys, marker=mk, ls=ls, color=RAFTC, label=f"Raft, {c} client{'s' if c > 1 else ''}")
        for n in xs:
            errbar(ax, n, agg(g[("Raft", n, c)], "admit_ms_mean"), RAFTC, mk)
        s3 = agg(g.get(("S3 single orchestrator", 1, c), []), "admit_ms_mean")
        if s3[0] is not None:
            ax.plot([1], [s3[0]], marker="D", color=S3C, ls="none",
                    label=f"S3 (no coordination), {c} client{'s' if c > 1 else ''}" if c == clients[0] else None)
            ax.annotate(f"S3 {s3[0]:.1f}", (1, s3[0]), xytext=(6, -2), textcoords="offset points", fontsize=7,
                        color=S3C)
    ax.set_title("Admission time (ms, median)", loc="left")
    ax.set_xlabel("Orchestrator replicas N")
    ax.set_xticks([1] + sizes)
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=7.5)
    ax = axs[1]
    c0 = clients[0]
    meas = [agg(g[("Raft", n, c0)], "msgs_per_admission")[0] for n in sizes]
    base = agg(g.get(("S3 single orchestrator", 1, c0), []), "msgs_per_admission")[0] or 6
    theo = [base + 4 * (n - 1) for n in sizes]
    ax.plot(sizes, meas, marker="o", color=RAFTC, label="measured")
    ax.plot(sizes, theo, ls="--", color=MUTED, lw=1.2, label="theory: S3 + 2 entries × 2(N−1)")
    for n, m in zip(sizes, meas):
        ax.annotate(f"{m:.1f}", (n, m), xytext=(4, -10), textcoords="offset points", fontsize=7.5, color=INK2)
    ax.set_title("Messages per admission (incl. its release)", loc="left")
    ax.set_xlabel("Orchestrator replicas N")
    ax.set_xticks(sizes)
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=7.5)
    ax = axs[2]
    meas = [agg(g[("Raft", n, c0)], "idle_msgs_per_s")[0] for n in sizes]
    s3idle = agg(g.get(("S3 single orchestrator", 1, c0), []), "idle_msgs_per_s")[0] or 5
    ax.plot(sizes, meas, marker="o", color=RAFTC, label="measured")
    ax.plot(sizes, [2 * (n - 1) * 20 + s3idle * n for n in sizes], ls="--", color=MUTED, lw=1.2,
            label="theory: 40(N−1) heartbeat msgs/s + 5 per replica")
    ax.set_title("Messages per second with NO load", loc="left")
    ax.set_xlabel("Orchestrator replicas N")
    ax.set_xticks(sizes)
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=7.5)
    fig.tight_layout()
    save(fig, "e1_overhead.png")

# ============================================================================ E2
pl = rows("placement.csv")
if pl:
    g = defaultdict(list)
    for r in pl:
        g[r["placement"]].append(r)
    names = list(dict.fromkeys(r["placement"] for r in pl))
    md.append("\n## E2 – Synchronization delay vs replica placement\n")
    md.append("| Placement | Replicas | Leader at | One-way delay leader → followers (ms) | Predicted commit (ms) "
              "| Measured commit (ms) | Admission (ms) |")
    md.append("|---|---|---|---|---|---|---|")
    for n in names:
        rs = g[n]
        md.append(f"| {n} | {rs[0]['replicas']} | {rs[0]['leader_at']} | {rs[0]['follower_delays_ms']} | "
                  f"{rs[0]['predicted_commit_ms']} | {fmt(agg(rs, 'commit_ms_mean'))} | {fmt(agg(rs, 'admit_ms_mean'))} |")
    fig, ax = plt.subplots(figsize=(9.5, 3.6))
    ys = range(len(names))
    for y, n in zip(ys, names):
        m, lo, hi = agg(g[n], "commit_ms_mean")
        ax.barh(y, m, color=RAFTC, height=0.55)
        ax.errorbar([m], [y], xerr=[[m - lo], [hi - m]], fmt="none", ecolor=INK2, capsize=3, lw=1)
        pr = num(g[n][0]["predicted_commit_ms"])
        ax.plot([pr, pr], [y - 0.35, y + 0.35], color=WALL, lw=2.2, label="predicted: 2 × delay to closest majority" if y == 0 else None)
        ax.annotate(f"{m:.1f} ms", (m, y), xytext=(6, -3), textcoords="offset points", fontsize=8, color=INK2)
    ax.set_yticks(list(ys))
    ax.set_yticklabels([f"{n}\n(leader: {g[n][0]['leader_at']})" if "leader in" not in n else n for n in names], fontsize=8)
    ax.invert_yaxis()
    ax.set_xlabel("Commit time (ms): leader appends → majority has stored the entry")
    ax.set_title("Where the replicas are decides how long agreement takes", loc="left")
    ax.legend(fontsize=8, loc="lower right")
    fig.tight_layout()
    save(fig, "e2_placement.png")

# ============================================================================ E3
fo = rows("failover.csv")
if fo:
    g = defaultdict(list)
    for r in fo:
        g[(int(r["replicas"]), r["election_ms"])].append(r)
    sizes = sorted({int(r["replicas"]) for r in fo})
    els = list(dict.fromkeys(r["election_ms"] for r in fo))
    md.append("\n## E3 – Leader crash: election and unavailability\n")
    md.append("| Replicas | Election timeout (ms) | Runs | New leader elected after (ms) | Admissions unavailable (ms) "
              "| Extra elections (split votes) | Client requests failed | Restarted replica catch-up (ms) "
              "| Replicas identical after recovery |")
    md.append("|---|---|---|---|---|---|---|---|---|")
    for n in sizes:
        for el in els:
            rs = g.get((n, el), [])
            if not rs:
                continue
            split = sum(max(0, int(r["elections_in_term_change"]) - 1) for r in rs)
            fails = sum(int(r["client_failures"]) for r in rs)
            eqs = sum(r["hashes_equal_after_recovery"] == "True" for r in rs)
            md.append(f"| {n} | {el.replace(',', '–')} | {len(rs)} | {fmt(agg(rs, 'detect_elect_ms'), 0)} | "
                      f"{fmt(agg(rs, 'unavailable_ms'), 0)} | {split} | {fails} | "
                      f"{fmt(agg(rs, 'restart_catchup_ms'), 0)} | {eqs}/{len(rs)} |")
    fig, axs = plt.subplots(1, 2, figsize=(11, 3.6), sharey=True)
    for ax, col, title in ((axs[0], "detect_elect_ms", "Crash → new leader elected (ms)"),
                           (axs[1], "unavailable_ms", "Admissions unavailable (ms)")):
        w = 0.36
        for k, n in enumerate(sizes):
            for i, el in enumerate(els):
                rs = g.get((n, el), [])
                vals = [num(r[col]) for r in rs if num(r[col]) is not None]
                x = i + (k - 0.5) * w
                ax.scatter([x + (j % 5 - 2) * 0.03 for j in range(len(vals))], vals, s=10, color=NC[n], alpha=0.45, lw=0)
                m = statistics.median(vals) if vals else None
                if m is not None:
                    ax.plot([x - w * 0.4, x + w * 0.4], [m, m], color=NC[n], lw=2.5,
                            label=f"N = {n} (median, dots = runs)" if i == 0 else None)
            lo_hi = [tuple(int(v) for v in el.split(",")) for el in els]
        for i, (lo, hi) in enumerate(lo_hi):
            ax.fill_between([i - 0.45, i + 0.45], lo, hi, color=MUTED, alpha=0.12, lw=0,
                            label="election timeout range" if i == 0 else None)
        ax.set_xticks(range(len(els)))
        ax.set_xticklabels([f"{el.replace(',', '–')} ms" for el in els])
        ax.set_xlabel("Election timeout")
        ax.set_title(title, loc="left")
        ax.set_ylim(bottom=0)
    axs[0].legend(fontsize=7.5, loc="upper left")
    fig.tight_layout()
    save(fig, "e3_failover.png")

# ============================================================================ E4
co = rows("consistency.csv")
if co:
    g = defaultdict(list)
    for r in co:
        g[r["mode"]].append(r)
    modes = list(dict.fromkeys(r["mode"] for r in co))
    md.append("\n## E4 – Consistency: uncoordinated orchestrators vs Raft\n")
    md.append("| Setup | Runs | Requests | Real capacity (instances) | Acknowledged to clients | Instances on sites "
              "| Overbooked sites (of 6) | Max site load % | Same id for different instances | Acked but lost "
              "| Same request admitted twice | Replica fingerprints identical |")
    md.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
    for m in modes:
        rs = g[m]
        lost = "–" if rs[0]["acked_but_lost"] == "" else str(sum(int(r["acked_but_lost"]) for r in rs))
        dup = "–" if rs[0]["duplicate_admissions_same_request"] == "" else str(sum(int(r["duplicate_admissions_same_request"]) for r in rs))
        eq = "–" if rs[0]["replica_hashes_equal"] == "" else f"{sum(r['replica_hashes_equal'] == 'True' for r in rs)}/{len(rs)}"
        md.append(f"| {m} | {len(rs)} | {rs[0]['requests']} | {rs[0]['true_capacity_instances']} | "
                  f"{fmt(agg(rs, 'acknowledged'), 0)} | {fmt(agg(rs, 'instances_on_sites'), 0)} | "
                  f"{fmt(agg(rs, 'overbooked_sites'), 0)} | {fmt(agg(rs, 'max_site_load_pct'), 0)} | "
                  f"{fmt(agg(rs, 'sid_collisions'), 0)} | {lost} | {dup} | {eq} |")
    fig, axs = plt.subplots(1, 2, figsize=(11, 3.4))
    cols = [S3C if "uncoord" in m else RAFTC for m in modes]
    short = [m.replace(" uncoordinated orchestrators", " independent\norchestrators").replace(" (2 leader crashes)", "\n(2 leader crashes)") for m in modes]
    cap = num(co[0]["true_capacity_instances"])
    for ax, col, title in ((axs[0], "acknowledged", "Instances promised to clients"),
                           (axs[1], "max_site_load_pct", "Most loaded site (% of its real capacity)")):
        ms = [agg(g[m], col)[0] for m in modes]
        ax.bar(range(len(modes)), ms, color=cols, width=0.55)
        for i, v in enumerate(ms):
            ax.annotate(f"{v:.0f}", (i, v), xytext=(0, 3), textcoords="offset points", ha="center", fontsize=8, color=INK2)
        ref = cap if col == "acknowledged" else 100
        ax.axhline(ref, color=WALL, lw=1.5, ls="--")
        ax.annotate("real capacity" if col == "acknowledged" else "100 % = full", (len(modes) - 0.5, ref),
                    xytext=(0, 3), textcoords="offset points", ha="right", fontsize=8, color=WALL)
        ax.set_xticks(range(len(modes)))
        ax.set_xticklabels(short, fontsize=8)
        ax.set_title(title, loc="left")
    fig.tight_layout()
    save(fig, "e4_consistency.png")

# ============================================================================ E5
ck = rows("clocks.csv")
if ck:
    g = defaultdict(list)
    for r in ck:
        g[num(r["max_skew_ms"])].append(r)
    skews = sorted(g)
    md.append("\n## E5 – Event ordering: skewed wall clocks vs Lamport clocks\n")
    md.append("| Max clock error per node (± ms) | Runs | Cause→effect pairs checked | Wrong order by wall clock "
              "| Wrong order by Lamport clock | Apparent send→receive gap by wall clock (ms, median) | Wrong by kind (all runs) |")
    md.append("|---|---|---|---|---|---|---|")
    pct_w = []
    for s in skews:
        rs = g[s]
        pairs = sum(int(r["causal_pairs"]) for r in rs)
        wv = sum(int(r["wall_violations"]) for r in rs)
        lv = sum(int(r["lamport_violations"]) for r in rs)
        kinds = defaultdict(lambda: [0, 0])
        for r in rs:
            for k, v in json.loads(r["violations_by_kind"]).items():
                a, b = v.split("/")
                kinds[k][0] += int(a)
                kinds[k][1] += int(b)
        names = {"allocate": "ALLOCATE", "exit": "INSTANCE_EXIT", "ae": "AppendEntries"}
        ks = ", ".join(f"{names.get(k, k)} {a}/{b}" for k, (a, b) in kinds.items())
        md.append(f"| ±{s:g} | {len(rs)} | {pairs} | {wv} ({100 * wv / pairs:.1f} %) | {lv} | "
                  f"{fmt(agg(rs, 'true_gap_ms_p50'), 2)} | {ks} |")
        pct_w.append([num(r["wall_violation_pct"]) for r in rs])
    fig, ax = plt.subplots(figsize=(8, 3.6))
    xs = list(range(len(skews)))
    pooled = []
    for sk in skews:
        rs = g[sk]
        pooled.append(100 * sum(int(r["wall_violations"]) for r in rs) / sum(int(r["causal_pairs"]) for r in rs))
    for x, v in zip(xs, pct_w):
        ax.scatter([x + (j % 5 - 2) * 0.04 for j in range(len(v))], v, s=12, color=WALL, alpha=0.35, lw=0)
    ax.plot(xs, pooled, marker="o", color=WALL, label="ordered by wall clock (all runs pooled; dots = single runs)")
    ax.plot(xs, [0] * len(skews), marker="s", color=LAMP, label="ordered by Lamport clock (every run)")
    ax.axhline(50, color=MUTED, lw=1, ls="--")
    ax.annotate("≈ 50 %: once clock error ≫ message delay (≈ 0.5–1 ms), a pair\nflips whenever the receiver's clock is behind the sender's",
                (len(skews) - 1, 50), xytext=(0, -26), textcoords="offset points", ha="right", fontsize=7.5, color=MUTED)
    for x, m in zip(xs, pooled):
        ax.annotate(f"{m:.0f} %", (x, m), xytext=(6, 4), textcoords="offset points", fontsize=8, color=WALL)
    ax.set_xticks(xs)
    ax.set_xticklabels([f"±{s:g}" for s in skews])
    ax.set_xlabel("Clock error of each node (ms, uniform random per node)")
    ax.set_ylabel("% of cause→effect pairs\nin the wrong order")
    ax.set_title("Effect appears before its cause", loc="left")
    ax.set_ylim(-3, 105)
    ax.legend(fontsize=8, loc="upper left")
    fig.tight_layout()
    save(fig, "e5_clocks.png")

open(os.path.join(ROOT, "results", "M4-summary.md"), "w").write("\n".join(md) + "\n")
print("\n".join(md))

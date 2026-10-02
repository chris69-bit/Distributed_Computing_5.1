"""Milestone 2 analysis: turns results/<folder>/runs.csv into charts and tables.

    python analyze.py results/M2-load-sweep

Writes into the same folder:
  charts/*.png   throughput, latency, jitter, loss, CPU, latency breakdown, load distribution
  summary.md     tables averaged over repetitions (mean, and min-max across repetitions)
Needs matplotlib (pip install matplotlib).
"""
import csv
import json
import os
import statistics
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# Colour = placement policy, the same in every chart (validated categorical slots 1 and 2).
COL = {"edge_first": "#2a78d6", "least_loaded": "#eb6834"}
MARK = {"edge_first": "o", "least_loaded": "s"}
INK, INK2, MUTED, GRID, AXIS, SURF = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": INK, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "figure.facecolor": "white",
    "axes.facecolor": SURF, "legend.frameon": False, "lines.linewidth": 2, "lines.markersize": 5})


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def load(folder):
    rows = list(csv.DictReader(open(os.path.join(folder, "runs.csv"))))
    cfg = json.load(open(os.path.join(folder, "config_used.json")))
    sites = [s["id"] for s in cfg["sites"]]
    groups = defaultdict(list)
    for r in rows:
        groups[(r["policy"], int(r["offered_load"]))].append(r)
    policies = [p for p in cfg["perf"]["policies"] if any(k[0] == p for k in groups)]
    loads = sorted({k[1] for k in groups})
    return rows, groups, policies, loads, sites, cfg


def agg(groups, policy, load, col):
    vals = [num(r.get(col)) for r in groups.get((policy, load), [])]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None, None
    return statistics.fmean(vals), min(vals), max(vals)


def series(groups, policies, loads, col):
    out = {}
    for p in policies:
        m, lo, hi = zip(*[agg(groups, p, L, col) for L in loads])
        out[p] = (list(m), list(lo), list(hi))
    return out


def line_panel(ax, groups, policies, loads, col, title, ylabel, logy=False, ideal=False):
    s = series(groups, policies, loads, col)
    if ideal:
        ax.plot(loads, loads, ls="--", lw=1, color=MUTED)
        ref = loads[-2] if len(loads) > 1 else loads[-1]
        ax.annotate("offered = completed", (ref, ref), xytext=(-6, 6), textcoords="offset points",
                    fontsize=8, color=MUTED, ha="right")
    for p in policies:
        m, lo, hi = s[p]
        xs = [x for x, v in zip(loads, m) if v is not None]
        ax.plot(xs, [v for v in m if v is not None], color=COL[p], marker=MARK[p], label=p,
                markeredgecolor="white", markeredgewidth=1)
        for x, a, b in zip(loads, lo, hi):                 # min-max across repetitions
            if a is not None and b is not None and b > a:
                ax.plot([x, x], [a, b], color=COL[p], lw=1, alpha=0.6)
    ax.set_title(title, loc="left")
    ax.set_xlabel("Offered load (tasks/s)")
    ax.set_ylabel(ylabel)
    if logy:
        ax.set_yscale("log")
    ax.set_xticks(loads)


def save(fig, folder, name):
    os.makedirs(os.path.join(folder, "charts"), exist_ok=True)
    path = os.path.join(folder, "charts", name)
    fig.savefig(path, dpi=170, bbox_inches="tight")
    plt.close(fig)
    return path


def main(folder):
    rows, groups, policies, loads, sites, cfg = load(folder)
    paths = []

    # 1. throughput
    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    line_panel(ax, groups, policies, loads, "throughput", "Throughput (completed tasks per second)",
               "Completed tasks/s", ideal=True)
    ax.legend(loc="upper left")
    paths.append(save(fig, folder, "1_throughput.png"))

    # 2. latency mean + p95
    fig, axs = plt.subplots(1, 2, figsize=(9.6, 3.4), sharey=True)
    line_panel(axs[0], groups, policies, loads, "lat_mean_ms", "Mean latency", "Latency (ms, log scale)", logy=True)
    line_panel(axs[1], groups, policies, loads, "lat_p95_ms", "95th-percentile latency", "", logy=True)
    axs[0].legend(loc="upper left")
    paths.append(save(fig, folder, "2_latency.png"))

    # 3. jitter + loss
    fig, axs = plt.subplots(1, 2, figsize=(9.6, 3.4))
    line_panel(axs[0], groups, policies, loads, "jitter_ms", "Jitter", "Mean |change in latency| (ms)")
    line_panel(axs[1], groups, policies, loads, "loss_pct", "Packet loss", "Lost tasks (%)")
    axs[0].legend(loc="upper left")
    paths.append(save(fig, folder, "3_jitter_loss.png"))

    # 4. CPU per site (workers busy) + whole machine
    fig, axs = plt.subplots(1, len(sites) + 1, figsize=(12, 3.2), sharey=True)
    for ax, sid in zip(axs, sites):
        line_panel(ax, groups, policies, loads, f"{sid}_worker_busy_pct", f"{sid}: workers busy", "")
        ax.set_ylim(0, 105)
    line_panel(axs[-1], groups, policies, loads, "sys_cpu_pct", "Whole machine CPU", "")
    axs[0].set_ylabel("Percent")
    axs[0].legend(loc="upper left")
    paths.append(save(fig, folder, "4_cpu.png"))

    # 5. latency breakdown
    parts = [("queue_ms", "Queue wait"), ("exec_ms", "Processing"),
             ("comm_ms", "Communication"), ("link_ms", "Link delay (model)")]
    fig, axs = plt.subplots(1, 4, figsize=(12, 3.2))
    for ax, (c, t) in zip(axs, parts):
        line_panel(ax, groups, policies, loads, c, t, "")
    axs[0].set_ylabel("Mean ms per task")
    axs[0].set_yscale("log")
    axs[0].legend(loc="upper left")
    paths.append(save(fig, folder, "5_latency_breakdown.png"))

    # 6. load distribution at the highest load without loss for both policies
    ok_loads = [L for L in loads if all((agg(groups, p, L, "loss_pct")[0] or 0) < 1 for p in policies)]
    Lref = max(ok_loads) if ok_loads else loads[0]
    fig, axs = plt.subplots(1, 2, figsize=(9.6, 3.2))
    width = 0.38
    for k, (c, t) in enumerate([("tasks_share_pct", f"Share of tasks per site at {Lref} tasks/s"),
                                 ("worker_busy_pct", f"Workers busy per site at {Lref} tasks/s")]):
        ax = axs[k]
        for j, p in enumerate(policies):
            vals = [agg(groups, p, Lref, f"{sid}_{c}")[0] or 0 for sid in sites]
            xs = [i + (j - 0.5) * width for i in range(len(sites))]
            bars = ax.bar(xs, vals, width - 0.04, color=COL[p], label=p, edgecolor="white", linewidth=1)
            for b, v in zip(bars, vals):
                ax.annotate(f"{v:.0f}%", (b.get_x() + b.get_width() / 2, v), xytext=(0, 2),
                            textcoords="offset points", ha="center", fontsize=7.5, color=INK2)
        ax.set_xticks(range(len(sites)), sites)
        ax.set_title(t, loc="left")
        ax.set_ylabel("Percent")
        ax.grid(axis="x", visible=False)
    axs[0].legend(loc="upper left")
    paths.append(save(fig, folder, "6_load_distribution.png"))

    # ---------------- tables ----------------
    def f(v, d=1):
        return "–" if v is None else f"{v:.{d}f}"

    def rng(p, L, c, d=1):
        m, lo, hi = agg(groups, p, L, c)
        if m is None:
            return "–"
        return f"{m:.{d}f}" if hi - lo < 10 ** -d else f"{m:.{d}f} ({lo:.{d}f}–{hi:.{d}f})"

    reps = max(len(v) for v in groups.values())
    md = [f"# Results summary\n\nAverages over {reps} repetition(s) per point; brackets show min–max "
          f"across repetitions.\n"]
    for p in policies:
        md.append(f"\n## {p}\n")
        md.append("| Offered (tasks/s) | Throughput (tasks/s) | Mean latency (ms) | p95 latency (ms) | Jitter (ms) "
                  "| Loss (%) | Machine CPU (%) |")
        md.append("|---|---|---|---|---|---|---|")
        for L in loads:
            md.append(f"| {L} | {rng(p, L, 'throughput')} | {rng(p, L, 'lat_mean_ms')} | {rng(p, L, 'lat_p95_ms')} "
                      f"| {rng(p, L, 'jitter_ms', 2)} | {rng(p, L, 'loss_pct', 2)} | {rng(p, L, 'sys_cpu_pct')} |")
    md.append("\n## Per-site resources (mean over repetitions)\n")
    md.append("| Policy | Offered | " + " | ".join(f"{s} busy % / CPU % / RAM MB" for s in sites) +
              " | Manager CPU % | Generator CPU % |")
    md.append("|---|---|" + "---|" * len(sites) + "---|---|")
    for p in policies:
        for L in loads:
            cells = [f"{f(agg(groups, p, L, f'{s}_worker_busy_pct')[0], 0)} / {f(agg(groups, p, L, f'{s}_cpu_pct')[0], 0)} / "
                     f"{f(agg(groups, p, L, f'{s}_mem_mb')[0], 0)}" for s in sites]
            md.append(f"| {p} | {L} | " + " | ".join(cells) +
                      f" | {f(agg(groups, p, L, 'mgr_cpu_pct')[0])} | {f(agg(groups, p, L, 'gen_cpu_pct')[0], 0)} |")
    md.append("\n## Latency breakdown (mean ms per task)\n")
    md.append("| Policy | Offered | Queue | Processing | Communication | Link (modelled) | Total mean |")
    md.append("|---|---|---|---|---|---|---|")
    for p in policies:
        for L in loads:
            md.append(f"| {p} | {L} | {f(agg(groups, p, L, 'queue_ms')[0], 2)} | {f(agg(groups, p, L, 'exec_ms')[0], 2)} "
                      f"| {f(agg(groups, p, L, 'comm_ms')[0], 2)} | {f(agg(groups, p, L, 'link_ms')[0], 2)} "
                      f"| {f(agg(groups, p, L, 'lat_mean_ms')[0], 2)} |")
    md.append(f"\n## Load distribution at {Lref} tasks/s\n")
    md.append("| Policy | " + " | ".join(f"{s} instances / task share % / latency ms / workers busy %" for s in sites)
              + " | Balance (Jain) |")
    md.append("|---|" + "---|" * len(sites) + "---|")
    for p in policies:
        cells = [f"{f(agg(groups, p, Lref, f'{s}_instances')[0], 0)} / {f(agg(groups, p, Lref, f'{s}_tasks_share_pct')[0], 0)} / "
                 f"{f(agg(groups, p, Lref, f'{s}_lat_mean_ms')[0])} / {f(agg(groups, p, Lref, f'{s}_worker_busy_pct')[0], 0)}"
                 for s in sites]
        md.append(f"| {p} | " + " | ".join(cells) + f" | {f(agg(groups, p, Lref, 'balance_jain')[0], 3)} |")
    # Utilisation law check: U = X * S / slots
    slots = {s["id"]: s["slots"] for s in cfg["sites"]}
    md.append(f"\n## Utilisation law check at {Lref} tasks/s  (U = X × S ÷ workers)\n")
    md.append("| Policy | Site | X (tasks/s) | S (ms) | Predicted U % | Measured busy % |")
    md.append("|---|---|---|---|---|---|")
    for p in policies:
        for s in sites:
            X = agg(groups, p, Lref, f"{s}_throughput")[0]
            S = agg(groups, p, Lref, f"{s}_exec_ms")[0]
            U = agg(groups, p, Lref, f"{s}_worker_busy_pct")[0]
            pred = 100 * X * S / 1000 / slots[s] if X is not None and S is not None else None
            md.append(f"| {p} | {s} | {f(X)} | {f(S, 2)} | {f(pred)} | {f(U)} |")
    # saturation summary
    md.append("\n## Saturation\n")
    for p in policies:
        thr = [(L, agg(groups, p, L, "throughput")[0]) for L in loads]
        peak = max(thr, key=lambda t: t[1] or 0)
        clean = [L for L in loads if (agg(groups, p, L, "loss_pct")[0] or 0) < 1]
        md.append(f"- **{p}**: highest throughput {peak[1]:.0f} tasks/s (at offered {peak[0]}); "
                  f"highest offered load with < 1 % loss: {max(clean) if clean else 'none'} tasks/s.")
    open(os.path.join(folder, "summary.md"), "w").write("\n".join(md) + "\n")
    print("\n".join(md))
    print("\nCharts:", *paths, sep="\n  ")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results/M2-load-sweep")

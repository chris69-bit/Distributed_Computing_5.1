"""Milestone 3 analysis: charts + summary tables from the five M3 experiments.

    python analyze_m3.py            # reads results/M3-*, writes results/M3-charts/*.png and results/M3-summary.md
"""
import csv
import glob
import json
import os
import statistics
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = os.path.dirname(os.path.abspath(__file__))
R = os.path.join(ROOT, "results")
OUT = os.path.join(R, "M3-charts")
os.makedirs(OUT, exist_ok=True)

POL = {"edge_first": "#2a78d6", "least_loaded": "#eb6834", "tier_aware": "#1baf7a"}
MARK = {"edge_first": "o", "least_loaded": "s", "tier_aware": "^"}
ARCH = {"s2": "#52514e", "s3": "#4a3aa7"}
ARCH_NAME = {"s2": "S2 monolith", "s3": "S3 microservices"}
MODE = {"per_site": "#eb6834", "single": "#2a78d6"}
MODE_NAME = {"per_site": "one search per site (S1/S2)", "single": "one search per admission (S3 fix)"}
INK, INK2, MUTED, GRID, AXIS, SURF = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#fcfcfb"
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": AXIS, "axes.labelcolor": INK2,
    "axes.titlesize": 10, "axes.titleweight": "bold", "axes.titlecolor": INK, "xtick.color": MUTED,
    "ytick.color": MUTED, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6,
    "axes.spines.top": False, "axes.spines.right": False, "figure.facecolor": "white",
    "axes.facecolor": SURF, "legend.frameon": False, "lines.linewidth": 2, "lines.markersize": 5})
SITES = ["edge-A", "edge-B", "core-1", "cloud-1"]
SERVICES = ["vRAN-DU", "UPF", "IMS-CSCF", "Transcoder", "IoT-GW"]
md = ["# Milestone 3 results summary\n", "Mean over repetitions; brackets show min–max across repetitions.\n"]


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def rows(path):
    return list(csv.DictReader(open(path))) if os.path.exists(path) else []


def agg(rs, col):
    vals = [num(r.get(col)) for r in rs]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None, None
    return statistics.fmean(vals), min(vals), max(vals)


def fmt(t, d=1):
    m, lo, hi = t
    if m is None:
        return "–"
    return f"{m:.{d}f}" if hi - lo < 10 ** -d else f"{m:.{d}f} ({lo:.{d}f}–{hi:.{d}f})"


def f1(t, d=1):
    return "–" if t[0] is None else f"{t[0]:.{d}f}"


def save(fig, name):
    p = os.path.join(OUT, name)
    fig.savefig(p, dpi=170, bbox_inches="tight")
    plt.close(fig)
    return p


def lines(ax, groups, keys, xs, col, colors, markers=None, labels=None, logy=False):
    for k in keys:
        ms, los, his = zip(*[agg(groups.get((k, x), []), col) for x in xs])
        pts = [(x, m) for x, m in zip(xs, ms) if m is not None]
        if not pts:
            continue
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=colors[k], marker=(markers or {}).get(k, "o"),
                label=(labels or {}).get(k, k), markeredgecolor="white", markeredgewidth=1)
        for x, a, b in zip(xs, los, his):
            if a is not None and b is not None and b > a:
                ax.plot([x, x], [a, b], color=colors[k], lw=1, alpha=0.6)
    if logy:
        ax.set_yscale("log")


# ============================================================================ E1 architecture
E1 = rows(os.path.join(R, "M3-arch", "runs.csv"))
if E1:
    g = defaultdict(list)
    for r in E1:
        g[(r["arch"], int(r["offered_load"]))].append(r)
    loads = sorted({k[1] for k in g})
    archs = ["s2", "s3"]
    ctrl = {"s2": ["manager"], "s3": ["registry", "orchestrator", "telemetry"]}

    def comp_cpu(rs, names):
        vals = []
        for r in rs:
            xs = [num(r.get(f"cpu_{n}")) for n in names]
            if any(x is not None for x in xs):
                vals.append(sum(x or 0 for x in xs))
        return (statistics.fmean(vals), min(vals), max(vals)) if vals else (None, None, None)

    fig, axs = plt.subplots(1, 3, figsize=(12, 3.3))
    width = 0.36
    panels = [("measured_mean_ms", "Task latency on the machine (ms)", "excl. modelled link delay"),
              ("admit_ms_mean", "Admission request (ms)", "generator → … → orchestrator"),
              (None, "Extra CPU outside the sites (% of a core)", "control plane + gateways")]
    for ax, (col, title, note) in zip(axs, panels):
        for j, a_ in enumerate(archs):
            vals = []
            for L in loads:
                rs = g.get((a_, L), [])
                if col:
                    vals.append(agg(rs, col)[0] or 0)
                else:
                    names = ctrl[a_] + (["gw-A", "gw-B"] if a_ == "s3" else [])
                    vals.append(comp_cpu(rs, names)[0] or 0)
            xs = [i + (j - 0.5) * width for i in range(len(loads))]
            bars = ax.bar(xs, vals, width - 0.04, color=ARCH[a_], label=ARCH_NAME[a_], edgecolor="white")
            for b, v_ in zip(bars, vals):
                ax.annotate(f"{v_:.1f}", (b.get_x() + b.get_width() / 2, v_), xytext=(0, 2),
                            textcoords="offset points", ha="center", fontsize=8, color=INK2)
        ax.set_xticks(range(len(loads)), [f"{L} tasks/s" for L in loads])
        ax.set_title(title, loc="left")
        ax.set_xlabel(note, color=MUTED)
        ax.grid(axis="x", visible=False)
    axs[0].legend(loc="upper left")
    save(fig, "e1_architecture_overhead.png")

    md.append("\n## E1 – S2 monolith vs S3 microservices (local processes, least_loaded, 4 sites)\n")
    md.append("| Architecture | Offered | Throughput | Task latency on machine (ms) | Comm. (ms) | Admission (ms) | "
              "Control-plane CPU % | Gateway CPU % | Control+gateway RAM (MB) | Processes |")
    md.append("|---|---|---|---|---|---|---|---|---|---|")
    for a_ in archs:
        for L in loads:
            rs = g.get((a_, L), [])
            ram = []
            for r in rs:
                names = ctrl[a_] + (["gw-A", "gw-B"] if a_ == "s3" else [])
                ram.append(sum(num(r.get(f"rss_{n}")) or 0 for n in names))
            nproc = len(ctrl[a_]) + (2 if a_ == "s3" else 0) + 4
            md.append(f"| {ARCH_NAME[a_]} | {L} | {fmt(agg(rs, 'throughput'))} | {fmt(agg(rs, 'measured_mean_ms'), 2)} | "
                      f"{fmt(agg(rs, 'comm_ms'), 2)} | {fmt(agg(rs, 'admit_ms_mean'), 2)} | "
                      f"{fmt(comp_cpu(rs, ctrl[a_]))} | {fmt(comp_cpu(rs, ['gw-A', 'gw-B'])) if a_ == 's3' else '–'} | "
                      f"{statistics.fmean(ram):.0f} | {nproc} + workers |")
    md.append("\nS3 communication split (mean ms): user↔gateway / gateway↔site")
    for L in loads:
        rs = g.get(("s3", L), [])
        md.append(f"- {L} tasks/s: {f1(agg(rs, 'comm_user_gw_ms'), 2)} / {f1(agg(rs, 'comm_gw_site_ms'), 2)}")
    md.append("\nControl-plane messages received per second (mean): " + "; ".join(
        f"{a_} {L}/s: " + ", ".join(f"{k[len('msgs_per_s_'):]} {f1(agg(g[(a_, L)], k))}"
                                    for k in g[(a_, L)][0] if k.startswith("msgs_per_s_") and g[(a_, L)][0][k])
        for a_ in archs for L in loads if g.get((a_, L))))

# ============================================================================ E2a blocking
blk = defaultdict(list)
for f in glob.glob(os.path.join(R, "M3-blocking", "*_seed*", "summary.json")):
    pol = os.path.basename(os.path.dirname(f)).rsplit("_seed", 1)[0]
    d = json.load(open(f))
    blk[pol].append(d["manager"])
if blk:
    pols = [p for p in POL if p in blk]
    fig, axs = plt.subplots(1, 2, figsize=(11, 3.4), gridspec_kw={"width_ratios": [2.2, 1]})
    width = 0.26
    for j, p in enumerate(pols):
        vals = []
        for s in SERVICES:
            per = [m["per_service"].get(s) for m in blk[p]]
            off = sum(x["offered"] for x in per if x)
            vals.append(100 * sum(x["blocked"] for x in per if x) / off if off else 0)
        xs = [i + (j - 1) * width for i in range(len(SERVICES))]
        bars = ax = axs[0].bar(xs, vals, width - 0.03, color=POL[p], label=p, edgecolor="white")
        for b, v_ in zip(bars, vals):
            axs[0].annotate(f"{v_:.0f}", (b.get_x() + b.get_width() / 2, v_), xytext=(0, 2),
                            textcoords="offset points", ha="center", fontsize=7.5, color=INK2)
    axs[0].set_xticks(range(len(SERVICES)), SERVICES)
    axs[0].set_title("Blocking probability per service (3 seeds pooled)", loc="left")
    axs[0].set_ylabel("Blocked (%)")
    axs[0].grid(axis="x", visible=False)
    axs[0].legend(loc="upper right")
    tiers = {"edge": ["edge-A", "edge-B"], "core": ["core-1"], "cloud": ["cloud-1"]}
    for j, p in enumerate(pols):
        acc = sum(m["stats"]["accepted"] for m in blk[p])
        share = []
        for t, ss in tiers.items():
            n = sum(m["per_service"][s]["placed_on"].get(x, 0) for m in blk[p] for s in m["per_service"] for x in ss)
            share.append(100 * n / acc if acc else 0)
        left = 0
        for k, (t, v_) in enumerate(zip(tiers, share)):
            axs[1].barh(j, v_, left=left, color=["#cde2fb", "#6da7ec", "#184f95"][k], edgecolor="white")
            if v_ > 7:
                axs[1].text(left + v_ / 2, j, f"{t} {v_:.0f}%", ha="center", va="center", fontsize=7.5,
                            color="white" if k == 2 else INK)
            left += v_
    axs[1].set_yticks(range(len(pols)), pols)
    axs[1].set_xlim(0, 100)
    axs[1].set_title("Where accepted instances went", loc="left")
    axs[1].set_xlabel("Share of accepted instances (%)")
    axs[1].grid(axis="y", visible=False)
    save(fig, "e2a_blocking.png")
    md.append("\n## E2a – Admission with the cloud tier (S1 workload, 3 seeds: 42, 43, 44)\n")
    md.append("| Policy | Requests | Blocked | P_block overall | " + " | ".join(f"{s} P_block" for s in SERVICES)
              + " | Edge share of instances |")
    md.append("|---|---|---|---|" + "---|" * len(SERVICES) + "---|")
    for p in pols:
        req = sum(m["stats"]["requests"] for m in blk[p])
        b = sum(m["stats"]["blocked"] for m in blk[p])
        cells = []
        for s in SERVICES:
            per = [m["per_service"].get(s) for m in blk[p]]
            off = sum(x["offered"] for x in per if x)
            cells.append(f"{100 * sum(x['blocked'] for x in per if x) / off:.1f} %" if off else "–")
        acc = sum(m["stats"]["accepted"] for m in blk[p])
        edge = sum(m["per_service"][s]["placed_on"].get(x, 0) for m in blk[p] for s in m["per_service"]
                   for x in ("edge-A", "edge-B"))
        per_seed = ", ".join(f"{100 * m['stats']['blocked'] / m['stats']['requests']:.1f}" for m in blk[p])
        md.append(f"| {p} | {req} | {b} | {100 * b / req:.1f} % (seeds: {per_seed}) | " + " | ".join(cells)
                  + f" | {100 * edge / acc:.0f} % |")
        reasons = defaultdict(int)
        for m in blk[p]:
            for s in m["per_service"].values():
                for k, n in s["block_reasons"].items():
                    reasons[k] += n
        md.append(f"|  ↳ block reasons | | | {dict(reasons)} |" + " |" * len(SERVICES) + " |")

# ============================================================================ E2b tiers (Docker)
E2 = rows(os.path.join(R, "M3-tiers", "runs.csv"))
if E2:
    g = defaultdict(list)
    for r in E2:
        g[(r["policy"], int(r["offered_load"]))].append(r)
    pols = [p for p in POL if any(k[0] == p for k in g)]
    loads = sorted({k[1] for k in g})
    fig, axs = plt.subplots(1, 3, figsize=(12.5, 3.4))
    lines(axs[0], g, pols, loads, "throughput", POL, MARK)
    axs[0].plot(loads, loads, ls="--", lw=1, color=MUTED)
    axs[0].set_title("Throughput (tasks/s)", loc="left")
    lines(axs[1], g, pols, loads, "lat_mean_ms", POL, MARK, logy=True)
    axs[1].set_title("Mean end-to-end latency (ms, log)", loc="left")
    lines(axs[2], g, pols, loads, "loss_pct", POL, MARK)
    axs[2].set_title("Packet loss (%)", loc="left")
    for ax in axs:
        ax.set_xlabel("Offered load (tasks/s)")
        ax.set_xticks(loads)
    axs[0].legend(loc="upper left")
    save(fig, "e2b_tiers_performance.png")

    fig, axs = plt.subplots(1, 4, figsize=(12.5, 3.1), sharey=True)
    for ax, s in zip(axs, SITES):
        lines(ax, g, pols, loads, f"cpu_{s}", POL, MARK)
        ax.set_title(f"{s} CPU (% of a core)", loc="left")
        ax.set_xlabel("Offered load (tasks/s)")
        ax.set_xticks(loads)
    quota = {"edge-A": 25, "edge-B": 25, "core-1": 50, "cloud-1": 75}
    for ax, s in zip(axs, SITES):
        ax.axhline(quota[s], ls=":", color=INK2, lw=1)
        ax.annotate(f"quota {quota[s]}%", (loads[0], quota[s]), xytext=(0, 3), textcoords="offset points",
                    fontsize=7.5, color=INK2)
    axs[0].legend(loc="upper left")
    save(fig, "e2b_tiers_cpu.png")

    Lmid = 150 if 150 in loads else loads[len(loads) // 2]
    fig, ax = plt.subplots(figsize=(9, 3.3))
    width = 0.26
    for j, p in enumerate(pols):
        vals = [agg(g.get((p, Lmid), []), f"svc_{s}_lat_mean_ms")[0] or 0 for s in SERVICES]
        xs = [i + (j - 1) * width for i in range(len(SERVICES))]
        bars = ax.bar(xs, vals, width - 0.03, color=POL[p], label=p, edgecolor="white")
        for b, v_ in zip(bars, vals):
            ax.annotate(f"{v_:.0f}", (b.get_x() + b.get_width() / 2, v_), xytext=(0, 2), textcoords="offset points",
                        ha="center", fontsize=7.5, color=INK2)
    ax.set_xticks(range(len(SERVICES)), SERVICES)
    ax.set_ylabel("Mean end-to-end latency (ms)")
    ax.set_title(f"Latency per service at {Lmid} tasks/s (where each policy put it)", loc="left")
    ax.grid(axis="x", visible=False)
    ax.legend(loc="upper left")
    save(fig, "e2b_tiers_per_service.png")

    md.append("\n## E2b – Edge vs core vs cloud traffic (Docker: CPU quotas, gateway-emulated link delay)\n")
    md.append("| Policy | Offered | Throughput | Mean latency (ms) | p95 (ms) | Jitter (ms) | Loss % | "
              "Task share edge / core / cloud % | CPU % edge-A / edge-B / core-1 / cloud-1 |")
    md.append("|---|---|---|---|---|---|---|---|---|")
    for p in pols:
        for L in loads:
            rs = g.get((p, L), [])
            md.append(f"| {p} | {L} | {fmt(agg(rs, 'throughput'))} | {fmt(agg(rs, 'lat_mean_ms'))} | "
                      f"{fmt(agg(rs, 'lat_p95_ms'))} | {fmt(agg(rs, 'jitter_ms'), 2)} | {fmt(agg(rs, 'loss_pct'), 2)} | "
                      f"{f1(agg(rs, 'edge_task_share_pct'), 0)} / {f1(agg(rs, 'core_task_share_pct'), 0)} / "
                      f"{f1(agg(rs, 'cloud_task_share_pct'), 0)} | "
                      + " / ".join(f1(agg(rs, f"cpu_{s}"), 0) for s in SITES) + " |")
    md.append(f"\n### Per service at {Lmid} tasks/s (mean end-to-end latency ms, tier)\n")
    md.append("| Policy | " + " | ".join(SERVICES) + " |")
    md.append("|---|" + "---|" * len(SERVICES))
    for p in pols:
        rs = g.get((p, Lmid), [])
        md.append(f"| {p} | " + " | ".join(
            f"{f1(agg(rs, f'svc_{s}_lat_mean_ms'))} ({rs[0].get(f'svc_{s}_tiers', '?') if rs else '?'})" for s in SERVICES) + " |")
    md.append("\n### Backhaul bandwidth reserved by the placement (Mb/s)\n")
    md.append("| Policy | edge-A↔core-1 | edge-B↔core-1 | edge-A↔edge-B | core-1↔cloud-1 |")
    md.append("|---|---|---|---|---|")
    for p in pols:
        rs = g.get((p, loads[0]), [])
        md.append(f"| {p} | " + " | ".join(f1(agg(rs, f"link_{k}_reserved_mbps"), 0) for k in
                                           ("edge-A<->core-1", "edge-B<->core-1", "edge-A<->edge-B", "core-1<->cloud-1")) + " |")

# ============================================================================ E3a scale-out (Docker)
E3 = rows(os.path.join(R, "M3-scaleout", "runs.csv"))
if E3:
    g = defaultdict(list)
    for r in E3:
        n = int(r["tag"].lstrip("N").split("-")[0])
        g[n].append(r)
    ns = sorted(g)
    fig, axs = plt.subplots(1, 2, figsize=(10, 3.3))
    m = [agg(g[n], "throughput") for n in ns]
    base = m[0][0]
    axs[0].plot(ns, [base * n for n in ns], ls="--", lw=1, color=MUTED)
    axs[0].annotate("perfect linear scaling", (ns[-1], base * ns[-1]), xytext=(-4, 4), textcoords="offset points",
                    ha="right", fontsize=8, color=MUTED)
    axs[0].plot(ns, [x[0] for x in m], color="#2a78d6", marker="o", markeredgecolor="white")
    for n, (mm, lo, hi) in zip(ns, m):
        axs[0].plot([n, n], [lo, hi], color="#2a78d6", lw=1, alpha=0.6)
        axs[0].annotate(f"{mm:.0f}", (n, mm), xytext=(6, -10), textcoords="offset points", fontsize=8, color=INK2)
    axs[0].set_xticks(ns)
    axs[0].set_xlabel("Compute sites (each: CPU quota 0.3, 2 transcoder instances)")
    axs[0].set_title("Max throughput (tasks/s, overloaded)", loc="left")
    eff = [100 * x[0] / (base * n) for n, x in zip(ns, m)]
    axs[1].bar(ns, eff, color="#2a78d6", width=0.5, edgecolor="white")
    for n, e_ in zip(ns, eff):
        axs[1].annotate(f"{e_:.0f}%", (n, e_), xytext=(0, 2), textcoords="offset points", ha="center", fontsize=8,
                        color=INK2)
    axs[1].set_ylim(0, 115)
    axs[1].set_xticks(ns)
    axs[1].set_xlabel("Compute sites")
    axs[1].set_title("Scaling efficiency (vs N × one site)", loc="left")
    axs[1].grid(axis="x", visible=False)
    save(fig, "e3a_scaleout.png")
    md.append("\n## E3a – Compute-tier scale-out (Docker, identical sites with CPU quota 0.3, overload)\n")
    md.append("| Sites | Offered | Throughput | Efficiency | Loss % | Mean CPU per site (%) | Gateways CPU % | Load generator CPU % |")
    md.append("|---|---|---|---|---|---|---|---|")
    for n, x in zip(ns, m):
        rs = g[n]
        site_cpu = statistics.fmean([v_ for r in rs for s in SITES if (v_ := num(r.get(f"cpu_{s}"))) is not None])
        gw = statistics.fmean([sum(num(r.get(f"cpu_{k}")) or 0 for k in ("gw-A", "gw-B")) for r in rs])
        md.append(f"| {n} | {rs[0]['offered_load']} | {fmt(x)} | {100 * x[0] / (base * n):.0f} % | "
                  f"{fmt(agg(rs, 'loss_pct'))} | {site_cpu:.1f} | {gw:.1f} | {f1(agg(rs, 'cpu_driver'))} |")

# ============================================================================ E3b control plane
E3b = rows(os.path.join(R, "M3-scalebench", "runs.csv"))
if E3b:
    g = defaultdict(list)
    for r in E3b:
        g[(r["path_mode"], int(r["sites"]))].append(r)
    modes = [m_ for m_ in MODE if any(k[0] == m_ for k in g)]
    ns = sorted({k[1] for k in g})
    fig, axs = plt.subplots(1, 3, figsize=(12.5, 3.4))
    lines(axs[0], g, modes, ns, "admission_ms_mean", MODE, {"per_site": "s", "single": "o"}, MODE_NAME)
    axs[0].set_xscale("log")
    axs[0].set_yscale("log")
    axs[0].set_title("Admission time (ms)", loc="left")
    lines(axs[1], g, ["single"], ns, "registry_cpu_pct", {"single": "#2a78d6"})
    axs[1].set_xscale("log")
    axs[1].set_title("Registry CPU (%), 1 heartbeat/s per site", loc="left")
    lines(axs[2], g, ["single"], ns, "list_sites_kb", {"single": "#2a78d6"})
    axs[2].set_xscale("log")
    axs[2].set_yscale("log")
    axs[2].set_title("LIST_SITES reply size (KB)", loc="left")
    for ax in axs:
        ax.set_xlabel("Number of sites (log scale)")
        ax.set_xticks(ns, [str(n) for n in ns])
        ax.minorticks_off()
    axs[0].legend(loc="upper left", fontsize=8)
    save(fig, "e3b_control_plane.png")
    md.append("\n## E3b – Control-plane scalability (simulated sites, 1 heartbeat/s each)\n")
    md.append("| Path search | Sites | Heartbeats/s | Registry CPU % | Heartbeat RTT p95 (ms) | Admission (ms) | "
              "↳ registry part | ↳ decision part | LIST_SITES (KB) |")
    md.append("|---|---|---|---|---|---|---|---|---|")
    for m_ in modes:
        for n in ns:
            rs = g.get((m_, n), [])
            md.append(f"| {m_} | {n} | {fmt(agg(rs, 'heartbeats_per_s'))} | {fmt(agg(rs, 'registry_cpu_pct'))} | "
                      f"{fmt(agg(rs, 'hb_rtt_p95_ms'), 2)} | {fmt(agg(rs, 'admission_ms_mean'), 1)} | "
                      f"{fmt(agg(rs, 'admission_registry_ms'), 1)} | {fmt(agg(rs, 'admission_decision_ms'), 1)} | "
                      f"{fmt(agg(rs, 'list_sites_kb'))} |")

# ============================================================================ failure evidence
C = rows(os.path.join(R, "M3-collapse", "runs.csv"))
if C:
    md.append("\n## Overload collapse test (2 sites, quota 0.3 each, 180 tasks/s offered)\n")
    md.append("| Deadline-aware dropping | Throughput | Loss % | Site CPU % (core-1 / cloud-1) |")
    md.append("|---|---|---|---|")
    for r in C:
        md.append(f"| {'off' if 'nodeadline' in r['tag'] else 'on (1 s)'} | {r['throughput']} | {r['loss_pct']} | "
                  f"{r.get('cpu_core-1')} / {r.get('cpu_cloud-1')} |")

open(os.path.join(R, "M3-summary.md"), "w").write("\n".join(md) + "\n")
print("\n".join(md))
print("\ncharts:", sorted(os.listdir(OUT)))

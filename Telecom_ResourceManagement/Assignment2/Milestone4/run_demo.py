"""Milestone 1 experiment runner for TeleRM.

Starts the resource manager and every site as separate OS processes, generates a
seeded Poisson stream of telecom service requests (with random holding times and
resize events), then prints and saves the results.

    python run_demo.py
    python run_demo.py --policy least_loaded
    python run_demo.py --rate 3 --window 40
    python run_demo.py --no-launch            # nodes already running (multi-machine)
"""
import argparse
import asyncio
import json
import os
import random
import subprocess
import sys
import time

from telerm.protocol import call

ROOT = os.path.dirname(os.path.abspath(__file__))


def launch(cfg, cfg_path, policy, out_dir):
    os.makedirs(os.path.join(out_dir, "logs"), exist_ok=True)
    m = cfg["manager"]
    procs = {}
    log = open(os.path.join(out_dir, "logs", "manager.log"), "w")
    procs["manager"] = subprocess.Popen(
        [sys.executable, "-m", "telerm.manager", "--port", str(m["port"]), "--topology", cfg_path,
         "--policy", policy, "--hb-timeout", str(m["heartbeat_timeout_s"]), "--out", out_dir],
        cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    time.sleep(0.5)
    for s in cfg["sites"]:
        log = open(os.path.join(out_dir, "logs", f"{s['id']}.log"), "w")
        procs[s["id"]] = subprocess.Popen(
            [sys.executable, "-m", "telerm.site", "--id", s["id"], "--tier", s["tier"], "--host", s["host"],
             "--port", str(s["port"]), "--manager", f"{m['host']}:{m['port']}", "--cpu", str(s["cpu"]),
             "--mem", str(s["mem"]), "--bw", str(s["bw"]), "--slots", str(s["slots"]),
             "--work-kb", str(cfg["job_work_kb"])],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    return procs


def build_arrivals(cfg, rng):
    """Poisson arrivals: exponential inter-arrival times, exponential holding times."""
    w, svcs = cfg["workload"], cfg["services"]
    names = list(svcs)
    weights = [svcs[n]["weight"] for n in names]
    t, events = 0.0, []
    while True:
        t += rng.expovariate(w["arrival_rate_per_s"])
        if t > w["arrival_window_s"]:
            break
        name = rng.choices(names, weights)[0]
        s = svcs[name]
        events.append({"t": round(t, 3), "kind": "request", "request": {
            "service": name, "ingress": rng.choice(w["ingress_sites"]),
            "demand": {"cpu": s["cpu"], "mem": s["mem"], "bw": s["bw"]},
            "max_latency_ms": s["max_latency_ms"],
            "holding_s": round(min(4 * w["mean_holding_s"], max(1.0, rng.expovariate(1 / w["mean_holding_s"]))), 2)}})
        if rng.random() < w["resize_probability"]:
            events.append({"t": round(t + 0.5, 3), "kind": "resize", "factor": rng.choice(w["resize_factors"]),
                           "pick": rng.random()})
    return sorted(events, key=lambda e: e["t"])


async def experiment(cfg, policy):
    m = cfg["manager"]
    mgr = (m["host"], m["port"])
    deadline = time.time() + 20
    while True:
        try:
            st = (await call(*mgr, {"type": "STATUS"}))["status"]
            if len(st["sites"]) == len(cfg["sites"]):
                break
        except Exception:
            pass
        if time.time() > deadline:
            raise RuntimeError("sites did not register - check the logs folder")
        await asyncio.sleep(0.3)

    rng = random.Random(cfg["seed"])
    events = build_arrivals(cfg, rng)
    n_req = sum(1 for e in events if e["kind"] == "request")
    print(f"All {len(cfg['sites'])} sites registered. Replaying {n_req} requests over "
          f"{cfg['workload']['arrival_window_s']} s (placement={policy}).\n")

    accepted, results, t0, last_print = [], [], time.time(), 0

    async def fire(e):
        if e["kind"] == "request":
            ts = time.time()
            r = await call(*mgr, {"type": "REQUEST", "request": e["request"]})
            rtt = (time.time() - ts) * 1000
            results.append({**e["request"], "t": e["t"], "status": r.get("status"), "sid": r.get("sid"),
                            "site": r.get("site"), "reason": r.get("reason"), "request_ms": round(rtt, 2),
                            "decision_ms": r.get("decision_ms")})
            if r.get("status") == "ACCEPTED":
                accepted.append((r["sid"], e["request"]["demand"]["cpu"]))
        else:
            if accepted:
                sid, cpu = accepted[int(e["pick"] * len(accepted))]
                r = await call(*mgr, {"type": "RESIZE", "sid": sid, "cpu": round(cpu * e["factor"])})
                results.append({"t": e["t"], "kind": "resize", "sid": sid, "factor": e["factor"],
                                "status": r.get("status"), "reason": r.get("reason")})

    pending = []
    for e in events:
        delay = e["t"] - (time.time() - t0)
        if delay > 0:
            await asyncio.sleep(delay)
        pending.append(asyncio.create_task(fire(e)))
        if time.time() - t0 - last_print >= 5:
            last_print = time.time() - t0
            st = (await call(*mgr, {"type": "STATUS"}))["status"]
            s = st["stats"]
            util = "  ".join(f"{k}:cpu {v['utilisation']['cpu']:.0%}" for k, v in st["sites"].items())
            print(f"  t+{last_print:4.0f}s  requests={s['requests']:<3} accepted={s['accepted']:<3} "
                  f"blocked={s['blocked']:<3} [{util}]")
    await asyncio.gather(*pending)
    await asyncio.sleep(cfg["workload"]["drain_s"])

    status = (await call(*mgr, {"type": "STATUS", "full": True}, timeout=10))["status"]
    site_status = {}
    for s in cfg["sites"]:
        try:
            site_status[s["id"]] = (await call(s["host"], s["port"], {"type": "STATUS"}))["status"]
        except Exception as exc:
            site_status[s["id"]] = {"error": repr(exc)}
    return results, status, site_status


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def report(results, status, site_status, cfg):
    s = status["stats"]
    print("\n=== ADMISSION ===")
    pb = s["blocked"] / s["requests"] if s["requests"] else 0
    print(f"requests={s['requests']} accepted={s['accepted']} blocked={s['blocked']} "
          f"blocking probability={pb:.1%}  resizes: {s['resize_accepted']} ok / {s['resize_rejected']} rejected")
    dec = [r["decision_ms"] for r in results if r.get("decision_ms") is not None]
    req = [r["request_ms"] for r in results if r.get("request_ms") is not None]
    if dec:
        print(f"admission decision time mean={mean(dec):.3f} ms   end-to-end request time mean={mean(req):.1f} ms")

    print("\n=== PER SERVICE ===")
    print(f"{'service':<11} {'offered':>7} {'accepted':>8} {'blocked':>7} {'P_block':>7}  placed on / block reasons")
    for name, v in sorted(status["per_service"].items()):
        p = v["blocked"] / v["offered"] if v["offered"] else 0
        print(f"{name:<11} {v['offered']:>7} {v['accepted']:>8} {v['blocked']:>7} {p:>7.1%}  "
              f"{v['placed_on']}  {v['block_reasons'] or ''}")

    print("\n=== SITE RESERVATION UTILISATION (1 s samples during run) ===")
    ts = [x for x in status.get("timeseries", []) if x["active"] > 0]
    print(f"{'site':<8} {'cpu mean':>8} {'cpu peak':>8} {'mem mean':>8} {'bw mean':>8} {'slot util':>9} "
          f"{'fairness':>8} {'jobs':>6}  workers")
    for sid in status["sites"]:
        cpu = [x["sites"][sid]["cpu"] for x in ts if sid in x["sites"]]
        mem = [x["sites"][sid]["mem"] for x in ts if sid in x["sites"]]
        bw = [x["sites"][sid]["bw"] for x in ts if sid in x["sites"]]
        su = [x["sites"][sid]["slot_util"] for x in ts if sid in x["sites"]]
        ss = site_status.get(sid, {})
        jobs = sum(i["jobs"] for i in ss.get("instances", []))
        fair = ss.get("fairness_mean")
        print(f"{sid:<8} {mean(cpu) or 0:>8.1%} {max(cpu or [0]):>8.1%} {mean(mem) or 0:>8.1%} {mean(bw) or 0:>8.1%} "
              f"{mean(su) or 0:>9.1%} {fair if fair is not None else '-':>8} {jobs:>6}  {ss.get('worker_pids', [])}")

    print("\n=== LINK UTILISATION ===")
    for k, l in status["links"].items():
        u = [x["links"][k] for x in ts]
        print(f"{k:<16} {l['capacity_mbps']:>6.0f} Mb/s {l['latency_ms']:>4.0f} ms   mean {mean(u) or 0:.1%}  peak {max(u or [0]):.1%}")

    print("\n=== ALLOCATION ENFORCEMENT (instances that ran >= 3 s) ===")
    print(f"{'sid':<7} {'service':<11} {'site':<7} {'cpu':>6} {'jobs/s':>7} {'jobs/s per core':>15}")
    rows = []
    for sid, a in status.get("allocations", {}).items():
        sm = a.get("summary")
        if sm is None:
            for i in site_status.get(a["site"], {}).get("instances", []):
                if i["sid"] == sid:
                    sm = i
        if sm and sm["jobs"] and a.get("t_start") and sm["jobs_per_s"] and sm["jobs"] / max(sm["jobs_per_s"], 1e-6) >= 3:
            rows.append((sid, a["service"], a["site"], sm["cpu"], sm["jobs_per_s"], sm["jobs_per_s"] / (sm["cpu"] / 1000)))
    for r in sorted(rows, key=lambda r: (r[2], -r[3]))[:15]:
        print(f"{r[0]:<7} {r[1]:<11} {r[2]:<7} {r[3]:>6.0f} {r[4]:>7.2f} {r[5]:>15.2f}")
    if len(rows) > 15:
        print(f"... {len(rows) - 15} more in summary.json")
    print(f"\nmanager messages: sent={status['comm']['msgs_sent']} recv={status['comm']['msgs_recv']} "
          f"bytes_recv={status['comm']['bytes_recv']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=os.path.join(ROOT, "config.json"))
    ap.add_argument("--policy")
    ap.add_argument("--rate", type=float, help="override arrival rate (requests/s)")
    ap.add_argument("--window", type=float, help="override arrival window (s)")
    ap.add_argument("--out")
    ap.add_argument("--no-launch", action="store_true")
    ap.add_argument("--seed", type=int, help="override the random seed")
    a = ap.parse_args()
    cfg = json.load(open(a.config))
    if a.seed is not None:
        cfg["seed"] = a.seed
    if a.rate:
        cfg["workload"]["arrival_rate_per_s"] = a.rate
    if a.window:
        cfg["workload"]["arrival_window_s"] = a.window
    policy = a.policy or cfg["manager"]["placement_policy"]
    out = a.out or os.path.join(ROOT, "results", f"{cfg['experiment']}_{policy}_{time.strftime('%Y%m%d-%H%M%S')}")
    os.makedirs(out, exist_ok=True)
    json.dump({**cfg, "effective_policy": policy, "python": sys.version, "platform": sys.platform},
              open(os.path.join(out, "config_used.json"), "w"), indent=2)

    procs = {} if a.no_launch else launch(cfg, os.path.abspath(a.config), policy, out)
    try:
        results, status, site_status = asyncio.run(experiment(cfg, policy))
        report(results, status, site_status, cfg)
        json.dump({"requests": results, "manager": status, "sites": site_status},
                  open(os.path.join(out, "summary.json"), "w"), indent=2, default=str)
        print(f"\nResults saved to {out}")
    finally:
        if not a.no_launch:
            async def stop_all():
                for s in cfg["sites"]:
                    try:
                        await call(s["host"], s["port"], {"type": "SHUTDOWN"}, timeout=1)
                    except Exception:
                        pass
                try:
                    await call(cfg["manager"]["host"], cfg["manager"]["port"], {"type": "SHUTDOWN"}, timeout=1)
                except Exception:
                    pass
            asyncio.run(stop_all())
            for p in procs.values():
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.terminate()


if __name__ == "__main__":
    main()

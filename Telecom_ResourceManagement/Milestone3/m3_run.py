"""Milestone 3 experiment driver.

  python m3_run.py traffic   ...   one or more traffic runs (S2 monolith or S3 microservices)
  python m3_run.py scalebench ...  control-plane scalability: registry + orchestrator vs number of sites

Traffic runs
  --arch s2       S2 monolith: manager + sites; generator sends tasks straight to the sites
  --arch s3       S3 microservices: registry, orchestrator, telemetry, edge gateways, sites;
                  generator only talks to the gateway of the edge each instance belongs to
  --deploy local     this script starts the services as local processes and measures them
  --deploy external  services are already running (e.g. Docker Compose); measure via telemetry

Every run writes one row to <out>/runs.csv and every task to <out>/<run>/tasks.csv.
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

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from telerm.monitor import ResourceMonitor               # noqa: E402
from telerm.protocol import STATS, Channel, call, mono, now  # noqa: E402

TIERS = ("edge", "core", "cloud")


# ----------------------------------------------------------------------------- helpers
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


def rnd(x, d=2):
    return None if x is None else round(x, d)


def addr(a):
    h, p = a.rsplit(":", 1)
    return h, int(p)


def instance_specs(cfg, preset):
    perf = cfg["perf"]
    if preset == "default":
        return [{**i, "work_kb": perf["task_work_kb"][i["service"]]} for i in perf["instances"]]
    if preset == "scaleout":                  # 2 transcoders per site, alternating ingress
        n = len(cfg["sites"])
        return [{"service": "Transcoder", "ingress": ("edge-A", "edge-B")[k % 2],
                 "work_kb": perf["task_work_kb"]["Transcoder"]} for k in range(2 * n)]
    raise ValueError(preset)


# ----------------------------------------------------------------------------- deployment
class Deployment:
    """Starts (local) or connects to (external) one TeleRM deployment."""

    def __init__(self, a, cfg, policy, run_dir):
        self.a, self.cfg, self.policy, self.run_dir = a, cfg, policy, run_dir
        self.procs, self.monitors, self.samples = {}, {}, {}
        self.s3 = a.arch == "s3"
        self.registry = a.registry or f"127.0.0.1:{cfg['s3']['registry']['port']}"

    async def start(self):
        cfg, a = self.cfg, self.a
        if a.deploy == "local":
            cfg_path = os.path.join(self.run_dir, "config_run.json")
            json.dump(cfg, open(cfg_path, "w"), indent=1)
            if self.s3:
                from telerm.deploy import launch_local
                self.procs = launch_local(cfg, self.run_dir, self.policy, delay_mode=a.delay_mode,
                                          topology=cfg_path)
            else:
                from run_demo import launch
                self.procs = launch(cfg, cfg_path, self.policy, self.run_dir)
        await self.wait_ready()
        if a.deploy == "local":
            for n, p in self.procs.items():
                self.monitors[n] = ResourceMonitor(p.pid)
                self.samples[n] = []
        self.monitors["driver"] = ResourceMonitor(include_children=False)
        self.samples["driver"] = []

    async def wait_ready(self):
        cfg = self.cfg
        n_sites = len(cfg["sites"])
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                if self.s3:
                    st = (await call(*addr(self.registry), {"type": "STATUS"}))["status"]
                    kinds = [v["kind"] for v in st["nodes"].values()]
                    if kinds.count("site") >= n_sites and kinds.count("gateway") >= len(cfg["s3"]["gateways"]) \
                            and "orchestrator" in st["nodes"] and "telemetry" in st["nodes"]:
                        self.gw = {}
                        for g in cfg["s3"]["gateways"]:
                            r = await call(*addr(self.registry), {"type": "LOOKUP", "name": g["name"]})
                            self.gw[g["location"]] = r["address"]
                        r = await call(*addr(self.registry), {"type": "LOOKUP", "name": "telemetry"})
                        self.telemetry = r["address"]
                        r = await call(*addr(self.registry), {"type": "LOOKUP", "name": "orchestrator"})
                        self.orch = r["address"]
                        return
                else:
                    m = cfg["manager"]
                    st = (await call(m["host"], m["port"], {"type": "STATUS"}))["status"]
                    if len(st["sites"]) >= n_sites:
                        self.orch = f"{m['host']}:{m['port']}"
                        return
            except Exception:
                pass
            await asyncio.sleep(0.5)
        raise RuntimeError("deployment did not become ready - check the logs")

    async def sampler(self, stop):
        while not stop.is_set():
            await asyncio.sleep(1.0)
            for n, m in self.monitors.items():
                s = m.sample()
                s["wall"] = now()
                self.samples[n].append(s)

    async def request(self, req):
        """Admission request: S2 -> manager directly; S3 -> gateway of the ingress edge."""
        target = self.gw[req["ingress"]] if self.s3 else self.orch
        t = mono()
        r = await call(*addr(target), {"type": "REQUEST", "request": req}, timeout=15)
        r["request_ms"] = (mono() - t) * 1000
        return r

    def task_target(self, inst):
        return self.gw[inst["ingress"]] if self.s3 else inst["address"]

    async def node_resources(self, ws, we):
        """Per-node mean CPU % and RSS MB in the window [ws, we] (wall clock)."""
        out = {}
        for n, ss in self.samples.items():
            w = [s for s in ss if ws <= s["wall"] <= we + 0.5]
            out[n] = (mean([s["cpu_pct"] for s in w]), mean([s["rss_mb"] for s in w]))
        if self.a.deploy == "external" and self.s3:
            r = await call(*addr(self.telemetry), {"type": "QUERY", "since": ws}, timeout=10)
            for n, ss in r["series"].items():
                w = [s for s in ss if ws <= s["wall"] <= we + 0.5]
                out[n] = (mean([s.get("cpu_pct") for s in w]), mean([s.get("rss_mb") for s in w]))
        return out

    async def control_messages(self):
        """Messages received by each control-plane node so far (for message-rate analysis)."""
        out = {}
        try:
            if self.s3:
                st = (await call(*addr(self.registry), {"type": "STATUS"}))["status"]
                out["registry"] = st["comm"]["msgs_recv"]
                out["orchestrator"] = (await call(*addr(self.orch), {"type": "STATUS"}))["status"]["comm"]["msgs_recv"]
                out["telemetry"] = (await call(*addr(self.telemetry), {"type": "STATUS"}))["status"]["comm"]["msgs_recv"]
            else:
                out["manager"] = (await call(*addr(self.orch), {"type": "STATUS"}))["status"]["comm"]["msgs_recv"]
        except Exception:
            pass
        return out

    async def links(self):
        try:
            st = (await call(*addr(self.orch), {"type": "STATUS"}))["status"]
            return st["links"]
        except Exception:
            return {}

    async def stop(self):
        if self.a.deploy != "local":
            return
        names = list(self.procs)
        for n in names:
            port = None
            if self.s3:
                s3 = self.cfg["s3"]
                ports = {"registry": s3["registry"]["port"], "orchestrator": s3["orchestrator"]["port"],
                         "telemetry": s3["telemetry"]["port"], **{g["name"]: g["port"] for g in s3["gateways"]}}
                port = ports.get(n)
            if port is None:
                port = {s["id"]: s["port"] for s in self.cfg["sites"]}.get(n, self.cfg["manager"]["port"])
            try:
                await call("127.0.0.1", port, {"type": "SHUTDOWN"}, timeout=1)
            except Exception:
                pass
        for p in self.procs.values():
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


# ----------------------------------------------------------------------------- one traffic run
async def traffic_run(a, cfg, policy, load, rep, out_dir):
    tag = f"{a.arch}_{policy}_{load}_r{rep}" + (f"_{a.tag}" if a.tag else "")
    run_dir = os.path.join(out_dir, tag)
    os.makedirs(run_dir, exist_ok=True)
    dep = Deployment(a, cfg, policy, run_dir)
    stop = asyncio.Event()
    try:
        await dep.start()
        svcs = cfg["services"]
        insts, admit_ms = [], []
        for spec in instance_specs(cfg, a.preset):
            s = svcs[spec["service"]]
            req = {"service": spec["service"], "ingress": spec["ingress"],
                   "demand": {"cpu": s["cpu"], "mem": s["mem"], "bw": s["bw"]},
                   "max_latency_ms": s["max_latency_ms"], "holding_s": a.warmup + a.measure + 120,
                   "work_kb": spec["work_kb"], "backlogged": False, "queue_limit": cfg["perf"]["queue_limit"]}
            r = await dep.request(req)
            admit_ms.append(r["request_ms"])
            if r.get("status") == "ACCEPTED":
                tier = r.get("tier") or {x["id"]: x["tier"] for x in cfg["sites"]}.get(r["site"])
                insts.append({"sid": r["sid"], "service": spec["service"], "ingress": spec["ingress"],
                              "site": r["site"], "tier": tier, "address": r.get("address"),
                              "link_ms": r["latency_ms"], "cpu": s["cpu"], "work_kb": spec["work_kb"]})
            else:
                print(f"    ! {spec['service']}@{spec['ingress']} blocked ({r.get('reason')})")
        links = await dep.links()
        await asyncio.sleep(1.2)
        sampler = asyncio.create_task(dep.sampler(stop))
        msgs0 = await dep.control_messages()
        t0_wall, t0 = now(), mono()
        records = await send_traffic(dep, insts, load, a.warmup, a.measure, a.timeout,
                                     cfg["seed"] * 1000 + load * 10 + rep)
        w0, w1 = t0 + a.warmup, t0 + a.warmup + a.measure
        ws, we = t0_wall + a.warmup, t0_wall + a.warmup + a.measure
        await asyncio.sleep(2.0)
        msgs1 = await dep.control_messages()
        stop.set()
        await sampler
        res = await dep.node_resources(ws, we)
        row = summarise(a, cfg, policy, load, rep, records, insts, w0, w1, res, admit_ms, msgs0, msgs1,
                        (now() - t0_wall), links)
        write_tasks(run_dir, records, t0)
        json.dump({"instances": insts, "links": links, "resources": res}, open(os.path.join(run_dir, "run.json"), "w"),
                  indent=1, default=str)
        return row, insts
    finally:
        stop.set()
        await dep.stop()


async def send_traffic(dep, insts, rate, warmup, measure, timeout, seed):
    rng = random.Random(seed)
    weights = [i["cpu"] / i["work_kb"] for i in insts]
    channels = {}
    for tgt in sorted({dep.task_target(i) for i in insts}):
        ch = Channel(*addr(tgt))
        await ch.connect()
        channels[tgt] = ch
    records, pending, seqs = [], set(), {i["sid"]: 0 for i in insts}
    emulated = dep.s3 and dep.a.delay_mode == "emulate"
    t0 = mono()
    w1 = t0 + warmup + measure

    async def one(inst, seq):
        rec = {"sid": inst["sid"], "service": inst["service"], "site": inst["site"], "tier": inst["tier"],
               "seq": seq, "link_ms": inst["link_ms"], "t_req": mono()}
        try:
            r = await channels[dep.task_target(inst)].request(
                {"type": "TASK", "sid": inst["sid"], "seq": seq, "work_kb": inst["work_kb"],
                 "deadline_ms": dep.a.deadline_ms}, timeout=timeout)
            rec["t_resp"] = mono()
            rec["status"] = r.get("status", "DONE" if r.get("ok") else "FAILED")
            rec["queue_ms"], rec["exec_ms"] = r.get("queue_ms"), r.get("exec_ms")
            rec["upstream_ms"], rec["link_emulated_ms"] = r.get("upstream_ms"), r.get("link_emulated_ms")
        except asyncio.TimeoutError:
            rec["t_resp"], rec["status"] = None, "TIMEOUT"
        except Exception:
            rec["t_resp"], rec["status"] = None, "ERROR"
        if rec["status"] == "DONE":
            rec["measured_ms"] = (rec["t_resp"] - rec["t_req"]) * 1000
            link = rec.get("link_emulated_ms") if emulated else 2 * rec["link_ms"]
            rec["link_total_ms"] = link or 0.0
            rec["e2e_ms"] = rec["measured_ms"] + (0.0 if emulated else rec["link_total_ms"])
            proc = (rec["queue_ms"] or 0) + (rec["exec_ms"] or 0)
            if rec.get("upstream_ms") is not None:        # S3: split communication into two hops
                rec["comm_gw_site_ms"] = max(0.0, rec["upstream_ms"] - proc)
                rec["comm_user_gw_ms"] = max(0.0, rec["measured_ms"] - rec["upstream_ms"]
                                             - (rec["link_total_ms"] if emulated else 0.0))
            rec["comm_ms"] = max(0.0, rec["measured_ms"] - proc - (rec["link_total_ms"] if emulated else 0.0))
        records.append(rec)

    nxt = t0
    while True:
        nxt += rng.expovariate(rate)
        if nxt > w1:
            break
        d = nxt - mono()
        if d > 0:
            await asyncio.sleep(d)
        inst = rng.choices(insts, weights)[0]
        seqs[inst["sid"]] += 1
        t = asyncio.create_task(one(inst, seqs[inst["sid"]]))
        pending.add(t)
        t.add_done_callback(pending.discard)
    if pending:
        await asyncio.wait(pending, timeout=timeout + 1)
    for ch in channels.values():
        await ch.close()
    return records


def summarise(a, cfg, policy, load, rep, records, insts, w0, w1, res, admit_ms, msgs0, msgs1, elapsed, links):
    D = w1 - w0
    win = [r for r in records if w0 <= r["t_req"] < w1]
    done = [r for r in win if r["status"] == "DONE"]
    lost = [r for r in win if r["status"] != "DONE"]
    comp = sum(1 for r in records if r["status"] == "DONE" and w0 <= r["t_resp"] < w1)
    lat = [r["e2e_ms"] for r in done]

    def jitter(rs):
        by = {}
        for r in sorted(rs, key=lambda r: r["t_req"]):
            by.setdefault(r["sid"], []).append(r["e2e_ms"])
        return mean([abs(x - y) for xs in by.values() for x, y in zip(xs[1:], xs[:-1])])

    row = {"arch": a.arch, "deploy": a.deploy, "tag": a.tag or "", "policy": policy, "offered_load": load,
           "rep": rep, "instances": len(insts), "sent": len(win), "completed": len(done),
           "throughput": rnd(comp / D, 1), "loss_pct": rnd(100 * len(lost) / len(win), 2) if win else None,
           "lat_mean_ms": rnd(mean(lat)), "lat_p50_ms": rnd(pct(lat, .5)), "lat_p95_ms": rnd(pct(lat, .95)),
           "lat_p99_ms": rnd(pct(lat, .99)), "jitter_ms": rnd(jitter(done)) if done else None,
           "measured_mean_ms": rnd(mean([r["measured_ms"] for r in done])),
           "queue_ms": rnd(mean([r["queue_ms"] for r in done])), "exec_ms": rnd(mean([r["exec_ms"] for r in done])),
           "comm_ms": rnd(mean([r["comm_ms"] for r in done])),
           "comm_user_gw_ms": rnd(mean([r.get("comm_user_gw_ms") for r in done])),
           "comm_gw_site_ms": rnd(mean([r.get("comm_gw_site_ms") for r in done])),
           "link_ms": rnd(mean([r["link_total_ms"] for r in done])),
           "admit_ms_mean": rnd(mean(admit_ms)), "admit_ms_p95": rnd(pct(admit_ms, .95))}
    for tier in TIERS:
        tw = [r for r in win if r["tier"] == tier]
        td = [r for r in done if r["tier"] == tier]
        row[f"{tier}_instances"] = sum(1 for i in insts if i["tier"] == tier)
        row[f"{tier}_task_share_pct"] = rnd(100 * len(tw) / len(win), 1) if win else None
        row[f"{tier}_lat_mean_ms"] = rnd(mean([r["e2e_ms"] for r in td]))
        row[f"{tier}_loss_pct"] = rnd(100 * sum(1 for r in tw if r["status"] != "DONE") / len(tw), 2) if tw else None
    for svc in cfg["services"]:
        sd = [r for r in done if r["service"] == svc]
        sw = [r for r in win if r["service"] == svc]
        if sw:
            row[f"svc_{svc}_lat_mean_ms"] = rnd(mean([r["e2e_ms"] for r in sd]))
            row[f"svc_{svc}_lat_p95_ms"] = rnd(pct([r["e2e_ms"] for r in sd], .95))
            row[f"svc_{svc}_loss_pct"] = rnd(100 * sum(1 for r in sw if r["status"] != "DONE") / len(sw), 2)
            row[f"svc_{svc}_tiers"] = "+".join(sorted({i["tier"] for i in insts if i["service"] == svc}))
    for n, (c, m) in sorted(res.items()):
        row[f"cpu_{n}"] = rnd(c, 1)
        row[f"rss_{n}"] = rnd(m, 1)
    for n in msgs1:
        if n in msgs0:
            row[f"msgs_per_s_{n}"] = rnd((msgs1[n] - msgs0[n]) / elapsed, 1)
    for k, l in links.items():
        row[f"link_{k}_reserved_mbps"] = l.get("allocated_mbps")
    return row


def write_tasks(run_dir, records, t0):
    keys = ["sid", "service", "site", "tier", "seq", "status", "t_req", "t_resp", "measured_ms", "e2e_ms", "queue_ms",
            "exec_ms", "comm_ms", "comm_user_gw_ms", "comm_gw_site_ms", "link_total_ms"]
    with open(os.path.join(run_dir, "tasks.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in sorted(records, key=lambda r: r["t_req"]):
            w.writerow({**r, "t_req": round(r["t_req"] - t0, 5),
                        "t_resp": round(r["t_resp"] - t0, 5) if r.get("t_resp") else None})


def append_row(out_dir, row):
    path = os.path.join(out_dir, "runs.csv")
    rows = list(csv.DictReader(open(path))) if os.path.exists(path) else []
    rows.append({k: ("" if v is None else v) for k, v in row.items()})
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def cmd_traffic(a):
    cfg = json.load(open(a.config))
    if a.sites:
        keep = set(a.sites.split(","))
        cfg["sites"] = [s for s in cfg["sites"] if s["id"] in keep]
    if a.equal_cpu:
        for s in cfg["sites"]:
            s["cpu"], s["slots"] = a.equal_cpu, a.equal_slots
    os.makedirs(a.out, exist_ok=True)
    try:
        import platform
        import psutil
        env = {"python": sys.version.split()[0], "platform": platform.platform(),
               "cpu_logical": psutil.cpu_count(), "ram_gb": round(psutil.virtual_memory().total / 2**30, 1),
               "psutil": psutil.__version__}
    except ImportError:
        env = {"python": sys.version.split()[0]}
    json.dump({**cfg, "environment": env, "args": vars(a)}, open(os.path.join(a.out, f"config_used_{a.arch}{('_' + a.tag) if a.tag else ''}.json"), "w"), indent=2)
    for rep in range(a.rep_start, a.rep_start + a.reps):
        for policy in a.policies:
            for load in a.loads:
                t = time.time()
                row, insts = asyncio.run(traffic_run(a, cfg, policy, load, rep, a.out))
                append_row(a.out, row)
                tiers = ", ".join(f"{i['service']}@{i['ingress']}->{i['site']}" for i in insts)
                print(f"[{a.arch} {a.tag} {policy} {load}/s r{rep}] thr {row['throughput']}  lat {row['lat_mean_ms']} ms "
                      f"(p95 {row['lat_p95_ms']})  loss {row['loss_pct']}%  admit {row['admit_ms_mean']} ms  "
                      f"[{time.time() - t:.0f}s]", flush=True)
                if a.verbose:
                    print("    placement:", tiers, flush=True)


# ----------------------------------------------------------------------------- control-plane scale bench
async def scalebench_one(n, out_dir, admissions, hb_rate, path_mode="single"):
    """Registry + orchestrator with n (simulated) sites heartbeating at hb_rate Hz."""
    run_dir = os.path.join(out_dir, f"scale_n{n}_{path_mode}")
    os.makedirs(os.path.join(run_dir, "logs"), exist_ok=True)
    reg_port, orch_port, fake_port = 8810, 8800, 8820
    topo = {"links": [{"a": f"s{k:04d}", "b": "core-1", "capacity_mbps": 1000, "latency_ms": 5} for k in range(n)]}
    tpath = os.path.join(run_dir, "topology.json")
    json.dump(topo, open(tpath, "w"))
    procs = {
        "registry": subprocess.Popen([sys.executable, "-m", "telerm.registry", "--port", str(reg_port),
                                      "--hb-timeout", "10"], cwd=ROOT,
                                     stdout=open(os.path.join(run_dir, "logs", "registry.log"), "w"),
                                     stderr=subprocess.STDOUT)}
    await asyncio.sleep(0.6)
    procs["orchestrator"] = subprocess.Popen(
        [sys.executable, "-m", "telerm.manager", "--port", str(orch_port), "--registry", f"127.0.0.1:{reg_port}",
         "--topology", tpath, "--policy", "least_loaded", "--out", run_dir], cwd=ROOT,
        env={**os.environ, "TELERM_PATH_MODE": path_mode},
        stdout=open(os.path.join(run_dir, "logs", "orchestrator.log"), "w"), stderr=subprocess.STDOUT)

    async def fake_site(msg):                       # one tiny server answers for every simulated site
        return {"ok": True}
    from telerm.protocol import make_server_callback
    server = await asyncio.start_server(make_server_callback(fake_site), "127.0.0.1", fake_port)
    for k in range(n):
        await call("127.0.0.1", reg_port, {"type": "REGISTER", "name": f"s{k:04d}", "kind": "site", "host": "127.0.0.1",
                                           "port": fake_port, "tier": "edge", "location": f"s{k:04d}",
                                           "capacity": {"cpu": 4000, "mem": 4096, "bw": 2000}})
    for _ in range(100):
        try:
            if (await call("127.0.0.1", reg_port, {"type": "LOOKUP", "name": "orchestrator"})).get("ok"):
                break
        except Exception:
            pass
        await asyncio.sleep(0.2)
    reg_mon = ResourceMonitor(procs["registry"].pid)
    orch_mon = ResourceMonitor(procs["orchestrator"].pid)
    hb_lat, stop = [], asyncio.Event()

    async def heartbeats(k):                        # each simulated site: real per-message connections
        await asyncio.sleep(random.random() / hb_rate)
        while not stop.is_set():
            t = mono()
            try:
                await call("127.0.0.1", reg_port, {"type": "HEARTBEAT", "name": f"s{k:04d}"}, timeout=5)
                hb_lat.append((mono() - t) * 1000)
            except Exception:
                hb_lat.append(None)
            await asyncio.sleep(max(0.0, 1 / hb_rate - (mono() - t)))
    hb_tasks = [asyncio.create_task(heartbeats(k)) for k in range(n)]
    await asyncio.sleep(3)                          # warm-up
    hb_lat.clear()
    reg_mon.sample(); orch_mon.sample()
    t_meas = mono()
    adm = []
    list_ms, list_bytes = [], []
    for j in range(admissions):
        t = mono()
        r = await call("127.0.0.1", orch_port, {"type": "REQUEST", "request": {
            "service": "IoT-GW", "ingress": "core-1", "demand": {"cpu": 10, "mem": 8, "bw": 1},
            "max_latency_ms": 100, "holding_s": 600}}, timeout=30)
        adm.append(((mono() - t) * 1000, r.get("registry_ms"), r.get("decision_ms")))
        t = mono()
        b0 = STATS["bytes_recv"]
        await call("127.0.0.1", reg_port, {"type": "LIST_SITES"}, timeout=30)
        list_ms.append((mono() - t) * 1000)
        list_bytes.append(STATS["bytes_recv"] - b0)
        await asyncio.sleep(0.25)
    while mono() - t_meas < 6:
        await asyncio.sleep(0.2)
    reg_cpu, orch_cpu = reg_mon.sample()["cpu_pct"], orch_mon.sample()["cpu_pct"]
    period = mono() - t_meas
    stop.set()
    await asyncio.gather(*hb_tasks, return_exceptions=True)
    st = (await call("127.0.0.1", reg_port, {"type": "STATUS"}))["status"]
    hb_ok = [x for x in hb_lat if x is not None]
    row = {"sites": n, "path_mode": path_mode, "heartbeat_hz": hb_rate, "heartbeats_per_s": rnd(len(hb_lat) / period, 1),
           "heartbeat_failures": sum(1 for x in hb_lat if x is None),
           "hb_rtt_p50_ms": rnd(pct(hb_ok, .5), 3), "hb_rtt_p95_ms": rnd(pct(hb_ok, .95), 3),
           "registry_cpu_pct": rnd(reg_cpu, 1), "orchestrator_cpu_pct": rnd(orch_cpu, 1),
           "registry_handle_hb_ms": st["handle_ms_mean"].get("HEARTBEAT"),
           "admission_ms_mean": rnd(mean([x[0] for x in adm]), 3),
           "admission_ms_p95": rnd(pct([x[0] for x in adm], .95), 3),
           "admission_registry_ms": rnd(mean([x[1] for x in adm]), 3),
           "admission_decision_ms": rnd(mean([x[2] for x in adm]), 3),
           "list_sites_ms": rnd(mean(list_ms), 3), "list_sites_kb": rnd(mean(list_bytes) / 1024, 1)}
    for name, port in (("orchestrator", orch_port), ("registry", reg_port)):
        try:
            await call("127.0.0.1", port, {"type": "SHUTDOWN"}, timeout=2)
        except Exception:
            pass
    server.close()
    for p in procs.values():
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()
    return row


def cmd_scalebench(a):
    os.makedirs(a.out, exist_ok=True)
    for rep in range(1, a.reps + 1):
        for mode in a.path_modes:
          for n in a.sizes:
            row = asyncio.run(scalebench_one(n, a.out, a.admissions, a.hb_rate, mode))
            row["rep"] = rep
            append_row(a.out, row)
            print(f"[{mode} N={n} r{rep}] hb/s {row['heartbeats_per_s']}  registry CPU {row['registry_cpu_pct']}%  "
                  f"hb p95 {row['hb_rtt_p95_ms']} ms  admission {row['admission_ms_mean']} ms "
                  f"(registry {row['admission_registry_ms']}, decision {row['admission_decision_ms']})  "
                  f"LIST_SITES {row['list_sites_kb']} KB", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("traffic")
    t.add_argument("--config", default=os.path.join(ROOT, "config.json"))
    t.add_argument("--arch", choices=["s2", "s3"], default="s3")
    t.add_argument("--deploy", choices=["local", "external"], default="local")
    t.add_argument("--registry", default=None, help="external S3: registry address, e.g. registry:8010")
    t.add_argument("--delay-mode", choices=["emulate", "none"], default="none",
                   help="S3: 'emulate' if the gateways inject link delay; 'none' = add the link delay as a model")
    t.add_argument("--policies", nargs="+", default=["tier_aware"])
    t.add_argument("--loads", nargs="+", type=int, default=[200])
    t.add_argument("--reps", type=int, default=1)
    t.add_argument("--rep-start", type=int, default=1)
    t.add_argument("--warmup", type=float, default=3)
    t.add_argument("--measure", type=float, default=15)
    t.add_argument("--timeout", type=float, default=1.5)
    t.add_argument("--deadline-ms", type=float, default=1000.0,
                   help="sites discard a task that waited longer than this (0 = never)")
    t.add_argument("--preset", choices=["default", "scaleout"], default="default")
    t.add_argument("--sites", default=None, help="comma-separated subset of sites to use")
    t.add_argument("--equal-cpu", type=float, default=None, help="give every site this many millicores")
    t.add_argument("--equal-slots", type=int, default=2)
    t.add_argument("--tag", default="")
    t.add_argument("--out", default=os.path.join(ROOT, "results", "M3"))
    t.add_argument("--verbose", action="store_true")
    s = sub.add_parser("scalebench")
    s.add_argument("--sizes", nargs="+", type=int, default=[4, 16, 64, 256, 1024])
    s.add_argument("--reps", type=int, default=1)
    s.add_argument("--admissions", type=int, default=12)
    s.add_argument("--hb-rate", type=float, default=1.0)
    s.add_argument("--path-modes", nargs="+", default=["per_site", "single"])
    s.add_argument("--out", default=os.path.join(ROOT, "results", "M3-scalebench"))
    a = ap.parse_args()
    if a.cmd == "traffic":
        cmd_traffic(a)
    else:
        cmd_scalebench(a)


if __name__ == "__main__":
    main()

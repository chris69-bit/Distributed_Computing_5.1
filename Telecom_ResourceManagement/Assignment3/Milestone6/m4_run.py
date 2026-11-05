"""Milestone 4 experiments: coordination with Raft and Lamport clocks.

  python m4_run.py overhead     E1 coordination overhead + message complexity vs cluster size
  python m4_run.py placement    E2 synchronization delay vs where replicas are placed
  python m4_run.py failover     E3 leader crash: election time and unavailability
  python m4_run.py consistency  E4 uncoordinated orchestrators vs Raft cluster (with crashes)
  python m4_run.py clocks       E5 event ordering: skewed wall clocks vs Lamport clocks
  python m4_run.py smoke        quick end-to-end check (3 replicas, one leader crash)

Everything runs as local processes: a registry, N orchestrator replicas (python -m
telerm.manager --raft-id ...), and light "fake sites" (this file's `fakesite` subcommand) that
accept ALLOCATE, record what they host, and send INSTANCE_EXIT when an instance's holding time
ends. Fake sites keep the experiments about coordination, not about task processing.
"""
import argparse
import asyncio
import csv
import json
import os
import random
import shutil
import signal
import subprocess
import sys
import time
import uuid

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from telerm.monitor import ResourceMonitor  # noqa: E402
from telerm.naming import RegistryClient  # noqa: E402
from telerm.protocol import STATS, call, log_event, make_server_callback, mono, now  # noqa: E402

PY = sys.executable
REG_PORT, ORCH_BASE, SITE_BASE = 8910, 8900, 8920


def pct(xs, p):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    k = (len(xs) - 1) * p
    f = int(k)
    return xs[f] + (xs[min(f + 1, len(xs) - 1)] - xs[f]) * (k - f)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def rnd(x, d=2):
    return None if x is None else round(x, d)


def append_row(path, row):
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if new:
            w.writeheader()
        w.writerow(row)


# ============================================================================ fake sites
async def fakesite_main(a):
    """One process serving several simulated sites (one TCP port each)."""
    reg = RegistryClient(f"127.0.0.1:{a.registry_port}")
    hosted = {}                                   # port -> {sid: demand}
    counters = {"allocate": 0, "allocate_retry": 0, "sid_collision": 0, "exits_sent": 0, "exit_failures": 0}

    async def exit_later(sid, holding):
        await asyncio.sleep(holding)
        log_event("exit_send", sid=sid)
        try:
            r = await reg.call_service("orchestrator", {"type": "INSTANCE_EXIT", "sid": sid}, timeout=2.0,
                                       retry_s=15.0)
            counters["exits_sent"] += 1
            if not r.get("ok"):
                counters["exit_failures"] += 1
        except Exception:
            counters["exit_failures"] += 1

    def make_handler(port):
        hosted[port] = {}

        async def handler(msg):
            t = msg.get("type")
            if t == "ALLOCATE":
                inst = msg["instance"]
                log_event("allocate_recv", sid=inst["sid"], site_port=port)
                counters["allocate"] += 1
                key = f'{inst["sid"]}|{inst.get("req_id")}'
                if key in hosted[port]:                   # same instance, same request: a retry
                    counters["allocate_retry"] += 1
                    return {"ok": True, "duplicate": True}
                if any(k.split("|")[0] == inst["sid"] for k in hosted[port]):
                    counters["sid_collision"] += 1        # a DIFFERENT instance with the same id
                hosted[port][key] = inst["demand"]
                if float(inst.get("holding_s", 1e9)) < 1e5:
                    asyncio.create_task(exit_later(inst["sid"], float(inst["holding_s"])))
                return {"ok": True}
            if t == "STATUS":
                return {"ok": True, "hosted": {str(p): h for p, h in hosted.items()}, "counters": counters}
            if t == "SHUTDOWN":
                stop.set()
                return {"ok": True}
            return {"ok": True}
        return handler

    stop = asyncio.Event()
    servers = []
    for port in a.ports:
        servers.append(await asyncio.start_server(make_server_callback(make_handler(port)), "127.0.0.1", port))
    await stop.wait()
    for s in servers:
        s.close()


# ============================================================================ cluster harness
class Cluster:
    def __init__(self, run_dir, n, election=None, hb_ms=50, delays=None, fsync=False, n_sites=6,
                 site_cpu=4000, skew_ms=0.0, eventlog=False, raft=True, extra_orch=0, commit_timeout=3.0,
                 seed=1):
        self.run_dir, self.n, self.hb_ms, self.fsync, self.raft = run_dir, n, hb_ms, fsync, raft
        self.ids = [f"o{i + 1}" for i in range(n)]
        self.election = election or {i: "300,600" for i in self.ids}
        if isinstance(self.election, str):
            self.election = {i: self.election for i in self.ids}
        self.delays = delays or {}                  # id -> {peer: one-way ms}
        self.n_sites, self.site_cpu = n_sites, site_cpu
        self.skew_ms, self.eventlog = skew_ms, eventlog
        self.extra_orch, self.commit_timeout = extra_orch, commit_timeout
        self.rng = random.Random(seed)
        self.procs: dict[str, subprocess.Popen] = {}
        self.skews: dict[str, float] = {}
        if os.path.exists(run_dir):
            shutil.rmtree(run_dir)
        os.makedirs(os.path.join(run_dir, "logs"))
        self.site_ports = [SITE_BASE + k for k in range(n_sites)]
        self.site_names = [f"site{k}" for k in range(n_sites)]

    def _env(self, name):
        env = {**os.environ, "TELERM_NODE": name}
        sk = self.rng.uniform(-self.skew_ms, self.skew_ms) if self.skew_ms else 0.0
        self.skews[name] = sk
        env["TELERM_CLOCK_SKEW_MS"] = str(sk)
        if self.eventlog:
            env["TELERM_EVENTLOG"] = os.path.join(self.run_dir, f"events-{name}.jsonl")
        return env

    def _spawn(self, name, args):
        self.procs[name] = subprocess.Popen(args, cwd=ROOT, env=self._env(name) if name not in self.skews else
                                            {**self._env_keep(name)},
                                            stdout=open(os.path.join(self.run_dir, "logs", f"{name}.log"), "a"),
                                            stderr=subprocess.STDOUT)

    def _env_keep(self, name):                     # restart keeps the same clock skew
        env = {**os.environ, "TELERM_NODE": name, "TELERM_CLOCK_SKEW_MS": str(self.skews[name])}
        if self.eventlog:
            env["TELERM_EVENTLOG"] = os.path.join(self.run_dir, f"events-{name}.jsonl")
        return env

    def orch_args(self, rid):
        i = self.ids.index(rid)
        peers = ",".join(f"{x}=127.0.0.1:{ORCH_BASE + k}" for k, x in enumerate(self.ids))
        args = [PY, "-m", "telerm.manager", "--port", str(ORCH_BASE + i), "--registry", f"127.0.0.1:{REG_PORT}",
                "--topology", self.topo_path, "--policy", "least_loaded",
                "--out", os.path.join(self.run_dir, rid)]
        if self.raft:
            args += ["--raft-id", rid, "--raft-peers", peers, "--election-ms", self.election[rid],
                     "--heartbeat-ms", str(self.hb_ms), "--commit-timeout", str(self.commit_timeout)]
            d = self.delays.get(rid)
            if d:
                args += ["--peer-delay-ms", ",".join(f"{p}={v}" for p, v in d.items())]
            if self.fsync:
                args.append("--fsync")
        return args

    async def start(self):
        topo = {"links": [{"a": s, "b": "core-1", "capacity_mbps": 1e6, "latency_ms": 1} for s in self.site_names]}
        self.topo_path = os.path.join(self.run_dir, "topology.json")
        json.dump(topo, open(self.topo_path, "w"))
        self._spawn("registry", [PY, "-m", "telerm.registry", "--port", str(REG_PORT), "--hb-timeout", "100000"])
        await self.wait_port(REG_PORT)
        self._spawn("sites", [PY, os.path.join(ROOT, "m4_run.py"), "fakesite", "--registry-port", str(REG_PORT),
                              "--ports", *map(str, self.site_ports)])
        for p in self.site_ports:
            await self.wait_port(p)
        for name, p in zip(self.site_names, self.site_ports):
            await call("127.0.0.1", REG_PORT, {"type": "REGISTER", "name": name, "kind": "site", "host": "127.0.0.1",
                                               "port": p, "tier": "core", "location": name,
                                               "capacity": {"cpu": self.site_cpu, "mem": 1e6, "bw": 1e6}})
        if self.raft:
            for rid in self.ids:
                self._spawn(rid, self.orch_args(rid))
        else:                                     # baseline: independent S3 orchestrators, no coordination
            self.ids = [f"u{i + 1}" for i in range(self.n)]
            for i, rid in enumerate(self.ids):
                args = [PY, "-m", "telerm.manager", "--port", str(ORCH_BASE + i), "--registry",
                        f"127.0.0.1:{REG_PORT}", "--topology", self.topo_path, "--policy", "least_loaded",
                        "--out", os.path.join(self.run_dir, rid)]
                self._spawn(rid, args)
        self.reg = RegistryClient(f"127.0.0.1:{REG_PORT}")
        if self.raft:
            await self.reg.lookup("orchestrator", wait_s=30)
        else:
            for i in range(self.n):
                await self.wait_port(ORCH_BASE + i)
        self.monitors = {k: ResourceMonitor(p.pid) for k, p in self.procs.items()}

    async def wait_port(self, port, t=20):
        end = mono() + t
        while mono() < end:
            try:
                r, w = await asyncio.open_connection("127.0.0.1", port)
                w.close()
                return
            except OSError:
                await asyncio.sleep(0.05)
        raise RuntimeError(f"port {port} did not open")

    async def status(self, rid, timeout=1.0):
        i = self.ids.index(rid)
        try:
            return (await call("127.0.0.1", ORCH_BASE + i, {"type": "STATUS"}, timeout=timeout))["status"]
        except Exception:
            return None

    async def statuses(self):
        return {rid: await self.status(rid) for rid in self.ids if self.procs[rid].poll() is None}

    async def leader(self):
        for rid, st in (await self.statuses()).items():
            if st and st.get("raft", {}).get("ready"):
                return rid, st
        return None, None

    async def wait_leader(self, t=20, exclude=None):
        end = mono() + t
        while mono() < end:
            rid, st = await self.leader()
            if rid and rid != exclude:
                return rid, st
            await asyncio.sleep(0.02)
        raise RuntimeError("no leader")

    def kill(self, rid):
        p = self.procs[rid]
        p.send_signal(signal.SIGKILL)
        p.wait()

    def restart(self, rid):
        self._spawn(rid, self.orch_args(rid))
        self.monitors[rid] = ResourceMonitor(self.procs[rid].pid)

    async def site_status(self):
        return (await call("127.0.0.1", self.site_ports[0], {"type": "STATUS"}, timeout=5))

    async def stop(self):
        for i, rid in enumerate(self.ids):
            if self.procs[rid].poll() is None:
                try:
                    await call("127.0.0.1", ORCH_BASE + i, {"type": "SHUTDOWN"}, timeout=1)
                except Exception:
                    pass
        for port, name in ((self.site_ports[0], "sites"), (REG_PORT, "registry")):
            try:
                await call("127.0.0.1", port, {"type": "SHUTDOWN"}, timeout=1)
            except Exception:
                pass
        for p in self.procs.values():
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()


def request_msg(req_id, cpu=50, holding=0.5, service="IoT-GW"):
    return {"type": "REQUEST", "request": {"req_id": req_id, "service": service, "ingress": "core-1",
                                           "demand": {"cpu": cpu, "mem": 1, "bw": 1}, "max_latency_ms": 100,
                                           "holding_s": holding}}


# ============================================================================ smoke
async def smoke(a):
    cl = Cluster(os.path.join(a.out, "smoke"), 3, eventlog=True)
    await cl.start()
    try:
        lid, st = await cl.wait_leader()
        print("leader", lid, "term", st["raft"]["term"])
        for k in range(5):
            r = await cl.reg.call_service("orchestrator", request_msg(f"r{k}", holding=1.0), retry_s=5)
            print(" admit", r.get("status"), r.get("sid"), "commit_ms", r.get("commit_ms"), "leader", r.get("leader"))
        cl.kill(lid)
        t = mono()
        r = await cl.reg.call_service("orchestrator", request_msg("after-kill", holding=1.0), timeout=1.0, retry_s=10)
        print(" after kill:", r.get("status"), r.get("sid"), "leader", r.get("leader"), "term", r.get("term"),
              f"{(mono() - t) * 1000:.0f} ms")
        cl.restart(lid)
        await asyncio.sleep(3)
        for rid, st in (await cl.statuses()).items():
            print(" ", rid, st["raft"]["role"], "term", st["raft"]["term"], "commit", st["raft"]["commit_index"],
                  "hash", st["state_hash"], "active", st["active"], "loaded", st["raft"]["loaded_entries"])
    finally:
        await cl.stop()


# ============================================================================ E1 overhead
async def overhead_one(a, n, rep, raft=True, fsync=False, clients=1):
    tag = f"{'raft' if raft else 's3'}{'-fsync' if fsync else ''}_n{n}_c{clients}_r{rep}"
    cl = Cluster(os.path.join(a.out, "overhead", tag), n, raft=raft, fsync=fsync, election="300,600")
    await cl.start()
    try:
        if raft:
            lid, _ = await cl.wait_leader()
        else:
            lid = cl.ids[0]
        await asyncio.sleep(1.0)
        # idle: heartbeat traffic only
        st0 = await cl.statuses()
        for m in cl.monitors.values():
            m.sample()
        t0 = mono()
        await asyncio.sleep(a.idle)
        st1 = await cl.statuses()
        idle_s = mono() - t0
        idle_msgs = sum(st1[r]["comm"]["msgs_sent"] - st0[r]["comm"]["msgs_sent"] for r in st1) / idle_s
        idle_cpu = {r: cl.monitors[r].sample()["cpu_pct"] for r in cl.ids}
        # load: closed loop clients
        lat, commit, orch = [], [], []
        k_counter = [0]

        async def client(c):
            while True:
                k_counter[0] += 1
                k = k_counter[0]
                if k > a.admissions:
                    return
                t = mono()
                r = await cl.reg.call_service("orchestrator", request_msg(f"c{c}-{k}", holding=a.holding),
                                              retry_s=5)
                if r.get("ok"):
                    lat.append((mono() - t) * 1000)
                    commit.append(r.get("commit_ms"))
                    orch.append(r.get("orchestrator_ms"))
        st0 = await cl.statuses()
        for m in cl.monitors.values():
            m.sample()
        t0 = mono()
        await asyncio.gather(*[client(c) for c in range(clients)])
        load_s = mono() - t0
        await asyncio.sleep(a.holding + 0.5)            # let the releases go through the log too
        dur = mono() - t0
        cpu = {r: cl.monitors[r].sample()["cpu_pct"] for r in cl.ids}
        st1 = await cl.statuses()
        done = len(lat)
        msgs = sum(st1[r]["comm"]["msgs_sent"] - st0[r]["comm"]["msgs_sent"] for r in st1) - idle_msgs * dur
        bytes_ = sum(st1[r]["comm"]["bytes_sent"] - st0[r]["comm"]["bytes_sent"] for r in st1)
        row = {"mode": "S3 single orchestrator" if not raft else ("Raft+fsync" if fsync else "Raft"),
               "replicas": n, "clients": clients, "rep": rep, "admissions": done,
               "throughput_adm_s": rnd(done / load_s, 1),
               "admit_ms_mean": rnd(mean(lat), 3), "admit_ms_p50": rnd(pct(lat, .5), 3),
               "admit_ms_p95": rnd(pct(lat, .95), 3), "commit_ms_mean": rnd(mean(commit), 3),
               "commit_ms_p95": rnd(pct(commit, .95), 3), "orchestrator_ms_mean": rnd(mean(orch), 3),
               "idle_msgs_per_s": rnd(idle_msgs, 1),
               "msgs_per_admission": rnd(msgs / done if done else None, 2),
               "kb_per_admission": rnd(bytes_ / done / 1024 if done else None, 2),
               "leader_cpu_pct": rnd(cpu[lid], 1),
               "follower_cpu_pct": rnd(mean([cpu[r] for r in cl.ids if r != lid]), 1),
               "idle_leader_cpu_pct": rnd(idle_cpu[lid], 1),
               "idle_follower_cpu_pct": rnd(mean([idle_cpu[r] for r in cl.ids if r != lid]), 1)}
        if raft:
            rs = st1[lid]["raft"]["counters"]
            row["leader_ae_sent"] = rs.get("sent_RAFT_AE", 0)
            row["entries_committed"] = st1[lid]["raft"]["commit_index"]
            row["state_hashes_equal"] = len({st1[r]["state_hash"] for r in st1}) == 1
        return row
    finally:
        await cl.stop()


async def overhead(a):
    out = os.path.join(a.out, "overhead.csv")
    for rep in range(1, a.reps + 1):
        for clients in a.clients:
            configs = [(1, False, False)] + [(n, True, False) for n in a.sizes] + [(3, True, True)]
            for n, raft, fsync in configs:
                row = await overhead_one(a, n, rep, raft, fsync, clients)
                append_row(out, row)
                print(f"[{row['mode']} N={n} C={clients} r{rep}] admit {row['admit_ms_mean']} ms "
                      f"(commit {row['commit_ms_mean']})  {row['throughput_adm_s']} adm/s  "
                      f"msgs/adm {row['msgs_per_admission']}  idle msgs/s {row['idle_msgs_per_s']}  "
                      f"leader CPU {row['leader_cpu_pct']}%", flush=True)


# ============================================================================ E2 placement
PLACEMENTS = {
    # name: (ids->location, one-way delay matrix between locations, preferred leader)
    "3 in core": (["core", "core", "core"], "core"),
    "core + edge-A + cloud": (["core", "edge-A", "cloud"], "core"),
    "core + edge-A + cloud (leader in cloud)": (["core", "edge-A", "cloud"], "cloud"),
    "core + 2 x cloud": (["core", "cloud", "cloud-2"], "core"),
    "5: core, 2 edges, 2 clouds": (["core", "edge-A", "edge-B", "cloud", "cloud-2"], "core"),
}
LOC_DELAY = {("core", "core"): 0, ("core", "edge-A"): 5, ("core", "edge-B"): 6, ("core", "cloud"): 20,
             ("core", "cloud-2"): 20, ("edge-A", "edge-B"): 3, ("edge-A", "cloud"): 25, ("edge-B", "cloud"): 26,
             ("edge-A", "cloud-2"): 25, ("edge-B", "cloud-2"): 26, ("cloud", "cloud-2"): 2}


def loc_delay(x, y):
    if x == y:
        return 0
    return LOC_DELAY.get((x, y), LOC_DELAY.get((y, x)))


async def placement(a):
    out = os.path.join(a.out, "placement.csv")
    for rep in range(1, a.reps + 1):
        for name, (locs, lead) in PLACEMENTS.items():
            n = len(locs)
            ids = [f"o{i + 1}" for i in range(n)]
            delays = {ids[i]: {ids[j]: loc_delay(locs[i], locs[j]) for j in range(n) if j != i} for i in range(n)}
            li = locs.index(lead)
            election = {ids[i]: ("50,80" if i == li else "1500,2500") for i in range(n)}
            cl = Cluster(os.path.join(a.out, "placement", f"{n}_{li}_{abs(hash(name)) % 10000}_r{rep}"), n,
                         election=election, delays=delays, commit_timeout=5.0)
            await cl.start()
            try:
                lid, _ = await cl.wait_leader()
                await asyncio.sleep(0.5)
                lat, commit = [], []
                for k in range(a.admissions):
                    t = mono()
                    r = await cl.reg.call_service("orchestrator", request_msg(f"p{k}", holding=1e6), retry_s=5)
                    if r.get("ok"):
                        lat.append((mono() - t) * 1000)
                        commit.append(r.get("commit_ms"))
                fd = sorted(delays[lid].values())
                predicted = 2 * fd[n // 2 - 1] if n > 1 else 0     # RTT to the closest majority
                row = {"placement": name, "replicas": n, "leader_at": locs[cl.ids.index(lid)], "rep": rep,
                       "follower_delays_ms": "/".join(str(x) for x in fd),
                       "predicted_commit_ms": predicted, "commit_ms_mean": rnd(mean(commit), 2),
                       "commit_ms_p95": rnd(pct(commit, .95), 2), "admit_ms_mean": rnd(mean(lat), 2),
                       "admissions": len(lat)}
                append_row(out, row)
                print(f"[{name} r{rep}] leader {row['leader_at']}  commit {row['commit_ms_mean']} ms "
                      f"(predicted {predicted})  admit {row['admit_ms_mean']} ms", flush=True)
            finally:
                await cl.stop()


# ============================================================================ E3 failover
async def failover_one(a, n, election, rep):
    cl = Cluster(os.path.join(a.out, "failover", f"n{n}_{election.replace(',', '-')}_r{rep}"), n,
                 election=election, seed=rep)
    await cl.start()
    try:
        lid, st = await cl.wait_leader()
        term0 = st["raft"]["term"]
        ok_times, fails, retried = [], 0, 0
        stop = asyncio.Event()

        async def client():
            nonlocal fails, retried
            k = 0
            while not stop.is_set():
                k += 1
                try:
                    r = await cl.reg.call_service("orchestrator", request_msg(f"f{k}", holding=1e6),
                                                  timeout=0.5, retry_s=15)
                    if r.get("ok"):
                        ok_times.append(mono())
                        retried += bool(r.get("retried"))
                    else:
                        fails += 1
                except Exception:
                    fails += 1
                await asyncio.sleep(0.01)
        ct = asyncio.create_task(client())
        await asyncio.sleep(1.5)
        before = len(ok_times)
        t_kill = mono()
        cl.kill(lid)
        new_lid, _ = await cl.wait_leader(t=30, exclude=lid)
        t_ready = mono()
        await asyncio.sleep(1.5)
        stop.set()
        await ct
        sts = await cl.statuses()
        nst = sts[new_lid]["raft"]
        lead_entry = [h for h in nst["history"] if h["role"] == "leader"][-1]
        t_elected = lead_entry["mono"]
        cand = [h for h in nst["history"] if h["role"] == "candidate" and h["mono"] >= t_kill]
        first_after = next((t for t in ok_times if t > t_kill), None)
        last_before = max((t for t in ok_times if t <= t_kill), default=None)
        elections = sum(s["raft"]["counters"].get("elections_started", 0) for s in sts.values())
        # client-visible outage = the longest gap between two consecutive successful admissions
        # around the crash (a reply already on its way from the old leader may arrive just after it)
        win = [t for t in ok_times if t_kill - 1.0 <= t <= t_ready + 1.0]
        outage = max((b - a for a, b in zip(win, win[1:])), default=None)
        # recovery of the crashed replica
        t_restart = mono()
        cl.restart(lid)
        caught = None
        for _ in range(400):
            s_old = await cl.status(lid, timeout=0.5)
            s_new = await cl.status(new_lid, timeout=0.5)
            if s_old and s_new and s_old["raft"]["applied"] >= s_new["raft"]["commit_index"]:
                caught = mono() - t_restart
                break
            await asyncio.sleep(0.02)
        s_old = await cl.status(lid)
        hashes = {r: s["state_hash"] for r, s in (await cl.statuses()).items() if s}
        row = {"replicas": n, "election_ms": election, "rep": rep,
               "detect_elect_ms": rnd((t_elected - t_kill) * 1000, 1),
               "first_candidate_ms": rnd(((cand[0]["mono"] if cand else t_elected) - t_kill) * 1000, 1),
               "leader_ready_ms": rnd((t_ready - t_kill) * 1000, 1),
               "unavailable_ms": rnd(outage * 1000, 1),
               "elections_in_term_change": nst["term"] - term0, "elections_started_total": elections,
               "admissions_before": before, "admissions_total": len(ok_times), "client_failures": fails,
               "retried_dedup": retried,
               "restart_catchup_ms": rnd(caught * 1000 if caught else None, 1),
               "restart_log_entries_loaded": s_old["raft"]["loaded_entries"] if s_old else None,
               "hashes_equal_after_recovery": len(set(hashes.values())) == 1}
        return row
    finally:
        await cl.stop()


async def failover(a):
    out = os.path.join(a.out, "failover.csv")
    for rep in range(1, a.reps + 1):
        for n in a.sizes:
            for el in a.elections:
                row = await failover_one(a, n, el, rep)
                append_row(out, row)
                print(f"[N={n} el={el} r{rep}] elected {row['detect_elect_ms']} ms, unavailable "
                      f"{row['unavailable_ms']} ms, term +{row['elections_in_term_change']}, client failures "
                      f"{row['client_failures']}, catch-up {row['restart_catchup_ms']} ms, "
                      f"hashes equal {row['hashes_equal_after_recovery']}", flush=True)


# ============================================================================ E4 consistency
async def consistency_one(a, mode, n, rep):
    raft = mode == "raft"
    cl = Cluster(os.path.join(a.out, "consistency", f"{mode}_n{n}_r{rep}"), n, raft=raft, n_sites=a.sites,
                 site_cpu=a.site_cpu, seed=rep)
    await cl.start()
    try:
        if raft:
            await cl.wait_leader()
        acked = {}                                    # req_id -> sid acknowledged to the client
        blocked = errors = 0
        k_counter = [0]

        async def client(c):
            nonlocal blocked, errors
            while True:
                k_counter[0] += 1
                k = k_counter[0]
                if k > a.requests:
                    return
                req = request_msg(f"q{k}", cpu=a.demand, holding=1e6)
                try:
                    if raft:
                        r = await cl.reg.call_service("orchestrator", req, timeout=1.0, retry_s=15)
                    else:                             # each client talks to "its" orchestrator
                        r = await call("127.0.0.1", ORCH_BASE + (c % n), req, timeout=5)
                except Exception:
                    errors += 1
                    continue
                if r.get("ok"):
                    acked[f"q{k}"] = r["sid"]
                elif r.get("status") == "BLOCKED":
                    blocked += 1
                else:
                    errors += 1
                await asyncio.sleep(a.pace)

        async def chaos():                            # Raft only: crash the leader twice, restart it
            for _ in range(2):
                await asyncio.sleep(a.crash_every)
                lid, _ = await cl.leader()
                if lid:
                    cl.kill(lid)
                    await cl.wait_leader(exclude=lid)
                    await asyncio.sleep(0.3)
                    cl.restart(lid)
        tasks = [asyncio.create_task(client(c)) for c in range(a.clients)]
        if raft:
            tasks.append(asyncio.create_task(chaos()))
        await asyncio.gather(*tasks)
        await asyncio.sleep(2.0)                      # let restarted replicas catch up
        ss = await cl.site_status()
        hosted = ss["hosted"]
        capacity_inst = int(a.site_cpu // a.demand) * a.sites
        per_site = {p: sum(d["cpu"] for d in h.values()) for p, h in hosted.items()}
        overbooked = sum(1 for v in per_site.values() if v > a.site_cpu + 1e-6)
        max_over = max((v / a.site_cpu for v in per_site.values()), default=0)
        all_keys = [k for h in hosted.values() for k in h]
        all_sids = [k.split("|")[0] for k in all_keys]
        distinct = len(set(all_sids))
        sts = await cl.statuses()
        if raft:
            lid, lst = await cl.leader()
            hashes = {r: s["state_hash"] for r, s in sts.items() if s}
            commits = {r: s["raft"]["commit_index"] for r, s in sts.items() if s}
            # every acknowledged admission must still be ACTIVE in the (new) leader's table
            st_full = (await call("127.0.0.1", ORCH_BASE + cl.ids.index(lid), {"type": "STATUS", "full": True},
                                  timeout=5))["status"]
            active = {sid for sid, x in st_full["allocations"].items() if x["state"] == "ACTIVE"}
            lost = sum(1 for sid in acked.values() if sid not in active)
            dup_req = len(st_full["allocations"]) - len({x.get("req_id") for x in st_full["allocations"].values()})
            row_extra = {"replica_hashes_equal": len(set(hashes.values())) == 1,
                         "replica_commit_indexes": "/".join(str(commits[r]) for r in sorted(commits)),
                         "acked_but_lost": lost, "duplicate_admissions_same_request": dup_req,
                         "leader_terms": lst["raft"]["term"]}
        else:
            row_extra = {"replica_hashes_equal": None, "replica_commit_indexes": None,
                         "acked_but_lost": None, "duplicate_admissions_same_request": None, "leader_terms": None}
            tables = [s["stats"]["accepted"] for s in sts.values() if s]
            row_extra["accepted_per_orchestrator"] = "/".join(map(str, tables))
        row = {"mode": "Raft cluster (2 leader crashes)" if raft else f"{n} uncoordinated orchestrators",
               "orchestrators": n, "rep": rep, "requests": a.requests, "true_capacity_instances": capacity_inst,
               "acknowledged": len(acked), "blocked": blocked, "client_errors": errors,
               "instances_on_sites": len(all_keys), "distinct_sids": distinct,
               "sid_collisions": len(all_sids) - distinct, "allocate_retries": ss["counters"]["allocate_retry"],
               "overbooked_sites": overbooked, "max_site_load_pct": rnd(max_over * 100, 1),
               "excess_instances": max(0, len(all_sids) - capacity_inst), **row_extra}
        return row
    finally:
        await cl.stop()


async def consistency(a):
    out = os.path.join(a.out, "consistency.csv")
    for rep in range(1, a.reps + 1):
        for mode, n in (("uncoordinated", 2), ("uncoordinated", 3), ("raft", 3)):
            row = await consistency_one(a, mode, n, rep)
            append_row(out, row)
            print(f"[{row['mode']} r{rep}] acked {row['acknowledged']} of capacity "
                  f"{row['true_capacity_instances']}; overbooked sites {row['overbooked_sites']} "
                  f"(max {row['max_site_load_pct']}%), sid collisions {row['sid_collisions']}, "
                  f"lost {row['acked_but_lost']}, hashes equal {row['replica_hashes_equal']}", flush=True)


# ============================================================================ E5 clocks
def analyse_events(run_dir):
    ev = []
    for f in os.listdir(run_dir):
        if f.startswith("events-"):
            for line in open(os.path.join(run_dir, f)):
                ev.append(json.loads(line))
    sends, recvs = {}, {}
    for e in ev:
        k = e["kind"]
        if k == "allocate_send":
            sends.setdefault(("alloc", e["sid"]), e)
        elif k == "allocate_recv":
            recvs.setdefault(("alloc", e["sid"]), e)
        elif k == "exit_send":
            sends.setdefault(("exit", e["sid"]), e)
        elif k == "exit_recv":
            recvs.setdefault(("exit", e["sid"]), e)
        elif k == "ae_send":
            sends.setdefault(("ae", e["to"], e["term"], e["first"], e["count"]), e)
        elif k == "ae_recv":
            recvs.setdefault((("ae", e["node"].replace("orch-", ""), e["term"], e["first"], e["count"])), e)
    pairs = [(sends[k], recvs[k]) for k in sends if k in recvs]
    wall_bad = sum(1 for s, r in pairs if r["wall"] < s["wall"])
    lamport_bad = sum(1 for s, r in pairs if r["lc"] <= s["lc"])
    by_kind = {}
    for (s, r) in pairs:
        kind = s["kind"].split("_")[0]
        d = by_kind.setdefault(kind, [0, 0])
        d[0] += 1
        d[1] += r["wall"] < s["wall"]
    gaps = [(r["wall"] - s["wall"]) * 1000 for s, r in pairs]
    return {"events": len(ev), "causal_pairs": len(pairs), "wall_violations": wall_bad,
            "wall_violation_pct": rnd(100 * wall_bad / len(pairs), 2) if pairs else None,
            "lamport_violations": lamport_bad, "true_gap_ms_p50": rnd(pct(gaps, .5), 3),
            "by_kind": {k: f"{v[1]}/{v[0]}" for k, v in by_kind.items()}}


async def clocks(a):
    out = os.path.join(a.out, "clocks.csv")
    for rep in range(1, a.reps + 1):
        for skew in a.skews:
            run_dir = os.path.join(a.out, "clocks", f"skew{skew}_r{rep}")
            cl = Cluster(run_dir, 3, eventlog=True, skew_ms=skew, seed=rep * 100 + int(skew))
            await cl.start()
            try:
                await cl.wait_leader()
                bytes0 = STATS["bytes_sent"]
                msgs0 = STATS["msgs_sent"]
                for k in range(a.admissions):
                    await cl.reg.call_service("orchestrator", request_msg(f"k{k}", holding=0.3), retry_s=5)
                await asyncio.sleep(1.5)
                sent_b = (STATS["bytes_sent"] - bytes0) / max(1, STATS["msgs_sent"] - msgs0)
            finally:
                await cl.stop()
            res = analyse_events(run_dir)
            row = {"max_skew_ms": skew, "rep": rep,
                   "node_skews_ms": "/".join(f"{v:.1f}" for v in cl.skews.values()),
                   **{k: v for k, v in res.items() if k != "by_kind"},
                   "violations_by_kind": json.dumps(res["by_kind"]),
                   "client_avg_msg_bytes": rnd(sent_b, 1)}
            append_row(out, row)
            print(f"[skew ±{skew} ms r{rep}] pairs {res['causal_pairs']}  wall-clock order wrong "
                  f"{res['wall_violations']} ({res['wall_violation_pct']}%)  Lamport wrong "
                  f"{res['lamport_violations']}  {res['by_kind']}", flush=True)


# ============================================================================ main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fakesite")
    f.add_argument("--registry-port", type=int, default=REG_PORT)
    f.add_argument("--ports", nargs="+", type=int, required=True)
    for name in ("smoke", "overhead", "placement", "failover", "consistency", "clocks"):
        s = sub.add_parser(name)
        s.add_argument("--out", default=os.path.join(ROOT, "results", "M4"))
        s.add_argument("--reps", type=int, default=3)
        if name == "overhead":
            s.add_argument("--sizes", nargs="+", type=int, default=[1, 3, 5, 7])
            s.add_argument("--clients", nargs="+", type=int, default=[1, 4])
            s.add_argument("--admissions", type=int, default=150)
            s.add_argument("--holding", type=float, default=0.5)
            s.add_argument("--idle", type=float, default=3.0)
        if name == "placement":
            s.add_argument("--admissions", type=int, default=40)
        if name == "failover":
            s.add_argument("--sizes", nargs="+", type=int, default=[3, 5])
            s.add_argument("--elections", nargs="+", default=["150,300", "300,600", "600,1200"])
        if name == "consistency":
            s.add_argument("--requests", type=int, default=150)
            s.add_argument("--clients", type=int, default=6)
            s.add_argument("--sites", type=int, default=6)
            s.add_argument("--site-cpu", type=float, default=4000)
            s.add_argument("--demand", type=float, default=400)
            s.add_argument("--crash-every", type=float, default=1.0)
            s.add_argument("--pace", type=float, default=0.15, help="pause between a client's requests (s)")
        if name == "clocks":
            s.add_argument("--skews", nargs="+", type=float, default=[0, 1, 5, 20, 50])
            s.add_argument("--admissions", type=int, default=60)
    a = ap.parse_args()
    if a.cmd == "fakesite":
        asyncio.run(fakesite_main(a))
        return
    os.makedirs(a.out, exist_ok=True)
    asyncio.run({"smoke": smoke, "overhead": overhead, "placement": placement, "failover": failover,
                 "consistency": consistency, "clocks": clocks}[a.cmd](a))


if __name__ == "__main__":
    main()

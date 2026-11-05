"""Test harness for the S5 experiments: a local cluster of OS processes.

  registry  +  N orchestrator replicas (Raft; N=1 is a classic single coordinator)
            +  K participant sites (python -m telerm.txnsite), each with its own WAL directory

Everything is a separate process, so a node can be killed with SIGKILL (a real crash, the
process memory is lost) and restarted (it recovers from its WAL / Raft log).
"""
import asyncio
import json
import os
import random
import shutil
import signal
import subprocess
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from telerm.naming import RegistryClient  # noqa: E402
from telerm.protocol import call, mono, now  # noqa: E402

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


def parts_of(services, holding=2.0, sites=None, cpu=None):
    """A chain request: one part per site, e.g. parts_of(3, sites=['site0','site1','site2'])."""
    out = []
    for i in range(services):
        p = {"service": f"svc{i}", "ingress": "core-1", "demand": {"cpu": cpu or 200, "mem": 1, "bw": 1},
             "max_latency_ms": 100, "holding_s": holding}
        if sites:
            p["site"] = sites[i % len(sites)]
        out.append(p)
    return out


class Cluster5:
    def __init__(self, run_dir, n_orch=3, n_sites=3, site_cpu=4000, delay_ms=0.0, fsync=False, indoubt_s=2.0,
                 election="300,600", hb_ms=50, vote_no=0.0, vote_timeout=1.5):
        self.run_dir, self.n_orch, self.n_sites = run_dir, n_orch, n_sites
        self.site_cpu, self.delay_ms, self.fsync, self.indoubt_s = site_cpu, delay_ms, fsync, indoubt_s
        self.election, self.hb_ms, self.vote_no, self.vote_timeout = election, hb_ms, vote_no, vote_timeout
        self.ids = [f"o{i + 1}" for i in range(n_orch)]
        self.site_names = [f"site{k}" for k in range(n_sites)]
        self.procs: dict = {}
        if os.path.exists(run_dir):
            shutil.rmtree(run_dir)
        os.makedirs(os.path.join(run_dir, "logs"))
        os.makedirs(os.path.join(run_dir, "wal"))

    def _spawn(self, name, args):
        self.procs[name] = subprocess.Popen(args, cwd=ROOT, env={**os.environ, "TELERM_NODE": name},
                                            stdout=open(os.path.join(self.run_dir, "logs", f"{name}.log"), "a"),
                                            stderr=subprocess.STDOUT)

    def site_args(self, k):
        a = [PY, "-m", "telerm.txnsite", "--id", self.site_names[k], "--port", str(SITE_BASE + k),
             "--registry", f"127.0.0.1:{REG_PORT}", "--cpu", str(self.site_cpu),
             "--wal-dir", os.path.join(self.run_dir, "wal"), "--delay-ms", str(self.delay_ms),
             "--indoubt-s", str(self.indoubt_s), "--vote-no", str(self.vote_no)]
        if self.fsync:
            a.append("--fsync")
        return a

    def orch_args(self, rid):
        i = self.ids.index(rid)
        peers = ",".join(f"{x}=127.0.0.1:{ORCH_BASE + k}" for k, x in enumerate(self.ids))
        a = [PY, "-m", "telerm.manager", "--port", str(ORCH_BASE + i), "--registry", f"127.0.0.1:{REG_PORT}",
             "--topology", self.topo_path, "--policy", "least_loaded", "--out", os.path.join(self.run_dir, rid),
             "--raft-id", rid, "--raft-peers", peers, "--election-ms", self.election, "--heartbeat-ms",
             str(self.hb_ms), "--vote-timeout", str(self.vote_timeout)]
        if self.fsync:
            a.append("--fsync")
        return a

    async def start(self):
        topo = {"links": [{"a": s, "b": "core-1", "capacity_mbps": 1e6, "latency_ms": 1} for s in self.site_names]}
        self.topo_path = os.path.join(self.run_dir, "topology.json")
        json.dump(topo, open(self.topo_path, "w"))
        self._spawn("registry", [PY, "-m", "telerm.registry", "--port", str(REG_PORT), "--hb-timeout", "100000"])
        await self.wait_port(REG_PORT)
        for k in range(self.n_sites):
            self._spawn(self.site_names[k], self.site_args(k))
        for k in range(self.n_sites):
            await self.wait_port(SITE_BASE + k)
        for rid in self.ids:
            self._spawn(rid, self.orch_args(rid))
        self.reg = RegistryClient(f"127.0.0.1:{REG_PORT}")
        await self.reg.lookup("orchestrator", wait_s=30)
        await self.wait_leader()

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

    # ---------------------------------------------------------------- queries
    async def ostatus(self, rid, timeout=1.0, full=False):
        i = self.ids.index(rid)
        try:
            return (await call("127.0.0.1", ORCH_BASE + i, {"type": "STATUS", "full": full}, timeout=timeout))["status"]
        except Exception:
            return None

    async def leader(self):
        for rid in self.ids:
            if self.procs[rid].poll() is None:
                st = await self.ostatus(rid)
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

    async def sstatus(self, k, timeout=1.0):
        try:
            return (await call("127.0.0.1", SITE_BASE + k, {"type": "STATUS"}, timeout=timeout))["status"]
        except Exception:
            return None

    def alive(self, name):
        return self.procs[name].poll() is None

    # ---------------------------------------------------------------- actions
    def kill(self, name):
        p = self.procs[name]
        if p.poll() is None:
            p.send_signal(signal.SIGKILL)
            p.wait()

    def restart(self, name):
        if name in self.ids:
            self._spawn(name, self.orch_args(name))
        else:
            self._spawn(name, self.site_args(self.site_names.index(name)))

    async def fault(self, target, **kw):
        port = ORCH_BASE + self.ids.index(target) if target in self.ids else SITE_BASE + self.site_names.index(target)
        return await call("127.0.0.1", port, {"type": "FAULT", **kw}, timeout=2.0)

    async def txn(self, parts, proto="2pc", req_id=None, timeout=10.0, retry_s=0.0):
        msg = {"type": "TXN_REQUEST", "request": {"parts": parts, "proto": proto, "req_id": req_id}}
        return await self.reg.call_service("orchestrator", msg, timeout=timeout, retry_s=retry_s)

    async def settle(self, t=30.0, need_done=True):
        """Wait until no site has a PREPARED/PRECOMMITTED txn and the leader's txns are all done."""
        end = mono() + t
        while mono() < end:
            ok = True
            for k in range(self.n_sites):
                if not self.alive(self.site_names[k]):
                    ok = False
                    break
                st = await self.sstatus(k)
                if st is None or any(v in ("PREPARED", "PRECOMMITTED") for v in st["txns"].values()):
                    ok = False
                    break
            if ok and need_done:
                rid, st = await self.leader()
                if st is None or any(not x["done"] for x in st.get("txns", {}).values()):
                    ok = False
            if ok:
                return True
            await asyncio.sleep(0.1)
        return False

    async def check(self):
        """Consistency invariants over the whole cluster. Returns a dict of violation counters."""
        rid, lst = await self.leader()
        sites = {}
        for k in range(self.n_sites):
            sites[self.site_names[k]] = await self.sstatus(k)
        v = {"atomicity": 0, "over_capacity": 0, "ledger_mismatch": 0, "stuck_prepared": 0,
             "coord_participant_mismatch": 0, "protocol_violations": 0, "txns": 0}
        txns = (lst or {}).get("txns", {})
        v["txns"] = len(txns)
        for t, rec in txns.items():
            states = {s: sites[s]["txns"].get(t) for s in rec["sites"] if sites.get(s)}
            vals = set(states.values())
            if "COMMITTED" in vals and ("ABORTED" in vals):
                v["atomicity"] += 1
            if rec["state"] == "COMMITTED" and vals - {"COMMITTED"}:
                v["coord_participant_mismatch"] += 1
            if rec["state"] == "ABORTED" and vals - {"ABORTED", None}:
                v["coord_participant_mismatch"] += 1
        for s, st in sites.items():
            if st is None:
                continue
            a, c = st["allocated"]["cpu"], st["capacity"]["cpu"]
            if a > c + 1e-6:
                v["over_capacity"] += 1
            used = sum(d["cpu"] for d in st["active"].values())
            if abs(a - used) > 1e-6 and not any(x in ("PREPARED", "PRECOMMITTED") for x in st["txns"].values()):
                v["ledger_mismatch"] += 1
            v["stuck_prepared"] += sum(1 for x in st["txns"].values() if x in ("PREPARED", "PRECOMMITTED"))
            v["protocol_violations"] += st["stats"]["violations"]
        return v, sites, lst

    async def stop(self):
        for i, rid in enumerate(self.ids):
            if self.procs[rid].poll() is None:
                try:
                    await call("127.0.0.1", ORCH_BASE + i, {"type": "SHUTDOWN"}, timeout=1)
                except Exception:
                    pass
        for k, n in enumerate(self.site_names):
            if self.procs[n].poll() is None:
                try:
                    await call("127.0.0.1", SITE_BASE + k, {"type": "SHUTDOWN"}, timeout=1)
                except Exception:
                    pass
        try:
            await call("127.0.0.1", REG_PORT, {"type": "SHUTDOWN"}, timeout=1)
        except Exception:
            pass
        for p in self.procs.values():
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

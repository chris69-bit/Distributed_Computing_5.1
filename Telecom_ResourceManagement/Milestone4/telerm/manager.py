"""Global Resource Manager / Orchestrator (the distributed-OS layer of TeleRM).

S3: with --registry, this process becomes the *orchestrator* microservice. It no longer
keeps the site list itself: before each admission it asks the registry which sites are
alive (LIST_SITES), it publishes where each instance runs (ROUTE_SET) so gateways can find
it, and it pushes its reservation snapshot to the telemetry service. Without --registry it
behaves exactly as in S1/S2 (sites register and send heartbeats here).

Responsibilities:
  * site registry + heartbeat failure detector
  * network topology and link-bandwidth reservations
  * admission control: node capacity + path bandwidth + latency bound
  * placement policy (edge_first | least_loaded | best_fit)
  * allocation table (global instance ids), RESIZE of live allocations, release on exit
  * utilisation time series and blocking statistics

S4: with --raft-id/--raft-peers, several orchestrator replicas form a Raft cluster
(telerm/raft.py). Only the elected leader admits; every admission, release and resize is a
command in the replicated log, and each replica applies committed commands, in order, to its
own allocation table (a replicated state machine). The leader registers the alias
"orchestrator" in the registry with its term, so gateways and sites always find it.

Run:  python -m telerm.manager --port 8000 --topology topology.json --policy edge_first --out results/run1
"""
import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal

from .monitor import ResourceMonitor
from .naming import RegistryClient
from .protocol import NODE, STATS, call, log_event, make_server_callback, mono, now
from .raft import NotLeader, RaftNode
from .resources import (PLACEMENT_POLICIES, NodeResources, Resources, Topology,
                        choose, evaluate_candidates)

log = logging.getLogger("manager")


class Manager:
    def __init__(self, a):
        self.port, self.bind, self.policy = a.port, a.bind, a.policy
        self.hb_timeout = a.hb_timeout
        self.out = a.out
        os.makedirs(self.out, exist_ok=True)
        self.topo = Topology()
        with open(a.topology) as f:
            for l in json.load(f)["links"]:
                self.topo.add_link(l["a"], l["b"], l["capacity_mbps"], l["latency_ms"])
        self.sites: dict[str, dict] = {}
        self.alloc: dict[str, dict] = {}
        self.seq = 0
        self.lock = asyncio.Lock()
        self.shutdown = asyncio.Event()
        self.started = now()
        self.stats = {"requests": 0, "accepted": 0, "blocked": 0, "released": 0,
                      "resize_requests": 0, "resize_accepted": 0, "resize_rejected": 0,
                      "allocate_failures": 0}
        self.per_service: dict[str, dict] = {}
        self.timeseries: list = []
        self.events: list = []
        self.alloc_log = open(os.path.join(self.out, "allocations.jsonl"), "a", buffering=1)
        self.a = a
        self.reg = RegistryClient(a.registry) if a.registry else None      # S3 mode
        self.registry_ms: list = []
        self.raft = None                                     # S4 mode
        self.req_index: dict[str, str] = {}                  # client request id -> sid (dedupe)
        self.sm = {"seq": 0, "applied": 0, "conflicts": 0}   # replicated state-machine counters
        if a.raft_id:
            addrs = {}
            for item in a.raft_peers.split(","):
                pid, ad = item.split("=")
                h, pt = ad.rsplit(":", 1)
                addrs[pid] = (h, int(pt))
            delays = {}
            if a.peer_delay_ms:
                for item in a.peer_delay_ms.split(","):
                    pid, d = item.split("=")
                    delays[pid] = float(d)
            lo, hi = (float(x) for x in a.election_ms.split(","))
            NODE["name"] = f"orch-{a.raft_id}"
            self.raft = RaftNode(a.raft_id, addrs, self.apply, a.state_dir or self.out, election_ms=(lo, hi),
                                 heartbeat_ms=a.heartbeat_ms, peer_delay_ms=delays, fsync=a.fsync,
                                 on_role=self.on_role, commit_timeout=a.commit_timeout)
        self.admit_ms: list = []

    async def sync_sites(self):
        """S3: refresh the site list from the registry (capacity, address, tier, liveness)."""
        r = await self.reg.rpc({"type": "LIST_SITES"}, timeout=3.0)
        for x in r.get("sites", []):
            s = self.sites.get(x["name"])
            if s is None:
                s = self.sites[x["name"]] = {"res": NodeResources(Resources.from_dict(x["capacity"])),
                                            "slot_utilisation": 0.0, "fairness": None, "last_hb": now()}
                self.event("site_discovered", site=x["name"])
            s.update({"host": x["host"], "port": x["port"], "tier": x["tier"], "pid": x["pid"],
                      "status": x["status"]})

    def tiers(self):
        return {sid: s.get("tier") for sid, s in self.sites.items()}

    def event(self, kind, **kw):
        e = {"t": round(now() - self.started, 3), "event": kind, **kw}
        self.events.append(e)
        self.alloc_log.write(json.dumps(e) + "\n")

    def alive(self):
        return {sid: s["res"] for sid, s in self.sites.items() if s["status"] == "ALIVE"}

    def svc(self, name):
        return self.per_service.setdefault(name, {"offered": 0, "accepted": 0, "blocked": 0,
                                                  "block_reasons": {}, "placed_on": {}})

    # ------------------------------------------------------------------ admission
    def not_leader(self, exc=None):
        addr = exc.leader_addr if exc is not None else self.raft.leader_addr()
        return {"ok": False, "error": "NOT_LEADER", "leader_addr": addr,
                "reason": str(exc) if exc is not None else "not leader"}

    async def request(self, r: dict) -> dict:
        if self.raft:
            return await self.request_raft(r)
        demand = Resources.from_dict(r["demand"])
        ps = self.svc(r["service"])
        async with self.lock:
            self.seq += 1
            sid = f"S{self.seq:05d}"
            self.stats["requests"] += 1
            ps["offered"] += 1
            t0 = now()
            reg_ms = None
            if self.reg:
                tr = mono()
                await self.sync_sites()
                reg_ms = (mono() - tr) * 1000
                self.registry_ms.append(reg_ms)
            nodes = self.alive()
            feasible, reasons = evaluate_candidates(demand, r["ingress"], float(r.get("max_latency_ms", 1e9)),
                                                    nodes, self.topo)
            pick = choose(feasible, demand, r["ingress"], nodes, self.policy, self.tiers())
            if pick is None:
                reason = max(reasons, key=reasons.get) if reasons else "no_sites"
                self.stats["blocked"] += 1
                ps["blocked"] += 1
                ps["block_reasons"][reason] = ps["block_reasons"].get(reason, 0) + 1
                self.event("blocked", sid=sid, service=r["service"], ingress=r["ingress"], reasons=reasons)
                return {"ok": False, "sid": sid, "status": "BLOCKED", "reason": reason, "reasons": reasons,
                        "decision_ms": round((now() - t0) * 1000, 3)}
            site_id, path, lat = pick
            nodes[site_id].allocate(demand)                  # reserve node + path before telling the site
            self.topo.reserve(path, demand.bw)
            decision_ms = (now() - t0) * 1000
            site = self.sites[site_id]
            inst = {"sid": sid, "service": r["service"], "ingress": r["ingress"],
                    "demand": demand.to_dict(), "holding_s": r["holding_s"]}
            for k in ("work_kb", "backlogged", "queue_limit", "req_id"):      # S2 traffic settings, if given
                if k in r:
                    inst[k] = r[k]
            try:
                rep = await call(site["host"], site["port"], {"type": "ALLOCATE", "instance": inst}, timeout=3.0)
                if not rep.get("ok"):
                    raise RuntimeError(rep.get("error"))
            except Exception as exc:
                nodes[site_id].release(demand)
                self.topo.release(path, demand.bw)
                self.stats["allocate_failures"] += 1
                self.stats["blocked"] += 1
                ps["blocked"] += 1
                ps["block_reasons"]["site_unreachable"] = ps["block_reasons"].get("site_unreachable", 0) + 1
                self.event("allocate_failed", sid=sid, site=site_id, error=repr(exc))
                return {"ok": False, "sid": sid, "status": "BLOCKED", "reason": "site_unreachable"}
            self.alloc[sid] = {"site": site_id, "path": path, "latency_ms": lat, "demand": demand,
                               "service": r["service"], "ingress": r["ingress"], "state": "ACTIVE",
                               "t_start": now(), "reports": []}
            self.stats["accepted"] += 1
            ps["accepted"] += 1
            ps["placed_on"][site_id] = ps["placed_on"].get(site_id, 0) + 1
            self.event("accepted", sid=sid, service=r["service"], ingress=r["ingress"], site=site_id,
                       path=path, latency_ms=lat)
            if self.reg:                                     # S3: publish the route for the gateways
                await self.reg.rpc({"type": "ROUTE_SET", "sid": sid, "site": site_id, "ingress": r["ingress"],
                                    "address": f"{site['host']}:{site['port']}", "link_ms": lat}, timeout=3.0)
            return {"ok": True, "sid": sid, "status": "ACCEPTED", "site": site_id, "path": path,
                    "tier": site.get("tier"),
                    "address": f"{site['host']}:{site['port']}",   # S2: where to send its traffic
                    "latency_ms": lat, "decision_ms": round(decision_ms, 3),
                    "registry_ms": round(reg_ms, 3) if reg_ms is not None else None}

    async def resize(self, sid: str, new_cpu: float) -> dict:
        async with self.lock:
            self.stats["resize_requests"] += 1
            a = self.alloc.get(sid)
            if a is None or a["state"] != "ACTIVE":
                self.stats["resize_rejected"] += 1
                return {"ok": False, "status": "REJECTED", "reason": "not_active"}
            res = self.sites[a["site"]]["res"]
            delta = Resources(new_cpu - a["demand"].cpu, 0, 0)
            if delta.cpu > 0 and not res.can_allocate(delta):
                self.stats["resize_rejected"] += 1
                self.event("resize_rejected", sid=sid, site=a["site"], cpu=new_cpu)
                return {"ok": False, "status": "REJECTED", "reason": "node_capacity"}
            site = self.sites[a["site"]]
            try:
                rep = await call(site["host"], site["port"], {"type": "RESIZE", "sid": sid, "cpu": new_cpu})
                if not rep.get("ok"):
                    raise RuntimeError("site refused (instance ended?)")
            except Exception as exc:
                self.stats["resize_rejected"] += 1
                return {"ok": False, "status": "REJECTED", "reason": repr(exc)}
            if delta.cpu > 0:
                res.allocate(delta)
            else:
                res.release(Resources(-delta.cpu, 0, 0))
            a["demand"] = Resources(new_cpu, a["demand"].mem, a["demand"].bw)
            self.stats["resize_accepted"] += 1
            self.event("resized", sid=sid, site=a["site"], cpu=new_cpu)
            return {"ok": True, "status": "RESIZED", "cpu": new_cpu}

    def release(self, sid: str, summary=None):
        a = self.alloc.get(sid)
        if not a or a["state"] != "ACTIVE":
            return
        a["state"] = "RELEASED"
        a["summary"] = summary
        self.sites[a["site"]]["res"].release(a["demand"])
        self.topo.release(a["path"], a["demand"].bw)
        self.stats["released"] += 1
        self.event("released", sid=sid, site=a["site"])
        if self.reg:
            asyncio.create_task(self._route_del(sid))

    # ------------------------------------------------------------------ S4: replicated state machine
    def apply(self, cmd, index):
        """Apply one committed log command. Runs on EVERY replica, in log order, deterministically:
        the same commands in the same order give the same allocation table everywhere."""
        self.sm["applied"] += 1
        op = cmd["op"]
        if op == "ADMIT":
            site_id = cmd["site"]
            s = self.sites.get(site_id)
            if s is None:                                    # a follower may not have synced this site yet
                s = self.sites[site_id] = {"res": NodeResources(Resources.from_dict(cmd["capacity"])),
                                           "host": cmd["host"], "port": cmd["port"], "tier": cmd.get("tier"),
                                           "pid": None, "status": "ALIVE", "slot_utilisation": 0.0,
                                           "fairness": None, "last_hb": now()}
            demand = Resources.from_dict(cmd["demand"])
            if not s["res"].can_allocate(demand) or not self.topo.path_has_bw(cmd["path"], demand.bw):
                self.sm["conflicts"] += 1                    # cannot happen with one leader; checked anyway
                return {"ok": False, "reason": "conflict"}
            self.sm["seq"] += 1
            sid = f"S{self.sm['seq']:05d}"
            s["res"].allocate(demand)
            self.topo.reserve(cmd["path"], demand.bw)
            self.alloc[sid] = {"site": site_id, "path": cmd["path"], "latency_ms": cmd["latency_ms"],
                               "demand": demand, "service": cmd["service"], "ingress": cmd["ingress"],
                               "state": "ACTIVE", "t_start": now(), "reports": [], "req_id": cmd.get("req_id"),
                               "log_index": index}
            if cmd.get("req_id"):
                self.req_index[cmd["req_id"]] = sid
            self.event("accepted", sid=sid, service=cmd["service"], ingress=cmd["ingress"], site=site_id,
                       path=cmd["path"], latency_ms=cmd["latency_ms"], log_index=index)
            return {"ok": True, "sid": sid}
        if op == "RELEASE":
            a = self.alloc.get(cmd["sid"])
            if not a or a["state"] != "ACTIVE":
                return {"ok": False, "reason": "not_active"}
            a["state"] = "RELEASED"
            self.sites[a["site"]]["res"].release(a["demand"])
            self.topo.release(a["path"], a["demand"].bw)
            self.stats["released"] += 1
            self.event("released", sid=cmd["sid"], site=a["site"], log_index=index)
            return {"ok": True}
        if op == "RESIZE":
            a = self.alloc.get(cmd["sid"])
            if not a or a["state"] != "ACTIVE":
                return {"ok": False, "reason": "not_active"}
            res = self.sites[a["site"]]["res"]
            delta = Resources(cmd["cpu"] - a["demand"].cpu, 0, 0)
            if delta.cpu > 0 and not res.can_allocate(delta):
                return {"ok": False, "reason": "node_capacity"}
            if delta.cpu > 0:
                res.allocate(delta)
            else:
                res.release(Resources(-delta.cpu, 0, 0))
            a["demand"] = Resources(cmd["cpu"], a["demand"].mem, a["demand"].bw)
            return {"ok": True}
        return {"ok": False, "reason": f"unknown op {op}"}

    def state_hash(self):
        """Fingerprint of the replicated state (identical on all replicas that applied the same log)."""
        allocs = sorted((sid, a["site"], a["state"], round(a["demand"].cpu, 3), round(a["demand"].mem, 3),
                         round(a["demand"].bw, 3)) for sid, a in self.alloc.items())
        sites = sorted((sid, round(s["res"].allocated.cpu, 3), round(s["res"].allocated.mem, 3),
                        round(s["res"].allocated.bw, 3)) for sid, s in self.sites.items()
                       if s["res"].allocated.cpu > 1e-9 or s["res"].allocated.mem > 1e-9 or s["res"].allocated.bw > 1e-9)
        # (sites with nothing allocated are skipped: the leader also knows idle sites from the registry)
        links = sorted((k, round(l.allocated, 3)) for k, l in self.topo.links.items())
        blob = json.dumps([allocs, sites, links, self.sm["seq"]]).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    async def on_role(self, role, term):
        if role != "leader" or not self.reg:
            return
        for _ in range(400):                                 # wait until our NOOP is committed
            if self.raft.is_ready() or self.raft.term != term:
                break
            await asyncio.sleep(0.005)
        if self.raft.is_ready() and self.raft.term == term:
            self.event("became_leader", term=term)
            try:
                r = await self.reg.rpc({"type": "REGISTER", "name": "orchestrator", "kind": "service",
                                        "host": self.a.host, "port": self.port, "location": self.a.location,
                                        "tier": "core", "term": term, "pid": os.getpid()}, timeout=2.0)
                self.event("alias_registered", term=term, ok=r.get("ok"))
            except Exception as exc:
                self.event("alias_register_failed", term=term, error=repr(exc))

    async def request_raft(self, r: dict) -> dict:
        ps = self.svc(r["service"])
        demand = Resources.from_dict(r["demand"])
        t_all = mono()
        # Critical section: decide on the fully applied state and commit the decision. The lock
        # is released as soon as the decision is committed (the reservation is then part of the
        # replicated state), so starting the instance on the site overlaps with the next decision.
        async with self.lock:
            if not self.raft.is_ready():
                return self.not_leader()
            req_id = r.get("req_id")
            if req_id and req_id in self.req_index:          # a retry of something already decided
                sid = self.req_index[req_id]
                retried, timing = True, {}
            else:
                self.stats["requests"] += 1
                ps["offered"] += 1
                t0 = mono()
                reg_ms = None
                if self.reg:
                    await self.sync_sites()
                    reg_ms = (mono() - t0) * 1000
                    self.registry_ms.append(reg_ms)
                td = mono()
                nodes = self.alive()
                feasible, reasons = evaluate_candidates(demand, r["ingress"], float(r.get("max_latency_ms", 1e9)),
                                                        nodes, self.topo)
                pick = choose(feasible, demand, r["ingress"], nodes, self.policy, self.tiers())
                decision_ms = (mono() - td) * 1000
                if pick is None:
                    reason = max(reasons, key=reasons.get) if reasons else "no_sites"
                    self.stats["blocked"] += 1
                    ps["blocked"] += 1
                    ps["block_reasons"][reason] = ps["block_reasons"].get(reason, 0) + 1
                    return {"ok": False, "status": "BLOCKED", "reason": reason, "decision_ms": round(decision_ms, 3)}
                site_id, path, lat = pick
                site = self.sites[site_id]
                cmd = {"op": "ADMIT", "req_id": req_id, "service": r["service"], "ingress": r["ingress"],
                       "site": site_id, "path": path, "latency_ms": lat, "demand": demand.to_dict(),
                       "capacity": site["res"].capacity.to_dict(), "host": site["host"], "port": site["port"],
                       "tier": site.get("tier")}
                tc = mono()
                try:
                    res = await self.raft.propose(cmd)
                except NotLeader as exc:
                    return self.not_leader(exc)
                commit_ms = (mono() - tc) * 1000
                if not res.get("ok"):
                    self.stats["blocked"] += 1
                    return {"ok": False, "status": "BLOCKED", "reason": res.get("reason")}
                sid = res["sid"]
                retried = False
                timing = {"registry_ms": round(reg_ms, 3) if reg_ms is not None else None,
                          "decision_ms": round(decision_ms, 3), "commit_ms": round(commit_ms, 3)}
        a = self.alloc[sid]
        out = await self._finish_admit(sid, a, r, retried=retried, t_all=t_all)
        out.update(timing)
        if out.get("ok") and not retried:
            self.stats["accepted"] += 1
            ps["accepted"] += 1
            ps["placed_on"][a["site"]] = ps["placed_on"].get(a["site"], 0) + 1
        return out

    async def _finish_admit(self, sid, a, r, retried, t_all):
        """Side effects after the decision is committed: start the instance, publish its route.
        Both are idempotent, so a new leader can safely redo them for a retried request."""
        site = self.sites[a["site"]]
        inst = {"sid": sid, "service": a["service"], "ingress": a["ingress"], "demand": a["demand"].to_dict(),
                "holding_s": r["holding_s"]}
        for k in ("work_kb", "backlogged", "queue_limit", "req_id"):
            if k in r:
                inst[k] = r[k]
        ta = mono()
        try:
            log_event("allocate_send", sid=sid)
            rep = await call(site["host"], site["port"], {"type": "ALLOCATE", "instance": inst}, timeout=3.0)
            if not rep.get("ok"):
                raise RuntimeError(rep.get("error"))
        except Exception as exc:
            self.stats["allocate_failures"] += 1
            try:
                await self.raft.propose({"op": "RELEASE", "sid": sid})
            except NotLeader:
                pass
            return {"ok": False, "sid": sid, "status": "BLOCKED", "reason": "site_unreachable", "error": repr(exc)}
        allocate_ms = (mono() - ta) * 1000
        if self.reg:
            await self.reg.rpc({"type": "ROUTE_SET", "sid": sid, "site": a["site"], "ingress": a["ingress"],
                                "address": f"{site['host']}:{site['port']}", "link_ms": a["latency_ms"]}, timeout=3.0)
        total = (mono() - t_all) * 1000
        self.admit_ms.append(total)
        return {"ok": True, "sid": sid, "status": "ACCEPTED", "site": a["site"], "path": a["path"],
                "tier": site.get("tier"), "address": f"{site['host']}:{site['port']}",
                "latency_ms": a["latency_ms"], "retried": retried, "allocate_ms": round(allocate_ms, 3),
                "leader": self.raft.me, "term": self.raft.term, "log_index": a.get("log_index"),
                "orchestrator_ms": round(total, 3)}

    async def release_raft(self, sid):
        if not self.raft.is_ready():
            return self.not_leader()
        log_event("exit_recv", sid=sid)
        a = self.alloc.get(sid)
        if not a or a["state"] != "ACTIVE":
            return {"ok": True, "note": "already released"}
        try:
            res = await self.raft.propose({"op": "RELEASE", "sid": sid})
        except NotLeader as exc:
            return self.not_leader(exc)
        if self.reg and res.get("ok"):
            asyncio.create_task(self._route_del(sid))
        return {"ok": True}

    async def _route_del(self, sid):
        try:
            await self.reg.rpc({"type": "ROUTE_DEL", "sid": sid}, timeout=3.0)
        except Exception:
            pass

    # ------------------------------------------------------------------ messages
    async def handle(self, msg):
        t = msg.get("type")
        if self.raft and t.startswith("RAFT_"):
            return self.raft.handle(msg)
        if self.raft and t == "INSTANCE_EXIT":
            return await self.release_raft(msg["sid"])
        if self.raft and t == "RESIZE":
            if not self.raft.is_ready():
                return self.not_leader()
            try:
                r = await self.raft.propose({"op": "RESIZE", "sid": msg["sid"], "cpu": float(msg["cpu"])})
            except NotLeader as exc:
                return self.not_leader(exc)
            return {"ok": r.get("ok"), "status": "RESIZED" if r.get("ok") else "REJECTED", **r}
        if t == "REGISTER":
            self.sites[msg["site"]] = {"host": msg["host"], "port": msg["port"], "tier": msg.get("tier"),
                                       "pid": msg.get("pid"), "res": NodeResources(Resources.from_dict(msg["capacity"])),
                                       "status": "ALIVE", "last_hb": now(), "slot_utilisation": 0.0,
                                       "fairness": None}
            self.event("site_registered", site=msg["site"])
            return {"ok": True}
        if t == "HEARTBEAT":
            s = self.sites.get(msg["site"])
            if s is None:
                return {"ok": False, "error": "unknown site"}
            s["last_hb"] = now()
            s["slot_utilisation"] = msg.get("slot_utilisation", 0.0)
            s["fairness"] = msg.get("fairness")
            s["resources"] = msg.get("resources")
            for rep in msg.get("instances", []):
                if rep["sid"] in self.alloc:
                    self.alloc[rep["sid"]]["last_report"] = rep
            if s["status"] != "ALIVE":
                s["status"] = "ALIVE"
                self.event("site_alive_again", site=msg["site"])
            return {"ok": True}
        if t == "REQUEST":
            return await self.request(msg["request"])
        if t == "RESIZE":
            return await self.resize(msg["sid"], float(msg["cpu"]))
        if t == "INSTANCE_EXIT":
            self.release(msg["sid"], msg.get("summary"))
            return {"ok": True}
        if t == "STATUS":
            return {"ok": True, "status": self.status(full=msg.get("full", False))}
        if t == "SHUTDOWN":
            self.shutdown.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown message type {t}"}

    # ------------------------------------------------------------------ background loops
    async def failure_detector(self):
        if self.reg:                      # S3: liveness is the registry's job
            return
        while not self.shutdown.is_set():
            t = now()
            for sid, s in self.sites.items():
                if s["status"] == "ALIVE" and t - s["last_hb"] > self.hb_timeout:
                    s["status"] = "SUSPECT"
                    self.event("site_suspect", site=sid)
            await asyncio.sleep(0.5)

    async def sampler(self):
        """Utilisation time series (1 s): reservations (S1) plus measured CPU/memory (S2)."""
        monitor = ResourceMonitor()
        while not self.shutdown.is_set():
            await asyncio.sleep(1.0)
            if self.reg:                  # S3: heartbeat to the registry, snapshot to telemetry
                await self.reg.heartbeat()
                if self.raft and self.raft.is_ready():       # S4: the leader keeps its alias alive
                    try:
                        await self.reg.rpc({"type": "HEARTBEAT", "name": "orchestrator"}, timeout=2.0)
                    except Exception:
                        pass
                sample = monitor.sample()
                snap = {**sample, "active": sum(1 for a in self.alloc.values() if a["state"] == "ACTIVE"),
                        "reservations": {sid: {k: round(v, 4) for k, v in s["res"].utilisation().items()}
                                         for sid, s in self.sites.items()},
                        "links": self.topo.utilisation(), "msgs_sent": STATS["msgs_sent"],
                        "msgs_recv": STATS["msgs_recv"]}
                try:
                    await self.reg.call_service("telemetry", {"type": "REPORT", "node": "orchestrator",
                                                              "kind": "service", "ts": now(), "sample": snap},
                                                timeout=2.0)
                except Exception:
                    pass
                self.timeseries.append({"t": round(now() - self.started, 2), "wall": round(now(), 3),
                                        "active": snap["active"], "manager": sample,
                                        "sites": {sid: {**snap["reservations"][sid], "slot_util": None,
                                                        "measured": None} for sid in self.sites},
                                        "links": snap["links"]})
                continue
            self.timeseries.append({
                "t": round(now() - self.started, 2), "wall": round(now(), 3),
                "active": sum(1 for a in self.alloc.values() if a["state"] == "ACTIVE"),
                "manager": monitor.sample(),
                "sites": {sid: {**{k: round(v, 4) for k, v in s["res"].utilisation().items()},
                                "slot_util": s["slot_utilisation"], "measured": s.get("resources")}
                          for sid, s in self.sites.items()},
                "links": self.topo.utilisation()})

    def status(self, full=False):
        sites = {sid: {"status": s["status"], "tier": s.get("tier"), "pid": s.get("pid"),
                       "address": f"{s['host']}:{s['port']}",
                       "capacity": s["res"].capacity.to_dict(), "allocated": s["res"].allocated.to_dict(),
                       "utilisation": {k: round(v, 3) for k, v in s["res"].utilisation().items()},
                       "slot_utilisation": s["slot_utilisation"], "fairness": s["fairness"],
                       "active": sum(1 for a in self.alloc.values() if a["site"] == sid and a["state"] == "ACTIVE")}
                 for sid, s in self.sites.items()}
        out = {"policy": self.policy, "uptime_s": round(now() - self.started, 2), "pid": os.getpid(),
               "mode": "orchestrator (S3)" if self.reg else "manager (S1/S2)",
               "registry_ms_mean": round(sum(self.registry_ms) / len(self.registry_ms), 3) if self.registry_ms else None,
               "stats": dict(self.stats), "per_service": self.per_service, "sites": sites,
               "links": {k: {"capacity_mbps": l.capacity, "allocated_mbps": l.allocated, "latency_ms": l.latency_ms}
                         for k, l in self.topo.links.items()},
               "comm": dict(STATS)}
        if self.raft:
            out["raft"] = self.raft.status()
            out["state_hash"] = self.state_hash()
            out["state_machine"] = dict(self.sm)
            out["active"] = sum(1 for a in self.alloc.values() if a["state"] == "ACTIVE")
            out["admit_ms_mean"] = round(sum(self.admit_ms) / len(self.admit_ms), 3) if self.admit_ms else None
        if full:
            out["timeseries"] = self.timeseries
            out["allocations"] = {sid: {k: (v.to_dict() if isinstance(v, Resources) else v)
                                        for k, v in a.items() if k != "reports"} for sid, a in self.alloc.items()}
            out["events"] = self.events
        return out

    async def main(self):
        server = await asyncio.start_server(make_server_callback(self.handle), self.bind, self.port)
        raft_task = None
        if self.raft:
            raft_task = self.raft.start()
            if self.reg:
                await self.reg.register(name=f"orch-{self.a.raft_id}", kind="service", host=self.a.host,
                                        port=self.port, location=self.a.location, tier="core")
        elif self.reg:
            await self.reg.register(name="orchestrator", kind="service", host=self.a.host, port=self.port,
                                    location=self.a.location, tier="core")
        log.info("%s pid %d on %s:%d (placement=%s)", "orchestrator" if self.reg else "resource manager",
                 os.getpid(), self.bind, self.port, self.policy)
        loops = [asyncio.create_task(self.failure_detector()), asyncio.create_task(self.sampler())]
        await self.shutdown.wait()
        for l in loops:
            l.cancel()
        if self.raft:
            raft_task.cancel()
            await self.raft.stop()
        server.close()
        self.alloc_log.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="TeleRM global resource manager")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--topology", default="topology.json")
    ap.add_argument("--policy", default="edge_first", choices=PLACEMENT_POLICIES)
    ap.add_argument("--hb-timeout", type=float, default=3.0)
    ap.add_argument("--out", default="results/manual")
    ap.add_argument("--registry", default=None, help="S3: registry address; turns this into the orchestrator")
    ap.add_argument("--host", default="127.0.0.1", help="S3: address others use to reach this service")
    ap.add_argument("--location", default="core-1", help="S3: site this service runs at")
    ap.add_argument("--raft-id", default=None, help="S4: this replica's id, e.g. o1")
    ap.add_argument("--raft-peers", default="", help="S4: all replicas, e.g. o1=127.0.0.1:8000,o2=127.0.0.1:8001")
    ap.add_argument("--election-ms", default="300,600", help="S4: election timeout range (ms)")
    ap.add_argument("--heartbeat-ms", type=float, default=50, help="S4: leader heartbeat interval (ms)")
    ap.add_argument("--peer-delay-ms", default="", help="S4: emulated one-way delay to peers, e.g. o2=5,o3=20")
    ap.add_argument("--commit-timeout", type=float, default=3.0)
    ap.add_argument("--fsync", action="store_true", help="S4: fsync the Raft log on every append")
    ap.add_argument("--state-dir", default=None, help="S4: where the Raft log is stored (default --out)")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [manager] %(name)s: %(message)s")
    m = Manager(a)
    try:
        signal.signal(signal.SIGTERM, lambda *_: m.shutdown.set())
    except (ValueError, AttributeError):
        pass
    asyncio.run(m.main())


if __name__ == "__main__":
    main()

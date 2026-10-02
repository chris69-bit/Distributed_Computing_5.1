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

Run:  python -m telerm.manager --port 8000 --topology topology.json --policy edge_first --out results/run1
"""
import argparse
import asyncio
import json
import logging
import os
import signal

from .monitor import ResourceMonitor
from .naming import RegistryClient
from .protocol import STATS, call, make_server_callback, mono, now
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
    async def request(self, r: dict) -> dict:
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
            for k in ("work_kb", "backlogged", "queue_limit"):      # S2 traffic settings, if given
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

    async def _route_del(self, sid):
        try:
            await self.reg.rpc({"type": "ROUTE_DEL", "sid": sid}, timeout=3.0)
        except Exception:
            pass

    # ------------------------------------------------------------------ messages
    async def handle(self, msg):
        t = msg.get("type")
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
        if full:
            out["timeseries"] = self.timeseries
            out["allocations"] = {sid: {k: (v.to_dict() if isinstance(v, Resources) else v)
                                        for k, v in a.items() if k != "reports"} for sid, a in self.alloc.items()}
            out["events"] = self.events
        return out

    async def main(self):
        server = await asyncio.start_server(make_server_callback(self.handle), self.bind, self.port)
        if self.reg:
            await self.reg.register(name="orchestrator", kind="service", host=self.a.host, port=self.port,
                                    location=self.a.location, tier="core")
        log.info("%s pid %d on %s:%d (placement=%s)", "orchestrator" if self.reg else "resource manager",
                 os.getpid(), self.bind, self.port, self.policy)
        loops = [asyncio.create_task(self.failure_detector()), asyncio.create_task(self.sampler())]
        await self.shutdown.wait()
        for l in loops:
            l.cancel()
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

"""Registry service (S3): the system's phone book.

Before S3 the manager did everything. In S3 these jobs moved into their own service:
  * registration  - sites and services announce themselves (name, address, tier, capacity)
  * liveness      - every node sends a HEARTBEAT each second; silence for hb_timeout => SUSPECT
  * naming        - LOOKUP "orchestrator" -> host:port, so nodes only need the registry's address
  * routes        - which site hosts service instance S00007 (set by the orchestrator, read by
                    the gateways), so users never need to know where an instance runs

S4: REGISTER may carry a "term". The orchestrator cluster's leader registers the alias
"orchestrator" with its Raft term; a REGISTER with an older term than the stored one is
refused (fencing), so a deposed leader can never steal the name back.

Messages: REGISTER, HEARTBEAT, LOOKUP, LIST_SITES, ROUTE_SET, ROUTE_DEL, RESOLVE, STATUS, SHUTDOWN

Run:  python -m telerm.registry --port 8010
"""
import argparse
import asyncio
import logging
import os
import signal
from collections import defaultdict

from .monitor import ResourceMonitor
from .protocol import STATS, call, make_server_callback, mono, now

log = logging.getLogger("registry")


class Registry:
    def __init__(self, a):
        self.port, self.bind, self.hb_timeout = a.port, a.bind, a.hb_timeout
        self.nodes: dict[str, dict] = {}         # name -> record (sites and services)
        self.routes: dict[str, dict] = {}        # instance id -> {site, address, link_ms}
        self.events: list = []
        self.started = now()
        self.shutdown = asyncio.Event()
        self.msg_count = defaultdict(int)        # per message type
        self.msg_time = defaultdict(float)       # seconds spent handling, per type
        self.monitor = None
        self.samples: list = []

    def event(self, what, **kw):
        self.events.append({"t": round(now() - self.started, 3), "event": what, **kw})
        if len(self.events) > 5000:
            self.events = self.events[-5000:]

    async def handle(self, msg):
        t = msg.get("type", "?")
        t0 = mono()
        try:
            return self._handle(t, msg)
        finally:
            self.msg_count[t] += 1
            self.msg_time[t] += mono() - t0

    def _handle(self, t, msg):
        if t == "REGISTER":
            name = msg["name"]
            old = self.nodes.get(name)
            term = msg.get("term")                 # S4: the leader alias carries its Raft term
            if term is not None and old is not None and (old.get("term") or -1) > term:
                self.event("stale_register_refused", name=name, term=term, current=old.get("term"))
                return {"ok": False, "error": "STALE_TERM", "current_term": old.get("term")}
            rec = {"term": term, "name": name, "kind": msg.get("kind", "site"), "host": msg["host"], "port": msg["port"],
                   "tier": msg.get("tier"), "location": msg.get("location", name), "pid": msg.get("pid"),
                   "capacity": msg.get("capacity"), "status": "ALIVE", "last_hb": now(),
                   "registered_at": now(), "resources": None}
            self.nodes[name] = rec
            self.event("registered", name=name, kind=rec["kind"], term=term,
                       address=f"{rec['host']}:{rec['port']}")
            log.info("%s %s registered at %s:%s", rec["kind"], name, rec["host"], rec["port"])
            return {"ok": True}
        if t == "HEARTBEAT":
            rec = self.nodes.get(msg.get("name"))
            if rec is None:
                return {"ok": False, "error": "unknown node, register first"}
            rec["last_hb"] = now()
            if "resources" in msg:
                rec["resources"] = msg["resources"]
            if rec["status"] != "ALIVE":
                rec["status"] = "ALIVE"
                self.event("alive_again", name=rec["name"])
            return {"ok": True}
        if t == "LOOKUP":
            rec = self.nodes.get(msg.get("name"))
            if rec is None:
                return {"ok": False, "error": "unknown name"}
            return {"ok": True, "address": f"{rec['host']}:{rec['port']}", "status": rec["status"],
                    "location": rec["location"], "tier": rec["tier"], "term": rec.get("term")}
        if t == "LIST_SITES":
            return {"ok": True, "sites": [
                {k: r[k] for k in ("name", "host", "port", "tier", "location", "capacity", "status", "pid")}
                for r in self.nodes.values() if r["kind"] == "site"]}
        if t == "ROUTE_SET":
            self.routes[msg["sid"]] = {"site": msg["site"], "address": msg["address"],
                                       "link_ms": msg.get("link_ms", 0.0), "ingress": msg.get("ingress")}
            return {"ok": True}
        if t == "ROUTE_DEL":
            self.routes.pop(msg.get("sid"), None)
            return {"ok": True}
        if t == "RESOLVE":
            r = self.routes.get(msg.get("sid"))
            return {"ok": True, **r} if r else {"ok": False, "error": "unknown instance"}
        if t == "STATUS":
            return {"ok": True, "status": self.status(full=msg.get("full", False))}
        if t == "SHUTDOWN":
            self.shutdown.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown message type {t}"}

    def status(self, full=False):
        out = {"uptime_s": round(now() - self.started, 2), "pid": os.getpid(),
               "nodes": {n: {k: r[k] for k in ("kind", "host", "port", "tier", "location", "status")}
                         for n, r in self.nodes.items()},
               "routes": len(self.routes), "comm": dict(STATS),
               "messages": dict(self.msg_count),
               "handle_ms_mean": {k: round(1000 * self.msg_time[k] / self.msg_count[k], 4)
                                  for k in self.msg_count if self.msg_count[k]}}
        if full:
            out["events"] = self.events
            out["samples"] = self.samples
        return out

    async def failure_detector(self):
        while not self.shutdown.is_set():
            t = now()
            for name, r in self.nodes.items():
                if r["status"] == "ALIVE" and t - r["last_hb"] > self.hb_timeout:
                    r["status"] = "SUSPECT"
                    self.event("suspect", name=name)
                    log.warning("%s SUSPECT (no heartbeat for %.1f s)", name, t - r["last_hb"])
            await asyncio.sleep(0.5)

    async def self_monitor(self):
        while not self.shutdown.is_set():
            await asyncio.sleep(1.0)
            s = self.monitor.sample()
            s.update({"wall": round(now(), 3), "nodes": len(self.nodes), "messages": sum(self.msg_count.values())})
            self.samples.append(s)
            if len(self.samples) > 3600:
                self.samples = self.samples[-3600:]
            tel = self.nodes.get("telemetry")              # report itself like every other node
            if tel and tel["status"] == "ALIVE":
                try:
                    await call(tel["host"], tel["port"], {"type": "REPORT", "node": "registry", "kind": "service",
                                                          "ts": now(), "sample": s}, timeout=2.0)
                except Exception:
                    pass

    async def main(self):
        self.monitor = ResourceMonitor()
        server = await asyncio.start_server(make_server_callback(self.handle), self.bind, self.port)
        log.info("registry pid %d on %s:%d", os.getpid(), self.bind, self.port)
        loops = [asyncio.create_task(self.failure_detector()), asyncio.create_task(self.self_monitor())]
        await self.shutdown.wait()
        for l in loops:
            l.cancel()
        server.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="TeleRM registry service")
    ap.add_argument("--port", type=int, default=8010)
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--hb-timeout", type=float, default=3.0)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [registry] %(name)s: %(message)s")
    r = Registry(a)
    try:
        signal.signal(signal.SIGTERM, lambda *_: r.shutdown.set())
    except (ValueError, AttributeError):
        pass
    asyncio.run(r.main())


if __name__ == "__main__":
    main()

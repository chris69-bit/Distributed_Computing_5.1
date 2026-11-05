"""Edge API gateway (S3): the front door of TeleRM at each edge site.

Users (here: the traffic generator) only ever talk to the gateway of the edge they are
attached to. They never need to know where anything runs.

  REQUEST -> forwarded to the orchestrator (found through the registry)
  TASK    -> the gateway finds which site hosts the instance (route cache, else ask the
             registry), forwards the task over a persistent channel, returns the reply

Link delay: when --delay-mode emulate, the gateway waits the one-way delay of the path
edge -> hosting site before forwarding the task, and again before returning the reply.
So a task for an instance in the cloud really takes 2 x (edge->core + core->cloud) longer.
This emulates the network in software (the test machine's kernel has no netem).

Run:  python -m telerm.gateway --name gw-A --location edge-A --port 8201 --registry 127.0.0.1:8010
"""
import argparse
import asyncio
import logging
import os
import signal

from .monitor import ResourceMonitor
from .naming import RegistryClient, split_addr
from .protocol import STATS, Channel, make_server_callback, mono, now

log = logging.getLogger("gateway")


class Gateway:
    def __init__(self, a):
        self.a = a
        self.reg = RegistryClient(a.registry)
        self.routes: dict[str, dict] = {}            # instance id -> route (cache)
        self.channels: dict[str, Channel] = {}       # site address -> persistent channel
        self.chan_lock = asyncio.Lock()
        self.counters = {"requests": 0, "tasks": 0, "tasks_ok": 0, "tasks_failed": 0,
                         "cache_hits": 0, "cache_misses": 0}
        self.shutdown = asyncio.Event()

    async def channel(self, address):
        ch = self.channels.get(address)
        if ch is None or ch.reader_task is None or ch.reader_task.done():
            async with self.chan_lock:
                ch = self.channels.get(address)
                if ch is None or ch.reader_task is None or ch.reader_task.done():
                    ch = Channel(*split_addr(address))
                    await ch.connect()
                    self.channels[address] = ch
        return ch

    async def route(self, sid):
        r = self.routes.get(sid)
        if r is not None:
            self.counters["cache_hits"] += 1
            return r
        self.counters["cache_misses"] += 1
        r = await self.reg.rpc({"type": "RESOLVE", "sid": sid}, timeout=3.0)
        if not r.get("ok"):
            return None
        self.routes[sid] = r
        return r

    async def handle_task(self, msg):
        self.counters["tasks"] += 1
        r = await self.route(msg["sid"])
        if r is None:
            self.counters["tasks_failed"] += 1
            return {"ok": False, "status": "NO_ROUTE"}
        one_way = r.get("link_ms", 0.0) / 1000 if self.a.delay_mode == "emulate" else 0.0
        if one_way:
            await asyncio.sleep(one_way)                      # edge -> hosting site
        t0 = mono()
        try:
            ch = await self.channel(r["address"])
            reply = await ch.request({k: v for k, v in msg.items() if k != "rid"}, timeout=self.a.timeout)
        except Exception as exc:
            self.routes.pop(msg["sid"], None)
            self.channels.pop(r["address"], None)
            self.counters["tasks_failed"] += 1
            return {"ok": False, "status": "UPSTREAM_ERROR", "error": repr(exc)[:80]}
        upstream_ms = (mono() - t0) * 1000
        if reply.get("status") == "NO_INSTANCE":
            self.routes.pop(msg["sid"], None)                 # stale route
        if one_way:
            await asyncio.sleep(one_way)                      # hosting site -> edge
        self.counters["tasks_ok" if reply.get("ok") else "tasks_failed"] += 1
        reply = {k: v for k, v in reply.items() if k != "rid"}
        return {**reply, "site": r["site"], "upstream_ms": round(upstream_ms, 3),
                "link_emulated_ms": round(2000 * one_way, 3)}

    async def handle(self, msg):
        t = msg.get("type")
        if t == "TASK":
            return await self.handle_task(msg)
        if t == "REQUEST":
            self.counters["requests"] += 1
            return await self.reg.call_service("orchestrator", {"type": "REQUEST", "request": msg["request"]},
                                               timeout=10.0)
        if t == "STATUS":
            return {"ok": True, "status": {"name": self.a.name, "pid": os.getpid(), "counters": dict(self.counters),
                                           "routes_cached": len(self.routes), "comm": dict(STATS)}}
        if t == "SHUTDOWN":
            self.shutdown.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown message type {t}"}

    async def main(self):
        a = self.a
        server = await asyncio.start_server(make_server_callback(self.handle), a.bind, a.port)
        await self.reg.register(name=a.name, kind="gateway", host=a.host, port=a.port, location=a.location,
                                tier="edge")
        log.info("gateway %s pid %d on %s:%d (location %s, delay %s)", a.name, os.getpid(), a.bind, a.port,
                 a.location, a.delay_mode)
        mon = ResourceMonitor()

        async def beat():
            while not self.shutdown.is_set():
                await asyncio.sleep(1.0)
                sample = {**mon.sample(), **self.counters, "msgs_sent": STATS["msgs_sent"],
                          "msgs_recv": STATS["msgs_recv"]}
                await self.reg.heartbeat()
                try:
                    await self.reg.call_service("telemetry", {"type": "REPORT", "node": a.name, "kind": "gateway",
                                                              "ts": now(), "sample": sample}, timeout=2.0)
                except Exception:
                    pass
        task = asyncio.create_task(beat())
        await self.shutdown.wait()
        task.cancel()
        server.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="TeleRM edge API gateway")
    ap.add_argument("--name", required=True)
    ap.add_argument("--location", required=True, help="the edge site this gateway serves, e.g. edge-A")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--registry", default="127.0.0.1:8010")
    ap.add_argument("--delay-mode", default="emulate", choices=["emulate", "none"])
    ap.add_argument("--timeout", type=float, default=2.0)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{a.name}] %(name)s: %(message)s")
    g = Gateway(a)
    try:
        signal.signal(signal.SIGTERM, lambda *_: g.shutdown.set())
    except (ValueError, AttributeError):
        pass
    asyncio.run(g.main())


if __name__ == "__main__":
    main()

"""Telemetry service (S3): collects measurements from every node.

In S2 the manager stored the CPU/memory samples. In S3 every node pushes a REPORT once a
second to this separate service. It lives in the cloud tier because nothing waits for it:
monitoring is off the critical path of admission and traffic.

Messages: REPORT {node, kind, sample}, QUERY {since}, STATUS, SHUTDOWN
Writes telemetry.jsonl (one line per report) into --out.

Run:  python -m telerm.telemetry --port 8020 --registry 127.0.0.1:8010 --out results/run
"""
import argparse
import asyncio
import json
import logging
import os
import signal
from collections import defaultdict

from .monitor import ResourceMonitor
from .naming import RegistryClient
from .protocol import STATS, make_server_callback, now

log = logging.getLogger("telemetry")


class Telemetry:
    def __init__(self, a):
        self.a = a
        self.series: dict[str, list] = defaultdict(list)
        self.kinds: dict[str, str] = {}
        self.reports = 0
        self.shutdown = asyncio.Event()
        os.makedirs(a.out, exist_ok=True)
        self.file = open(os.path.join(a.out, "telemetry.jsonl"), "a", buffering=1)

    async def handle(self, msg):
        t = msg.get("type")
        if t == "REPORT":
            node = msg["node"]
            s = {**msg.get("sample", {}), "wall": msg.get("ts", now())}
            self.series[node].append(s)
            if len(self.series[node]) > 3600:
                self.series[node] = self.series[node][-3600:]
            self.kinds[node] = msg.get("kind", "?")
            self.reports += 1
            self.file.write(json.dumps({"node": node, "kind": self.kinds[node], **s}) + "\n")
            return {"ok": True}
        if t == "QUERY":
            since = float(msg.get("since", 0))
            return {"ok": True, "kinds": self.kinds,
                    "series": {n: [s for s in v if s["wall"] >= since] for n, v in self.series.items()}}
        if t == "STATUS":
            return {"ok": True, "status": {"pid": os.getpid(), "nodes": dict(self.kinds),
                                           "reports": self.reports, "comm": dict(STATS)}}
        if t == "SHUTDOWN":
            self.shutdown.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown message type {t}"}

    async def main(self):
        a = self.a
        server = await asyncio.start_server(make_server_callback(self.handle), a.bind, a.port)
        reg = RegistryClient(a.registry)
        await reg.register(name="telemetry", kind="service", host=a.host, port=a.port, location=a.location)
        mon = ResourceMonitor()
        log.info("telemetry pid %d on %s:%d", os.getpid(), a.bind, a.port)

        async def beat():
            while not self.shutdown.is_set():
                await asyncio.sleep(1.0)
                sample = mon.sample()
                await self.handle({"type": "REPORT", "node": "telemetry", "kind": "service", "sample": sample})
                await reg.heartbeat()
        task = asyncio.create_task(beat())
        await self.shutdown.wait()
        task.cancel()
        server.close()
        self.file.close()


def main(argv=None):
    ap = argparse.ArgumentParser(description="TeleRM telemetry service")
    ap.add_argument("--port", type=int, default=8020)
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--host", default="127.0.0.1", help="address others use to reach this service")
    ap.add_argument("--registry", default="127.0.0.1:8010")
    ap.add_argument("--location", default="cloud-1")
    ap.add_argument("--out", default="results/manual")
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [telemetry] %(name)s: %(message)s")
    t = Telemetry(a)
    try:
        signal.signal(signal.SIGTERM, lambda *_: t.shutdown.set())
    except (ValueError, AttributeError):
        pass
    asyncio.run(t.main())


if __name__ == "__main__":
    main()

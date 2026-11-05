"""Light participant node for the S5 experiments: a site's resource manager (ledger + WAL +
in-doubt resolver) without the stride scheduler and worker pool.

It runs the same Participant class as telerm/site.py, so the experiments exercise the real
protocol code, but one instance costs almost no CPU. That keeps the measurements about the
commit protocol, not about packet processing.

    python -m telerm.txnsite --id site0 --port 8920 --registry 127.0.0.1:8910 --cpu 4000 --wal-dir results/x
"""
import argparse
import asyncio
import logging
import os
import signal

from .naming import RegistryClient
from .protocol import NODE, make_server_callback, now
from .resources import Resources
from .txn import Participant

log = logging.getLogger("txnsite")


async def main_async(a):
    NODE["name"] = a.id
    reg = RegistryClient(a.registry)
    timers: dict = {}
    stop = asyncio.Event()

    async def notify_exit(sid):
        try:
            await reg.call_service("orchestrator", {"type": "INSTANCE_EXIT", "sid": sid, "site": a.id},
                                   timeout=2.0, retry_s=20.0)
        except Exception:
            pass

    def start_instance(sid, part, remaining):
        async def run():
            await asyncio.sleep(remaining)
            await part_obj.exit_instance(sid)
        timers[sid] = asyncio.create_task(run())

    async def coord_call(msg):
        return await reg.call_service("orchestrator", msg, timeout=1.0, retry_s=0.0)

    part_obj = Participant(a.id, Resources(a.cpu, a.mem, a.bw), a.wal_dir, fsync=a.fsync, delay_ms=a.delay_ms,
                           indoubt_s=a.indoubt_s, coord_call=coord_call, start_instance=start_instance,
                           notify_exit=notify_exit, vote_no=a.vote_no, race=a.race,
                           discipline=a.discipline, aging_s=a.aging_s)

    async def handle(m):
        if m.get("type") == "SHUTDOWN":
            stop.set()
            return {"ok": True}
        return await part_obj.handle(m)

    server = await asyncio.start_server(make_server_callback(handle), "127.0.0.1", a.port)
    await reg.register(name=a.id, kind="site", host="127.0.0.1", port=a.port, location=a.location or a.id,
                       tier=a.tier, capacity={"cpu": a.cpu, "mem": a.mem, "bw": a.bw})
    await part_obj.restart_instances()                 # after a crash: resume what the WAL says is running
    asyncio.create_task(part_obj.resolver_loop())
    await stop.wait()
    server.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--registry", required=True)
    ap.add_argument("--tier", default="core")
    ap.add_argument("--location", default=None)
    ap.add_argument("--cpu", type=float, default=4000)
    ap.add_argument("--mem", type=float, default=1e6)
    ap.add_argument("--bw", type=float, default=1e6)
    ap.add_argument("--wal-dir", required=True)
    ap.add_argument("--fsync", action="store_true")
    ap.add_argument("--delay-ms", type=float, default=0.0, help="emulated one-way network delay to this site")
    ap.add_argument("--indoubt-s", type=float, default=2.0)
    ap.add_argument("--race", default="atomic", choices=["atomic", "locked", "unsafe"], help="S6 ledger update mode")
    ap.add_argument("--discipline", default="greedy", choices=["greedy", "fifo", "aging"], help="S6 queue discipline")
    ap.add_argument("--aging-s", type=float, default=1.5)
    ap.add_argument("--vote-no", type=float, default=0.0)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{a.id}] %(name)s: %(message)s")
    try:
        signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
    except Exception:
        pass
    asyncio.run(main_async(a))


if __name__ == "__main__":
    main()

"""Site node (edge or core telecom site).

Responsibilities (local 'kernel'):
  * registers its resource capacity with the resource manager, sends heartbeats
  * creates service-instance processes on ALLOCATE, resizes them on RESIZE,
    terminates them when their holding time ends or on RELEASE
  * enforces CPU allocations with a stride scheduler over a pool of worker OS processes
  * S2: accepts traffic TASKs for its instances, queues them, runs them on the workers and
    replies with how long each task waited and how long it took to process
  * S2: measures its real CPU and memory use (ResourceMonitor) and reports it in heartbeats

Run:  python -m telerm.site --id edge-A --port 8101 --manager 127.0.0.1:8000 --cpu 4000 --mem 4096 --bw 2000 --slots 2
"""
import argparse
import asyncio
import hashlib
import logging
import os
import signal
from concurrent.futures import ProcessPoolExecutor

from .monitor import ResourceMonitor
from .process import Instance, State, Task
from .protocol import STATS, call, make_server_callback, mono, now
from .resources import Resources
from .scheduler import StrideScheduler

log = logging.getLogger("site")

_BUF = None
_MAX_KB = 16 * 1024


def process_batch(work_kb: int) -> int:
    """One unit of 'traffic processing' executed in a worker OS process.
    Hashing work_kb kilobytes stands in for packet processing / encoding work."""
    global _BUF
    if _BUF is None:
        _BUF = os.urandom(_MAX_KB * 1024)
    n = max(1, min(int(work_kb), _MAX_KB)) * 1024
    hashlib.sha256(memoryview(_BUF)[:n]).digest()
    return os.getpid()


def jain(xs):
    xs = [x for x in xs if x is not None]
    if not xs or sum(xs) == 0:
        return None
    return (sum(xs) ** 2) / (len(xs) * sum(x * x for x in xs))


class Site:
    def __init__(self, a):
        self.id, self.host, self.port, self.bind = a.id, a.host, a.port, a.bind
        mh, mp = a.manager.rsplit(":", 1)
        self.manager = (mh, int(mp))
        self.capacity = Resources(a.cpu, a.mem, a.bw)
        self.tier = a.tier
        self.work_kb = a.work_kb
        self.hb_interval = a.heartbeat
        self.pool = ProcessPoolExecutor(max_workers=a.slots)
        self.sched = StrideScheduler(a.slots, self.run_job, self.on_exit)
        self.worker_pids: set[int] = set()
        self.fairness_samples: list = []
        self.resource_samples: list = []
        self.monitor = None
        self.counters = {"tasks_received": 0, "tasks_done": 0, "tasks_dropped": 0, "tasks_no_instance": 0}
        self.shutdown = asyncio.Event()
        self.started = now()

    async def run_job(self, inst: Instance, work_kb: int) -> int:
        pid = await asyncio.get_running_loop().run_in_executor(self.pool, process_batch, work_kb)
        self.worker_pids.add(pid)
        return pid

    async def on_exit(self, inst: Instance):
        await self._to_manager({"type": "INSTANCE_EXIT", "site": self.id, "sid": inst.sid,
                                "summary": inst.summary()})

    async def _to_manager(self, msg):
        try:
            return await call(*self.manager, msg, timeout=3.0)
        except Exception as exc:
            log.warning("manager unreachable (%s): %r", msg.get("type"), exc)

    # ---------------------------------------------------------------- S2 data plane
    async def handle_task(self, msg):
        """One traffic task: queue it for its instance, wait until a worker has run it."""
        self.counters["tasks_received"] += 1
        inst = self.sched.instances.get(msg.get("sid"))
        if inst is None or inst.state != State.RUNNING:
            self.counters["tasks_no_instance"] += 1
            return {"ok": False, "status": "NO_INSTANCE"}
        fut = asyncio.get_running_loop().create_future()
        task = Task(seq=int(msg.get("seq", 0)), work_kb=int(msg.get("work_kb", inst.work_kb)),
                    t_arrive=mono(), future=fut)
        if not self.sched.enqueue(inst, task):
            self.counters["tasks_dropped"] += 1
            return {"ok": False, "status": "DROPPED"}         # queue full: packet loss
        reply = await fut
        if reply.get("ok"):
            self.counters["tasks_done"] += 1
        return reply

    # ---------------------------------------------------------------- control plane
    async def handle(self, msg):
        t = msg.get("type")
        if t == "TASK":
            return await self.handle_task(msg)
        if t == "ALLOCATE":
            s = msg["instance"]
            if s["sid"] in self.sched.instances:
                return {"ok": False, "error": "duplicate sid"}
            inst = Instance(sid=s["sid"], service=s["service"], ingress=s["ingress"],
                            demand=Resources.from_dict(s["demand"]), holding_s=float(s["holding_s"]),
                            work_kb=int(s.get("work_kb", self.work_kb)),
                            backlogged=bool(s.get("backlogged", True)),
                            queue_limit=int(s.get("queue_limit", 64)))
            self.sched.create(inst)
            log.info("instance %s (%s) running, cpu=%.0f, %s", inst.sid, inst.service, inst.demand.cpu,
                     "backlogged" if inst.backlogged else "traffic-driven")
            return {"ok": True}
        if t == "RESIZE":
            ok = self.sched.resize(msg["sid"], float(msg["cpu"]))
            if ok:
                inst = self.sched.instances[msg["sid"]]
                inst.demand = Resources(float(msg["cpu"]), inst.demand.mem, inst.demand.bw)
            return {"ok": ok}
        if t == "RELEASE":
            return {"ok": self.sched.terminate(msg["sid"])}
        if t == "STATUS":
            return {"ok": True, "status": self.status()}
        if t == "SHUTDOWN":
            self.shutdown.set()
            return {"ok": True}
        return {"ok": False, "error": f"unknown message type {t}"}

    def status(self):
        fs = [f for f in self.fairness_samples if f is not None]
        return {"site": self.id, "pid": os.getpid(), "uptime_s": round(now() - self.started, 2),
                "scheduler": self.sched.snapshot(), "worker_pids": sorted(self.worker_pids),
                "fairness_mean": round(sum(fs) / len(fs), 4) if fs else None, "fairness_samples": len(fs),
                "counters": dict(self.counters), "resources": self.resource_samples[-600:],
                "comm": dict(STATS), "instances": [i.summary() for i in self.sched.instances.values()]}

    async def heartbeat_loop(self):
        last_busy, last_t = 0.0, now()
        while not self.shutdown.is_set():
            try:
                await asyncio.wait_for(self.shutdown.wait(), timeout=self.hb_interval)
                break
            except asyncio.TimeoutError:
                pass
            t = now()
            dt = max(1e-6, t - last_t)
            util = (self.sched.busy_s - last_busy) / (self.sched.slots * dt)
            last_busy, last_t = self.sched.busy_s, t
            per_inst, norm = [], []
            for inst in self.sched.instances.values():
                if inst.state != State.RUNNING:
                    continue
                d = inst.jobs_done - inst.last_jobs
                inst.last_jobs = inst.jobs_done
                per_inst.append({"sid": inst.sid, "cpu": inst.tickets, "jobs": d, "jobs_per_s": round(d / dt, 2),
                                 "queue": len(inst.queue)})
                if now() - inst.created_at > dt and inst.backlogged:
                    norm.append(d / inst.tickets)
            fair = jain(norm) if len(norm) >= 2 else None
            if fair is not None and util > 0.9:
                self.fairness_samples.append(fair)
            res = self.monitor.sample()
            res.update({"t": round(t, 3), "slot_util": round(min(1.0, util), 3),
                        "queued": sum(len(i.queue) for i in self.sched.instances.values()),
                        **self.counters})
            self.resource_samples.append(res)
            await self._to_manager({"type": "HEARTBEAT", "site": self.id, "ts": t,
                                    "slot_utilisation": round(min(1.0, util), 3),
                                    "instances": per_inst, "fairness": fair, "resources": res})

    async def register(self):
        msg = {"type": "REGISTER", "site": self.id, "host": self.host, "port": self.port, "tier": self.tier,
               "capacity": self.capacity.to_dict(), "pid": os.getpid()}
        for _ in range(50):
            try:
                if (await call(*self.manager, msg, timeout=2.0)).get("ok"):
                    return
            except Exception:
                pass
            await asyncio.sleep(0.2)
        raise RuntimeError("manager unreachable")

    async def main(self):
        loop = asyncio.get_running_loop()
        pids = await asyncio.gather(*[loop.run_in_executor(self.pool, process_batch, 1)
                                      for _ in range(self.sched.slots * 2)])
        log.info("site pid %d, worker pids %s", os.getpid(), sorted(set(pids)))
        self.monitor = ResourceMonitor()                 # after the workers exist
        server = await asyncio.start_server(make_server_callback(self.handle), self.bind, self.port)
        await self.register()
        tasks = [asyncio.create_task(self.sched.run()), asyncio.create_task(self.heartbeat_loop())]
        await self.shutdown.wait()
        self.sched.stop()
        server.close()
        for t in tasks:
            t.cancel()
        self.pool.shutdown(cancel_futures=True)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Telecom site node")
    ap.add_argument("--id", required=True)
    ap.add_argument("--tier", default="edge", choices=["edge", "core"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--bind", default="127.0.0.1")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--manager", default="127.0.0.1:8000")
    ap.add_argument("--cpu", type=float, default=4000)
    ap.add_argument("--mem", type=float, default=4096)
    ap.add_argument("--bw", type=float, default=2000)
    ap.add_argument("--slots", type=int, default=2, help="worker OS processes")
    ap.add_argument("--work-kb", type=int, default=8192, help="size of one backlogged (S1) job")
    ap.add_argument("--heartbeat", type=float, default=1.0)
    a = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{a.id}] %(name)s: %(message)s")
    site = Site(a)
    try:
        signal.signal(signal.SIGTERM, lambda *_: site.shutdown.set())
    except (ValueError, AttributeError):
        pass
    asyncio.run(site.main())


if __name__ == "__main__":
    main()

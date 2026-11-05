"""Local CPU scheduler of a site node: stride scheduling (proportional share).

The node has `slots` worker OS processes. Each instance holds tickets equal to its
allocated millicores. Among the instances that HAVE WORK, the one with the smallest pass
value goes next, then its pass grows by its stride:

    stride_i = STRIDE1 / tickets_i
    pass_i  <- pass_i + stride_i

So when several instances compete, each gets CPU turns in proportion to its allocation.
An instance may run at most ceil(cpu_i / 1000) jobs at once.

S2 change: work-conserving. An instance only competes while its queue has tasks (S1
instances were always busy). When an idle instance gets new work, its pass is raised to
the current minimum so it cannot claim "back pay" for the time it was idle.
"""
import asyncio
import logging
import math

from .process import Instance, State
from .protocol import mono, now

log = logging.getLogger("scheduler")


class StrideScheduler:
    def __init__(self, slots: int, runner, on_exit):
        self.slots = max(1, slots)
        self.runner = runner          # async (inst, work_kb) -> worker pid ; executes one job
        self.on_exit = on_exit        # async (inst)
        self.instances: dict[str, Instance] = {}
        self.running = 0
        self.busy_s = 0.0
        self.dispatches = 0
        self.expired = 0
        self._wake = asyncio.Event()
        self._stopped = False

    def _min_pass(self, exclude=None):
        live = [i.pass_value for i in self.instances.values()
                if i.state == State.RUNNING and i.has_work() and i is not exclude]
        return min(live) if live else None

    # ---- process management -------------------------------------------------------
    def create(self, inst: Instance):
        inst.set_tickets(inst.demand.cpu)
        mp = self._min_pass()
        inst.pass_value = mp if mp is not None else 0.0
        inst.expires_at = now() + inst.holding_s
        self.instances[inst.sid] = inst
        inst.transition(State.RUNNING)
        self._wake.set()

    def resize(self, sid: str, cpu: float) -> bool:
        inst = self.instances.get(sid)
        if inst is None or inst.state != State.RUNNING:
            return False
        inst.set_tickets(cpu)
        inst.resizes += 1
        return True

    def terminate(self, sid: str) -> bool:
        inst = self.instances.get(sid)
        if inst is None or inst.state == State.TERMINATED:
            return False
        inst.transition(State.TERMINATED)
        while inst.queue:                                  # tasks still waiting are lost
            task = inst.queue.popleft()
            if not task.future.done():
                task.future.set_result({"ok": False, "status": "NO_INSTANCE"})
        asyncio.create_task(self.on_exit(inst))
        self._wake.set()
        return True

    # ---- S2: traffic arrival ----------------------------------------------------------
    def enqueue(self, inst: Instance, task) -> bool:
        """Put a task in the instance's waiting queue. Returns False if the queue is full."""
        inst.tasks_received += 1
        if len(inst.queue) >= inst.queue_limit:
            inst.tasks_dropped += 1
            return False
        if not inst.queue and not inst.backlogged:         # waking up from idle
            mp = self._min_pass(exclude=inst)
            if mp is not None and inst.pass_value < mp:
                inst.pass_value = mp
        inst.queue.append(task)
        self._wake.set()
        return True

    # ---- S3: deadline-aware dropping -----------------------------------------------------
    def _purge_expired(self):
        """Discard tasks that waited past their deadline BEFORE spending CPU on them.
        Without this, an overloaded site keeps processing tasks whose clients already gave up,
        and useful throughput collapses towards zero (seen in the first scale-out test)."""
        t = mono()
        for inst in self.instances.values():
            while inst.queue and inst.queue[0].deadline_s and t - inst.queue[0].t_arrive > inst.queue[0].deadline_s:
                task = inst.queue.popleft()
                inst.tasks_expired += 1
                self.expired += 1
                if not task.future.done():
                    task.future.set_result({"ok": False, "status": "EXPIRED",
                                            "queue_ms": round((t - task.t_arrive) * 1000, 3)})

    # ---- dispatcher -------------------------------------------------------------------
    def _eligible(self):
        return [i for i in self.instances.values()
                if i.state == State.RUNNING and i.has_work()
                and i.running_jobs < max(1, math.ceil(i.tickets / 1000))]

    async def run(self):
        while not self._stopped:
            t = now()
            for inst in list(self.instances.values()):          # holding time elapsed -> release
                if inst.state == State.RUNNING and inst.expires_at <= t:
                    self.terminate(inst.sid)
            self._purge_expired()
            while self.running < self.slots:
                cands = self._eligible()
                if not cands:
                    break
                inst = min(cands, key=lambda i: (i.pass_value, i.sid))
                inst.pass_value += inst.stride
                inst.running_jobs += 1
                self.running += 1
                self.dispatches += 1
                task = inst.queue.popleft() if inst.queue else None
                asyncio.create_task(self._execute(inst, task))
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=0.05)
            except asyncio.TimeoutError:
                pass

    async def _execute(self, inst: Instance, task):
        start = mono()
        pid, ok = None, True
        try:
            pid = await self.runner(inst, task.work_kb if task else inst.work_kb)
        except Exception as exc:
            ok = False
            log.warning("job for %s failed: %r", inst.sid, exc)
        end = mono()
        dur = end - start
        self.running -= 1
        self.busy_s += dur
        inst.running_jobs -= 1
        if ok:
            inst.jobs_done += 1
            inst.busy_s += dur
            inst.latency_sum_ms += dur * 1000
        if task is not None and not task.future.done():
            task.future.set_result({
                "ok": ok, "status": "DONE" if ok else "FAILED",
                "queue_ms": round((start - task.t_arrive) * 1000, 3),   # waiting in the queue
                "exec_ms": round(dur * 1000, 3),                        # worker processing
                "worker_pid": pid})
        self._wake.set()

    def stop(self):
        self._stopped = True
        self._wake.set()

    def snapshot(self):
        counts = {s.value: 0 for s in State}
        for i in self.instances.values():
            counts[i.state.value] += 1
        return {"slots": self.slots, "running_jobs": self.running, "dispatches": self.dispatches,
                "busy_s": round(self.busy_s, 3), "instances": counts,
                "queued_tasks": sum(len(i.queue) for i in self.instances.values()), "expired": self.expired,
                "allocated_tickets": sum(i.tickets for i in self.instances.values() if i.state == State.RUNNING)}

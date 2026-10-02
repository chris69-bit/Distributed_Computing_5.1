"""Local CPU scheduler of a site node: stride scheduling (proportional share).

Every running instance is backlogged with traffic-processing jobs. The node has
`slots` worker OS processes. Each instance holds tickets equal to its allocated
millicores, so over time it receives CPU in proportion to its allocation:

    stride_i = STRIDE1 / tickets_i
    dispatch the instance with the smallest pass_i, then pass_i += stride_i

An instance may run at most ceil(cpu_i / 1000) jobs at once, so a 1-core allocation
cannot occupy four slots. New instances start at the current minimum pass, so they
neither starve nor monopolise the node.
"""
import asyncio
import logging
import math

from .process import Instance, State
from .protocol import now

log = logging.getLogger("scheduler")


class StrideScheduler:
    def __init__(self, slots: int, runner, on_exit):
        self.slots = max(1, slots)
        self.runner = runner          # async (inst) -> None ; executes one job
        self.on_exit = on_exit        # async (inst)
        self.instances: dict[str, Instance] = {}
        self.running = 0
        self.busy_s = 0.0
        self.dispatches = 0
        self._wake = asyncio.Event()
        self._stopped = False

    def _min_pass(self):
        live = [i.pass_value for i in self.instances.values() if i.state == State.RUNNING]
        return min(live) if live else 0.0

    # ---- process management -------------------------------------------------------
    def create(self, inst: Instance):
        inst.set_tickets(inst.demand.cpu)
        inst.pass_value = self._min_pass()
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
        asyncio.create_task(self.on_exit(inst))
        self._wake.set()
        return True

    # ---- dispatcher -------------------------------------------------------------------
    def _eligible(self):
        return [i for i in self.instances.values()
                if i.state == State.RUNNING and i.running_jobs < max(1, math.ceil(i.tickets / 1000))]

    async def run(self):
        while not self._stopped:
            t = now()
            for inst in list(self.instances.values()):          # holding time elapsed -> release
                if inst.state == State.RUNNING and inst.expires_at <= t:
                    self.terminate(inst.sid)
            while self.running < self.slots:
                cands = self._eligible()
                if not cands:
                    break
                inst = min(cands, key=lambda i: (i.pass_value, i.sid))
                inst.pass_value += inst.stride
                inst.running_jobs += 1
                self.running += 1
                self.dispatches += 1
                asyncio.create_task(self._execute(inst))
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=0.05)
            except asyncio.TimeoutError:
                pass

    async def _execute(self, inst: Instance):
        start = now()
        try:
            await self.runner(inst)
            ok = True
        except Exception as exc:
            ok = False
            log.warning("job for %s failed: %r", inst.sid, exc)
        dur = now() - start
        self.running -= 1
        self.busy_s += dur
        inst.running_jobs -= 1
        if ok:
            inst.jobs_done += 1
            inst.busy_s += dur
            inst.latency_sum_ms += dur * 1000
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
                "allocated_tickets": sum(i.tickets for i in self.instances.values() if i.state == State.RUNNING)}

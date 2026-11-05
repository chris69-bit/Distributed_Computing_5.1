"""Process model: a telecom service instance (e.g. a vRAN DU, a UPF) is a process that
holds a resource allocation for a holding time.

Life cycle on the node:  ADMITTED -> RUNNING -> TERMINATED   (RESIZE keeps it RUNNING)
Global outcome on the manager: ACCEPTED | BLOCKED, then RELEASED when it ends.

S2: an instance now does real work only when traffic TASKs arrive for it. Each instance
has its own bounded waiting queue. A full queue drops new tasks (that is packet loss).
`backlogged=True` keeps the S1 behaviour (always busy) for the admission demo.
"""
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

from .protocol import now
from .resources import Resources

STRIDE1 = 1_000_000  # stride-scheduling constant


class State(str, Enum):
    ADMITTED = "ADMITTED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"


_ALLOWED = {State.ADMITTED: {State.RUNNING, State.TERMINATED},
            State.RUNNING: {State.TERMINATED},
            State.TERMINATED: set()}


class IllegalTransition(Exception):
    pass


@dataclass
class Task:
    """One unit of traffic work for an instance (e.g. a batch of packets to process)."""
    seq: int
    work_kb: int
    t_arrive: float               # site's monotonic clock
    future: object                # asyncio.Future resolved with the reply


@dataclass
class Instance:
    sid: str                      # global service-instance id, assigned by the manager
    service: str                  # service type, e.g. "UPF"
    ingress: str                  # site where its users/traffic enter
    demand: Resources             # current allocation
    holding_s: float              # lifetime of the allocation
    work_kb: int = 1024           # default work per task (S1 backlogged jobs use this)
    backlogged: bool = True       # True = S1 behaviour (always has work)
    queue_limit: int = 64         # S2: max tasks waiting for this instance
    state: State = State.ADMITTED
    created_at: float = field(default_factory=now)
    expires_at: float = 0.0
    tickets: float = 0.0          # = allocated millicores (share of the node's CPU)
    stride: float = 0.0
    pass_value: float = 0.0
    running_jobs: int = 0
    queue: deque = field(default_factory=deque)
    jobs_done: int = 0
    tasks_received: int = 0
    tasks_dropped: int = 0
    busy_s: float = 0.0
    latency_sum_ms: float = 0.0
    last_jobs: int = 0            # for per-interval accounting
    resizes: int = 0
    history: list = field(default_factory=list)

    def set_tickets(self, cpu_millicores: float):
        self.tickets = max(1.0, cpu_millicores)
        self.stride = STRIDE1 / self.tickets

    def has_work(self) -> bool:
        return self.backlogged or bool(self.queue)

    def transition(self, new: State):
        if new not in _ALLOWED[self.state]:
            raise IllegalTransition(f"{self.sid}: {self.state.value} -> {new.value}")
        self.history.append((round(now(), 3), self.state.value, new.value))
        self.state = new

    def summary(self) -> dict:
        life = max(1e-6, now() - self.created_at)
        return {"sid": self.sid, "service": self.service, "state": self.state.value,
                "cpu": self.demand.cpu, "jobs": self.jobs_done,
                "jobs_per_s": round(self.jobs_done / life, 2),
                "mean_job_latency_ms": round(self.latency_sum_ms / self.jobs_done, 2) if self.jobs_done else None,
                "tasks_received": self.tasks_received, "tasks_dropped": self.tasks_dropped,
                "queue_now": len(self.queue), "resizes": self.resizes}

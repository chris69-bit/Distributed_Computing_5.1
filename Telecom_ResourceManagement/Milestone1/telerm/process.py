"""Process model: a telecom service instance (e.g. a vRAN DU, a UPF) is a process that
holds a resource allocation for a holding time.

Life cycle on the node:  ADMITTED -> RUNNING -> TERMINATED   (RESIZE keeps it RUNNING)
Global outcome on the manager: ACCEPTED | BLOCKED, then RELEASED when it ends.
"""
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
class Instance:
    sid: str                      # global service-instance id, assigned by the manager
    service: str                  # service type, e.g. "UPF"
    ingress: str                  # site where its users/traffic enter
    demand: Resources             # current allocation
    holding_s: float              # lifetime of the allocation
    state: State = State.ADMITTED
    created_at: float = field(default_factory=now)
    expires_at: float = 0.0
    tickets: float = 0.0          # = allocated millicores (share of the node's CPU)
    stride: float = 0.0
    pass_value: float = 0.0
    running_jobs: int = 0
    jobs_done: int = 0
    busy_s: float = 0.0
    latency_sum_ms: float = 0.0
    last_jobs: int = 0            # for per-interval accounting
    resizes: int = 0
    history: list = field(default_factory=list)

    def set_tickets(self, cpu_millicores: float):
        self.tickets = max(1.0, cpu_millicores)
        self.stride = STRIDE1 / self.tickets

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
                "resizes": self.resizes}

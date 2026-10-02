"""Resource monitor (S2): measures real CPU and memory use of a node.

A node is its own OS process plus any child processes (a site's worker pool).

  cpu_pct  - CPU used by the node and its children since the last sample.
             100 = one CPU core fully busy, so a site with 2 busy workers can show ~200.
  rss_mb   - resident memory (RAM actually held) of the node and its children, in MB.
  sys_cpu_pct / sys_mem_pct - the whole machine, for context. All nodes on one laptop
             share these.

Uses psutil (pip install psutil). Without psutil, the values are None and nothing breaks.
"""
import os

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None


class ResourceMonitor:
    def __init__(self, pid: int | None = None, include_children: bool = True):
        self.ok = psutil is not None
        self.include_children = include_children
        self.procs: dict[int, "psutil.Process"] = {}
        if self.ok:
            self.root = psutil.Process(pid or os.getpid())
            psutil.cpu_percent(None)                    # prime the system-wide counter
            self._track(self.root)

    def _track(self, p):
        if p.pid not in self.procs:
            try:
                p.cpu_percent(None)                     # prime: first call always returns 0
                self.procs[p.pid] = p
            except psutil.Error:
                pass

    def sample(self) -> dict:
        if not self.ok:
            return {"cpu_pct": None, "rss_mb": None, "sys_cpu_pct": None, "sys_mem_pct": None, "procs": 0}
        if self.include_children:
            try:
                for c in self.root.children(recursive=True):
                    self._track(c)
            except psutil.Error:
                pass
        cpu = rss = 0.0
        for pid, p in list(self.procs.items()):
            try:
                cpu += p.cpu_percent(None)
                rss += p.memory_info().rss
            except psutil.Error:                        # process ended
                self.procs.pop(pid, None)
        return {"cpu_pct": round(cpu, 1), "rss_mb": round(rss / 2**20, 1),
                "sys_cpu_pct": psutil.cpu_percent(None), "sys_mem_pct": psutil.virtual_memory().percent,
                "procs": len(self.procs)}

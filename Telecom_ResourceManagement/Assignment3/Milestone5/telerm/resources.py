"""Resource model: node resources + network (link) resources.

Node resource vector  R = (cpu, mem, bw)
  cpu - millicores (1000 = one core)
  mem - MB
  bw  - Mbit/s of the node's network interface

Network resources: links between sites, each with capacity (Mbit/s) and latency (ms).
A service request enters the network at an ingress site. If it is hosted at another
site, its bandwidth is also reserved on every link of the path ingress -> host.
"""
import heapq
import os
from dataclasses import dataclass, asdict

_EPS = 1e-9

# S3 scalability fix. "single": one shortest-path search from the ingress gives the path to
# every site (O(E log V) per admission). "per_site": the S1/S2 way, one search per candidate
# site (O(V * E log V)), kept so the scalability benchmark can compare the two.
PATH_MODE = os.environ.get("TELERM_PATH_MODE", "single")


@dataclass(frozen=True)
class Resources:
    cpu: float = 0.0
    mem: float = 0.0
    bw: float = 0.0

    def __add__(self, o): return Resources(self.cpu + o.cpu, self.mem + o.mem, self.bw + o.bw)
    def __sub__(self, o): return Resources(self.cpu - o.cpu, self.mem - o.mem, self.bw - o.bw)

    def fits_in(self, o) -> bool:
        return self.cpu <= o.cpu + _EPS and self.mem <= o.mem + _EPS and self.bw <= o.bw + _EPS

    def dominant_share(self, cap) -> float:
        s = [v / c for v, c in ((self.cpu, cap.cpu), (self.mem, cap.mem), (self.bw, cap.bw)) if c > 0]
        return max(s) if s else 0.0

    def clamp(self):
        return Resources(max(0.0, self.cpu), max(0.0, self.mem), max(0.0, self.bw))

    def to_dict(self): return asdict(self)

    @staticmethod
    def from_dict(d): return Resources(float(d.get("cpu", 0)), float(d.get("mem", 0)), float(d.get("bw", 0)))


class NodeResources:
    def __init__(self, capacity: Resources):
        self.capacity = capacity
        self.allocated = Resources()

    @property
    def free(self): return self.capacity - self.allocated

    def can_allocate(self, d): return d.fits_in(self.free)

    def allocate(self, d):
        if not self.can_allocate(d):
            return False
        self.allocated = self.allocated + d
        return True

    def release(self, d): self.allocated = (self.allocated - d).clamp()

    def utilisation(self):
        c, a = self.capacity, self.allocated
        return {k: (getattr(a, k) / getattr(c, k) if getattr(c, k) else 0.0) for k in ("cpu", "mem", "bw")}


class Link:
    def __init__(self, a, b, capacity, latency_ms):
        self.a, self.b = a, b
        self.capacity, self.latency_ms = float(capacity), float(latency_ms)
        self.allocated = 0.0

    @property
    def key(self): return f"{self.a}<->{self.b}"

    @property
    def free(self): return self.capacity - self.allocated


class Topology:
    """Undirected site graph. Paths are shortest by latency (Dijkstra)."""

    def __init__(self):
        self.links: dict[str, Link] = {}
        self.adj: dict[str, list] = {}

    def add_link(self, a, b, capacity, latency_ms):
        link = Link(a, b, capacity, latency_ms)
        self.links[link.key] = link
        self.adj.setdefault(a, []).append((b, link))
        self.adj.setdefault(b, []).append((a, link))

    def path(self, src, dst):
        """Returns (list_of_link_keys, total_latency_ms) or (None, inf)."""
        if src == dst:
            return [], 0.0
        dist, prev, heap = {src: 0.0}, {}, [(0.0, src)]
        while heap:
            d, u = heapq.heappop(heap)
            if u == dst:
                break
            if d > dist.get(u, float("inf")):
                continue
            for v, link in self.adj.get(u, []):
                nd = d + link.latency_ms
                if nd < dist.get(v, float("inf")):
                    dist[v], prev[v] = nd, (u, link)
                    heapq.heappush(heap, (nd, v))
        if dst not in dist:
            return None, float("inf")
        keys, node = [], dst
        while node != src:
            node, link = prev[node]
            keys.append(link.key)
        return list(reversed(keys)), dist[dst]

    def paths_from(self, src):
        """One Dijkstra from src. Returns {node: (link_keys, latency_ms)} for every reachable node."""
        dist, prev, heap = {src: 0.0}, {}, [(0.0, src)]
        while heap:
            d, u = heapq.heappop(heap)
            if d > dist.get(u, float("inf")):
                continue
            for v, link in self.adj.get(u, []):
                nd = d + link.latency_ms
                if nd < dist.get(v, float("inf")):
                    dist[v], prev[v] = nd, (u, link)
                    heapq.heappush(heap, (nd, v))
        out = {src: ([], 0.0)}
        for node in dist:
            if node == src:
                continue
            keys, n = [], node
            while n != src:
                n, link = prev[n]
                keys.append(link.key)
            out[node] = (list(reversed(keys)), dist[node])
        return out

    def path_has_bw(self, keys, bw): return all(self.links[k].free + _EPS >= bw for k in keys)

    def reserve(self, keys, bw):
        for k in keys:
            self.links[k].allocated += bw

    def release(self, keys, bw):
        for k in keys:
            self.links[k].allocated = max(0.0, self.links[k].allocated - bw)

    def utilisation(self):
        return {k: round(l.allocated / l.capacity, 4) if l.capacity else 0.0 for k, l in self.links.items()}


PLACEMENT_POLICIES = ("edge_first", "least_loaded", "best_fit", "tier_aware")

# S3: how far a tier is from the users. tier_aware prefers the FARTHEST tier that still meets
# the latency budget, so scarce edge capacity is kept for services that cannot go anywhere else.
TIER_RANK = {"edge": 0, "core": 1, "cloud": 2}


def evaluate_candidates(demand: Resources, ingress: str, max_latency_ms: float, nodes: dict, topo: Topology):
    """Checks every alive node against node capacity, path bandwidth and latency bound.

    Returns (feasible, reasons). feasible = [(node_id, path_keys, latency_ms)], and
    reasons counts why each rejected node failed ('latency', 'link_bw', 'node_capacity').
    """
    feasible, reasons = [], {}
    paths = topo.paths_from(ingress) if PATH_MODE == "single" else None
    for nid, nr in sorted(nodes.items()):
        keys, lat = paths.get(nid, (None, float("inf"))) if paths is not None else topo.path(ingress, nid)
        if keys is None or lat > max_latency_ms + _EPS:
            reasons["latency"] = reasons.get("latency", 0) + 1
        elif not nr.can_allocate(demand):
            reasons["node_capacity"] = reasons.get("node_capacity", 0) + 1
        elif not topo.path_has_bw(keys, demand.bw):
            reasons["link_bw"] = reasons.get("link_bw", 0) + 1
        else:
            feasible.append((nid, keys, lat))
    return feasible, reasons


def choose(feasible, demand, ingress, nodes, policy, tiers=None):
    if not feasible:
        return None
    if policy == "tier_aware":      # farthest tier within the latency budget, then least loaded there
        tiers = tiers or {}
        return min(feasible, key=lambda f: (-TIER_RANK.get(tiers.get(f[0]), 0),
                                            (nodes[f[0]].allocated + demand).dominant_share(nodes[f[0]].capacity),
                                            f[2]))
    if policy == "edge_first":       # serve at the ingress site if possible, else lowest latency, then least loaded
        return min(feasible, key=lambda f: (f[0] != ingress, f[2],
                                            (nodes[f[0]].allocated + demand).dominant_share(nodes[f[0]].capacity)))
    if policy == "least_loaded":
        return min(feasible, key=lambda f: ((nodes[f[0]].allocated + demand).dominant_share(nodes[f[0]].capacity), f[2]))
    if policy == "best_fit":
        return min(feasible, key=lambda f: ((nodes[f[0]].free - demand).dominant_share(nodes[f[0]].capacity), f[2]))
    raise ValueError(f"unknown placement policy {policy!r}")

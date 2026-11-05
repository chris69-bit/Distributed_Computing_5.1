"""S6 - deadlock reasoning for TeleRM: resource-allocation graph and detection.

Model (Silberschatz / Coffman):
  processes   P_i  = transactions (a chain being installed)
  resources   R_j  = one resource TYPE per (site, dimension): "edge-A/cpu", "core-1/mem" ...
                     A type has many identical units (millicores, MB), so this is the
                     multi-instance case.
  edges       P_i -> R_j   request edge: P_i waits for units of R_j
              R_j -> P_i   assignment edge: P_i holds units of R_j

For single-unit resources a cycle in this graph is necessary AND sufficient for deadlock. For
multi-unit resources a cycle is only NECESSARY (a unit may come free elsewhere), so detection
uses the matrix algorithm instead:

    Work   = Available                        (free units of every resource type)
    Finish_i = (Allocation_i == 0)
    repeat: find i with Finish_i = false and Request_i <= Work
            Work = Work + Allocation_i ; Finish_i = true       (i can finish and give its units back)
    the transactions with Finish_i = false are deadlocked.

Cost O(n^2 * r) for n transactions and r resource types; with one pass per candidate it is cheap
at TeleRM sizes (measured in experiment X5).

A transaction that only holds resources and waits for nothing (for example one already COMMITTED:
it will end by itself when its holding time is over) has Request = 0, so it always finishes and
its units flow back into Work. That is why an overloaded system that is merely slow is not
reported as deadlocked.
"""
_EPS = 1e-9
DIMS = ("cpu", "mem", "bw")


def assemble(snaps):
    """Build (free, alloc, req, ts) from the LOCKSTATE snapshots of all sites.

    free[(site, dim)], alloc[txn][(site, dim)], req[txn][(site, dim)], ts[txn]."""
    free, alloc, req, ts = {}, {}, {}, {}
    for sn in snaps:
        s = sn["site"]
        for d in DIMS:
            free[(s, d)] = float(sn["free"].get(d, 0))
        for t, vec in sn["alloc"].items():
            for d in DIMS:
                if vec.get(d, 0) > _EPS:
                    alloc.setdefault(t, {})[(s, d)] = float(vec[d])
        for t, vec in sn["waits"].items():
            for d in DIMS:
                if vec.get(d, 0) > _EPS:
                    req.setdefault(t, {})[(s, d)] = float(vec[d])
        ts.update(sn.get("ts", {}))
    return free, alloc, req, ts


def detect(free, alloc, req):
    """Matrix detection for several instances per resource type. Returns the sorted deadlocked set."""
    txns = set(alloc) | set(req)
    work = dict(free)
    finish = {t: not alloc.get(t) for t in txns}          # holds nothing: cannot be part of a deadlock
    progress = True
    while progress:
        progress = False
        for t in txns:
            if finish[t]:
                continue
            if all(work.get(r, 0.0) + _EPS >= amt for r, amt in req.get(t, {}).items()):
                for r, amt in alloc.get(t, {}).items():
                    work[r] = work.get(r, 0.0) + amt
                finish[t] = True
                progress = True
    return sorted(t for t in txns if not finish[t])


def wait_for_graph(free, alloc, req, deadlocked=None):
    """Projection on transactions: T_i -> T_j if T_i waits for a resource type that T_j holds and the
    request does not fit in the free units. Used to find the cycle for the report (and for victim choice)."""
    edges = {}
    for t, rq in req.items():
        for r, amt in rq.items():
            if free.get(r, 0.0) + _EPS >= amt:
                continue
            for u, al in alloc.items():
                if u != t and al.get(r, 0) > _EPS:
                    edges.setdefault(t, set()).add(u)
    return edges


def find_cycle(edges):
    """One cycle of the wait-for graph (list of transactions), or []."""
    color, stack = {}, []

    def dfs(u):
        color[u] = 1
        stack.append(u)
        for v in sorted(edges.get(u, ())):
            if color.get(v, 0) == 0:
                c = dfs(v)
                if c:
                    return c
            elif color.get(v) == 1:
                return stack[stack.index(v):] + [v]
        stack.pop()
        color[u] = 2
        return []
    for n in sorted(edges):
        if color.get(n, 0) == 0:
            c = dfs(n)
            if c:
                return c
    return []


def victim_of(deadlocked, alloc, ts, policy="youngest"):
    """Choose which transaction to abort to break the deadlock."""
    if not deadlocked:
        return None
    if policy == "cheapest":                               # least work thrown away: fewest units held
        return min(deadlocked, key=lambda t: (sum(alloc.get(t, {}).values()), -ts.get(t, 0)))
    return max(deadlocked, key=lambda t: ts.get(t, 0))     # youngest = largest timestamp


def rag_edges(free, alloc, req, only=None):
    """(request_edges, assignment_edges) as lists of (from, to, amount) for drawing P_i -> R_j."""
    ts = set(only) if only else None
    rq = [(t, f"{r[0]}/{r[1]}", amt) for t, v in req.items() for r, amt in v.items() if ts is None or t in ts]
    asg = [(f"{r[0]}/{r[1]}", t, amt) for t, v in alloc.items() for r, amt in v.items() if ts is None or t in ts]
    return rq, asg

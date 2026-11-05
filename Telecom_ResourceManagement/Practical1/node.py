#!/usr/bin/env python3
"""Worker node for the ICS 2403 Week 1 distributed OS prototype.

A node owns a resource table (CPU cores, memory MB, bandwidth Mbps), runs the
processes placed on it (one thread per process, simulated work), and answers
JSON-line requests over TCP from the coordinator.
"""
import argparse
import json
import socketserver
import threading
import time

RESOURCES = ("cpu", "mem", "bw")


class ResourceTable:
    """Tracks capacity and usage per resource type. Thread-safe."""

    def __init__(self, capacity):
        self.capacity = {r: int(capacity[r]) for r in RESOURCES}
        self.used = {r: 0 for r in RESOURCES}
        self._lock = threading.Lock()

    def try_allocate(self, demand):
        """Reserve `demand` atomically, or reserve nothing and return False."""
        with self._lock:
            if any(self.used[r] + demand[r] > self.capacity[r] for r in RESOURCES):
                return False
            for r in RESOURCES:
                self.used[r] += demand[r]
            return True

    def release(self, demand):
        with self._lock:
            for r in RESOURCES:
                self.used[r] -= demand[r]

    def snapshot(self):
        with self._lock:
            return {"capacity": dict(self.capacity), "used": dict(self.used)}


class Node:
    """A single compute node: process table plus resource table."""

    def __init__(self, node_id, capacity):
        self.node_id = node_id
        self.resources = ResourceTable(capacity)
        self.processes = {}  # pid -> process record
        self._lock = threading.Lock()

    def create_process(self, pid, demand, work_s):
        with self._lock:
            if pid in self.processes:
                return {"ok": False, "error": "duplicate pid"}
        if not self.resources.try_allocate(demand):
            return {"ok": False, "error": "insufficient resources"}
        with self._lock:
            self.processes[pid] = {
                "pid": pid,
                "demand": dict(demand),
                "state": "RUNNING",
                "started": time.time(),
                "finished": None,
            }
        threading.Thread(target=self._run, args=(pid, work_s), daemon=True).start()
        return {"ok": True, "node": self.node_id}

    def _run(self, pid, work_s):
        time.sleep(work_s)  # simulated work
        with self._lock:
            proc = self.processes[pid]
            proc["state"] = "TERMINATED"
            proc["finished"] = time.time()
            demand = proc["demand"]
        self.resources.release(demand)

    def status(self):
        with self._lock:
            procs = [dict(p) for p in self.processes.values()]
        return {
            "ok": True,
            "node": self.node_id,
            "resources": self.resources.snapshot(),
            "processes": procs,
        }


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        line = self.rfile.readline()
        try:
            req = json.loads(line)
            resp = self.server.dispatch(req)
        except (ValueError, KeyError, TypeError) as exc:
            resp = {"ok": False, "error": f"bad request: {exc!r}"}
        self.wfile.write((json.dumps(resp) + "\n").encode())
        if resp.get("stop_server"):
            threading.Thread(target=self.server.shutdown, daemon=True).start()


class NodeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, addr, node):
        super().__init__(addr, Handler)
        self.node = node

    def dispatch(self, req):
        kind = req["type"]
        if kind == "ping":
            return {"ok": True, "node": self.node.node_id}
        if kind == "create_process":
            demand = {r: int(req["demand"][r]) for r in RESOURCES}
            return self.node.create_process(req["pid"], demand, float(req["work_s"]))
        if kind == "status":
            return self.node.status()
        if kind == "shutdown":
            return {"ok": True, "stop_server": True}
        return {"ok": False, "error": f"unknown request type {kind!r}"}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--cpu", type=int, default=4)
    ap.add_argument("--mem", type=int, default=2048)
    ap.add_argument("--bw", type=int, default=100)
    args = ap.parse_args()

    node = Node(args.id, {"cpu": args.cpu, "mem": args.mem, "bw": args.bw})
    with NodeServer(("127.0.0.1", args.port), node) as server:
        server.serve_forever()


if __name__ == "__main__":
    main()

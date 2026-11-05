#!/usr/bin/env python3
"""Coordinator for the ICS 2403 Week 1 distributed OS prototype.

Starts the worker nodes listed in config.json, admits the processes in
workload.json, places each one on a feasible node (least-loaded first), tracks
completion through node status polls, and writes metrics to results.json.
"""
import argparse
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RESOURCES = ("cpu", "mem", "bw")


def rpc(port, msg, timeout=3.0):
    """Send one JSON-line request to a node; return (reply, round_trip_ms)."""
    t0 = time.perf_counter()
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.sendall((json.dumps(msg) + "\n").encode())
        with sock.makefile("r") as reader:
            reply = json.loads(reader.readline())
    return reply, (time.perf_counter() - t0) * 1000.0


def fits(snapshot, demand):
    cap, used = snapshot["capacity"], snapshot["used"]
    return all(used[r] + demand[r] <= cap[r] for r in RESOURCES)


def load(snapshot):
    """Dominant-resource utilisation of a node, in [0, 1]."""
    cap, used = snapshot["capacity"], snapshot["used"]
    return max(used[r] / cap[r] for r in RESOURCES)


def start_nodes(config, procs):
    for n in config["nodes"]:
        cmd = [
            sys.executable, str(HERE / "node.py"),
            "--id", n["id"], "--port", str(n["port"]),
            "--cpu", str(n["cpu"]), "--mem", str(n["mem"]), "--bw", str(n["bw"]),
        ]
        procs.append(subprocess.Popen(cmd))
    deadline = time.time() + 10
    for n in config["nodes"]:
        while True:
            try:
                rpc(n["port"], {"type": "ping"})
                break
            except OSError:
                if time.time() > deadline:
                    raise RuntimeError(f"node {n['id']} did not start")
                time.sleep(0.1)


def stop_nodes(config, procs):
    for n in config["nodes"]:
        try:
            rpc(n["port"], {"type": "shutdown"})
        except OSError:
            pass
    for p in procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


def run(config, workload, timeout_s):
    nodes = config["nodes"]

    def admissible(demand):
        # A process is admissible only if some node could hold it when empty.
        return any(all(n[r] >= demand[r] for r in RESOURCES) for n in nodes)

    records = {}
    for p in workload:
        demand = {r: int(p[r]) for r in RESOURCES}
        records[p["pid"]] = {
            "pid": p["pid"],
            "name": p["name"],
            "priority": p["priority"],
            "demand": demand,
            "work_s": float(p["work_s"]),
            "submitted_s": float(p["arrival_s"]),
            "state": "PENDING" if admissible(demand) else "REJECTED",
            "node": None,
            "placed_s": None,
            "finished_s": None,
            "create_rpc_ms": None,
        }

    # Higher priority first; ties broken by arrival time, then pid.
    queue = sorted(
        (pid for pid, r in records.items() if r["state"] == "PENDING"),
        key=lambda pid: (-records[pid]["priority"], records[pid]["submitted_s"], pid),
    )
    running = set()
    start = time.time()
    polls = 0

    while queue or running:
        elapsed = time.time() - start
        if elapsed > timeout_s:
            raise TimeoutError(f"workload not finished after {timeout_s}s; "
                               f"queued={queue} running={sorted(running)}")

        # 1. Poll every node for its current state.
        snaps = {}
        for n in nodes:
            snaps[n["id"]], _ = rpc(n["port"], {"type": "status"})
        polls += 1

        # 2. Mark processes that node has finished.
        for snap in snaps.values():
            for proc in snap["processes"]:
                if proc["state"] == "TERMINATED" and proc["pid"] in running:
                    running.discard(proc["pid"])
                    rec = records[proc["pid"]]
                    rec["state"] = "DONE"
                    rec["finished_s"] = round(proc["finished"] - start, 3)

        # 3. Place every arrived, queued process that fits somewhere now.
        for pid in list(queue):
            rec = records[pid]
            if rec["submitted_s"] > elapsed:
                continue
            candidates = [n for n in nodes
                          if fits(snaps[n["id"]]["resources"], rec["demand"])]
            if not candidates:
                continue  # stays queued until a running process frees resources
            target = min(candidates,
                         key=lambda n: load(snaps[n["id"]]["resources"]))
            reply, ms = rpc(target["port"], {
                "type": "create_process",
                "pid": pid,
                "demand": rec["demand"],
                "work_s": rec["work_s"],
            })
            if not reply.get("ok"):
                continue  # node refused; retry on the next poll
            rec.update(state="RUNNING", node=target["id"],
                       placed_s=round(time.time() - start, 3),
                       create_rpc_ms=round(ms, 3))
            queue.remove(pid)
            running.add(pid)
            # Reserve locally so later placements in this pass see the usage.
            used = snaps[target["id"]]["resources"]["used"]
            for r in RESOURCES:
                used[r] += rec["demand"][r]

        time.sleep(0.05)

    done = [r for r in records.values() if r["state"] == "DONE"]
    waits = [r["placed_s"] - r["submitted_s"] for r in done]
    rpcs = [r["create_rpc_ms"] for r in done]
    per_node = {n["id"]: sum(1 for r in done if r["node"] == n["id"]) for n in nodes}
    summary = {
        "processes_total": len(records),
        "processes_done": len(done),
        "processes_rejected": sum(r["state"] == "REJECTED" for r in records.values()),
        "makespan_s": round(max((r["finished_s"] for r in done), default=0.0), 3),
        "mean_queue_wait_s": round(sum(waits) / len(waits), 3) if waits else 0.0,
        "max_queue_wait_s": round(max(waits), 3) if waits else 0.0,
        "mean_create_rpc_ms": round(sum(rpcs) / len(rpcs), 3) if rpcs else 0.0,
        "status_polls": polls,
        "placements_per_node": per_node,
    }
    return records, summary


def print_table(records, summary):
    print(f"{'pid':<5}{'name':<20}{'node':<8}{'state':<10}"
          f"{'submit':>8}{'placed':>8}{'finish':>8}{'wait':>7}")
    for r in sorted(records.values(), key=lambda x: x["pid"]):
        wait = (r["placed_s"] - r["submitted_s"]) if r["placed_s"] is not None else None
        fmt = lambda v: f"{v:8.2f}" if v is not None else f"{'-':>8}"
        wait_s = f"{wait:7.2f}" if wait is not None else f"{'-':>7}"
        print(f"{r['pid']:<5}{r['name']:<20}{str(r['node'] or '-'):<8}{r['state']:<10}"
              f"{fmt(r['submitted_s'])}{fmt(r['placed_s'])}{fmt(r['finished_s'])}{wait_s}")
    print()
    for k, v in summary.items():
        print(f"{k:<24}{v}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default=str(HERE / "config.json"))
    ap.add_argument("--workload", default=str(HERE / "workload.json"))
    ap.add_argument("--out", default=str(HERE / "results.json"))
    ap.add_argument("--timeout", type=float, default=60.0)
    args = ap.parse_args()

    config = json.loads(Path(args.config).read_text())
    workload = json.loads(Path(args.workload).read_text())

    procs = []
    try:
        start_nodes(config, procs)
        records, summary = run(config, workload, args.timeout)
    finally:
        stop_nodes(config, procs)

    Path(args.out).write_text(json.dumps(
        {"config": config, "summary": summary,
         "records": sorted(records.values(), key=lambda x: x["pid"])}, indent=2))
    print_table(records, summary)
    print(f"\nwrote {args.out}")


if __name__ == "__main__":
    main()

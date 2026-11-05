# ICS 2403 – Practical 1

**System theme:** Theme 3 – Distributed Telecom Resource Management System
**Prototype stage:** distributed prototype.

## 1. Files

| File | Purpose |
|---|---|
| `node.py` | Worker node: resource table, process table, simulated process execution, TCP request handler |
| `cluster.py` | Coordinator: starts nodes, admits workload, places processes, polls state, writes metrics |
| `config.json` | Node topology and capacities |
| `workload.json` | 12 telecom processes with priority, arrival time, duration and resource demand |
| `results.json` | Output of the last run |

Run: `python3 cluster.py`

## 2. System Model

**Nodes + Processes + Resources + Communication**

- **Nodes:** `edge-1` (2 CPU, 1024 MB, 50 Mbps), `core-1` and `core-2` (4 CPU, 2048 MB, 200 Mbps each). Each node is a separate OS process listening on `127.0.0.1` on its own port.
- **Processes:** Each telecom function has a PID, a priority, an arrival time, a work duration and a resource demand vector `d = (cpu, mem, bw)`.
- **Resources:** Each node has a capacity vector `C = (cpu, mem, bw)`.
- **Communication:** Newline-delimited JSON over TCP. Request types: `ping`, `create_process`, `status`, `shutdown`.

## 3. Architecture

Each node is an independent OS process, so a node's state is only changed by its own process or by a request from the coordinator. There is no shared memory between nodes.

## 4. Process Model

State machine for each process:

```
PENDING ──(admissible)──► PENDING(queued) ──(placed on node)──► RUNNING ──(work done)──► DONE
   │
   └──(no node could ever hold it)──► REJECTED
```

- **Admission:** A process is admitted only if some node could hold it when empty. Otherwise it is `REJECTED` immediately, so it never blocks the queue.
- **Scheduling:** The ready queue is ordered by priority, then arrival time, then PID. The coordinator scans the whole queue on every tick, so a process that does not fit does not block lower-priority processes that do.
- **Placement:** Among nodes that currently fit the demand, the process goes to the node with the lowest dominant-resource utilisation.
- **Completion:** A node marks a process `TERMINATED` and releases its resources. The coordinator learns this from the next status poll.

## 5. Design Justification

| Decision | Reason |
|---|---|
| Nodes as separate OS processes with TCP | Gives real message passing and real isolation. A shared-memory simulation would hide the communication and failure issues the course is about. |
| Coordinator-driven placement | Simple, and easy to reason about and test. Placement can be made distributed in later weeks (Milestone 4 coordination). |
| Vector resource demand, all-or-nothing allocation | Telecom workloads need CPU, memory and bandwidth together. Partial allocation would leave processes unable to run, so it is avoided. |
| Least-loaded placement | Cheap heuristic that spreads load. It is a baseline for the proposed improvement in later weeks. |
| Reject impossible demands at admission | A process that no node can ever hold would otherwise wait forever and stall progress checks. |
| JSON over TCP | Human-readable and easy to inspect, which helps during debugging. Its overhead is measured in Week 2. |

## 6. Measurements from the Recorded Run

Source: `results.json`, produced by `python3 cluster.py`.

| Metric | Value |
|---|---|
| Processes total / done / rejected | 12 / 11 / 1 |
| Makespan | 9.121 s |
| Mean queue wait (placed − submitted) | 0.716 s |
| Max queue wait | 4.515 s (`video-transcoder`, p05) |
| Mean create-process RPC round trip | 0.544 ms |
| Status polls | 176 |
| Placements per node | edge-1: 1, core-1: 3, core-2: 7 |

Per-process results:

| PID | Name | Node | Submit (s) | Placed (s) | Finish (s) | Wait (s) |
|---|---|---|---|---|---|---|
| p01 | ran-scheduler | edge-1 | 0.00 | 0.00 | 3.00 | 0.00 |
| p02 | signalling-gw | core-1 | 0.00 | 0.00 | 4.00 | 0.00 |
| p03 | packet-classifier | core-2 | 0.00 | 0.00 | 2.00 | 0.00 |
| p04 | billing-batch | core-1 | 0.00 | 0.00 | 5.00 | 0.00 |
| p05 | video-transcoder | core-1 | 0.50 | 5.01 | 9.02 | 4.51 |
| p06 | edge-cache | core-2 | 1.00 | 1.05 | 4.05 | 0.05 |
| p07 | alarm-correlator | core-2 | 1.00 | 1.04 | 3.04 | 0.04 |
| p08 | slice-monitor | core-2 | 1.50 | 1.52 | 4.52 | 0.02 |
| p09 | analytics-job | core-2 | 2.00 | 5.12 | 9.12 | 3.12 |
| p10 | handover-predictor | core-2 | 2.00 | 2.04 | 4.04 | 0.04 |
| p11 | model-trainer | – | 2.50 | – | – | – (rejected: 8 CPU > max 4) |
| p12 | telemetry-agg | core-2 | 3.00 | 3.08 | 5.08 | 0.08 |

**Note on these numbers:** Process work is simulated with `time.sleep`, so latency and makespan reflect the scheduling and polling policy, not real telecom workloads. Timings are wall-clock on one machine and will vary slightly between runs. Week 2 measures performance properly.

## 7. Observed Behaviour

- `video-transcoder` (p05, priority 3) waited 4.51 s, and `analytics-job` (p09, priority 2) waited 3.12 s. Both are large (4 CPU and 3 CPU). Each time capacity freed up, a stream of smaller, higher-priority processes arrived and took the space first. The large processes only fit once a whole core node was empty. This is priority-driven starvation of large jobs, and it is the main weakness of this scheduler.
- `model-trainer` (p11) was rejected at admission, as designed. It does not block the queue.

## 8. Failure-Driven Engineering Log

| Question | Response |
|---|---|
| What changed? | Created the node/coordinator prototype: TCP node servers, resource table, process table, priority queue, least-loaded placement, and metrics output. |
| What failed? | **Observed:** large processes (p05, p09) waited several seconds behind a stream of smaller, higher-priority processes. **Not tested yet:** a node crash or a lost message (see Section 10). |
| Why did it fail? | Priority ordering always picks small jobs first if they fit, and large jobs need a node to be almost empty. Nothing ages a waiting job's priority, so large jobs can be starved. |
| How was it fixed? | Not fixed yet. The planned fix is priority aging, where a process's effective priority rises the longer it waits, or a reservation for large jobs. Planned for Week 2 (performance) and Week 8 (model comparison). |
| What alternative was considered? | Strict FCFS (fair, but head-of-line blocking) versus the current priority-with-skipping scan (no head-of-line blocking, but starvation). Also considered first-fit versus least-loaded. Least-loaded was chosen because it spreads work across nodes. |
| What was learned? | Admission control and skipping blocked jobs prevent stalls, but they trade fairness for throughput. The scheduling policy must be measured, not assumed. |

## 9. Known Limitations and Next Steps

- **Not yet tested:** deliberate node failure. The course needs failure injection in Week 7; Week 1 only shows the normal path.
- **Race in admission:** `create_process` checks for a duplicate PID and then allocates in two separate locked steps. The coordinator is the only sender, so this has not caused a problem, but two concurrent senders could hit it. Week 6 will make this a deliberate test.
- **Central coordinator:** a single point of failure and a bottleneck. Replaced by coordination protocols in Week 4 and fault tolerance in Week 7.
- **Simulated work:** processes sleep instead of computing. Real telecom functions come in Week 2 and later.

## 10. Reproducibility

- **Hardware:** single Linux host, loopback network (`127.0.0.1`). Record CPU model and core count from `lscpu` when reporting.
- **OS:** Linux 6.x (Ubuntu/Debian or similar).
- **Language and version:** Python 3.13.18, standard library only.
- **Topology:** 3 nodes (`config.json`), ports 9101–9103.
- **Workload:** `workload.json` (12 processes, fixed arrival times and durations). No random seeds: the run is deterministic in its inputs, but wall-clock timings vary by about ±50 ms.
- **Command:** `python3 cluster.py` from this directory. Output goes to `results.json`.
- **Timeout:** 60 s by default (`--timeout`).


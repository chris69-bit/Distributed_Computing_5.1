# ICS 2403 – Distributed Computing & Applications
## Milestones 7 and 8: Fault Tolerance and Recovery; Distributed System Models

**Artifacts**
- `sim_w7_w8.py` – simulation and fault-injection harness (Python 3, standard library only)
- `results/results.json` – all raw and aggregated results with the full parameter record
- `results/m7_tables.md`, `results/m8_tables.md` – the tables reproduced below
- `results/run_stdout.txt` – console output of the run that produced the tables

**Reproduce**
```
python3 sim_w7_w8.py --out results --seeds 8 --m8-seeds 5
```
Runtime on the build machine: about 45 s. Every random draw uses an explicit seed (`random.Random(seed)`), so the same command gives the same numbers.

**Scope note.** The system is a session-routing service for a telecom application, modelled as a generic case. Map it to your group's chosen theme (Themes 1–7) before submission. Fault rates and service times are accelerated relative to production. Results are model-based comparisons, not measurements of a real network, and they are not production SLA claims.

---

## Part A – Milestone 7: Fault Tolerance and Recovery (Week 7)

### A.1 Hypothesis
1. Replicating the service and electing a new leader after a detected failure raises availability over a single node that must restart.
2. Quorum replication (majority for election and serving) prevents split-brain, at some cost to availability when faults are frequent.
3. A shorter failure-detection timeout reduces outage length (MTTR) but increases false failovers, so there is an optimum.

### A.2 Experimental design
| Element | Setting |
|---|---|
| Independent variables | Replication configuration (n = 1, 2, 3, 5 with/without quorum); fault rate (MTBF); detection timeout `hb_k` |
| Dependent variables | Availability (A = T_uptime / T_total); MTTR (mean outage duration, from SERVICE DOWN to SERVICE RESTORED); outages per run; false failovers (a healthy leader deposed by suspicion); split-brain exposure (seconds during which a deposed leader that is still up accepts local writes, with no quorum fencing) |
| Controlled variables | Tick = 1 s; horizon 40,000 s per run; heartbeat loss 1% background, 50% during omission bursts; election 2 s; catch-up 5 s; restart 10 s (n = 1); fault mix 40% crash / 20% hang / 25% partition / 15% omission; seeds 1–8 |

### A.3 Fault model
| Fault class | Modelled as | Recovery path |
|---|---|---|
| Crash (fail-stop) | Node stops; repaired after Exp(mean 120 s) | Detection → election → catch-up |
| Service failure | Process alive but not serving; health checks fail; repaired after Exp(mean 60 s) | Same as crash |
| Communication failure | Node unreachable for U(10, 120) s; it keeps running | Detection → election; old leader stays a risk until it is reachable again |
| Omission | Heartbeats dropped with p = 0.5 for U(5, 30) s | Can trigger false suspicion of a healthy leader |
| Node failure | Covered by crash, hang and partition above | – |
| Byzantine | Analysed, not simulated (see A.7) | – |

### A.4 Fault-injection framework
- **Stochastic injection:** each healthy node receives a fault with probability `dt / MTBF` per tick. The fault type is drawn from the mix above.
- **Scripted injection:** `run_m7(..., scripted=[(t, node, kind, duration)])` injects a fault at a fixed second with a fixed duration. The scripted scenarios run with random faults turned off, so the timeline is deterministic.
- **Detection:** a node is suspected after `hb_k` consecutive missed heartbeats. Heartbeats are lost with the background or omission probability, and always lost when the node is down, hung or partitioned.
- **Event log:** FAULT, SUSPECT, LEADER, SERVICE DOWN/RESTORED, and REPAIR events are time-stamped to the second.

### A.5 Results – baseline (MTBF 4000 s, hb_k = 3; mean ± 95% CI over 8 seeds)

| Configuration | Availability | Downtime (min/yr) | Outages/run | MTTR (s) | Max outage (s) | Faults/run | Healthy-leader deposals/run | Split-brain s/run |
|---|---|---|---|---|---|---|---|---|
| A: single node + restart | 98.168 ± 0.883% | 9630 ± 4639 | 8.9 ± 1.9 | 81.9 ± 36.4 | 231 ± 117 | 8.9 ± 2.1 | 2.0 ± 1.3 | 0 |
| B: primary-backup (n=2) | 99.907 ± 0.031% | 491 ± 161 | 9.2 ± 1.9 | 4.0 ± 0.5 | 8 ± 7 | 18.6 ± 3.3 | 1.6 ± 1.3 | 89 ± 35 |
| C: majority quorum (n=3) | 99.867 ± 0.053% | 696 ± 279 | 10.2 ± 2.7 | 5.1 ± 1.6 | 16 ± 13 | 27.4 ± 2.5 | 1.0 ± 1.0 | 0 |
| D: majority quorum (n=5) | 99.904 ± 0.027% | 504 ± 142 | 9.9 ± 2.7 | 3.9 ± 0.2 | 5 ± 1 | 47.6 ± 3.6 | 0.9 ± 0.7 | 0 |

Reading the table:
- A single node with restart is clearly worst. Its MTTR is about 82 s because recovery waits for hardware repair, sync and restart. Its longest outage averages 231 s.
- B, C and D all reach 99.87–99.91% availability. The CIs overlap, so **the difference between them is not statistically significant** at 8 seeds.
- Only B has split-brain exposure (89 s per run). It is the only configuration that serves without a majority, so it stays available by accepting the risk of divergent writes.

### A.6 Results – sweeps

**Fault-rate sweep** (availability, mean ± 95% CI):

| Configuration | MTBF 8000 s | MTBF 4000 s | MTBF 2000 s | MTBF 1000 s |
|---|---|---|---|---|
| A: single node | 98.678 ± 0.925% | 98.168 ± 0.883% | 96.196 ± 1.468% | 91.503 ± 1.762% |
| B: primary-backup (n=2) | 99.955 ± 0.016% | 99.907 ± 0.031% | 99.727 ± 0.071% | 99.242 ± 0.347% |
| C: majority quorum (n=3) | 99.934 ± 0.027% | 99.867 ± 0.053% | 99.592 ± 0.172% | 98.495 ± 0.476% |

Under heavy fault load, C falls below B. A quorum of 2 of 3 is lost more often than a single surviving primary, so the quorum's safety costs availability when faults are frequent.

**Detection-timeout sweep** (configuration C, MTBF 4000 s):

| hb_k | Detection (s) | Availability | MTTR (s) | Healthy-leader deposals/run |
|---|---|---|---|---|
| 1 | 1 | 97.863 ± 0.119% | 2.1 ± 0.1 | 399.1 ± 22.8 |
| 2 | 2 | 99.857 ± 0.058% | 3.6 ± 1.3 | 5.6 ± 1.7 |
| 3 | 3 | 99.867 ± 0.053% | 5.1 ± 1.6 | 1.0 ± 1.0 |
| 5 | 5 | 99.831 ± 0.055% | 7.1 ± 1.7 | 0.5 ± 0.6 |

Shortening detection cuts MTTR, as expected. Below `hb_k = 2` the loss rate dominates: a single dropped heartbeat becomes a suspicion, so leaders are deposed about 400 times per run, and availability falls.

**Scripted failure timelines** (seed 1, no random faults):

Scenario 1 – leader crash at t = 100 s (repair 120 s):

| Configuration | Outages | Longest outage (s) | Split-brain s |
|---|---|---|---|
| A: single node | 1 | 135 | 0 |
| B: primary-backup | 1 | 4 | 0 |
| C: majority quorum (n=3) | 1 | 4 | 0 |
| D: majority quorum (n=5) | 1 | 4 | 0 |

Event timeline, C:
```
t=   100s  FAULT crash injected on node 0
t=   100s  SERVICE DOWN
t=   102s  SUSPECT node 0 (3 missed heartbeats)
t=   102s  leader suspected; failover started
t=   102s  LEADER -> node 1, serving at t+2s
t=   104s  SERVICE RESTORED
t=   220s  REPAIR node 0
```

Scenario 2 – leader partitioned at t = 100 s (60 s):

| Configuration | Outages | Longest outage (s) | Split-brain s |
|---|---|---|---|
| A: single node | 2 | 70 | 0 |
| B: primary-backup | 1 | 4 | 58 |
| C: majority quorum (n=3) | 1 | 4 | 0 |
| D: majority quorum (n=5) | 1 | 4 | 0 |

In B, the partitioned old primary keeps accepting writes for 58 s while the backup serves. Under C and D, the minority side cannot form a quorum, so writes stop there. That is exactly the difference quorum makes.

### A.7 Byzantine analysis (not simulated)
- **Crash tolerance:** a majority-quorum system of n = 2f + 1 replicas tolerates f crash faults. C (n = 3) tolerates one crash. D (n = 5) tolerates two.
- **Byzantine tolerance:** tolerating f arbitrary (Byzantine) faults needs n ≥ 3f + 1. One Byzantine node needs 4 replicas, and an odd-sized crash-tolerant quorum does not provide that. The simulated configurations are therefore crash-fault tolerant only.
- **Implication for telecom:** if a replica can send wrong results (for example, corrupted routing state), crash-quorum replication is insufficient. The system would need signed messages or a Byzantine protocol such as PBFT, at the cost of more replicas and messages.

### A.8 Analysis
- **Hypothesis 1 – supported.** A (98.17%) is well below B, C and D (≥99.87%). Recovery from a single crash takes about 135 s, against 4 s with a standby.
- **Hypothesis 2 – partly supported.** Quorum removes split-brain exposure entirely (C: 0 s, B: 89 s per run). But at n = 3 it does not improve availability at baseline (C 99.867% vs B 99.907%, overlapping CIs), and under heavy faults it is worse (98.50% vs 99.24% at MTBF 1000 s). n = 5 keeps zero split-brain at baseline availability equal to B's, at the price of more replicas and more faults to handle.
- **Hypothesis 3 – supported.** Shorter detection reduces MTTR (2.1 s at k = 1 vs 7.1 s at k = 5), but k = 1 is unstable with 1% background loss. k = 2 to 3 is the operating region; the optimum is around k = 2–3 for this loss rate.

### A.9 Conclusion (Week 7)
For this service, a 2-replica primary-backup with a 3-second detection timeout gives the best availability per replica but accepts split-brain exposure. A 3-replica majority quorum gives safety with statistically similar availability at baseline and lower availability under heavy faults. Use a 5-replica quorum where stronger fault tolerance is required. Detection timeouts should be at least 2 heartbeats to avoid false failovers.

---

## Part B – Milestone 8: Distributed System Models (Week 8)

### B.1 Hypothesis
1. A shared processor pool gives lower tail latency than host-based processing under skewed load.
2. Workstation migration narrows the gap to the pool at a modest message cost.
3. Server-based processing adds network latency and fragments capacity, so it is slower than the pool.
4. The integrated model's advantage depends on how capacity is split between edge and pool, not just on the topology.

### B.2 Experimental design
| Element | Setting |
|---|---|
| Models compared | Host-based; workstation with idle-node migration; processor pool; server-based (4 processors per server); integrated (50/50 edge/pool); integrated (capacity-matched) |
| Workload | Poisson arrivals; exponential service at 100 req/s per processor (mean 10 ms); 60,000 requests per run |
| Load | Offered load 0.7 per processor by default; 0.3–0.85 in the load sweep |
| Skew | Node 0 receives 1.2 × its fair share of attachments, so its utilization is ≈ 0.84 (stable at any scale) |
| Latency-critical share | 40% of requests (used by the integrated models) |
| Network | Pool 1 ms RTT; server 2 ms RTT; integrated pool 1 ms; host and workstation local |
| Migration and spill | Workstation migrates when home-queue wait exceeds 5 ms (3 ms cost); integrated edge spills to pool above 5 ms |
| Failure | Node 0 fails for the middle 20% of the run (one processor removed from the pool; node 0 removed in other models) |
| Common random numbers | Same arrivals, service times and request classes for every model at a given seed |
| Seeds | 1–5 |

### B.3 Mapping of models to telecom roles
| Model | Telecom analogue |
|---|---|
| Host-based | Each base station or edge box runs its own sessions |
| Workstation (+migration) | Edge boxes that shed overflow to idle neighbours |
| Processor pool | Central cloud or core compute pool shared by all sessions |
| Server-based | Regional session servers reached by client-server calls |
| Integrated | Edge handles latency-critical sessions; core pool handles the rest |

### B.4 Results

**Scalability** (load 0.7 per processor; mean ± 95% CI over 5 seeds; selected node counts):

| Nodes | Model | Mean (ms) | p95 (ms) | SLA (50 ms) violations |
|---|---|---|---|---|
| 4 | Host-based | 37.2 ± 2.2 | 117.9 ± 9.5 | 25.17 ± 1.92% |
| 4 | Workstation (+migration) | 15.2 ± 0.4 | 39.4 ± 1.3 | 1.88 ± 0.37% |
| 4 | Processor pool | 14.5 ± 0.4 | 38.2 ± 1.2 | 1.62 ± 0.31% |
| 4 | Server-based | 15.5 ± 0.4 | 39.2 ± 1.2 | 1.79 ± 0.33% |
| 4 | Integrated (capacity-matched) | 237.0 ± 238.0 | 747.8 ± 639.4 | 60.47 ± 6.45% |
| 16 | Host-based | 34.1 ± 0.9 | 103.5 ± 2.4 | 22.85 ± 1.15% |
| 16 | Workstation (+migration) | 12.2 ± 0.2 | 32.3 ± 0.2 | 0.85 ± 0.07% |
| 16 | Processor pool | 11.2 ± 0.1 | 31.2 ± 0.2 | 0.75 ± 0.06% |
| 16 | Server-based | 15.5 ± 0.3 | 39.3 ± 0.9 | 1.82 ± 0.22% |
| 16 | Integrated (50/50 split) | 55.7 ± 36.8 | 170.9 ± 103.8 | 35.41 ± 15.42% |
| 16 | Integrated (capacity-matched) | 12.8 ± 0.4 | 34.2 ± 1.0 | 1.02 ± 0.17% |
| 64 | Host-based | 33.0 ± 1.2 | 98.6 ± 3.2 | 21.91 ± 1.20% |
| 64 | Workstation (+migration) | 11.9 ± 0.1 | 32.0 ± 0.2 | 0.82 ± 0.06% |
| 64 | Processor pool | 11.0 ± 0.1 | 30.9 ± 0.2 | 0.73 ± 0.06% |
| 64 | Server-based | 15.6 ± 0.3 | 39.6 ± 0.7 | 1.91 ± 0.17% |
| 64 | Integrated (capacity-matched) | 11.2 ± 0.2 | 31.2 ± 0.3 | 0.75 ± 0.07% |

The complete 4–64 node table is in `results/m8_tables.md`. At 4 nodes, the two integrated variants coincide because both give 2 edge processors and 2 pool processors. The pool then carries about 60% of the traffic (≈167 req/s on 2 processors, roughly 84% utilization before spill), so both variants perform poorly at that scale. Splitting a 4-processor cluster between edge and pool leaves too little pool capacity to absorb overflow.

**Load sweep** (16 nodes; p95 in ms, SLA violations):

| Load | Host-based | Workstation | Pool | Server | Integrated 50/50 | Integrated cap.-matched |
|---|---|---|---|---|---|---|
| 0.30 | 42.8 / 3.07% | 30.8 / 0.73% | 30.9 / 0.73% | 32.1 / 0.82% | 30.7 / 0.71% | 30.7 / 0.71% |
| 0.50 | 60.3 / 8.31% | 31.4 / 0.77% | 31.0 / 0.73% | 33.4 / 0.95% | 31.8 / 0.79% | 31.0 / 0.73% |
| 0.70 | 103.5 / 22.85% | 32.3 / 0.85% | 31.2 / 0.75% | 39.3 / 1.82% | 170.9 / 35.41% | 34.2 / 1.02% |
| 0.85 | 420.3 / 47.93% | 35.1 / 1.13% | 33.4 / 0.95% | 59.1 / 8.67% | 9100.9 / 72.03% | 716.2 / 69.32% |

Host-based latency degrades fastest with load because the hot node saturates first. The 50/50 integrated split collapses as soon as the pool runs short of capacity; the capacity-matched variant holds up until saturation at 0.85.

**Reliability under a node failure** (16 nodes, load 0.7, node 0 down for the middle 20%):

| Model | Overall success | Success during failure window | p95 (ms) | SLA violations |
|---|---|---|---|---|
| Host-based | 98.52 ± 0.11% | 92.61 ± 0.55% | 102.0 ± 2.6 | 23.69 ± 1.20% |
| Workstation (+migration) | 100.00% | 100.00% | 32.4 ± 0.3 | 0.85 ± 0.07% |
| Processor pool | 100.00% | 100.00% | 31.3 ± 0.2 | 0.75 ± 0.06% |
| Server-based | 100.00% | 100.00% | 44.9 ± 4.2 | 3.37 ± 1.33% |
| Integrated 50/50 | 100.00% | 100.00% | 554.7 ± 276.2 | 54.87 ± 3.80% |
| Integrated cap.-matched | 100.00% | 100.00% | 35.2 ± 1.2 | 1.16 ± 0.27% |

Host-based is the only model that loses requests: a host cannot hand off its sessions, so attached users are dropped during the outage. The other models reroute. Rerouting keeps success at 100%, but latency still rises, most of all in the 50/50 integrated model.

**Cost proxies** (16 processors in every model):

| Model | Processors | Network messages per request |
|---|---|---|
| Host-based | 16 | 0.00 |
| Workstation (+migration) | 16 | 0.92 ± 0.02 |
| Processor pool | 16 | 2.00 |
| Server-based | 16 | 2.00 |
| Integrated 50/50 | 16 | 1.41 |
| Integrated cap.-matched | 16 | 1.46 |

Hardware cost is equal by design. The differences are in messaging. Utilization (about 69.7%) is identical across models because they process the same work; it does not separate the models here.

### B.5 Analysis
- **Hypothesis 1 – supported.** At 16 nodes and load 0.7, host-based p95 is 103.5 ms against 31.2 ms for the pool, and SLA violations are 22.85% against 0.75%. The hot node drives the gap.
- **Hypothesis 2 – supported.** Workstation migration brings p95 to 32.3 ms (vs 31.2 for the pool) at 0.92 extra messages per request. Most of the pooling benefit is recovered by migrating only when a queue grows.
- **Hypothesis 3 – supported.** Server-based p95 is about 8 ms higher than the pool (39.3 vs 31.2 ms) and its SLA violations are about 2.4 times higher (1.82% vs 0.75%). The 2 ms RTT and smaller pools per server account for the gap.
- **Hypothesis 4 – supported.** At load 0.7, the 50/50 integrated split has p95 170.9 ms, against 34.2 ms for the capacity-matched split. The topology is the same in both cases. Only the split differs.
- **Reliability.** Host-based is the only model that drops requests. The pool loses one processor and barely changes. Integrated (capacity-matched) tolerates the failure of an edge node with almost no latency penalty.

### B.6 Recommendation for the telecom system
Use a **capacity-matched integrated model**: edge processors in proportion to the latency-critical share, a core pool for the rest, and workstation-style migration for overflow. It matches the pool's latency at 0.7 load, tolerates a node failure, and needs fewer network messages per request than the pure pool. Its weakness is saturation at very high load (0.85), so capacity planning must keep the edge tier below about 0.8 utilization.

---

## Part C – Failure-Driven Engineering Log (Weeks 7 and 8)

**Week 7 – Fault tolerance and recovery**

| Question | Response |
|---|---|
| What changed? | Added a fault-injection framework (stochastic and scripted), heartbeat-based failure detection, leader election with failover, and quorum rules for serving. Measured availability, MTTR, false failovers and split-brain exposure. |
| What failed? | (1) The single-node configuration reported 140 s of split-brain exposure per run, although there was no second leader. (2) With `hb_k = 1`, the quorum configuration deposed healthy leaders about 399 times per run and availability fell to 97.9%. (3) Quorum with n = 3 did not beat primary-backup on availability at baseline, and was worse under heavy faults (98.50% vs 99.24% at MTBF 1000 s). |
| Why did it fail? | (1) The split-brain counter counted any deposed leader still up, without checking that a second leader existed. (2) A single dropped heartbeat (from 1% background loss) became a suspicion at `hb_k = 1`. (3) Quorum needs 2 of 3 nodes, and n = 3 sees more faults per run (27.4 vs 18.6 for n = 2). Primary-backup keeps serving with one node, which is the same as accepting split-brain exposure. |
| How was it fixed? | (1) The counter now increments only when a new leader coexists with a deposed, still-running leader and the system has no quorum fencing. The single-node split-brain result is now 0. (2) Not changed: this is a finding. Detection should use at least 2 heartbeats at this loss rate. (3) Not a defect. The result is reported as a trade-off, and n = 5 was added to show the alternative. |
| What alternative was considered? | Adaptive (accrual-style) failure detectors that set the timeout from observed heartbeat jitter; not implemented. Quorum with n = 5 gives zero split-brain at baseline availability equal to primary-backup, at the cost of more replicas and faults. |
| What was learned? | Quorum buys safety, not availability, at small n. Detection timeouts must exceed the length of loss bursts, or failover becomes a source of outages. Metric definitions need the same precision as the fault model; a loose split-brain counter reported exposure for a configuration that cannot have split-brain. |

**Week 8 – Distributed system models**

| Question | Response |
|---|---|
| What changed? | Added five distributed-system models (host-based, workstation with migration, processor pool, server-based, integrated) plus a capacity-matched integrated variant, under one workload with common random numbers. Added scalability, load, node-failure and cost experiments. |
| What failed? | (1) In the first run, host-based latency depended on run length: p95 at 16 nodes was about 5.9 s and still rising. (2) The 50/50 integrated split saturated the pool at load 0.7 (p95 170.9 ms vs 34.2 ms when matched). |
| Why did it fail? | (1) A Zipf skew of exponent 0.3 gave the hottest host about 1.5× the average load across 16 nodes. Its utilization exceeded 1, so the queue grew for the whole run. The problem worsens as the cluster grows. (2) The 50/50 split put 60% of the traffic on half the processors. The pool, at 8 processors, ran at about 84% utilization. |
| How was it fixed? | (1) Replaced the Zipf skew with a hot-spot model in which node 0 receives 1.2× its fair share. Its utilization is about 0.84 at every scale, so results are steady-state. The first run's results were discarded. (2) Added the capacity-matched variant (edge processors ≈ 40% of total) and kept the 50/50 split as a labelled comparison. |
| What alternative was considered? | A Zipf skew with a stability check on every station before reporting tail latency. Dynamic capacity splitting between edge and pool, not implemented. |
| What was learned? | Check utilization at each station before reading tail latency. The integrated model's advantage comes from provisioning, not from its topology alone. Utilization is identical across models here because they process the same work, so it does not discriminate; latency, success and message cost do. |

---

## Part D – Research Method Summary

**Milestone 7**
- *Hypothesis:* replication and quorum improve availability and safety. Shorter detection reduces MTTR, up to a point where false failovers dominate.
- *Experimental design:* four replication configurations, three fault rates, four detection timeouts, eight seeds, 40,000 s per run; scripted scenarios for deterministic failure timelines.
- *Implementation:* `run_m7()` in `sim_w7_w8.py`, time-stepped at 1 s.
- *Measurement:* availability, MTTR, outages, false failovers, split-brain exposure, with 95% CIs across seeds.
- *Analysis:* see A.8.
- *Conclusion:* see A.9.

**Milestone 8**
- *Hypothesis:* pooling reduces tail latency under skew; migration narrows the gap; server-based is slower; integrated depends on its split.
- *Experimental design:* six variants, 16 processors each, load 0.3–0.85, node counts 4–64, node-0 failure, five seeds, 60,000 requests per run.
- *Implementation:* `run_m8()` in `sim_w7_w8.py`, event-driven with FCFS multi-server queues.
- *Measurement:* mean, p95, p99 latency; utilization; throughput; success rate; SLA violations; messages per request.
- *Analysis:* see B.5.
- *Conclusion:* see B.6.

**Controlled variables for both milestones:** seeds, processor count, service rate, workload shape, fault mix, timing constants and horizon (all listed in `results/results.json`, key `meta`).

---

## Part E – Reproducibility Record

| Item | Value |
|---|---|
| Language and runtime | Python 3.13.18 (standard library only; no third-party packages) |
| Platform | Linux (exact string in `results.json`, `meta.platform`) |
| Program | `sim_w7_w8.py` |
| Seeds | M7: 1–8; M8: 1–5 |
| Command | `python3 sim_w7_w8.py --out results --seeds 8 --m8-seeds 5` |
| Random number generator | Python `random.Random` (Mersenne Twister), explicit seed per run |
| Confidence intervals | t-distribution, 95%, across seeds |
| Full parameter record | `results/results.json`, key `meta` |

---

## Part F – Limitations

1. **Accelerated faults.** MTBF values of 1,000–8,000 s are much more frequent than real hardware. Downtime in min/year is extrapolated from the simulated fault rate and should not be read as a production SLA.
2. **Abstract network.** Network behaviour is a fixed RTT and a Bernoulli loss rate. There is no queueing in the network, no bandwidth limit, and no jitter.
3. **In-flight requests ignored at failure.** Requests in service when a node fails are counted as completed (optimistic). This understates failure impact for host-based and server-based models.
4. **No Byzantine simulation.** Byzantine behaviour is analysed in A.7 but not simulated.
5. **Simplified election.** The election is a fixed delay, not a message-level protocol. Message complexity and election races are not modelled.
6. **Sensitivity not studied.** Results depend on chosen values for RTT, migration cost, spill threshold and hot-spot factor. These were set to plausible values and were not swept.
7. **Few seeds.** Eight seeds (M7) and five seeds (M8) give CIs that are wide in places. Where CIs overlap, differences are not claimed.
8. **Generic workload.** The workload is not a real telecom trace. Map the findings onto your group's chosen theme and workload before drawing conclusions for it.

---

## Part G – Summary for the Oral Defense (Milestones 7 and 8)

- **Failure → Detection → Recovery → Restoration:** the scripted C timeline shows a leader crash at t = 100 s, suspicion at 102 s, a new leader at 102 s, and service restored at 104 s. Recovery takes about 4 s. The repair of the failed node completes at 220 s, after service is already restored.
- **Quantitative:** availability 99.87–99.91% for replicated configurations against 98.17% for a single node. MTTR 4–5 s against 82 s. Pool p95 31 ms against host-based 104 ms at load 0.7.
- **Research contribution beyond the baseline:** a capacity-matched integrated architecture that holds pool-level latency at 0.7 load with edge-local failover, and a quantified split-brain-versus-availability trade-off for replication sizes 2, 3 and 5.

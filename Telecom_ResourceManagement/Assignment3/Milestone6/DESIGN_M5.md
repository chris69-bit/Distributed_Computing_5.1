---
title: "Milestone 5 – Distributed Transactions"
subtitle: "Theme 3: Distributed Telecom Resource Management System (TeleRM) · ICS 2403 Distributed Computing & Applications · Week 5 · System version S5 (evolved from S4)"
---

# 0. The short version

In S4 the orchestrator became a cluster of three replicas that agree on every decision through Raft. That solved "who decides". It did not solve a different problem: **a decision that must happen on several machines at once**.

A real telecom service is a *chain*. A "5G session slice" is a vRAN-DU at the user's edge site, plus a UPF, plus an IMS-CSCF. Each part needs CPU and memory that belongs to its own site, and the links between the sites need bandwidth. If the DU is installed and the UPF is not, the DU burns CPU and bandwidth for nothing. **Either the whole chain exists, or none of it may.** That is atomicity, and it is the first letter of ACID.

In S4 that guarantee did not exist. The orchestrator's table was consistent, but the sites were told one after the other ("allocate here", "allocate there"). A crash between two messages left half a chain on the network, and nothing remembered that it was half.

In Milestone 5 we added a **transaction subsystem**:

* **Two-Phase Commit (2PC).** The coordinator asks every site to *prepare* (lock the resources and promise to commit if told to), collects the votes, writes the decision, and only then tells every site to *commit* or *abort*.
* **Three-Phase Commit (3PC).** The same, with an extra *pre-commit* round, so that the sites can finish a transaction on their own if the coordinator disappears.
* **A write-ahead log (WAL) at every site**, so a site that crashes in the middle of a transaction knows, after it restarts, what it promised.
* **A coordinator whose decision log is the Raft log from S4.** This is the contribution of this milestone. Classic 2PC has one well-known flaw: if the coordinator dies after the sites have voted, they are stuck holding locks until it comes back (*blocking*). Because our coordinator is a Raft cluster, another replica takes over, reads the decision from the replicated log and finishes the job.
* **Recovery on every side**: a new coordinator finishes unfinished transactions; a restarted site asks what happened; sites can ask each other.
* **A fault-injection framework** (named crash points and dropped messages) and a harness that kills real OS processes with SIGKILL.

In hospital terms (the analogy used in the earlier reports): moving a patient into a new ward needs a bed in the ward, a nurse on the shift and a porter. Nobody wants a bed with no nurse. The *coordinator* phones each department and asks "can you hold one for me?" (prepare). Only when all three say yes does it phone again and say "confirm" (commit). If one says no, it phones the others and says "release" (abort). Each department writes the promise in its own logbook before answering. If the coordinator collapses after the departments said yes, they are all stuck holding a bed. Our fix: the coordinator is a *board of three*, and the decision is written in minutes that all three keep, so another member just takes over.

What we found (details in section 7):

| Question | Answer |
|---|---|
| What does atomic commit cost? | On one machine (no network delay) a 3-site chain takes **4.8 ms** with 2PC against **1.5 ms** with plain sequential allocation: about 3 ms for two Raft commits and the vote round. With 5 ms one-way delay to each site, 2PC takes **15.0 ms** and sequential allocation **33.9 ms**, because 2PC talks to all sites in parallel. |
| 2PC against 3PC? | 3PC costs **one extra round trip** (+11.4 ms at 5 ms delay, 26.4 ms in total) and **1.5× the messages** (6k against 4k for k sites). Peak throughput is **19 % lower** (130 against 161 commits/s). |
| What happens when the coordinator crashes after the votes? | Classic 2PC (one coordinator) kept the sites' resources locked for **6.2 s**, exactly as long as the coordinator was down. With the Raft coordinator the locks were released after **0.93 s**. With 3PC after **1.15 s**. |
| Availability when the coordinator is killed under load | 2PC + Raft(3): **0.970**, recovery in **0.64 s**. Single coordinator: **0.705**, recovery in **6.47 s**. The price: 28 % lower peak throughput (104 against 145 commits/s) because every decision is replicated. |
| Is it consistent? | In 3 × 24 s runs with random SIGKILLs of sites and of the coordinator leader (31 kills, 6 800 requests), **no atomicity violation, no over-committed site, no leaked lock** with 2PC or 3PC. The baseline without transactions left **64 half-installed chains out of 1 026** (6.2 %). |

# 1. What changed from S4 to S5

| Part | S4 (Week 4) | S5 (Week 5) |
|---|---|---|
| Unit of work | One instance, one site | A **transaction** = a chain of parts on one or more sites |
| Who owns the resources | The orchestrator's table (sites are told) | **The site's own ledger** (the orchestrator's table is only a view) |
| Admission | `ADMIT` entry in the Raft log | `TXN_BEGIN`, `TXN_DECIDE`, `TXN_DONE` entries; sites vote |
| Site state | Instances | Instances **and** transaction states, plus a **write-ahead log** |
| Atomic commit | None | **2PC** and **3PC**, selectable per request |
| Coordinator failure | Raft failover (S4), but a half-done chain stays half-done | Raft failover **plus** a recovery loop that finishes or aborts every unfinished transaction |
| Site failure | Not handled | WAL replay, in-doubt resolution (ask coordinator, then peers) |
| Link bandwidth | Reserved at `ADMIT` | Reserved at `TXN_BEGIN` for the whole chain, released on abort or on instance end |
| Testing | Failover experiments | **Crash points inside the protocol**, dropped messages, SIGKILL chaos, invariant checker |

New files: `telerm/txn.py` (participant, coordinator, fault points), `telerm/txnsite.py` (light participant process used by the experiments), `m5_cluster.py` (process harness and invariant checker), `m5_run.py` (experiments), `analyze_m5.py` (tables). Changed: `telerm/manager.py` (TXN messages, Raft apply, recovery loop), `telerm/site.py` (the real site is now a participant).

# 2. The transaction model

## 2.1 What a transaction is

A request now carries a list of **parts**. Each part has a service, an ingress site, a demand `d = (cpu, mem, bw)`, a latency bound and a holding time. The coordinator plans a site and a shortest-latency path for every part (the S1–S4 placement policy, run on a scratch copy of its view so the parts of one chain see each other). A site that receives several parts of the same chain treats them as one request.

The resources involved are of two kinds:

* **Node resources** (CPU, memory) belong to a site. The site is the **participant**, and its **ledger** is the authority.
* **Link bandwidth** belongs to the transport network. It is reserved **inside the coordinator's replicated state machine** when the transaction begins. That is a single-owner resource, so no vote is needed: either the whole chain fits on the links or the request is refused before any site is contacted.

## 2.2 ACID in TeleRM

| Property | What it means here | Mechanism |
|---|---|---|
| **A**tomicity | Every part of a chain is installed, or none | 2PC/3PC; the decision is a single Raft entry (`TXN_DECIDE`); the first decision wins |
| **C**onsistency | No site hands out more than it has; every ledger equals what runs on it; every link reservation belongs to a live transaction | Participant checks capacity before voting YES; invariants (section 6) are checked after every experiment |
| **I**solation | Two chains competing for the same site do not see each other's half-finished state | A PREPARE locks resources in the ledger (the check and the lock are one step with no `await` between them); a competing request gets NO. S6 studies waiting and deadlock |
| **D**urability | A promise survives a crash | Participant WAL (PREPARED, COMMITTED, ABORTED, EXIT); coordinator decisions in the Raft log, replicated on a majority |

## 2.3 The participant state machine

```
            PREPARE (fits)               PRECOMMIT (3PC only)             COMMIT
   (none) ----------------> PREPARED ----------------------> PRECOMMITTED ------> COMMITTED --(holding time)--> EXIT
      |                        |  \                                 |
      | PREPARE (does not fit) |   \------------- ABORT -----------/
      v                        v
   ABORTED <-------------- ABORT
```

Rules that every handler follows:

1. **Write before reply.** A state change is written to the WAL *before* the site answers. A crash point (`p_after_prepared_log`, `p_after_commit_log`) sits right after the write, so we can crash exactly there.
2. **Idempotent.** A repeated PREPARE gets the same vote; a repeated COMMIT or ABORT is acknowledged again. This is what makes retrying safe.
3. **Never contradict.** COMMIT for a transaction that is ABORTED (or ABORT for a COMMITTED one) is refused and counted as a *protocol violation*. The experiments must show zero.
4. **ABORT before PREPARE.** An ABORT for a transaction that was never prepared here is recorded, so a late PREPARE (delayed in the network) is refused.

## 2.4 Coordinator, step by step (2PC)

1. **Plan** on a scratch copy of the view. If a part cannot be placed (latency, node view, link bandwidth) the request is `BLOCKED` immediately, with no message to any site.
2. **`TXN_BEGIN`** is proposed to Raft. When it is committed, the links are reserved and the transaction has an id (`T00007`). From here on every replica knows the transaction exists.
3. **PREPARE** goes to all participant sites in parallel. A site votes YES (resources locked) or NO (reason: `node_capacity`, `local_policy`) or does not answer in time (timeout, counted as NO).
4. **Decision.** All YES means COMMIT, anything else ABORT. The coordinator proposes **`TXN_DECIDE`** to Raft. **This is the commit point**: once a majority of replicas hold the entry, the outcome is fixed forever, even if the coordinator crashes the next microsecond. The client is answered here.
5. **Phase 2.** COMMIT or ABORT goes to every participant; the coordinator repeats it until each one acknowledges (a site may be down).
6. **`TXN_DONE`** is proposed when every participant has acknowledged. A finished transaction needs no more attention.

3PC adds a step between 3 and 4: if all voted YES, **PRECOMMIT** goes to every site and the coordinator waits for acknowledgements before deciding. A site in PRECOMMITTED knows that *every* site voted YES.

## 2.5 What happens when something dies

**A site crashes.** On restart it replays its WAL. Transactions that were PREPARED or PRECOMMITTED are *in doubt*: the locks are rebuilt and the site asks the coordinator (`TXN_STATUS`). COMMITTED instances whose holding time has not run out keep running with the remaining time.

**The coordinator crashes.** Another replica becomes leader (S4). A **recovery loop** on the leader looks at every transaction that is not DONE and is not being driven by a live request:

| What the log says | What the new coordinator does | Why it is safe |
|---|---|---|
| `BEGIN` only (2PC) | Decide **ABORT** (presumed abort) and tell everyone | No participant can have committed without a `DECIDE` |
| `BEGIN` only (3PC) | **Ask the participants** for their states and apply the termination rule below | A log entry cannot say whether PRECOMMIT reached anyone |
| `DECIDE` but not `DONE` | Repeat phase 2 until everyone acknowledges | The decision is final; phase 2 is idempotent |

**Cooperative termination (sites without a coordinator).** A site that is PREPARED and has heard nothing for `indoubt_s` asks the coordinator. If the coordinator cannot be reached it asks the other participants:

| Peers report | 2PC | 3PC |
|---|---|---|
| any COMMITTED | commit | commit |
| any ABORTED or "never prepared" | abort | abort |
| all PREPARED | **blocked, keep asking** | the lowest-id reachable site decides: **ABORT** |
| some PRECOMMITTED, none ABORTED | (not possible in 2PC) | **COMMIT** |

The 2PC "all PREPARED" row is the blocking problem. 3PC removes it for crash failures by making PRECOMMITTED the proof that everybody voted YES.

# 3. Mathematical model

Let `k` be the number of participant sites, `RTT_i` the round trip to site `i`, `P_i` the time site `i` needs to process a message, `C` the time of one Raft commit (about two delays to the closest majority plus 1 to 3 ms) and `T_plan` the planning time (0.1 ms measured).

**Latency to decision**

* Sequential allocation (baseline without transactions): `T_seq ≈ Σ_i (RTT_i + P_i)`. It **grows with k**.
* 2PC: `T_2pc ≈ T_plan + C + max_i(RTT_i + P_i) + C`. The two `C` terms are `TXN_BEGIN` and `TXN_DECIDE`. Because PREPARE goes out in parallel, the time depends on the slowest site, **not on k**.
* 3PC: `T_3pc ≈ T_2pc + max_i(RTT_i + P_i)`. One more round.

Phase 2 (`TXN_DONE`) runs after the client has its answer and takes about one more `max_i(RTT_i + P_i)`.

**Messages (per transaction, excluding Raft traffic)**

* 2PC: `4k` (PREPARE, vote, COMMIT/ABORT, ack). 3PC: `6k`. Sequential baseline: `2k`.
* Raft log entries: 3 (`BEGIN`, `DECIDE`, `DONE`), each costing `2(n−1)` messages for `n` coordinator replicas.

**Blocking time.** For a coordinator crash after the votes, the time the sites stay locked is `T_block = T_detect + T_recover`:

* single coordinator (2PC): `T_recover = downtime of the coordinator`;
* Raft coordinator: `T_recover = election timeout + one recovery round`;
* 3PC without coordinator: `T_recover = indoubt_s + one peer round`.

**Availability and recovery.** `A = T_up / T_total`, `MTTR` = mean time to recovery (the Week 7 definitions). Here a 0.5 s window counts as "up" when it commits at least half of the pre-failure rate.

**Invariants** checked after every run (all must be 0): (I1) atomicity: no transaction COMMITTED at one site and ABORTED at another; (I2) coordinator and sites agree on every transaction's outcome; (I3) no site's allocated CPU exceeds its capacity; (I4) each ledger equals the sum of the instances on it plus the locks of in-flight transactions; (I5) no lock left in PREPARED/PRECOMMITTED after quiescence; (I6) no protocol violation counter (a COMMIT after an ABORT or the reverse).

# 4. Fault model and fault injection

| Failure | How it is injected | Where it is tested |
|---|---|---|
| Participant crash | Crash point `p_before_vote`, `p_after_prepared_log`, `p_after_commit_log` (`os._exit`), restart after 3 s | E3, E4 |
| Coordinator crash | Crash point `c_after_begin`, `c_after_votes`, `c_after_decide`, `c_mid_phase2`, `c_after_first_precommit` | E3, E5 |
| Random crash | `SIGKILL` of a random site or of the current leader, restart after 1 to 2 s | E4, E5 |
| Message loss | `FAULT drop=COMMIT mode=request/reply`: the message or its reply never arrives | supported, unit-tested |
| Pause | `FAULT action=sleep`: a process freezes | supported |

A *crash* here is the loss of the process memory (SIGKILL). WAL writes sit in the OS cache, so they survive a process crash. A machine crash would also need `fsync`, which we measure (E1) but do not kill machines to test (section 9).

# 5. Implementation notes

* **Participant** (`telerm/txn.py`, class `Participant`): ledger, WAL, the handlers of section 2.3, the in-doubt resolver and the cooperative termination. The same class runs inside the real site (`site.py`, which starts the instance under the stride scheduler on COMMIT) and inside the light `txnsite.py` used by the experiments.
* **Coordinator** (`Coordinator`): planning, the protocol of section 2.4, the replicated state machine (`apply`), the recovery loop, the naive baseline (`request_naive`).
* **Replicated state** (`TXN_BEGIN`, `TXN_DECIDE`, `TXN_PART_EXIT`, `TXN_DONE`): deterministic; included in the replicas' state hash, so the S4 consistency check also covers transactions.
* **Why not 2PC inside the sites only?** The coordinator state must survive the coordinator. Putting it in the Raft log reuses S4 and costs two extra commits (about 1.2 ms on one machine).
* **Instances**: on COMMIT, the site creates one instance per part (global id `T00007.0`, `T00007.1`). When a part's holding time ends the site releases its ledger and sends `INSTANCE_EXIT`, which the coordinator turns into a `TXN_PART_EXIT` entry (releases the link bandwidth of that part).

# 6. Experimental method

Every experiment follows hypothesis, design, implementation, measurement, analysis, conclusion. Each run starts a fresh cluster of separate OS processes: registry, `N` coordinator replicas, `K` sites. Sites emulate network delay by sleeping before they handle and before they reply (`RTT = 2 × delay`). **The machine has one CPU core**, so absolute throughput is low and all components compete; the comparisons between protocols are still valid because every protocol ran under the same conditions.

## E1 Latency and message cost

**Hypothesis.** 2PC latency depends on the slowest site, not on `k`; 3PC adds one round trip; sequential allocation grows linearly with `k`.
**Independent variables.** protocol (sequential, 2PC, 3PC), `k` = 1 to 4, one-way delay 0 and 5 ms, fsync off/on. **Controlled.** 3 coordinator replicas, 50 mc per part, 80 sequential transactions after 5 warm-up, 3 repetitions.

Mean latency to decision in ms (mean ± standard deviation over 3 runs):

| Protocol | k | delay 0 ms | delay 5 ms | messages |
|---|---|---|---|---|
| sequential (no transaction) | 1 | 0.6 ± 0.1 | 11.5 ± 0.3 | 2k |
| | 2 | 0.9 ± 0.0 | 22.4 ± 0.1 | |
| | 3 | 1.5 ± 0.1 | 33.9 ± 0.2 | |
| | 4 | 3.8 ± 0.8 | 44.2 ± 0.3 | |
| 2PC | 1 | 2.4 ± 0.2 | 12.9 ± 0.2 | 4 |
| | 2 | 3.6 ± 0.3 | 13.9 ± 0.1 | 8 |
| | 3 | 4.8 ± 0.4 | 15.0 ± 0.8 | 12 |
| | 4 | 6.1 ± 0.7 | 15.4 ± 0.5 | 16 |
| 3PC | 1 | 2.7 ± 0.2 | 23.7 ± 0.3 | 6 |
| | 2 | 4.3 ± 0.6 | 25.4 ± 0.3 | 12 |
| | 3 | 6.0 ± 0.5 | 26.4 ± 0.5 | 18 |
| | 4 | 8.1 ± 1.1 | 27.3 ± 0.6 | 24 |

Breakdown for `k = 3`, 5 ms delay (ms): 2PC: plan 0.12, `TXN_BEGIN` 0.92, PREPARE round 13.31, `TXN_DECIDE` 0.59, total 15.01, phase 2 (after the answer) 14.82. 3PC: plan 0.12, BEGIN 0.81, PREPARE 12.76, **PRECOMMIT 12.07**, DECIDE 0.53, total 26.37.

With fsync on every WAL and Raft write (`k = 3`, no delay): 2PC 4.8 → **8.2 ms**, 3PC 6.0 → **10.7 ms**.

**Analysis.** The model of section 3 fits. With a 10 ms round trip, sequential allocation costs about one round trip per part (11.5, 22.4, 33.9, 44.2 ms: +11 ms per part). 2PC is flat (12.9 to 15.4 ms): going from 1 to 4 sites costs +2.5 ms of processing, not +30 ms of waiting. 3PC is flat as well but sits one round trip higher (+11.4 ms at k = 3, against the 10 ms RTT plus 1.4 ms of processing). Message counts match `4k` and `6k` exactly. On a LAN (delay 0) the baseline wins (1.5 ms against 4.8 ms for k = 3): atomicity costs about 3 ms, made of two Raft commits (about 1.5 ms) and the vote round. Replication to disk (fsync) adds 3.4 ms for 2PC (the log writes of the coordinator and of three sites are on the critical path) and 4.7 ms for 3PC.
**Conclusion.** The hypothesis holds. Atomic commit is cheap relative to the WAN delays that telecom transport adds, and its cost does not grow with the number of sites. The price is a fixed 3 ms on a LAN.

## E2 Throughput and aborts under load

**Hypothesis.** Throughput grows with concurrency until the single CPU saturates; 3PC peaks lower than 2PC; when capacity is tight, throughput is set by capacity and not by the protocol.
**Design.** 1 to 32 concurrent closed-loop clients for 6 s, 3-part chains (200 mc each, holding 0.6 s), 2 ms delay to every site, two capacity settings (ample: 40 000 mc per site; tight: 2 000 mc per site). 41 runs: 24 in the first repetition, 17 in a second one that was cut short.

| Clients | 2PC commits/s | 2PC p95 ms | 3PC commits/s | 3PC p95 ms |
|---|---|---|---|---|
| 1 | 85.6 | 12.7 | 58.0 | 18.2 |
| 2 | 125.1 | 18.6 | 91.0 | 27.0 |
| 4 | 150.3 | 31.6 | 120.3 | 42.1 |
| 8 | 153.8 | 61.8 | 132.1 | 73.1 |
| 16 | **161.3** | 114.4 | 129.8 | 157.2 |
| 32 | 144.8 | 267.6 | 130.6 | 290.2 |

Ample capacity: every request committed (ratio 1.00) and all invariants were 0. With **tight capacity** both protocols commit exactly **16.7 chains/s** at every concurrency level (2PC 16.6–16.7, 3PC 16.4–16.7).

**Analysis.** 2PC peaks at about 160 commits/s with 16 clients and then loses throughput to queueing and context switching on one core; 3PC peaks at about 130 (**19 % lower**), because each transaction carries 50 % more messages. The tight-capacity result is not a protocol limit: a site holds 2 000 mc and a chain takes 200 mc on each of 3 sites for 0.6 s, so at most 10 chains run at once and `10 / 0.6 s = 16.7` chains/s. The remaining requests are refused cheaply at planning (over 99 % of the offered load; between 0 and 195 requests per run were aborted by a NO vote because the planner's view does not include locks of chains that are still in flight).
**Conclusion.** The commit protocol is not the bottleneck: capacity is. Under contention, 3PC's extra round costs throughput but not correctness; both kept every invariant at zero.

## E3 Failure matrix

**Hypothesis.** Every failure of one process, at every point of the protocol, ends in an all-or-nothing outcome. After a coordinator crash following the votes, a single 2PC coordinator blocks the sites for its whole downtime, the Raft coordinator for about one election, 3PC for its in-doubt timeout.
**Design.** 8 crash points × 3 configurations × 3 repetitions (66 runs: the PRECOMMIT crash point exists only for 3PC). A 3-part chain (500 mc each, holding 3 s) over 3 sites, 2 ms delay, `indoubt_s` = 1 s. The coordinator (or the participant `site1`) is killed at a named point. A single coordinator is restarted after 6 s, a failed participant after 3 s, the Raft replicas are **not** restarted. The client retries with the same request id for up to 25 s. We report the longest time any site kept resources locked in the protocol (`max held`) and `locked cpu·s` = sum over sites of locked millicores × seconds beyond 0.5 s.

| Crash point | 2PC + Raft(3) | 2PC single coordinator | 3PC single coordinator |
|---|---|---|---|
| participant before vote | ABORT, 0.01 s | ABORT, 0.01 s | ABORT, 0.01 s |
| participant after PREPARED written | ABORT, 3.23 s (its own downtime) | ABORT, 3.23 s | ABORT, 3.23 s |
| participant after COMMIT written | COMMIT, 0.01 s | COMMIT, 0.01 s | COMMIT, 0.01 s |
| coordinator after BEGIN | ABORT, no locks | ABORT, no locks | ABORT, no locks |
| coordinator after votes | ABORT, **0.93 s** (1 389 cpu·s) | ABORT, **6.17 s** (9 257 cpu·s) | ABORT, **1.15 s** (1 719 cpu·s) |
| coordinator after DECIDE | COMMIT, 0.50 s (327 cpu·s) | COMMIT, **6.17 s** (9 245 cpu·s) | COMMIT, 1.12 s (1 677 cpu·s) |
| coordinator in phase 2 | COMMIT, 0.01 s | COMMIT, 0.01 s | COMMIT, 0.01 s |
| coordinator after first PRECOMMIT | n/a | n/a | COMMIT, 1.15 s (1 723 cpu·s) |

In all 66 runs: **0** atomicity violations, **0** coordinator/site disagreements, **0** locks left behind, **0** over-committed sites, **0** protocol violations. The client got the right answer every time (after retries), including the ABORTED answer after a coordinator crash.

**Analysis.**

* **Participant crashes** never block anyone else: the coordinator times out the vote (NO) and aborts, or keeps retrying phase 2 until the site is back. The 3.23 s in row 2 is the site's *own* downtime: the lock sits in its own WAL until it restarts and learns the decision.
* **Single 2PC coordinator, crash after the votes:** the three sites were stuck in PREPARED for **6.17 s**, exactly the coordinator's downtime (6 s) plus restart. They asked peers, found everyone PREPARED, and could do nothing else. This is the classic blocking problem, measured: 9 257 cpu·s of CPU held for nothing.
* **Raft coordinator:** the same crash is resolved in **0.93 s**: an election (300 to 600 ms), then the new leader's recovery loop finds a `BEGIN` without a `DECIDE`, aborts, and runs phase 2. After `DECIDE` it only has to repeat phase 2 (0.50 s). Nobody had to wait for the dead node to come back.
* **3PC:** the sites resolved themselves in **1.15 s**, which is their in-doubt timer (1 s) plus one peer round. After PRECOMMIT reached one site the survivors **committed** (the only scenario that exercises the PRECOMMITTED branch).
* A coordinator crash **after BEGIN, before any PREPARE** costs nothing: nothing was locked yet.

**Conclusion.** Hypothesis confirmed. Blocking is 85 % shorter with a replicated decision log (0.93 s against 6.17 s) and bounded by the election timeout instead of by the repair time of one machine.

## E4 Consistency under random crashes

**Hypothesis.** With 2PC or 3PC no random sequence of crashes breaks atomicity; without transactions, half-installed chains appear.
**Design.** 24 s of load from 3 closed-loop clients: chains of 2 to 4 parts on random sites (300 mc, holding 1 s). A chaos process picks a victim every 2 to 5 s (a random site, or the current Raft leader, which has twice the chance of any single site), SIGKILLs it, restarts it after 1 to 2 s. After the load stops we restart everything, wait for quiescence and check the invariants. 3 repetitions × 3 protocols. The baseline (`naive`) installs the parts one after the other with plain `DIRECT` messages and undoes the installed parts on failure; it has no log and no vote.

| Protocol | kills | requests | committed | aborted | refused at planning | client errors | atomicity / mismatch / over-capacity / ledger / stuck / protocol violations | partial chains |
|---|---|---|---|---|---|---|---|---|
| 2PC + Raft | 15 | 3 516 | 1 536 | 714 | 1 266 | 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 |
| 3PC + Raft | 16 | 3 284 | 1 518 | 677 | 1 089 | 0 | 0 / 0 / 0 / 0 / 0 / 0 | 0 |
| no transaction | 16 | 3 096 | 1 536 | 1 466 | 0 | 94 | n/a (no coordinator record) | **64 of 1 026 installed** |

**Analysis.** No client ever saw an error with 2PC or 3PC: requests interrupted by a coordinator crash were retried with the same id and returned the recorded outcome. The baseline lost **94 requests** to errors and, more importantly, left **64 chains (6.2 %) installed in part**: a coordinator crash between two `DIRECT` messages, or a site crash after the first part, with nobody to undo it. Such a chain keeps its resources until its holding time ends and serves no traffic.
**Conclusion.** The transaction subsystem converts "possible silent inconsistency" into "zero inconsistency, a few more explicit aborts".

## E5 Availability when the coordinator dies under load

**Hypothesis.** A single coordinator makes the whole service unavailable for its downtime; the Raft coordinator recovers in about one election; 3PC does not help *new* requests (they still need a coordinator) but frees the sites' resources earlier.
**Design.** 4 closed-loop clients for 22 s on 3 sites; the leader is killed at 6 s; the single coordinator is restarted after 6 s. A 0.5 s window is "up" if it commits at least 50 % of the pre-failure rate. 3 repetitions.

| Configuration | availability | MTTR | locks held (max) | locked cpu·s | client errors | throughput before the kill |
|---|---|---|---|---|---|---|
| 2PC + Raft(3) | **0.970** | **0.64 ± 0.30 s** | 0.68 s | 314 | 19 | 104 /s |
| 2PC single coordinator | 0.705 | 6.47 s | 4.32 s | 1 941 | 207 | **145 /s** |
| 3PC single coordinator | 0.705 | 6.48 s | **1.21 s** | 1 272 | 209 | 109 /s |

**Analysis.** The Raft coordinator keeps serving after a brief election (one or two empty windows). The single coordinators serve nothing for 6 s plus a recovery. **The price of availability is throughput**: replication makes every transaction wait for a Raft majority twice, so the replicated coordinator peaks 28 % below the single coordinator (104 against 145 commits/s on one core). 3PC does not improve MTTR (clients cannot start new transactions without a coordinator) but it releases the sites' locks after 1.2 s, against 4.3 s for 2PC.
**Conclusion.** Replicating the coordinator is the only variant that improves availability. 3PC improves only what in-flight transactions hold.

## E6 Integration check on the real sites

The experiments above use the light participant process (`txnsite.py`) so that they measure the commit protocol and not packet processing. To show that the same `Participant` class works inside the real site, `m5_real_sites.py` starts three real `site.py` sites (stride scheduler, worker processes) and installs a 3-part chain of 1 000 mc each with 2PC. Result: COMMITTED in 10.8 ms; every site lists the transaction as COMMITTED and runs one instance under the stride scheduler; after the 4 s holding time every ledger is back to 0 mc and all invariants are 0 (`results/M5/real_sites.txt`). This is a single functional run, not a measurement.

# 7. Baseline against proposed system

| | **Baseline: classic 2PC** (one coordinator, local decision log) | **Proposed: 2PC with a Raft-replicated decision log** |
|---|---|---|
| Locks held after a coordinator crash (E3) | 6.17 s (the whole downtime) | **0.93 s** (−85 %) |
| Availability under a coordinator crash (E5) | 0.705 | **0.970** |
| Recovery time (E5) | 6.47 s | **0.64 s** |
| Extra latency per transaction | none from replication | two replicated commits per transaction: `TXN_BEGIN` 0.9 ms + `TXN_DECIDE` 0.6 ms (E1, n = 3, 5 ms delay to sites) |
| Peak throughput (E5, one core) | **145 /s** | 104 /s (−28 %) |
| Consistency (E3, E4) | 0 violations | 0 violations |

The **contribution** is not 2PC itself (Gray 1978) nor Raft (Ongaro 2014), nor the idea of logging 2PC decisions in a replicated log (Spanner and others). It is the integration in a telecom resource manager: a transaction coordinator that is the S4 orchestrator, sites that own their ledgers and log their promises, a recovery loop for unfinished transactions, and the measurement of what it buys (−85 % blocking, availability 0.705 → 0.970) and costs (−28 % throughput) under crash injection.

# 8. Failure-driven engineering log – Week 5

| | Entry 1 | Entry 2 | Entry 3 | Entry 4 |
|---|---|---|---|---|
| **What changed?** | Coordinator recovery loop | Blocking-time measurement | Availability metric | Tight-capacity throughput |
| **What failed?** | After the leader was killed, the new leader did not finish the transaction: it stayed `BEGUN` and three sites stayed PREPARED for 40 s | The first measurement said 3PC sites needed 6.6 s to resolve, no better than 2PC, against a theoretical 1.2 s | "Availability" was 1.000 for the Raft coordinator and MTTR printed 0.0 s | Throughput stuck at 16.7/s for every client count, in both protocols |
| **Why?** | The client's retry (same request id) added the transaction to the set of *transactions being driven*, so the recovery loop skipped it, but a retry only waits and drives nothing | The harness measured the time after it had restarted the coordinator, not the time at which the sites left PREPARED | A window with a few commits counted as up; stragglers finishing after the kill counted as recovery | Capacity: 10 chains × 0.6 s holding; the planner refused the rest |
| **How fixed?** | Only the request that *created* the transaction marks it as driven; the recovery loop ignores a transaction younger than 0.3 s. Recovery then took 0.82 s | A watcher task polls the sites from the moment of the crash; sites also report how long each lock was held | A window is up if it commits at least half of the pre-failure rate; MTTR is read from the windows | Not a bug. Analysed, and reported as capacity-bound |
| **Alternative considered** | Leases on transaction ownership with fencing tokens | Reading resolution times from the sites' WAL | Probing a dummy request every 100 ms | Raising the capacity (done for "ample") |
| **What was learned?** | "Who is waiting for this transaction" and "who is responsible for finishing it" are different facts and must be stored separately | A measurement taken in the harness must not depend on the harness's own actions; take it from the system | Pick the availability definition before the experiment and make it hard to pass | Check the resource before blaming the protocol |

# 9. Limitations of S5

* **Crashes lose process memory, not the OS cache.** WAL writes survive `SIGKILL` without `fsync`; a power loss would not. We measured the cost of `fsync` (E1) but did not test machine crashes.
* **Blocking is reduced, not removed.** With three replicas, losing two of them (no majority) stops all decisions; participants then block as in classic 2PC. This is the CAP trade-off we chose (consistency over availability).
* **3PC is only safe for crash failures.** Under a network partition, two sides can terminate differently. We tested crashes, not partitions. 3PC is kept as a comparison, not as the recommended protocol.
* **Optimistic planner.** The coordinator's view counts committed instances only. Two chains planned at the same time for the same site can both be told "it fits", and the second then gets NO from the site. The site's ledger is the authority, so the result is correct, but it costs an abort. Fixing it (counting locks in the view, or waiting at the site) is Week 6.
* **No waiting at the sites.** A PREPARE that does not fit is refused at once (no-wait locking). That makes deadlock impossible, at the price of aborts. Week 6 introduces waiting and with it deadlock.
* **One machine, one core.** Absolute numbers (commits/s) are specific to this machine; the ratios between protocols are what the experiments establish. Network delay is emulated.
* **Link bandwidth** is reserved in the coordinator's state machine, not by a participant of its own; a transport controller with its own failures would be a further participant.

# 10. Reproducibility record

| Item | Value |
|---|---|
| Hardware | 1 vCPU (Linux sandbox), x86-64; run on a laptop to compare |
| Operating system | Ubuntu 24 (Linux) |
| Language / libraries | Python 3.12, standard library only (matplotlib is not needed; `analyze_m5.py` prints tables) |
| Topology | single host, loopback; ports 8900+ (coordinators), 8910 (registry), 8920+ (sites); star topology, 1 ms per link; per-site delay emulated by `--delay-ms` |
| Configuration | per experiment in `m5_run.py`; copied into every result folder (`logs/`, `wal/`, `topology.json`) |
| Workload | closed-loop clients, chains of identical parts; sizes in section 6 |
| Random seed | 42 (`--seed`) for E4 victims and chain shapes |
| Parameters | election timeout 300–600 ms, heartbeat 50 ms, vote timeout 1.5 s, phase-2 timeout 1 s, `indoubt_s` 1 s (E3–E5) or 2 s (default) |
| Command | `./run_m5_all.sh` (about 50 min), or one experiment: `python3 m5_run.py failures --reps 3` |
| Output | `results/M5/*.csv`, `results/M5/summary_tables.txt` (`python3 analyze_m5.py results/M5`) |

# 11. How to run

```
python3 m5_run.py smoke                      # one transaction of each protocol
python3 m5_run.py latency    --reps 3        # E1
python3 m5_run.py throughput --reps 2        # E2
python3 m5_run.py failures   --reps 3        # E3: crash points x configurations
python3 m5_run.py chaos      --reps 3        # E4: random SIGKILLs, invariants
python3 m5_run.py avail      --reps 3        # E5
python3 m5_real_sites.py                     # E6: same transaction on the real sites
python3 analyze_m5.py results/M5             # tables
```

A request from any client: `TXN_REQUEST` with `{"parts": [...], "proto": "2pc" | "3pc" | "naive", "req_id": "..."}` to the service name `orchestrator`. A crash point is armed with `{"type": "FAULT", "point": "c_after_votes"}` sent to the node.

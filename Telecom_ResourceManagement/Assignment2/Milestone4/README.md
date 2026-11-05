# TeleRM – Distributed Telecom Resource Management System (S4)

ICS 2403 Theme 3.
- Milestone 1 (S1, Distributed OS Foundation): **DESIGN.md**, `TeleRM_S1_Architecture.png`
- Milestone 2 (S2, Distributed Processing and Performance): **DESIGN_M2.md** (also as PDF),
  `TeleRM_S2_Architecture.png`, results in `results/M2-load-sweep/`
  (reproduce with `--config config_m2.json`)
- Milestone 3 (S3, Distributed Architecture, edge ↔ core ↔ cloud microservices): **DESIGN_M3.md**
  (also as PDF), `TeleRM_S3_Architecture.png`, results in `results/M3-*`
  (reproduce with `--config config_m3.json`)
- Milestone 4 (S4, Distributed Algorithms and Coordination: Raft + Lamport clocks):
  **DESIGN_M4.md** (also as PDF), `TeleRM_S4_Architecture.png`, results in `results/M4*`

## Milestone 4: Raft-replicated orchestrator + Lamport clocks
The orchestrator runs as 3 replicas (`orch-o1`, `orch-o2`, `orch-o3`; see `config.json` → `s4`).
They elect a leader, and every admission/release/resize is committed to a replicated log on a
majority before it counts (`telerm/raft.py`). Every message carries a Lamport clock (`lc`).
```bash
python m4_run.py smoke                       # 20 s demo: elect, admit, kill the leader, recover
sh run_m4_all.sh && python analyze_m4.py     # all M4 experiments (about 35 min) + charts/tables
python m3_run.py traffic --arch s4 --loads 150 --kill-leader-at 8 --policies tier_aware   # traffic during a leader crash
python -m telerm.deploy local                 # whole S4 system as local processes (--no-raft = S3 layout)
python -m telerm.deploy compose > docker-compose.yml && docker compose up -d --build   # 12 containers
```
Single replica options: `python -m telerm.manager --raft-id o1 --raft-peers o1=host:8000,o2=host:8001,o3=host:8002
--registry host:8010 [--election-ms 300,600 --heartbeat-ms 50 --peer-delay-ms o3=20 --fsync]`.
Clock-skew experiments: `TELERM_CLOCK_SKEW_MS=5 TELERM_EVENTLOG=events.jsonl python -m ...`.

## Milestone 3: microservices over edge, core and cloud
Services: registry, orchestrator, telemetry, two edge API gateways, four compute sites
(edge-A, edge-B, core-1, cloud-1). Only the registry's address is configured.

**Without Docker** (all services as local processes):
```bash
python m3_run.py traffic --arch s3 --deploy local --policies tier_aware --loads 150 --measure 10
python m3_run.py traffic --arch s2 --deploy local --policies least_loaded --loads 200   # S2 monolith, same workload
python m3_run.py scalebench --sizes 4 64 256                                           # registry/orchestrator vs N sites
python -m telerm.deploy local --policy tier_aware      # just start everything (Ctrl+C to stop)
```

**With Docker** (one container per service, CPU quota per site):
```bash
python -m telerm.deploy compose > docker-compose.yml
POLICY=tier_aware docker compose up -d --build
docker compose run --rm loadgen python m3_run.py traffic --arch s3 --deploy external \
       --registry registry:8010 --delay-mode emulate --policies tier_aware --loads 150
docker compose down
python docker_sweep.py tiers        # edge_first vs least_loaded vs tier_aware, fresh stack per run
python docker_sweep.py scaleout     # 1..4 identical compute sites
```
On Windows PowerShell set the variable with `$env:POLICY="tier_aware"` first.

All Milestone 3 experiments in one go (about an hour): `./run_m3_all.sh`, then
`python analyze_m3.py` for charts (`results/M3-charts/`) and tables (`results/M3-summary.md`).

## Requirements
Python 3.10 or newer, on Linux, macOS or Windows.
```bash
pip install -r requirements.txt      # psutil (CPU/memory measurement), matplotlib (charts)
```
The S1 demo still runs without any packages; without psutil the CPU/memory columns are empty.

## Milestone 2: performance experiment
```bash
python perf_experiment.py                  # full sweep: 2 policies x 7 loads x 3 repetitions (~20 min)
python analyze.py results/<folder>         # charts + summary.md (tables)
```
Quick single run (about 30 s):
```bash
python perf_experiment.py --policies edge_first --loads 300 --reps 1 --measure 10
```
Each run starts all nodes fresh, admits 10 service instances through the manager, then
sends seeded Poisson traffic tasks straight to the sites and measures throughput, latency,
jitter, packet loss, CPU and memory. Settings live in the `perf` section of `config.json`.

> Set `offered_loads` for your machine. A good range runs from well below to just above
> the load where loss starts. On a 2-core machine that point is about 500 tasks/s.
> Close other heavy programs while measuring.

**Several machines (recommended for real distributed results):** start the manager and
the sites as shown in "Running across several machines" below, put the LAN addresses in
`config.json`, then run `python perf_experiment.py --no-launch --policies edge_first`
(the policy is the one the manager was started with).

## Milestone 1: admission demo (all nodes on one machine)
```bash
python run_demo.py
```
This starts the resource manager and three sites (`edge-A`, `edge-B`, `core-1`) as separate
OS processes. It then replays a seeded Poisson stream of telecom service requests:

- about 52 requests of vRAN-DU, UPF, IMS-CSCF, Transcoder and IoT-GW over 30 s
- random holding times, with occasional resize requests against running instances

At the end it prints admission and blocking per service, site and link utilisation, fairness
of CPU enforcement, and per-instance throughput. Results are saved to `results/<run>/`.

Useful variations:
```bash
python run_demo.py --policy least_loaded      # edge_first | least_loaded | best_fit
python run_demo.py --rate 3 --window 40       # heavier load, longer run
```
To change site capacities, links, the service catalogue, the workload or the seed, edit
`config.json`.

> Every running instance is kept busy, so the worker processes use all the CPU they get
> while instances are active. On a small machine, reduce `job_work_kb` or the site `slots`
> in `config.json`.

## Running across several machines
Manager machine:
```bash
python -m telerm.manager --bind 0.0.0.0 --port 8000 --topology config.json --policy edge_first --out results/lan_run
```
Each site machine (use that machine's LAN IP as `--host`):
```bash
python -m telerm.site --id edge-A --tier edge --bind 0.0.0.0 --host 192.168.1.21 --port 8101 \
       --manager 192.168.1.10:8000 --cpu 4000 --mem 4096 --bw 2000 --slots 2
```
Next, put the LAN addresses into `config.json` (`manager.host` and each site's `host`). Then
replay the workload from any machine:
```bash
python run_demo.py --no-launch --out results/lan_run
```
Open ports 8000 and 8101–8103 in each machine's firewall.

## Project layout
```
telerm/
  protocol.py   length-prefixed JSON over TCP; call() / Channel (S2); Lamport clock on every message (S4)
  resources.py  node resource vector, topology, link reservations, admission checks, placement
  process.py    service-instance record and enforced state machine
  scheduler.py  stride scheduler (proportional share of CPU = allocation; work-conserving in S2)
  site.py       site node: ALLOCATE/RESIZE/RELEASE, TASK queues (S2), worker pool, heartbeats
  manager.py    global resource manager / orchestrator; S4: Raft replica + replicated state machine
  monitor.py    S2: measures real CPU % and memory of a node (psutil)
  registry.py   S3: naming, liveness, routes (instance -> site)
  telemetry.py  S3: collects measurements from every node
  gateway.py    S3: edge API gateway, only entry for users; injects link delay
  naming.py     S3: registry client used by every service
  deploy.py     S3/S4: start all services locally, or generate docker-compose.yml
  raft.py       S4: Raft consensus (election, log replication, commit, persistence)
run_demo.py         S1: launches all nodes, replays the admission workload, reports
perf_experiment.py  S2: traffic generator + meter, runs the load sweep
analyze.py          S2: charts and tables from runs.csv
config.json         sites, links, services, S1 workload, S2 `perf` experiment, seed
DESIGN.md           Milestone 1 design document and Week 1 failure log
DESIGN_M2.md        Milestone 2 report and Week 2 failure log
m3_run.py           S3: traffic runs (S2 or S3, local or Docker) and control-plane benchmark
docker_sweep.py     S3: runs experiments on Docker Compose (fresh stack per run)
analyze_m3.py       S3: charts and tables for Milestone 3
run_m3_all.sh       S3: every Milestone 3 experiment in order
Dockerfile          S3: one image for every service
DESIGN_M3.md        Milestone 3 report and Week 3 failure log
m4_run.py           S4: coordination experiments E1-E5 (+ fake sites, smoke demo)
analyze_m4.py       S4: charts and tables for Milestone 4
run_m4_all.sh       S4: every Milestone 4 experiment in order
DESIGN_M4.md        Milestone 4 report and Week 4 failure log
config_m3.json      S3 configuration (frozen)
```

## Output files
Milestone 2 (`results/<folder>/`):

| File | Contents |
|---|---|
| `config_used.json` | Exact config plus hardware, OS, Python and psutil versions |
| `runs.csv` | One row per run: every metric, per site |
| `<policy>_<load>_r<rep>/tasks.csv` | Every task: send/receive time, status, queue/processing/communication time |
| `<policy>_<load>_r<rep>/run.json` | Placement of each instance, 1 s resource time series |
| `charts/`, `summary.md` | Produced by `analyze.py` |

Milestone 1 (`run_demo.py`):

| File | Contents |
|---|---|
| `config_used.json` | Exact config, Python version, platform |
| `allocations.jsonl` | Every accepted / blocked / resized / released event with reasons and paths |
| `summary.json` | All requests and outcomes, 1 s utilisation time series, final state of every node |
| `logs/*.log` | One log per node (shows real PIDs of nodes and worker processes) |

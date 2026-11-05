# TeleRM – Distributed Telecom Resource Management System (S1)

ICS 2403 Theme 3 · Milestone 1: Distributed OS Foundation.
Design, process model, resource model and justification: see **DESIGN.md**.
Architecture diagram: `TeleRM_S1_Architecture.png` (editable: `.drawio`).

## Requirements
Python 3.10 or newer. **No third-party packages.** It runs on Linux, macOS and Windows.

## Quick start (all nodes on one machine)
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
  protocol.py   length-prefixed JSON over TCP, message/byte counters
  resources.py  node resource vector, topology, link reservations, admission checks, placement
  process.py    service-instance record and enforced state machine
  scheduler.py  stride scheduler (proportional share of CPU = allocation)
  site.py       site node: ALLOCATE/RESIZE/RELEASE, worker pool, heartbeats, fairness
  manager.py    global resource manager: registry, admission, placement, allocation table
run_demo.py     launches all nodes, replays the workload, reports, saves results
config.json     sites, links, service catalogue, workload, seed
DESIGN.md       Milestone 1 design document and Week 1 failure log
```

## Output files
| File | Contents |
|---|---|
| `config_used.json` | Exact config, Python version, platform |
| `allocations.jsonl` | Every accepted / blocked / resized / released event with reasons and paths |
| `summary.json` | All requests and outcomes, 1 s utilisation time series, final state of every node |
| `logs/*.log` | One log per node (shows real PIDs of nodes and worker processes) |

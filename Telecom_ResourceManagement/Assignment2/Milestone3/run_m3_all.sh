#!/bin/sh
# Runs every Milestone 3 experiment in order (about 50 minutes on a 2-core machine).
set -e
cd "$(dirname "$0")"
echo "== E1: S2 monolith vs S3 microservices (local processes)"
python3 m3_run.py traffic --arch s2 --policies least_loaded --loads 200 400 --reps 3 --out results/M3-arch
python3 m3_run.py traffic --arch s3 --policies least_loaded --loads 200 400 --reps 3 --out results/M3-arch
echo "== E2a: blocking with the cloud tier, three placement policies, three seeds"
for seed in 42 43 44; do for p in edge_first least_loaded tier_aware; do
  python3 run_demo.py --policy $p --seed $seed --out results/M3-blocking/${p}_seed${seed} > results/M3-blocking-${p}-${seed}.log 2>&1 || mkdir -p results/M3-blocking
done; done
echo "== E3b: control-plane scalability (registry + orchestrator vs number of sites)"
python3 m3_run.py scalebench --sizes 4 16 64 256 1024 --reps 3 --admissions 10 --out results/M3-scalebench
echo "== E2b: edge vs core vs cloud traffic (Docker, CPU quotas, emulated link delay)"
python3 docker_sweep.py tiers --loads 50 100 150 200 250 300 --reps 3
echo "== E3a: compute-tier scale-out (Docker, 1-4 equal sites)"
python3 docker_sweep.py scaleout --sizes 1 2 3 4 --reps 3 --overload 70
echo "== all done"

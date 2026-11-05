#!/bin/sh
# Runs every Milestone 5 experiment (about 50 minutes on a 1-2 core machine).
set -e
cd "$(dirname "$0")"
OUT=results/M5
echo "== E1: latency and message cost";        python3 m5_run.py latency    --reps 3 --out $OUT
echo "== E2: throughput and aborts";           python3 m5_run.py throughput --reps 2 --out $OUT
echo "== E3: failure matrix";                  python3 m5_run.py failures   --reps 3 --out $OUT
echo "== E4: consistency under random crashes"; python3 m5_run.py chaos     --reps 3 --out $OUT
echo "== E5: availability when the coordinator dies"; python3 m5_run.py avail --reps 3 --out $OUT
echo "== all done"

#!/bin/sh
# Runs every Milestone 6 experiment (about 35 minutes on a 1-2 core machine).
cd "$(dirname "$0")"
OUT=results/M6
for e in "race 5" "deadlock 5" "workload 2" "workload1 2" "starvation 2" "detector 3"; do set -- $e; echo "== $1"; python3 m6_run.py $1 --reps $2 --out $OUT; done
echo "== all done"

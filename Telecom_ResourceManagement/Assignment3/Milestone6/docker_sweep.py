"""Run Milestone 3 experiments on the Docker Compose deployment (fresh stack per run).

    python docker_sweep.py tiers     --loads 50 100 150 200 250 --reps 3
    python docker_sweep.py scaleout  --sizes 1 2 3 4 --reps 3

tiers     edge vs core vs cloud: placement policies edge_first / least_loaded / tier_aware,
          sites with CPU quotas from config.json (s3.docker.cpus), link delay emulated by gateways
scaleout  horizontal scalability of the compute tier: 1..4 identical sites (same CPU quota),
          offered load above capacity, so throughput = what the sites can process

Needs Docker with Compose v2. Set BASE_IMAGE to use a different base image (default
python:3.12-slim). Results go to results/<experiment>/runs.csv.
"""
import argparse
import copy
import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)
from telerm.deploy import make_compose   # noqa: E402

SCALE_ORDER = ["core-1", "cloud-1", "edge-A", "edge-B"]


def sh(args, env=None, check=True, quiet=False):
    r = subprocess.run(args, cwd=ROOT, env={**os.environ, **(env or {})},
                       stdout=subprocess.PIPE if quiet else None, stderr=subprocess.STDOUT if quiet else None,
                       text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(args)}\n{r.stdout if quiet else ''}")
    return r


def run_one(compose_file, env, loadgen_args):
    base = ["docker", "compose", "-f", compose_file]
    sh(base + ["down", "--remove-orphans"], env, check=False, quiet=True)
    sh(base + ["up", "-d"], env, quiet=True)
    try:
        sh(base + ["run", "--rm", "loadgen", "python", "m3_run.py", "traffic", "--arch", "s3", "--deploy", "external",
                   "--registry", "registry:8010"] + loadgen_args, env)
    finally:
        sh(base + ["down", "--remove-orphans"], env, check=False, quiet=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("experiment", choices=["tiers", "scaleout"])
    ap.add_argument("--policies", nargs="+", default=["edge_first", "least_loaded", "tier_aware"])
    ap.add_argument("--loads", nargs="+", type=int, default=[50, 100, 150, 200, 250])
    ap.add_argument("--sizes", nargs="+", type=int, default=[1, 2, 3, 4])
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--measure", type=float, default=12)
    ap.add_argument("--site-cpus", type=float, default=0.3, help="scaleout: CPU quota of each site")
    ap.add_argument("--overload", type=int, default=90, help="scaleout: offered tasks/s per site")
    ap.add_argument("--deadline-ms", type=float, default=1000.0, help="0 = sites never discard late tasks")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    cfg = json.load(open(os.path.join(ROOT, "config.json")))
    env = {"BASE_IMAGE": os.environ.get("BASE_IMAGE", "python:3.12-slim")}
    out_rel = a.out or f"results/M3-{a.experiment}"
    os.makedirs(os.path.join(ROOT, out_rel), exist_ok=True)
    sh(["docker", "compose", "-f", "docker-compose.yml", "build"], env, quiet=True)
    t_all = time.time()
    if a.experiment == "tiers":
        cf = "docker-compose.yml"
        open(os.path.join(ROOT, cf), "w").write(make_compose(cfg, "tier_aware"))
        for rep in range(1, a.reps + 1):
            for policy in a.policies:
                for load in a.loads:
                    run_one(cf, {**env, "POLICY": policy, "RUN": f"{out_rel.split('/')[-1]}/tel_{policy}_{load}_r{rep}"},
                            ["--delay-mode", "emulate", "--policies", policy, "--loads", str(load), "--reps", "1",
                             "--rep-start", str(rep), "--measure", str(a.measure), "--deadline-ms", str(a.deadline_ms),
                             "--out", "/app/" + out_rel])
    else:
        for rep in range(1, a.reps + 1):
            for n in a.sizes:
                sites = SCALE_ORDER[:n]
                c = copy.deepcopy(cfg)
                for s in c["sites"]:
                    s["cpu"], s["slots"] = 4000, 2
                cpus = {s: a.site_cpus for s in SCALE_ORDER}
                cf = f"docker-compose.scaleout-{n}.yml"
                open(os.path.join(ROOT, cf), "w").write(
                    make_compose(c, "least_loaded", sites=sites, delay_mode="none", cpus=cpus))
                run_one(cf, {**env, "POLICY": "least_loaded", "RUN": f"{out_rel.split('/')[-1]}/tel_n{n}_r{rep}"},
                        ["--delay-mode", "none", "--policies", "least_loaded", "--preset", "scaleout",
                         "--sites", ",".join(sites), "--equal-cpu", "4000", "--equal-slots", "2",
                         "--loads", str(a.overload * n), "--reps", "1", "--rep-start", str(rep),
                         "--measure", str(a.measure), "--deadline-ms", str(a.deadline_ms),
                         "--tag", f"N{n}" + ("" if a.deadline_ms else "-nodeadline"), "--out", "/app/" + out_rel])
                os.remove(os.path.join(ROOT, cf))
    print(f"done in {time.time() - t_all:.0f} s -> {out_rel}/runs.csv")


if __name__ == "__main__":
    main()

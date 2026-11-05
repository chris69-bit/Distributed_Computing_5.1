"""Integration check: the same transactions on the REAL sites (telerm/site.py: stride scheduler + worker pool).

A 3-part chain is installed with 2PC. After COMMIT every site must be running its instance under the
stride scheduler (jobs_done grows), and after the holding time the link/ledger resources must be released.
    python3 m5_real_sites.py
"""
import asyncio
import json
import sys

from m5_cluster import Cluster5, PY, REG_PORT, SITE_BASE, parts_of
from telerm.protocol import call


class RealCluster(Cluster5):
    def site_args(self, k):
        a = [PY, "-m", "telerm.site", "--id", self.site_names[k], "--tier", "edge", "--port", str(SITE_BASE + k),
             "--registry", f"127.0.0.1:{REG_PORT}", "--cpu", str(self.site_cpu), "--mem", "4096", "--bw", "2000",
             "--slots", "2", "--work-kb", "512", "--wal-dir", self.run_dir + "/wal", "--indoubt-s", str(self.indoubt_s)]
        return a


async def main():
    cl = RealCluster("results/M5/real_sites", n_orch=3, n_sites=3, site_cpu=4000)
    await cl.start()
    try:
        await asyncio.sleep(1.5)
        r = await cl.txn(parts_of(3, holding=4.0, sites=cl.site_names, cpu=1000), proto="2pc", timeout=8)
        print("transaction:", r.get("status"), r.get("txn"), "total_ms", r.get("total_ms"), r.get("placements"))
        await asyncio.sleep(2.5)
        rows = []
        for k in range(3):
            st = (await call("127.0.0.1", SITE_BASE + k, {"type": "STATUS"}, timeout=3))["status"]
            insts = st.get("instances") or {}
            rows.append({"site": cl.site_names[k], "txn_states": st.get("txns"), "instances": len(insts) if hasattr(insts, "__len__") else insts,
                         "keys": sorted(st.keys())[:12]})
        print(json.dumps(rows, indent=1)[:1500])
        await asyncio.sleep(3.5)
        await cl.settle(15)
        v, sites, lst = await cl.check()
        print("after holding time, invariants:", v)
        print("allocated cpu per site:", {s: x["allocated"]["cpu"] for s, x in sites.items()})
    finally:
        await cl.stop()

asyncio.run(main())

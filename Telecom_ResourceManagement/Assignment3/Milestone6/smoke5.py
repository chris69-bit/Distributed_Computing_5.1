import asyncio, sys, json
from m5_cluster import *
async def main():
    cl = Cluster5("/tmp/s5smoke", n_orch=3, n_sites=3)
    await cl.start()
    try:
        for proto in ("2pc","3pc","naive"):
            r = await cl.txn(parts_of(3, holding=1.0, sites=cl.site_names), proto=proto)
            print(proto, {k: r.get(k) for k in ("status","txn","total_ms","prepare_ms","precommit_ms","decide_ms","msgs","reason")})
        # NO vote: capacity
        r = await cl.txn(parts_of(2, cpu=5000, sites=cl.site_names), proto="2pc"); print("big", r.get("status"), r.get("reason"))
        await asyncio.sleep(2.5)
        print("settle", await cl.settle())
        v, sites, lst = await cl.check(); print(v)
        print(lst["txn_stats"], lst["phase2_ms"])
    finally:
        await cl.stop()
asyncio.run(main())

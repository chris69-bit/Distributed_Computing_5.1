"""S5 - distributed transactions: atomic installation of a multi-site service chain.

Why transactions?  A telecom service is usually a CHAIN: for example a "5G session slice" is a
vRAN-DU at the user's edge site + a UPF + an IMS-CSCF, and a "video slice" is a transcoder + a
UPF. Every part needs CPU/memory on its own site (each site owns its resources) and bandwidth
on the links between sites. Either the whole chain exists or none of it may: a DU with no UPF
behind it only wastes CPU and bandwidth. That is the atomicity requirement.

Roles
-----
  Coordinator   = the Raft leader orchestrator (class Coordinator, used by manager.py).
  Participants  = the sites (class Participant, used by site.py and by the light txnsite.py).
  Network       = link bandwidth, reserved inside the coordinator's replicated state machine.

Two-Phase Commit (2PC)                      Three-Phase Commit (3PC)
  BEGIN   (decision log, replicated)          BEGIN
  PREPARE -> sites lock resources, vote       CAN_COMMIT (= PREPARE) -> votes
  DECIDE  (replicated; the commit point)      PRECOMMIT  -> acks     (extra round)
  COMMIT / ABORT -> sites apply, ack          DECIDE, DO_COMMIT / ABORT
  DONE                                        DONE

What S5 adds to textbook 2PC:  the coordinator's decision log is the Raft log (S4). A
coordinator crash no longer blocks the sites: another replica becomes leader, reads BEGIN /
DECIDE from the replicated log, and finishes the transaction (presumed abort if there is no
DECIDE). Run with ONE replica and you get classic 2PC (blocking while the coordinator is down).

Participant rules (all handlers are idempotent, every state change is written to a WAL first):
  PREPARE: if the resources fit, lock them (state PREPARED) and vote YES, else vote NO
           (no waiting: "no-wait" locking, so no deadlock; S6 studies waiting).
  COMMIT : PREPARED/PRECOMMITTED -> COMMITTED, start the instances.   ABORT: release the locks.
  In doubt (PREPARED, nothing heard from the coordinator for indoubt_s): ask the coordinator
  (TXN_STATUS); if it cannot be reached, ask the other participants (cooperative termination;
  3PC: the termination protocol).

S6 - concurrency: with cc != "nowait" a PREPARE that does not fit WAITS in a queue at the site
(lock manager) instead of voting NO. Waiting while holding resources elsewhere is "hold and
wait", so distributed deadlock becomes possible. Strategies (request field "cc"):
  nowait   vote NO at once (S5; deadlock impossible, many aborts)
  timeout  wait, give up after lock_wait_s (deadlock only ends by timeout)
  detect   wait; the coordinator's detector finds deadlocks (telerm/deadlock.py) and aborts a victim
  waitdie  wait only if older than every holder, else die (prevention with transaction timestamps)
  ordered  acquire sites one after the other in a fixed order (prevention: no circular wait)
Queue disciplines at a site: greedy (anyone who fits), fifo (no overtaking), aging (greedy, but a
request that waited too long blocks overtaking). Race modes of the ledger: atomic, locked, unsafe.

Fault injection (set with a FAULT message, used by m5_run.py): crash points such as
"c_after_votes" or "p_after_prepared_log", dropped requests/replies, and pauses.
"""
import asyncio
import json
import logging
import os

from .protocol import call, mono, now
from .deadlock import assemble, detect, victim_of
from .resources import NodeResources, Resources, evaluate_candidates, choose

log = logging.getLogger("txn")

PREPARED, PRECOMMITTED, COMMITTED, ABORTED = "PREPARED", "PRECOMMITTED", "COMMITTED", "ABORTED"


# ====================================================================== fault injection
FAULTS: dict = {}      # point -> {"skip": n, "txn": id|None, "action": "crash"|"sleep", "sleep": s}
DROPS: list = []       # [{"msg": "COMMIT", "mode": "request"|"reply", "count": n, "txn": id|None}]
FAULT_LOG: list = []


def set_fault(msg: dict):
    """FAULT message: {"point":..., "skip":0, "txn":None, "action":"crash"} or {"drop": "COMMIT", "mode":"reply"}"""
    if msg.get("clear"):
        FAULTS.clear()
        DROPS.clear()
        return
    if "drop" in msg:
        DROPS.append({"msg": msg["drop"], "mode": msg.get("mode", "reply"), "count": int(msg.get("count", 1)),
                      "txn": msg.get("txn")})
    else:
        FAULTS[msg["point"]] = {"skip": int(msg.get("skip", 0)), "txn": msg.get("txn"),
                                "action": msg.get("action", "crash"), "sleep": float(msg.get("sleep", 0))}


async def fp(name: str, txn=None):
    """A named fault point. Does nothing unless a FAULT message armed it."""
    f = FAULTS.get(name)
    if not f or f.get("txn") not in (None, txn):
        return
    if f["skip"] > 0:
        f["skip"] -= 1
        return
    FAULTS.pop(name, None)
    FAULT_LOG.append({"point": name, "txn": txn, "action": f["action"], "t": now()})
    if f["action"] == "crash":
        os._exit(99)                      # a crash: no cleanup, buffers of the process are lost
    await asyncio.sleep(f["sleep"])       # "sleep": the process freezes for a while (a pause)


def take_drop(msg_type: str, mode: str, txn):
    for d in DROPS:
        if d["msg"] == msg_type and d["mode"] == mode and d["txn"] in (None, txn) and d["count"] > 0:
            d["count"] -= 1
            return True
    return False


# ====================================================================== participant
class Participant:
    """The resource manager of one site: a ledger of what is locked/used, plus a write-ahead log."""

    def __init__(self, site, capacity: Resources, wal_dir, *, fsync=False, delay_ms=0.0, indoubt_s=2.0,
                 coord_call=None, start_instance=None, notify_exit=None, vote_no=0.0, seed=1,
                 race="atomic", discipline="greedy", aging_s=1.5):
        self.site, self.capacity = site, capacity
        self.ledger = NodeResources(capacity)
        self.fsync, self.delay = fsync, delay_ms / 1000.0
        self.indoubt_s = indoubt_s
        self.coord_call, self.start_instance, self.notify_exit = coord_call, start_instance, notify_exit
        self.txns: dict[str, dict] = {}
        self.active: dict[str, dict] = {}          # sid -> {"txn", "demand", "expires_at"}
        self.stats = {"prepare": 0, "yes": 0, "no": 0, "commit": 0, "abort": 0, "precommit": 0, "dup": 0,
                      "violations": 0, "indoubt_resolved": 0, "indoubt_blocked_checks": 0, "wal_writes": 0,
                      "wal_ms": 0.0, "direct": 0, "recovered_indoubt": 0, "recovered_active": 0, "granted_after_wait": 0, "lock_timeout": 0,
                      "wait_die": 0, "cancelled": 0, "over_capacity_events": 0}
        self.race, self.discipline, self.aging_s = race, discipline, aging_s   # S6
        self.waiters: list = []                    # S6: requests waiting for resources at this site
        self.wait_log: list = []                   # S6: how long each request waited, and how it ended
        self.cs = asyncio.Lock()                   # S6: critical section for race="locked"
        self.max_over = 0.0
        self.held_log: list = []                   # every PREPARED -> outcome: how long resources were locked
        self.indoubt_log: list = []                # how long each transaction was in doubt, and how it ended
        self.over_capacity = 0
        self.resolvers: set = set()
        os.makedirs(wal_dir, exist_ok=True)
        self.wal_path = os.path.join(wal_dir, f"wal-{site}.jsonl")
        self._recover()
        self.wal = open(self.wal_path, "a", buffering=1)
        import random
        self.rng = random.Random(seed)
        self.vote_no = vote_no                     # probability of an injected NO vote (e.g. local policy)

    # ------------------------------------------------------------ write-ahead log
    def _log(self, rec):
        t0 = mono()
        self.wal.write(json.dumps(rec, separators=(",", ":")) + "\n")
        if self.fsync:
            self.wal.flush()
            os.fsync(self.wal.fileno())
        self.stats["wal_writes"] += 1
        self.stats["wal_ms"] += (mono() - t0) * 1000

    def _recover(self):
        """Rebuild ledger and transaction states from the WAL after a restart."""
        if not os.path.exists(self.wal_path):
            return
        txns, active = {}, {}
        for line in open(self.wal_path):
            try:
                r = json.loads(line)
            except ValueError:
                break                                  # torn last line
            k, txn = r["r"], r.get("txn")
            if k == "PREPARED":
                txns[txn] = {"state": PREPARED, "parts": r["parts"], "total": r["total"], "participants": r["participants"],
                             "proto": r["proto"], "t_wall": r["t"], "last_contact": now(), "coord": r.get("coord")}
            elif k == "PRECOMMITTED" and txn in txns:
                txns[txn]["state"] = PRECOMMITTED
            elif k == "COMMITTED" and txn in txns:
                txns[txn]["state"] = COMMITTED
                for p in txns[txn]["parts"]:
                    sid = f"{txn}.{p['i']}"
                    active[sid] = {"txn": txn, "demand": p["demand"], "expires_at": r["expires"][sid], "part": p}
            elif k == "ABORTED":
                txns.setdefault(txn, {"parts": [], "total": {}, "participants": {}, "proto": "2pc", "t_wall": r["t"],
                                      "last_contact": now()})["state"] = ABORTED
            elif k == "EXIT":
                active.pop(r["sid"], None)
            elif k == "DIRECT":
                active[r["sid"]] = {"txn": None, "demand": r["demand"], "expires_at": r["expires_at"],
                                    "part": r.get("part")}
            elif k == "DIRECT_UNDO":
                active.pop(r["sid"], None)
        self.txns, self.active = txns, active
        alloc = Resources()
        for t in txns.values():
            if t["state"] in (PREPARED, PRECOMMITTED):
                alloc = alloc + Resources.from_dict(t["total"])
                self.stats["recovered_indoubt"] += 1
        for a in active.values():
            alloc = alloc + Resources.from_dict(a["demand"])
            self.stats["recovered_active"] += 1
        self.ledger.allocated = alloc

    async def restart_instances(self):
        """After recovery: instances whose holding time is not over keep running, the others end now."""
        for sid, a in list(self.active.items()):
            rem = a["expires_at"] - now()
            if rem <= 0:
                await self.exit_instance(sid)
            elif self.start_instance:
                self.start_instance(sid, a["part"], rem)

    # ------------------------------------------------------------ message handling
    async def handle(self, m):
        t = m.get("type")
        txn = m.get("txn")
        if t == "FAULT":
            set_fault(m)
            return {"ok": True}
        if t == "STATUS":
            return {"ok": True, "status": self.status()}
        if take_drop(t, "request", txn):               # request lost on the way: not executed, no reply
            await asyncio.sleep(3600)
        if self.delay:
            await asyncio.sleep(self.delay)
        fn = {"PREPARE": self.on_prepare, "PRECOMMIT": self.on_precommit, "COMMIT": self.on_commit,
              "ABORT": self.on_abort, "TXN_STATE": self.on_state, "DIRECT": self.on_direct,
              "DIRECT_UNDO": self.on_direct_undo, "LOCKSTATE": self.on_lockstate}.get(t)
        if fn is None:
            return None
        r = await fn(m)
        if take_drop(t, "reply", txn):                 # executed, but the reply is lost
            await asyncio.sleep(3600)
        if self.delay:
            await asyncio.sleep(self.delay)
        return r

    def _touch(self, t):
        t["last_contact"] = now()

    # ------------------------------------------------------------ S6: lock manager (wait queue)
    def _blocked_by_queue(self, ts):
        """May a NEW request take free resources now, or must it queue behind the waiters?"""
        if not self.waiters:
            return False
        if self.discipline == "fifo":
            return True
        if self.discipline == "aging":
            return any(mono() - w["t0"] > self.aging_s for w in self.waiters)
        return False                                   # greedy: anyone who fits may go

    def _grant(self):
        """Resources were released: wake the waiters that may run now (according to the discipline)."""
        if not self.waiters:
            return
        for w in list(self.waiters):
            fits = self.ledger.can_allocate(w["demand"])
            aged = mono() - w["t0"] > self.aging_s
            if fits:
                self.ledger.allocate(w["demand"])
                self.waiters.remove(w)
                self.stats["granted_after_wait"] += 1
                self.wait_log.append({"txn": w["txn"], "wait_s": round(mono() - w["t0"], 4), "cpu": w["demand"].cpu,
                                      "outcome": "GRANTED"})
                if not w["fut"].done():
                    w["fut"].set_result("GRANTED")
            elif self.discipline == "fifo" or (self.discipline == "aging" and aged):
                break                                  # the head of the line must not be overtaken

    def _cancel_waiter(self, txn, why):
        for w in list(self.waiters):
            if w["txn"] == txn:
                self.waiters.remove(w)
                self.wait_log.append({"txn": txn, "wait_s": round(mono() - w["t0"], 4), "cpu": w["demand"].cpu,
                                      "outcome": why})
                if not w["fut"].done():
                    w["fut"].set_result(why)
                self._grant()                          # the head may have been removed
                return True
        return False

    async def _acquire(self, txn, total, ts, mode, wait_s):
        """Returns ("GRANTED", None) or ("NO", reason)."""
        if self.vote_no and self.rng.random() < self.vote_no:
            return "NO", "local_policy"
        if self.race == "unsafe":                      # BUG ON PURPOSE: check ... await ... update
            if self.ledger.can_allocate(total):
                await asyncio.sleep(0.001)             # e.g. the WAL write that sits between check and update
                self.ledger.allocated = self.ledger.allocated + total
                over = self.ledger.allocated.cpu - self.capacity.cpu
                if over > 1e-6:
                    self.stats["over_capacity_events"] += 1
                    self.max_over = max(self.max_over, over)
                return "GRANTED", None
            return "NO", "node_capacity"
        if self.race == "locked":                      # mutual exclusion: one lock around check + update
            async with self.cs:
                if self.ledger.can_allocate(total) and not self._blocked_by_queue(ts):
                    await asyncio.sleep(0.001)
                    self.ledger.allocate(total)
                    return "GRANTED", None
        else:                                          # atomic: no await between check and update (S5)
            if self.ledger.can_allocate(total) and not self._blocked_by_queue(ts):
                self.ledger.allocate(total)
                return "GRANTED", None
        if mode == "nowait":
            return "NO", "node_capacity"
        if mode == "waitdie":
            holders = [self.txn_ts(t) for t, r in self.txns.items() if r["state"] in (PREPARED, PRECOMMITTED)]
            if any(ts > h for h in holders):           # younger than a holder: die instead of waiting
                self.stats["wait_die"] += 1
                return "NO", "wait_die"
        w = {"txn": txn, "demand": total, "ts": ts, "t0": mono(), "fut": asyncio.get_running_loop().create_future()}
        self.waiters.append(w)
        try:
            res = await asyncio.wait_for(asyncio.shield(w["fut"]), wait_s)
        except asyncio.TimeoutError:
            if w in self.waiters:
                self.waiters.remove(w)
                self.wait_log.append({"txn": txn, "wait_s": round(mono() - w["t0"], 4), "cpu": total.cpu,
                                      "outcome": "TIMEOUT"})
                self.stats["lock_timeout"] += 1
                self._grant()
                return "NO", "lock_timeout"
            res = w["fut"].result() if w["fut"].done() else "CANCELLED"
        if res == "GRANTED":
            return "GRANTED", None
        self.stats["cancelled"] += 1
        return "NO", "cancelled"

    def txn_ts(self, txn):
        return int(txn[1:]) if txn[:1] in "TN" and txn[1:].isdigit() else 0

    async def on_lockstate(self, m):
        """Snapshot for the deadlock detector: who holds what here, who waits for what."""
        alloc = {}
        for t, r in self.txns.items():
            if r["state"] in (PREPARED, PRECOMMITTED):
                alloc[t] = Resources.from_dict(r["total"]).to_dict()
        for sid, a in self.active.items():
            key = a["txn"] or f"D:{sid}"
            cur = Resources.from_dict(alloc.get(key, {}))
            alloc[key] = (cur + Resources.from_dict(a["demand"])).to_dict()
        waits = {w["txn"]: w["demand"].to_dict() for w in self.waiters}
        active_cpu = sum(Resources.from_dict(a["demand"]).cpu for a in self.active.values())
        return {"ok": True, "site": self.site, "cap": self.capacity.to_dict(), "free": self.ledger.free.to_dict(),
                "active_cpu": active_cpu, "n_waiting": len(self.waiters),
                "alloc": alloc, "waits": waits, "ts": {t: self.txn_ts(t) for t in set(alloc) | set(waits)}}

    async def on_prepare(self, m):
        txn = m["txn"]
        self.stats["prepare"] += 1
        t = self.txns.get(txn)
        if t is not None:                              # a retried PREPARE: same answer as before
            self.stats["dup"] += 1
            self._touch(t)
            return {"ok": True, "vote": "NO" if t["state"] in (ABORTED, "WAITING") else "YES", "dup": True}
        await fp("p_before_vote", txn)
        dd = max([float(p.get("debug_delay_ms", 0)) for p in m["parts"]] or [0])
        if dd:
            await asyncio.sleep(dd / 1000.0)           # test hook: make two transactions interleave on purpose
            if self.txns.get(txn) is not None:         # aborted by the coordinator while we were delayed
                return {"ok": True, "vote": "NO", "reason": "cancelled"}
        total = Resources()
        for p in m["parts"]:
            total = total + Resources.from_dict(p["demand"])
        self.txns[txn] = {"state": "WAITING", "parts": [], "total": {}, "participants": {}, "proto": m["proto"],
                          "t_wall": now(), "last_contact": now()}
        outcome, reason = await self._acquire(txn, total, int(m.get("ts", self.txn_ts(txn))), m.get("cc", "nowait"),
                                              float(m.get("lock_wait_s", 2.0)))
        cur = self.txns.get(txn)
        if outcome == "GRANTED" and cur is not None and cur["state"] == ABORTED:      # aborted while waiting
            self.ledger.release(total)
            self._grant()
            outcome, reason = "NO", "cancelled"
        if outcome != "GRANTED":
            self.stats["no"] += 1
            self.txns[txn] = {"state": ABORTED, "parts": [], "total": {}, "participants": {}, "proto": m["proto"],
                              "t_wall": now(), "last_contact": now()}
            self._log({"r": "ABORTED", "txn": txn, "t": now(), "why": reason})
            return {"ok": True, "vote": "NO", "reason": reason}
        t = {"state": PREPARED, "parts": m["parts"], "total": total.to_dict(), "participants": m["participants"],
             "proto": m["proto"], "t_wall": now(), "last_contact": now(), "coord": m.get("coord")}
        self.txns[txn] = t
        self._log({"r": "PREPARED", "txn": txn, "t": t["t_wall"], "parts": m["parts"], "total": t["total"],
                   "participants": m["participants"], "proto": m["proto"], "coord": m.get("coord")})
        self.stats["yes"] += 1
        await fp("p_after_prepared_log", txn)
        return {"ok": True, "vote": "YES"}

    async def on_precommit(self, m):
        t = self.txns.get(m["txn"])
        if t is None or t["state"] == ABORTED:
            return {"ok": False, "error": "not_prepared"}
        if t["state"] == PREPARED:
            t["state"] = PRECOMMITTED
            self._log({"r": "PRECOMMITTED", "txn": m["txn"], "t": now()})
            self.stats["precommit"] += 1
        self._touch(t)
        return {"ok": True}

    async def on_commit(self, m):
        return await self._commit(m["txn"], how="coordinator")

    async def _commit(self, txn, how):
        t = self.txns.get(txn)
        if t is None:
            self.stats["violations"] += 1              # cannot commit what was never prepared
            return {"ok": False, "error": "unknown_txn"}
        if t["state"] == COMMITTED:
            self.stats["dup"] += 1
            return {"ok": True, "dup": True}
        if t["state"] == ABORTED:
            self.stats["violations"] += 1              # atomicity violation: would commit after abort
            return {"ok": False, "error": "already_aborted"}
        t["state"] = COMMITTED
        expires = {}
        for p in t["parts"]:
            sid = f"{txn}.{p['i']}"
            expires[sid] = now() + float(p["holding_s"])
            self.active[sid] = {"txn": txn, "demand": p["demand"], "expires_at": expires[sid], "part": p}
        self._log({"r": "COMMITTED", "txn": txn, "t": now(), "expires": expires, "how": how})
        self.stats["commit"] += 1
        in_doubt_s = now() - t["t_wall"]
        t["prepared_s"] = in_doubt_s
        self.held_log.append({"txn": txn, "held_s": round(in_doubt_s, 4), "cpu": Resources.from_dict(t["total"]).cpu,
                              "outcome": "COMMIT", "how": how})
        await fp("p_after_commit_log", txn)
        for p in t["parts"]:
            sid = f"{txn}.{p['i']}"
            if self.start_instance:
                self.start_instance(sid, p, float(p["holding_s"]))
        return {"ok": True}

    async def on_abort(self, m):
        return await self._abort(m["txn"], how="coordinator")

    async def _abort(self, txn, how):
        t = self.txns.get(txn)
        if t is None:                                  # never prepared here: remember, so a late PREPARE is refused
            self.txns[txn] = {"state": ABORTED, "parts": [], "total": {}, "participants": {}, "proto": "2pc",
                              "t_wall": now(), "last_contact": now()}
            self._log({"r": "ABORTED", "txn": txn, "t": now(), "why": "abort_before_prepare"})
            self.stats["abort"] += 1
            return {"ok": True}
        if t["state"] == ABORTED:
            self.stats["dup"] += 1
            return {"ok": True, "dup": True}
        if t["state"] == COMMITTED:
            self.stats["violations"] += 1
            return {"ok": False, "error": "already_committed"}
        if t["state"] == "WAITING":                    # S6: still queued: take it out of the queue
            t["state"] = ABORTED
            self._cancel_waiter(txn, "CANCELLED")
            self._log({"r": "ABORTED", "txn": txn, "t": now(), "why": how})
            self.stats["abort"] += 1
            return {"ok": True}
        self.ledger.release(Resources.from_dict(t["total"]))
        self._grant()
        t["state"] = ABORTED
        t["prepared_s"] = now() - t["t_wall"]
        self.held_log.append({"txn": txn, "held_s": round(t["prepared_s"], 4), "cpu": Resources.from_dict(t["total"]).cpu,
                              "outcome": "ABORT", "how": how})
        self._log({"r": "ABORTED", "txn": txn, "t": now(), "why": how})
        self.stats["abort"] += 1
        return {"ok": True}

    async def on_state(self, m):
        t = self.txns.get(m["txn"])
        return {"ok": True, "state": t["state"] if t else "UNKNOWN", "site": self.site}

    # naive baseline (no transaction): install at once, undo on request
    async def on_direct(self, m):
        total = Resources.from_dict(m["demand"])
        sid = m["sid"]
        if sid in self.active:
            return {"ok": True, "dup": True}
        if not self.ledger.can_allocate(total):
            return {"ok": False, "reason": "node_capacity"}
        self.ledger.allocate(total)
        exp = now() + float(m["holding_s"])
        self.active[sid] = {"txn": None, "demand": total.to_dict(), "expires_at": exp, "part": m.get("part")}
        self._log({"r": "DIRECT", "sid": sid, "demand": total.to_dict(), "expires_at": exp, "part": m.get("part")})
        self.stats["direct"] += 1
        if self.start_instance:
            self.start_instance(sid, m.get("part") or {"i": 0, "holding_s": m["holding_s"]}, float(m["holding_s"]))
        return {"ok": True}

    async def on_direct_undo(self, m):
        a = self.active.pop(m["sid"], None)
        if a:
            self.ledger.release(Resources.from_dict(a["demand"]))
            self._grant()
            self._log({"r": "DIRECT_UNDO", "sid": m["sid"]})
        return {"ok": True}

    # ------------------------------------------------------------ instance end
    async def exit_instance(self, sid):
        a = self.active.pop(sid, None)
        if a is None:
            return
        self.ledger.release(Resources.from_dict(a["demand"]))
        self._grant()
        self._log({"r": "EXIT", "sid": sid, "t": now()})
        if self.notify_exit:
            await self.notify_exit(sid)

    # ------------------------------------------------------------ in-doubt resolution
    async def resolver_loop(self):
        while True:
            await asyncio.sleep(0.2)
            for txn, t in list(self.txns.items()):
                if t["state"] in (PREPARED, PRECOMMITTED) and txn not in self.resolvers \
                        and now() - t["last_contact"] > self.indoubt_s:
                    self.resolvers.add(txn)
                    asyncio.create_task(self._resolve(txn))

    async def _resolve(self, txn):
        t = self.txns[txn]
        t0 = t["last_contact"]
        try:
            while t["state"] in (PREPARED, PRECOMMITTED):
                how, decision = "coordinator", None
                try:
                    r = await self.coord_call({"type": "TXN_STATUS", "txn": txn})
                    d = r.get("decision")
                    if d in ("COMMIT", "ABORT"):
                        decision = d
                    elif d == "UNKNOWN":
                        decision = "ABORT"             # the coordinator never began it: it cannot commit
                    # "PENDING": the coordinator is alive and driving this transaction; keep waiting
                except Exception:
                    how = "peers"                      # coordinator unreachable: ask the other participants
                    decision = await self._terminate(txn)
                if decision == "COMMIT":
                    await self._commit(txn, how=f"resolved:{how}")
                elif decision == "ABORT":
                    await self._abort(txn, how=f"resolved:{how}")
                if t["state"] in (COMMITTED, ABORTED):
                    self.stats["indoubt_resolved"] += 1
                    self.indoubt_log.append({"txn": txn, "how": how, "outcome": t["state"], "proto": t["proto"],
                                             "in_doubt_s": round(now() - t["t_wall"], 3),
                                             "demand_cpu": Resources.from_dict(t["total"]).cpu})
                    break
                self.stats["indoubt_blocked_checks"] += 1
                await asyncio.sleep(0.3)
        finally:
            self.resolvers.discard(txn)

    async def _peer_states(self, txn):
        t = self.txns[txn]
        out = {self.site: t["state"]}
        async def ask(site, addr):
            try:
                r = await call(addr[0], addr[1], {"type": "TXN_STATE", "txn": txn}, timeout=0.8)
                return site, r.get("state")
            except Exception:
                return site, None                      # unreachable
        res = await asyncio.gather(*[ask(s, a) for s, a in t["participants"].items() if s != self.site])
        for s, st in res:
            out[s] = st
        return out

    async def _terminate(self, txn):
        """Decide without the coordinator. Returns COMMIT, ABORT or None (still blocked)."""
        t = self.txns[txn]
        states = await self._peer_states(txn)
        seen = {s: st for s, st in states.items() if st is not None}
        vals = set(seen.values())
        if COMMITTED in vals:
            return "COMMIT"
        if ABORTED in vals or "UNKNOWN" in vals:       # a participant that never voted YES: nobody can have committed
            return "ABORT"
        if t["proto"] != "3pc":
            return None                                # 2PC: everyone reachable is PREPARED -> must wait (blocking)
        # 3PC termination protocol: the lowest-id reachable participant decides for everybody
        if self.site != min(seen):
            return None
        decision = "COMMIT" if PRECOMMITTED in vals else "ABORT"
        for s in seen:
            if s == self.site:
                continue
            try:
                await call(*t["participants"][s], {"type": decision, "txn": txn}, timeout=0.8)
            except Exception:
                pass
        return decision

    def status(self):
        return {"site": self.site, "capacity": self.capacity.to_dict(), "allocated": self.ledger.allocated.to_dict(),
                "active": {k: v["demand"] for k, v in self.active.items()},
                "txns": {k: v["state"] for k, v in self.txns.items()}, "stats": dict(self.stats),
                "wait_log": list(self.wait_log[-5000:]), "max_over": self.max_over, "waiting": len(self.waiters),
                "indoubt_log": list(self.indoubt_log), "held_log": list(self.held_log[-5000:]), "faults": list(FAULT_LOG)}


# ====================================================================== coordinator
class Coordinator:
    """Transaction coordinator running inside the orchestrator (the Raft leader drives)."""

    def __init__(self, mgr, vote_timeout=1.5, phase_timeout=1.0, phase2_deadline=60.0, detect_s=0.0,
                 stable_rounds=2, victim_policy="youngest"):
        self.m = mgr
        self.detect_s, self.stable_rounds, self.victim_policy = detect_s, stable_rounds, victim_policy   # S6
        self.cc: dict = {}                         # txn -> (cc mode, lock wait) for the transactions this node drives
        self.victims: dict = {}                    # txn -> reason it was aborted by the detector
        self.det = {"rounds": 0, "msgs": 0, "deadlocks": 0, "victims": 0, "detect_ms": [], "first_seen": {},
                    "resolve_s": [], "events": []}
        self._prev_dead: dict = {}
        self.vote_timeout, self.phase_timeout, self.phase2_deadline = vote_timeout, phase_timeout, phase2_deadline
        self.sm: dict[str, dict] = {}              # replicated: txn -> record
        self.req_index: dict[str, str] = {}
        self.seq = 0
        self.driving: set = set()
        self.stats = {"requests": 0, "committed": 0, "aborted": 0, "blocked_plan": 0, "no_votes": 0,
                      "timeouts": 0, "recovered": 0, "phase2_retries": 0, "msgs": 0, "orphans": 0}
        self.records: list = []
        self.t_decided: dict = {}
        self.seen: dict = {}
        self.phase2_ms: dict = {}
        self._sites_t = 0.0

    # ------------------------------------------------------------ replicated state machine
    def _ensure_site(self, p):
        """A follower may not have synced a site from the registry yet: build it from the command."""
        m = self.m
        if p["site"] not in m.sites:
            m.sites[p["site"]] = {"res": NodeResources(Resources.from_dict(p["cap"])), "host": p["host"],
                                  "port": p["port"], "tier": p.get("tier"), "pid": None, "status": "ALIVE",
                                  "slot_utilisation": 0.0, "fairness": None, "last_hb": now()}

    def apply(self, cmd, index):
        m = self.m
        op = cmd["op"]
        if op == "TXN_BEGIN":
            rid = cmd.get("req_id")
            if rid and rid in self.req_index:
                return {"ok": True, "txn": self.req_index[rid], "dup": True}
            for p in cmd["plan"]:
                self._ensure_site(p)
            reserved = []
            for p in cmd["plan"]:
                if not m.topo.path_has_bw(p["path"], p["demand"]["bw"]):
                    for path, bw in reserved:
                        m.topo.release(path, bw)
                    return {"ok": False, "reason": "link_bw"}
                m.topo.reserve(p["path"], p["demand"]["bw"])
                reserved.append((p["path"], p["demand"]["bw"]))
            self.seq += 1
            txn = f"T{self.seq:05d}"
            self.sm[txn] = {"txn": txn, "state": "BEGUN", "proto": cmd["proto"], "plan": cmd["plan"], "released": [],
                            "done": False, "req_id": rid, "index": index}
            if rid:
                self.req_index[rid] = txn
            self.seen[txn] = mono()
            m.event("txn_begin", txn=txn, proto=cmd["proto"], sites=sorted({p["site"] for p in cmd["plan"]}))
            return {"ok": True, "txn": txn}
        r = self.sm.get(cmd.get("txn"))
        if r is None:
            return {"ok": False, "reason": "unknown_txn"}
        if op == "TXN_DECIDE":
            if r["state"] != "BEGUN":                  # the first decision is final
                return {"ok": True, "state": r["state"], "dup": True}
            if cmd["decision"] == "COMMIT":
                r["state"] = COMMITTED
                for p in r["plan"]:
                    m.sites[p["site"]]["res"].allocated = m.sites[p["site"]]["res"].allocated + Resources.from_dict(p["demand"])
            else:
                r["state"] = ABORTED
                for p in r["plan"]:
                    m.topo.release(p["path"], p["demand"]["bw"])
            m.event("txn_decided", txn=r["txn"], decision=cmd["decision"])
            return {"ok": True, "state": r["state"]}
        if op == "TXN_PART_EXIT":
            i = cmd["i"]
            if r["state"] == COMMITTED and i not in r["released"]:
                p = r["plan"][i]
                m.sites[p["site"]]["res"].release(Resources.from_dict(p["demand"]))
                m.topo.release(p["path"], p["demand"]["bw"])
                r["released"].append(i)
            return {"ok": True}
        if op == "TXN_DONE":
            r["done"] = True
            return {"ok": True}
        return {"ok": False, "reason": "unknown op"}

    def hash_part(self):
        return sorted((t, r["state"], len(r["released"]), r["done"]) for t, r in self.sm.items())

    # ------------------------------------------------------------ planning
    def plan(self, parts):
        """Choose a site and path for every part on a scratch copy of the view. Returns (plan, reason)."""
        m = self.m
        nodes = m.alive()
        scratch = {}
        for sid, nr in nodes.items():
            s = NodeResources(nr.capacity)
            s.allocated = nr.allocated
            scratch[sid] = s
        reserved, plan = [], []
        try:
            for i, p in enumerate(parts):
                d = Resources.from_dict(p["demand"])
                feas, reasons = evaluate_candidates(d, p["ingress"], float(p.get("max_latency_ms", 1e9)), scratch, m.topo)
                if p.get("site"):
                    feas = [f for f in feas if f[0] == p["site"]]
                    if p.get("force") and p["site"] in scratch:      # S6 tests: let the SITE decide (skip the view check)
                        path, lat = m.topo.paths_from(p["ingress"]).get(p["site"], ([], 0.0))
                        feas = [(p["site"], path, lat)]
                pick = choose(feas, d, p["ingress"], scratch, m.policy, m.tiers())
                if pick is None:
                    return None, (max(reasons, key=reasons.get) if reasons else "no_sites")
                site, path, lat = pick
                scratch[site].allocate(d)
                m.topo.reserve(path, d.bw)
                reserved.append((path, d.bw))
                sv = m.sites[site]
                plan.append({"i": i, "site": site, "path": path, "latency_ms": lat, "service": p["service"],
                             "ingress": p["ingress"], "demand": d.to_dict(), "holding_s": float(p["holding_s"]),
                             "debug_delay_ms": p.get("debug_delay_ms", 0), "cap": sv["res"].capacity.to_dict(), "host": sv["host"], "port": sv["port"],
                             "tier": sv.get("tier")})
            return plan, None
        finally:
            for path, bw in reserved:
                m.topo.release(path, bw)

    # ------------------------------------------------------------ client request
    async def request(self, r):
        m = self.m
        self.stats["requests"] += 1
        t_all = mono()
        proto = r.get("proto", "2pc")
        rid = r.get("req_id")
        async with m.lock:
            if not m.raft.is_ready():
                return m.not_leader()
            if rid and rid in self.req_index:           # a retry of a request we already started
                txn = self.req_index[rid]
                t_plan = None
            else:
                if mono() - self._sites_t > 0.5:
                    await m.sync_sites()
                    self._sites_t = mono()
                tp = mono()
                plan, reason = self.plan(r["parts"])
                plan_ms = (mono() - tp) * 1000
                if plan is None:
                    self.stats["blocked_plan"] += 1
                    return {"ok": False, "status": "BLOCKED", "reason": reason, "plan_ms": round(plan_ms, 3)}
                tb = mono()
                try:
                    res = await m.raft.propose({"op": "TXN_BEGIN", "req_id": rid, "proto": proto, "plan": plan})
                except Exception as exc:
                    return m.not_leader(exc) if hasattr(exc, "leader_addr") else {"ok": False, "error": repr(exc)}
                begin_ms = (mono() - tb) * 1000
                if not res.get("ok"):
                    self.stats["blocked_plan"] += 1
                    return {"ok": False, "status": "BLOCKED", "reason": res.get("reason")}
                txn = res["txn"]
                t_plan = (plan_ms, begin_ms)
                self.cc[txn] = (r.get("cc", "nowait"), float(r.get("lock_wait_s", 2.0)))
                self.driving.add(txn)                    # only for a transaction THIS request created
        if t_plan is None:                               # retry: wait for the outcome of the existing txn
            for _ in range(300):
                if self.sm[txn]["state"] != "BEGUN":
                    break
                await asyncio.sleep(0.05)
            st = self.sm[txn]["state"]
            return {"ok": st == COMMITTED, "status": st, "txn": txn, "retried": True}
        out = await self.drive(txn)
        out.update({"plan_ms": round(t_plan[0], 3), "begin_ms": round(t_plan[1], 3),
                    "total_ms": round((mono() - t_all) * 1000, 3), "proto": proto})
        self.records.append({k: out.get(k) for k in ("txn", "status", "reason", "proto", "total_ms", "decision_ms",
                                                    "prepare_ms", "precommit_ms", "decide_ms", "phase2_ms", "msgs")})
        return out

    # ------------------------------------------------------------ the protocol
    async def _send(self, site, msg, timeout):
        s = self.m.sites.get(site)
        if s is None:
            raise RuntimeError("unknown site")
        self.stats["msgs"] += 2                          # request + reply
        return await call(s["host"], s["port"], msg, timeout=timeout)

    def _participants(self, rec):
        by_site = {}
        for p in rec["plan"]:
            by_site.setdefault(p["site"], []).append(p)
        addrs = {sid: [self.m.sites[sid]["host"], self.m.sites[sid]["port"]] for sid in by_site}
        return by_site, addrs

    async def drive(self, txn):
        """Run the protocol for a BEGUN transaction: phase 1, (3PC: precommit), decision, then phase 2
        in the background. The client is answered as soon as the decision is durable."""
        m = self.m
        rec = self.sm[txn]
        proto = rec["proto"]
        by_site, addrs = self._participants(rec)
        k = len(by_site)
        t0 = mono()
        out = {"txn": txn}
        await fp("c_after_begin", txn)
        # ---- phase 1: PREPARE / vote
        cc, lock_wait = self.cc.get(txn, ("nowait", 2.0))
        ts = int(txn[1:])
        tp = mono()
        votes = {}
        wait_extra = lock_wait if cc != "nowait" else 0.0

        async def ask(site, parts):
            try:
                r = await self._send(site, {"type": "PREPARE", "txn": txn, "proto": proto, "parts": parts,
                                            "participants": addrs, "coord": "orchestrator", "cc": cc, "ts": ts,
                                            "lock_wait_s": lock_wait}, self.vote_timeout + wait_extra + 1.0)
                return site, r.get("vote", "NO"), r.get("reason")
            except Exception:
                self.stats["timeouts"] += 1
                return site, "NO", "timeout"
        first = True
        if cc == "ordered":                          # prevention: take the sites one by one in a fixed order
            for site in sorted(by_site):
                site, vote, why = await ask(site, by_site[site])
                votes[site] = (vote, why)
                if first:
                    first = False
                    await fp("c_after_first_vote", txn)
                if vote != "YES":
                    break
            for site in by_site:
                votes.setdefault(site, ("NO", "not_asked"))
        else:
            for fut in asyncio.as_completed([ask(s, p) for s, p in by_site.items()]):
                site, vote, why = await fut
                votes[site] = (vote, why)
                if first:
                    first = False
                    await fp("c_after_first_vote", txn)
        out["prepare_ms"] = round((mono() - tp) * 1000, 3)
        all_yes = all(v[0] == "YES" for v in votes.values())
        out["votes"] = {s: v[0] for s, v in votes.items()}
        if not all_yes:
            out["reason"] = next((v[1] for v in votes.values() if v[0] != "YES" and v[1] != "not_asked"), "no")
            if txn in self.victims:
                out["reason"] = "deadlock_victim"
            self.stats["no_votes"] += 1
        await fp("c_after_votes", txn)
        msgs = 2 * k
        # ---- 3PC: the extra round
        if proto == "3pc" and all_yes:
            tq = mono()

            async def pre(site):
                try:
                    await self._send(site, {"type": "PRECOMMIT", "txn": txn}, self.phase_timeout)
                except Exception:
                    self.stats["timeouts"] += 1       # that site will learn the outcome from the others
            n = 0
            for fut in asyncio.as_completed([pre(s) for s in by_site]):
                await fut
                n += 1
                if n == 1:
                    await fp("c_after_first_precommit", txn)
            out["precommit_ms"] = round((mono() - tq) * 1000, 3)
            msgs += 2 * k
            await fp("c_after_precommit", txn)
        # ---- decision = the commit point (replicated on a majority of coordinator replicas)
        td = mono()
        res = await m.raft.propose({"op": "TXN_DECIDE", "txn": txn, "decision": "COMMIT" if all_yes else "ABORT"})
        out["decide_ms"] = round((mono() - td) * 1000, 3)
        out["decision_ms"] = round((mono() - t0) * 1000, 3)
        await fp("c_after_decide", txn)
        rec = self.sm[txn]
        decision = "COMMIT" if rec["state"] == COMMITTED else "ABORT"
        out["status"] = rec["state"]
        out["ok"] = rec["state"] == COMMITTED
        out["msgs"] = msgs + 2 * k
        out["raft_entries"] = 3
        out["placements"] = [{"i": p["i"], "service": p["service"], "site": p["site"]} for p in rec["plan"]]
        self.stats["committed" if out["ok"] else "aborted"] += 1
        self.t_decided[txn] = mono()
        asyncio.create_task(self.phase2(txn, decision, by_site))      # phase 2 continues in the background
        return out

    async def phase2(self, txn, decision, by_site):
        """Send COMMIT/ABORT to every participant until it acknowledges; then log DONE."""
        m = self.m
        t0 = mono()
        pending = set(by_site)
        first = True
        try:
            while pending and mono() - t0 < self.phase2_deadline:
                async def one(site):
                    try:
                        r = await self._send(site, {"type": decision, "txn": txn}, self.phase_timeout)
                        return site, bool(r.get("ok"))
                    except Exception:
                        return site, False
                for fut in asyncio.as_completed([one(s) for s in sorted(pending)]):
                    site, ok = await fut
                    if ok:
                        pending.discard(site)
                    else:
                        self.stats["phase2_retries"] += 1
                    if first:
                        first = False
                        await fp("c_mid_phase2", txn)
                if pending:
                    await asyncio.sleep(0.2)
            if not pending:
                await m.raft.propose({"op": "TXN_DONE", "txn": txn})
                if txn in self.t_decided:
                    self.phase2_ms[txn] = round((mono() - self.t_decided[txn]) * 1000, 3)
        except Exception as exc:
            log.warning("phase 2 of %s interrupted: %r", txn, exc)   # leadership lost: the next leader redoes it
        finally:
            self.driving.discard(txn)

    # ------------------------------------------------------------ S6: deadlock detector (runs on the leader)
    async def detector_loop(self):
        """Every detect_s: collect LOCKSTATE from all sites, build the resource-allocation graph, run the
        matrix detection, and abort one victim per deadlock that has been seen in stable_rounds rounds."""
        while not self.m.shutdown.is_set():
            await asyncio.sleep(self.detect_s)
            if not self.m.raft.is_ready():
                continue
            t0 = mono()
            sites = [sid for sid, sv in self.m.sites.items() if sv["status"] == "ALIVE"]

            async def snap(sid):
                try:
                    return await self._send(sid, {"type": "LOCKSTATE"}, 0.8)
                except Exception:
                    return None
            snaps = [x for x in await asyncio.gather(*[snap(s_) for s_ in sites]) if x]
            self.det["rounds"] += 1
            self.det["msgs"] += 2 * len(sites)
            free, alloc, req, ts = assemble(snaps)
            dead = detect(free, alloc, req)
            self.det["detect_ms"].append(round((mono() - t0) * 1000, 3))
            now_set = set(dead)
            stable = [t for t in dead if self._prev_dead.get(t, 0) + 1 >= self.stable_rounds]
            self._prev_dead = {t: self._prev_dead.get(t, 0) + 1 for t in now_set}
            for t in dead:
                self.det["first_seen"].setdefault(t, mono())
            if not stable:
                continue
            self.det["deadlocks"] += 1
            cand = [t for t in stable if self.sm.get(t, {}).get("state") == "BEGUN"]
            v = victim_of(cand, alloc, ts, self.victim_policy)
            self.det["events"].append({"t": round(now(), 3), "deadlocked": stable, "victim": v})
            if v:
                await self.abort_victim(v)

    async def abort_victim(self, txn):
        rec = self.sm.get(txn)
        if rec is None or rec["state"] != "BEGUN":
            return
        self.victims[txn] = "deadlock"
        self.det["victims"] += 1
        try:
            await self.m.raft.propose({"op": "TXN_DECIDE", "txn": txn, "decision": "ABORT"})
        except Exception:
            return
        if txn in self.det["first_seen"]:
            self.det["resolve_s"].append(round(mono() - self.det["first_seen"][txn], 3))
        by_site, _ = self._participants(rec)
        for site in by_site:                         # wake the queued request at the site: it votes NO
            try:
                await self._send(site, {"type": "ABORT", "txn": txn}, 0.8)
            except Exception:
                pass

    # ------------------------------------------------------------ recovery and status
    async def terminate_by_participants(self, rec):
        """3PC recovery of a BEGUN transaction: query the participants' states (never trust a log entry
        for a decision that was not made)."""
        by_site, addrs = self._participants(rec)
        states = {}
        for s in by_site:
            try:
                r = await self._send(s, {"type": "TXN_STATE", "txn": rec["txn"]}, 0.8)
                states[s] = r.get("state")
            except Exception:
                states[s] = None
        vals = {v for v in states.values() if v is not None}
        if any(v is None for v in states.values()) and not (COMMITTED in vals or ABORTED in vals):
            return None                                   # a participant is unreachable: cannot decide yet
        if COMMITTED in vals or (PRECOMMITTED in vals and ABORTED not in vals and "UNKNOWN" not in vals):
            return "COMMIT"
        return "ABORT"

    async def decide_orphan(self, txn):
        rec = self.sm[txn]
        if rec["state"] != "BEGUN":
            return rec["state"]
        decision = "ABORT"                                # presumed abort (2PC)
        if rec["proto"] == "3pc":
            d = await self.terminate_by_participants(rec)
            if d is None:
                return None
            decision = d
        res = await self.m.raft.propose({"op": "TXN_DECIDE", "txn": txn, "decision": decision})
        return res.get("state")

    async def status(self, txn):
        """Answer a participant's TXN_STATUS."""
        rec = self.sm.get(txn)
        if rec is None:
            return {"ok": True, "decision": "UNKNOWN"}
        if rec["state"] == COMMITTED:
            return {"ok": True, "decision": "COMMIT"}
        if rec["state"] == ABORTED:
            return {"ok": True, "decision": "ABORT"}
        if txn in self.driving:
            return {"ok": True, "decision": "PENDING"}
        st = await self.decide_orphan(txn)
        return {"ok": True, "decision": {"COMMITTED": "COMMIT", "ABORTED": "ABORT"}.get(st, "PENDING")}

    async def maintenance(self):
        """Leader loop: finish transactions nobody is driving (after a coordinator crash/failover)."""
        while not self.m.shutdown.is_set():
            await asyncio.sleep(0.25)
            if not self.m.raft.is_ready():
                continue
            for txn, rec in list(self.sm.items()):
                if rec["done"] or txn in self.driving or mono() - self.seen.get(txn, 0) < 0.3:
                    continue                              # (a young transaction is still being driven by its request)
                self.driving.add(txn)
                self.stats["recovered"] += 1
                asyncio.create_task(self._recover(txn))

    async def _recover(self, txn):
        try:
            rec = self.sm[txn]
            if rec["state"] == "BEGUN":
                st = await self.decide_orphan(txn)
                if st is None:
                    self.driving.discard(txn)
                    return
            rec = self.sm[txn]
            by_site, _ = self._participants(rec)
            decision = "COMMIT" if rec["state"] == COMMITTED else "ABORT"
            await self.phase2(txn, decision, by_site)
        except Exception as exc:
            log.warning("recovery of %s failed: %r", txn, exc, exc_info=True)
            self.driving.discard(txn)

    # ------------------------------------------------------------ naive baseline (no atomic commit)
    async def request_naive(self, r):
        """Install the parts one after the other with plain ALLOCATE messages; undo what was done if a
        part fails. No log, no vote: a crash in the middle leaves a half-installed chain."""
        m = self.m
        self.stats["requests"] += 1
        async with m.lock:
            if not m.raft.is_ready():
                return m.not_leader()
            if mono() - self._sites_t > 0.5:
                await m.sync_sites()
                self._sites_t = mono()
            plan, reason = self.plan(r["parts"])
            if plan is None:
                self.stats["blocked_plan"] += 1
                return {"ok": False, "status": "BLOCKED", "reason": reason}
        self.seq += 1
        txn = f"N{self.seq:05d}"
        done = []
        t0 = mono()
        for p in plan:
            try:
                rep = await self._send(p["site"], {"type": "DIRECT", "sid": f"{txn}.{p['i']}", "demand": p["demand"],
                                                  "holding_s": p["holding_s"], "part": {**p, "k": len(plan)}}, self.vote_timeout)
                ok = rep.get("ok")
            except Exception:
                ok = False
            if not ok:
                for q in done:
                    try:
                        await self._send(q["site"], {"type": "DIRECT_UNDO", "sid": f"{txn}.{q['i']}"}, 0.8)
                    except Exception:
                        pass
                self.stats["aborted"] += 1
                return {"ok": False, "status": "ABORTED", "txn": txn, "reason": "part_failed",
                        "total_ms": round((mono() - t0) * 1000, 3)}
            done.append(p)
            await fp("c_naive_after_part", txn)
        self.stats["committed"] += 1
        return {"ok": True, "status": COMMITTED, "txn": txn, "total_ms": round((mono() - t0) * 1000, 3),
                "placements": [{"i": p["i"], "service": p["service"], "site": p["site"]} for p in plan]}

"""Raft consensus (S4): keeps the replicated orchestrators' allocation tables identical.

Plain-language summary
----------------------
N replicas each keep a *log* (a numbered list of commands such as "ADMIT instance on core-1").
One replica is the **leader**; only it accepts new commands. A command counts as decided
(*committed*) once a **majority** of replicas have stored it. Every replica applies committed
commands in log order to its own copy of the allocation table, so all copies stay identical.

  1. Leader election  - every replica waits a random "election timeout" (e.g. 300-600 ms).
     If it hears nothing from a leader in that time it becomes a *candidate*: it starts a new
     *term* (an election number), votes for itself and asks the others for their vote
     (RAFT_RV). A replica gives at most one vote per term, and only to a candidate whose log is
     at least as up to date as its own. A majority of votes makes the candidate leader.
  2. Log replication  - the leader sends RAFT_AE ("append entries") to every follower: new
     entries plus the index/term of the entry just before them, so a follower can tell whether
     its log matches. Empty RAFT_AE every heartbeat_ms is the leader's "I am alive" signal.
  3. Commit           - when a majority hold an entry from the leader's current term, the
     leader marks it committed, applies it, and tells followers in the next RAFT_AE.

Safety (why the copies cannot diverge): two majorities always share at least one replica, so
two leaders can never both be elected in one term, and a new leader must hold every committed
entry (the voting rule above guarantees it).

Also implemented: persistence of term/vote/log to disk (a restarted replica rejoins with its
log), conflict back-off hints, batching (up to `batch` entries per RAFT_AE), a NOOP entry on
election (so a new leader learns which old entries are committed before it serves requests),
check-quorum (a leader that cannot reach a majority steps down), and optional emulated one-way
delay per peer (to place replicas "far apart" on one machine).
Not implemented: log compaction/snapshots, membership changes.
"""
import asyncio
import json
import logging
import os
import random
from collections import defaultdict

from .protocol import Channel, log_event, mono, now

log = logging.getLogger("raft")


class NotLeader(Exception):
    def __init__(self, leader_addr=None, reason="not leader"):
        super().__init__(reason)
        self.leader_addr = leader_addr


class RaftNode:
    def __init__(self, me, addrs, apply_fn, state_dir, election_ms=(300, 600), heartbeat_ms=50,
                 peer_delay_ms=None, fsync=False, batch=256, on_role=None, commit_timeout=3.0):
        self.me = me
        self.addrs = dict(addrs)                              # id -> (host, port), includes me
        self.peers = [p for p in self.addrs if p != me]
        self.n = len(self.addrs)
        self.apply_fn = apply_fn
        self.el_min, self.el_max = election_ms[0] / 1000, election_ms[1] / 1000
        self.hb = heartbeat_ms / 1000
        self.delay = {p: (peer_delay_ms or {}).get(p, 0.0) / 1000 for p in self.peers}
        self.fsync, self.batch, self.on_role = fsync, batch, on_role
        self.commit_timeout = commit_timeout
        # persistent state
        self.term, self.voted_for = 0, None
        self.log = [{"term": 0, "cmd": None}]                 # index 0 is a sentinel
        # volatile state
        self.commit = self.applied = 0
        self.role, self.leader = "follower", None
        self.ready_index = None                               # leader: index of its NOOP
        self.next, self.match, self.last_ack = {}, {}, {}
        self.pending: dict[int, tuple] = {}                   # index -> (term, future, t_proposed)
        self.wake = {p: asyncio.Event() for p in self.peers}
        self.chans: dict = {}
        self.repl_tasks: list = []
        self.counters = defaultdict(int)                      # messages/entries sent and received
        self.history: list = []                               # role changes
        self.commit_ms: list = []                             # propose -> committed (leader)
        self.stopped = False
        os.makedirs(state_dir, exist_ok=True)
        self.meta_path = os.path.join(state_dir, f"raft-{me}.meta.json")
        self.log_path = os.path.join(state_dir, f"raft-{me}.log.jsonl")
        self._load()
        self._reset_deadline()
        self._logf = open(self.log_path, "a", buffering=1)

    # ------------------------------------------------------------------ persistence
    def _load(self):
        if os.path.exists(self.meta_path):
            m = json.load(open(self.meta_path))
            self.term, self.voted_for = m["term"], m["voted_for"]
        if os.path.exists(self.log_path):
            for line in open(self.log_path):
                try:
                    e = json.loads(line)
                except ValueError:
                    break                                     # torn last line after a crash
                del self.log[e["i"]:]
                self.log.append({"term": e["term"], "cmd": e["cmd"]})
        self.loaded_entries = len(self.log) - 1

    def _save_meta(self):
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"term": self.term, "voted_for": self.voted_for}, f)
            if self.fsync:
                f.flush()
                os.fsync(f.fileno())
        os.replace(tmp, self.meta_path)

    def _persist_from(self, i):
        """Append entries i.. to the log file (a re-written index overrides older lines on load)."""
        for k in range(i, len(self.log)):
            self._logf.write(json.dumps({"i": k, **self.log[k]}, separators=(",", ":")) + "\n")
        if self.fsync:
            self._logf.flush()
            os.fsync(self._logf.fileno())

    # ------------------------------------------------------------------ helpers
    def _reset_deadline(self):
        self.deadline = mono() + random.uniform(self.el_min, self.el_max)

    def last(self):
        return len(self.log) - 1, self.log[-1]["term"]

    def leader_addr(self):
        if self.leader in self.addrs:
            h, p = self.addrs[self.leader]
            return f"{h}:{p}"
        return None

    def is_ready(self):
        return self.role == "leader" and self.ready_index is not None and self.commit >= self.ready_index

    def _set_role(self, role):
        if role != self.role:
            self.history.append({"t": round(now(), 4), "mono": round(mono(), 4), "role": role, "term": self.term})
            log_event("raft_role", role=role, term=self.term)
            log.info("%s -> %s (term %d)", self.me, role, self.term)
            self.role = role
            if self.on_role:
                asyncio.get_running_loop().create_task(self.on_role(role, self.term))

    def _step_down(self, term):
        if term > self.term:
            self.term, self.voted_for = term, None
            self._save_meta()
        if self.role != "follower":
            for t in self.repl_tasks:
                t.cancel()
            self.repl_tasks = []
            self.ready_index = None
            for idx, (_, fut, _) in list(self.pending.items()):
                if not fut.done():
                    fut.set_exception(NotLeader(None, "lost leadership; outcome unknown"))
            self.pending.clear()
            self._set_role("follower")
        self._reset_deadline()

    async def _rpc(self, peer, msg, timeout):
        """One request/reply to a peer over a persistent channel, with emulated one-way delay."""
        d = self.delay.get(peer, 0.0)
        if d:
            await asyncio.sleep(d)
        ch = self.chans.get(peer)
        try:
            if ch is None:
                ch = Channel(*self.addrs[peer])
                await ch.connect(timeout=min(timeout, 0.3))
                self.chans[peer] = ch
            self.counters["sent_" + msg["type"]] += 1
            r = await ch.request(msg, timeout=timeout)
        except Exception:
            self.chans.pop(peer, None)
            if ch is not None:
                await ch.close()
            raise
        if d:
            await asyncio.sleep(d)
        return r

    # ------------------------------------------------------------------ election
    async def ticker(self):
        while not self.stopped:
            await asyncio.sleep(0.005)
            t = mono()
            if self.role == "leader":
                # check-quorum: a leader that has not heard from a majority for el_max steps down
                fresh = 1 + sum(1 for p in self.peers if t - self.last_ack.get(p, 0) < self.el_max)
                if fresh * 2 <= self.n and t - self.history[-1]["mono"] > self.el_max:
                    log.warning("%s lost contact with the majority, stepping down", self.me)
                    self._step_down(self.term)
            elif t >= self.deadline:
                await self.start_election()

    async def start_election(self):
        self.term += 1
        self.voted_for = self.me
        self._save_meta()
        self.counters["elections_started"] += 1
        self._set_role("candidate")
        self._reset_deadline()
        term = self.term
        li, lt = self.last()
        votes = {self.me}
        if len(votes) * 2 > self.n:
            return self._become_leader()
        msg = {"type": "RAFT_RV", "term": term, "cand": self.me, "last_i": li, "last_t": lt}

        async def ask(p):
            try:
                return p, await self._rpc(p, msg, timeout=self.el_min)
            except Exception:
                return p, None
        for coro in asyncio.as_completed([ask(p) for p in self.peers]):
            p, r = await coro
            if self.role != "candidate" or self.term != term:
                return
            if r is None:
                continue
            if r["term"] > self.term:
                self._step_down(r["term"])
                return
            if r.get("granted"):
                votes.add(p)
                if len(votes) * 2 > self.n:
                    self._become_leader()
                    return

    def _become_leader(self):
        self.leader = self.me
        self.next = {p: len(self.log) for p in self.peers}
        self.match = {p: 0 for p in self.peers}
        self.last_ack = {p: mono() for p in self.peers}
        self._set_role("leader")
        self.log.append({"term": self.term, "cmd": {"op": "NOOP", "leader": self.me}})
        self._persist_from(len(self.log) - 1)
        self.ready_index = len(self.log) - 1
        self.repl_tasks = [asyncio.get_running_loop().create_task(self._replicate(p, self.term)) for p in self.peers]
        self._advance_commit()

    # ------------------------------------------------------------------ replication (leader)
    async def _replicate(self, p, term):
        while self.role == "leader" and self.term == term and not self.stopped:
            self.wake[p].clear()
            ni = self.next[p]
            prev = ni - 1
            entries = self.log[ni:ni + self.batch]
            msg = {"type": "RAFT_AE", "term": term, "leader": self.me, "prev_i": prev,
                   "prev_t": self.log[prev]["term"], "entries": entries, "commit": self.commit}
            if entries:
                log_event("ae_send", to=p, term=term, first=ni, count=len(entries))
            try:
                r = await self._rpc(p, msg, timeout=max(0.3, self.el_min))
            except Exception:
                await asyncio.sleep(self.hb)
                continue
            if self.role != "leader" or self.term != term:
                return
            if r["term"] > self.term:
                self._step_down(r["term"])
                return
            self.last_ack[p] = mono()
            self.counters["entries_sent"] += len(entries)
            if r.get("ok"):
                self.match[p] = max(self.match[p], prev + len(entries))
                self.next[p] = self.match[p] + 1
                self._advance_commit()
            else:                                             # log mismatch: jump back using the hint
                self.next[p] = max(1, min(ni - 1, r.get("last_i", ni - 1) + 1))
                continue
            if self.next[p] < len(self.log):
                continue
            try:
                await asyncio.wait_for(self.wake[p].wait(), self.hb)
            except asyncio.TimeoutError:
                pass

    def _advance_commit(self):
        if self.role != "leader":
            return
        matched = sorted([len(self.log) - 1] + [self.match[p] for p in self.peers], reverse=True)
        n = matched[self.n // 2]                              # highest index stored on a majority
        if n > self.commit and self.log[n]["term"] == self.term:
            self.commit = n
            self._apply()

    def _apply(self):
        while self.applied < self.commit:
            self.applied += 1
            e = self.log[self.applied]
            res = None
            if e["cmd"] is not None and e["cmd"].get("op") != "NOOP":
                res = self.apply_fn(e["cmd"], self.applied)
            item = self.pending.pop(self.applied, None)
            if item:
                term, fut, t0 = item
                if fut.done():
                    continue
                if e["term"] == term:
                    self.commit_ms.append((mono() - t0) * 1000)
                    fut.set_result(res)
                else:
                    fut.set_exception(NotLeader(self.leader_addr(), "entry overwritten"))

    async def propose(self, cmd):
        """Leader only: append a command, wait until it is committed and applied; return apply result."""
        if not self.is_ready():
            raise NotLeader(self.leader_addr() if self.role != "leader" else None,
                            "not leader" if self.role != "leader" else "leader not ready yet")
        self.log.append({"term": self.term, "cmd": cmd})
        idx = len(self.log) - 1
        self._persist_from(idx)
        fut = asyncio.get_running_loop().create_future()
        self.pending[idx] = (self.term, fut, mono())
        for p in self.peers:
            self.wake[p].set()
        self._advance_commit()
        try:
            return await asyncio.wait_for(fut, self.commit_timeout)
        except asyncio.TimeoutError:
            self.pending.pop(idx, None)
            raise NotLeader(None, "commit timed out (no majority reachable)")

    # ------------------------------------------------------------------ message handlers
    def handle(self, m):
        t = m["type"]
        self.counters["recv_" + t] += 1
        if t == "RAFT_RV":
            if m["term"] > self.term:
                self._step_down(m["term"])
            li, lt = self.last()
            up_to_date = (m["last_t"], m["last_i"]) >= (lt, li)
            granted = (m["term"] == self.term and self.voted_for in (None, m["cand"]) and up_to_date)
            if granted:
                self.voted_for = m["cand"]
                self._save_meta()
                self._reset_deadline()
            return {"ok": True, "term": self.term, "granted": granted}
        if t == "RAFT_AE":
            if m["term"] < self.term:
                return {"ok": False, "term": self.term, "last_i": len(self.log) - 1}
            if m["term"] > self.term or self.role != "follower":
                self._step_down(m["term"])
            if self.leader != m["leader"]:
                self.leader = m["leader"]
                log_event("raft_leader_seen", leader=m["leader"], term=m["term"])
            self._reset_deadline()
            prev = m["prev_i"]
            if m["entries"]:
                log_event("ae_recv", frm=m["leader"], term=m["term"], first=prev + 1, count=len(m["entries"]))
            if prev >= len(self.log) or self.log[prev]["term"] != m["prev_t"]:
                return {"ok": False, "term": self.term, "last_i": min(len(self.log) - 1, prev - 1)}
            first_new = None
            for k, e in enumerate(m["entries"]):
                idx = prev + 1 + k
                if idx < len(self.log):
                    if self.log[idx]["term"] == e["term"]:
                        continue
                    del self.log[idx:]                        # conflicting suffix from an old leader
                self.log.append({"term": e["term"], "cmd": e["cmd"]})
                if first_new is None:
                    first_new = idx
            if first_new is not None:
                self._persist_from(first_new)
            last_new = prev + len(m["entries"])
            if m["commit"] > self.commit:
                self.commit = min(m["commit"], last_new)
                self._apply()
            return {"ok": True, "term": self.term}
        return {"ok": False, "error": "unknown raft message"}

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        if self.n == 1:                                       # single replica: elect itself at once
            self.deadline = mono()
        return asyncio.get_running_loop().create_task(self.ticker())

    async def stop(self):
        self.stopped = True
        for t in self.repl_tasks:
            t.cancel()
        for ch in self.chans.values():
            await ch.close()
        self._logf.close()

    def status(self):
        li, lt = self.last()
        return {"id": self.me, "role": self.role, "term": self.term, "leader": self.leader,
                "ready": self.is_ready(), "last_index": li, "last_term": lt, "commit_index": self.commit,
                "applied": self.applied, "voted_for": self.voted_for, "loaded_entries": self.loaded_entries,
                "counters": dict(self.counters), "history": self.history[-50:],
                "commit_ms_mean": round(sum(self.commit_ms) / len(self.commit_ms), 3) if self.commit_ms else None,
                "commits": len(self.commit_ms)}

"""Inter-node communication: length-prefixed JSON messages over TCP.

Every message is  [4-byte big-endian length][UTF-8 JSON body].

Two ways to talk (S2):
  call()   - control plane. Open a connection, send one request, read one reply, close.
             Used for REGISTER, HEARTBEAT, REQUEST, ALLOCATE ... (low rate).
  Channel  - data plane. One long-lived connection that carries many requests at once.
             Each request carries an id ("rid"); the reply echoes it so answers that come
             back in a different order still reach the right caller. Used for TASK traffic.

Servers accept both: a connection may carry one message or many.

S4 - logical time. Every message carries a Lamport clock value "lc":
  * before an event or a send, a process increments its counter;
  * a message carries the counter value at the moment it is sent;
  * on receive, the counter becomes max(own, received) + 1.
So if event a can have caused event b, lc(a) < lc(b), whatever the wall clocks say.
TELERM_CLOCK_SKEW_MS shifts this process's wall clock (to test ordering with wrong clocks).
TELERM_EVENTLOG=<file> makes log_event() write {node, kind, lc, wall, ...} lines to that file.
"""
import asyncio
import itertools
import json
import os
import struct
import time

_HDR = struct.Struct("!I")
MAX_MSG_BYTES = 4 * 1024 * 1024

# Per-process communication counters (communication-overhead analysis).
STATS = {"msgs_sent": 0, "bytes_sent": 0, "msgs_recv": 0, "bytes_recv": 0,
         "call_errors": 0, "connections_opened": 0}


SKEW_S = float(os.environ.get("TELERM_CLOCK_SKEW_MS", "0")) / 1000.0
CLOCK = {"lc": 0}                      # this process's Lamport clock
_EVENTLOG = None
NODE = {"name": os.environ.get("TELERM_NODE", f"pid{os.getpid()}")}


def now() -> float:
    """Wall-clock time (seconds). Includes the injected skew, like a badly synchronised clock."""
    return time.time() + SKEW_S


def tick() -> int:
    """Lamport rule 1: a local event (or a send) increments the clock."""
    CLOCK["lc"] += 1
    return CLOCK["lc"]


def observe(lc) -> None:
    """Lamport rule 2: on receive, jump past the sender's clock."""
    if isinstance(lc, int):
        CLOCK["lc"] = max(CLOCK["lc"], lc) + 1


def log_event(kind: str, **kw) -> None:
    """Record an event with both its Lamport time and its (possibly skewed) wall time."""
    global _EVENTLOG
    path = os.environ.get("TELERM_EVENTLOG")
    if not path:
        return
    if _EVENTLOG is None:
        _EVENTLOG = open(path, "a", buffering=1)
    _EVENTLOG.write(json.dumps({"node": NODE["name"], "kind": kind, "lc": tick(), "wall": now(), **kw},
                               separators=(",", ":")) + "\n")


def mono() -> float:
    """Monotonic clock for measuring durations on one machine (never jumps backwards)."""
    return time.perf_counter()


async def send_msg(writer: asyncio.StreamWriter, msg: dict) -> None:
    body = json.dumps({**msg, "lc": tick()}, separators=(",", ":")).encode("utf-8")
    writer.write(_HDR.pack(len(body)) + body)
    await writer.drain()
    STATS["msgs_sent"] += 1
    STATS["bytes_sent"] += _HDR.size + len(body)


async def recv_msg(reader: asyncio.StreamReader) -> dict:
    (length,) = _HDR.unpack(await reader.readexactly(_HDR.size))
    if length > MAX_MSG_BYTES:
        raise ValueError(f"message too large: {length} bytes")
    body = await reader.readexactly(length)
    STATS["msgs_recv"] += 1
    STATS["bytes_recv"] += _HDR.size + length
    msg = json.loads(body)
    observe(msg.get("lc"))
    return msg


async def call(host: str, port: int, msg: dict, timeout: float = 5.0) -> dict:
    """Control plane: send one request on a fresh connection and wait for its reply."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
        STATS["connections_opened"] += 1
    except Exception:
        STATS["call_errors"] += 1
        raise
    try:
        await send_msg(writer, msg)
        return await asyncio.wait_for(recv_msg(reader), timeout)
    except Exception:
        STATS["call_errors"] += 1
        raise
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


class Channel:
    """Data plane: one persistent connection, many requests in flight at once."""

    def __init__(self, host: str, port: int):
        self.host, self.port = host, port
        self.reader = self.writer = None
        self.pending: dict[int, asyncio.Future] = {}
        self.ids = itertools.count(1)
        self.lock = asyncio.Lock()
        self.reader_task = None

    async def connect(self, timeout: float = 5.0):
        self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), timeout)
        STATS["connections_opened"] += 1
        self.reader_task = asyncio.create_task(self._read_loop())

    async def _read_loop(self):
        try:
            while True:
                msg = await recv_msg(self.reader)
                fut = self.pending.pop(msg.get("rid"), None)
                if fut is not None and not fut.done():
                    fut.set_result(msg)
        except Exception as exc:                       # connection closed or broken
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError(f"channel closed: {exc!r}"))
            self.pending.clear()

    async def request(self, msg: dict, timeout: float = 5.0) -> dict:
        rid = next(self.ids)
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        async with self.lock:                          # one writer at a time on the socket
            await send_msg(self.writer, {**msg, "rid": rid})
        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            self.pending.pop(rid, None)

    async def close(self):
        if self.reader_task:
            self.reader_task.cancel()
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass


def make_server_callback(handler):
    """Wrap `async handler(msg) -> reply` as an asyncio stream-server callback.

    Reads messages until the client closes the connection. Each message is handled in its
    own task, so a slow request (e.g. a TASK waiting in a queue) does not hold up others on
    the same connection. Replies echo the request's "rid".
    """
    async def _callback(reader, writer):
        lock = asyncio.Lock()
        inflight = set()

        async def _serve(msg):
            try:
                reply = await handler(msg)
                if reply is None:
                    reply = {"ok": True}
            except Exception as exc:                   # never let one bad message kill the node
                reply = {"ok": False, "error": repr(exc)}
            if "rid" in msg:
                reply = {**reply, "rid": msg["rid"]}
            try:
                async with lock:
                    await send_msg(writer, reply)
            except Exception:
                pass

        try:
            while True:
                msg = await recv_msg(reader)
                t = asyncio.create_task(_serve(msg))
                inflight.add(t)
                t.add_done_callback(inflight.discard)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception:
            pass
        finally:
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)
            writer.close()
    return _callback

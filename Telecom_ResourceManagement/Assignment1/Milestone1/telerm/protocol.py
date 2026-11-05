"""Inter-node communication: length-prefixed JSON messages over TCP.

Every message is  [4-byte big-endian length][UTF-8 JSON body].
Every exchange is one request -> one reply on a fresh TCP connection.
This is deliberately simple for Milestone 1; Milestone 11 replaces it with RPC.
"""
import asyncio
import json
import struct
import time

_HDR = struct.Struct("!I")
MAX_MSG_BYTES = 4 * 1024 * 1024

# Per-process communication counters (used for overhead analysis in Week 2).
STATS = {"msgs_sent": 0, "bytes_sent": 0, "msgs_recv": 0, "bytes_recv": 0, "call_errors": 0}


def now() -> float:
    return time.time()


async def send_msg(writer: asyncio.StreamWriter, msg: dict) -> None:
    body = json.dumps(msg, separators=(",", ":")).encode("utf-8")
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
    return json.loads(body)


async def call(host: str, port: int, msg: dict, timeout: float = 5.0) -> dict:
    """Send one request to a node and wait for its reply."""
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
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


def make_server_callback(handler):
    """Wrap `async handler(msg) -> reply` as an asyncio stream-server callback."""
    async def _callback(reader, writer):
        try:
            msg = await recv_msg(reader)
            reply = await handler(msg)
            await send_msg(writer, reply if reply is not None else {"ok": True})
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as exc:  # never let one bad message kill the node
            try:
                await send_msg(writer, {"ok": False, "error": repr(exc)})
            except Exception:
                pass
        finally:
            writer.close()
    return _callback

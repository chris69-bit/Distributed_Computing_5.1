"""Registry client (S3): what every service uses to find the others.

A node only needs to know the registry's address. Everything else is looked up by name:

    reg = RegistryClient("registry:8010")
    await reg.register(name="gw-A", kind="service", host=..., port=..., location="edge-A")
    addr = await reg.lookup("orchestrator")        # -> ("core-host", 8000), cached
"""
import asyncio
import logging
import os

from .protocol import call

log = logging.getLogger("naming")


def split_addr(addr: str):
    h, p = addr.rsplit(":", 1)
    return h, int(p)


class RegistryClient:
    def __init__(self, address: str):
        self.addr = split_addr(address)
        self.cache: dict[str, tuple] = {}
        self.name = None

    async def register(self, *, name, kind, host, port, location=None, tier=None, capacity=None,
                       retries=300, delay=0.2):
        self.name = name
        msg = {"type": "REGISTER", "name": name, "kind": kind, "host": host, "port": port,
               "location": location or name, "tier": tier, "capacity": capacity, "pid": os.getpid()}
        for _ in range(retries):                        # the registry may still be starting
            try:
                if (await call(*self.addr, msg, timeout=2.0)).get("ok"):
                    return True
            except Exception:
                pass
            await asyncio.sleep(delay)
        raise RuntimeError(f"registry {self.addr} unreachable")

    async def heartbeat(self, **extra):
        try:
            r = await call(*self.addr, {"type": "HEARTBEAT", "name": self.name, **extra}, timeout=2.0)
            return r.get("ok", False)
        except Exception as exc:
            log.warning("heartbeat to registry failed: %r", exc)
            return False

    async def lookup(self, name: str, fresh: bool = False, wait_s: float = 30.0):
        """Address of a named service, e.g. 'orchestrator'. Waits until it has registered."""
        if not fresh and name in self.cache:
            return self.cache[name]
        deadline = asyncio.get_running_loop().time() + wait_s
        while True:
            try:
                r = await call(*self.addr, {"type": "LOOKUP", "name": name}, timeout=2.0)
                if r.get("ok"):
                    self.cache[name] = split_addr(r["address"])
                    return self.cache[name]
            except Exception:
                pass
            if asyncio.get_running_loop().time() > deadline:
                raise RuntimeError(f"name '{name}' not found in registry")
            await asyncio.sleep(0.2)

    def forget(self, name: str):
        self.cache.pop(name, None)

    async def call_service(self, name: str, msg: dict, timeout: float = 5.0):
        """Send a message to a named service; on failure, look the name up again once."""
        try:
            return await call(*(await self.lookup(name)), msg, timeout=timeout)
        except Exception:
            self.forget(name)
            return await call(*(await self.lookup(name)), msg, timeout=timeout)

    async def rpc(self, msg: dict, timeout: float = 5.0):
        return await call(*self.addr, msg, timeout=timeout)

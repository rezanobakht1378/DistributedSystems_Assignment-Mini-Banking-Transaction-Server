"""TCP load balancer in front of the multi-VM cluster (runs outside the VMs).

    python edge_proxy.py --route 18080=10.0.0.3:30080,10.0.0.4:30080 \
                         --route 18443=10.0.0.2:8443,10.0.0.3:8443

Each route listens on a port and forwards every connection to the first
upstream that accepts it, starting at a rotating position so connections are
spread. An upstream that refuses or times out is skipped, so a dead VM only
costs a failed connect attempt. Like a cloud load balancer, it knows nothing
about HTTP or the application.
"""
from __future__ import annotations

import argparse
import asyncio
import itertools
import logging

_LOG = logging.getLogger("edge_proxy")


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        try:
            writer.close()
        except OSError:
            pass


def _handler(port: int, upstreams: list[tuple[str, int]]):
    rotation = itertools.cycle(range(len(upstreams)))

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        start = next(rotation)
        for offset in range(len(upstreams)):
            host, upstream_port = upstreams[(start + offset) % len(upstreams)]
            try:
                upstream_reader, upstream_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, upstream_port), timeout=1.0)
            except (OSError, asyncio.TimeoutError):
                _LOG.warning("upstream_down port=%s upstream=%s:%s", port, host, upstream_port)
                continue
            await asyncio.gather(
                _pipe(reader, upstream_writer), _pipe(upstream_reader, writer))
            return
        _LOG.error("no_upstream port=%s", port)
        writer.close()

    return handle


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--route", action="append", required=True,
                        help="LISTEN_PORT=HOST:PORT[,HOST:PORT...]")
    args = parser.parse_args()
    servers = []
    for route in args.route:
        port, targets = route.split("=", 1)
        upstreams = [(item.rsplit(":", 1)[0], int(item.rsplit(":", 1)[1]))
                     for item in targets.split(",") if item]
        servers.append(await asyncio.start_server(_handler(int(port), upstreams), "0.0.0.0", int(port)))
        _LOG.info("route port=%s upstreams=%s", port, upstreams)
    await asyncio.gather(*(server.serve_forever() for server in servers))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())

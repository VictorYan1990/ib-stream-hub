"""Entry point: connect to IB Gateway and stream market data to stdout."""

import asyncio
import logging
import sys
from pathlib import Path

from ib_insync import util

from ib_stream.config import ConnectionSettings, load_config
from ib_stream.gateway import IBGateway
from ib_stream.ingestor import Ingestor

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path(__file__).parent / "ib_stream" / "config" / "contracts.yaml"
_STALENESS_TIMEOUT = 60.0


async def _drain_queues(ingestor: Ingestor) -> None:
    """Consume all queues and print each item. Replace with your own logic."""
    async def drain(symbol: str) -> None:
        q = ingestor.get_queue(symbol)
        while True:
            try:
                item = await asyncio.wait_for(q.get(), timeout=_STALENESS_TIMEOUT)
                logger.info("[%s] %s", symbol, item)
            except asyncio.TimeoutError:
                logger.warning("[%s] No data received in %.0fs", symbol, _STALENESS_TIMEOUT)

    await asyncio.gather(*(drain(sym) for sym in ingestor.queues))


def main(config_path: str | None = None) -> None:
    conn = ConnectionSettings()   # reads IB_* env vars / .env file
    cfg = load_config(config_path or DEFAULT_CONFIG)

    gateway = IBGateway.from_config(conn)
    ingestor = Ingestor(gateway, cfg.contracts)
    gateway.on_reconnected = ingestor.restart  # re-subscribe after every reconnect

    gateway.connect()
    ingestor.start()

    try:
        util.run(_drain_queues(ingestor))
    except KeyboardInterrupt:
        logger.info("Shutting down …")
    finally:
        ingestor.stop()
        gateway.disconnect()


if __name__ == "__main__":
    from ib_stream.cli import cli
    cli(sys.argv[1:])

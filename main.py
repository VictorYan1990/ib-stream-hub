"""Entry point: connect to IB Gateway and stream market data through pipelines."""

import logging
import sys

from ib_insync import util

from ib_stream.cli import (
    DEFAULT_CONTRACTS_CONFIG,
    DEFAULT_PIPELINES_CONFIG,
    DEFAULT_SINKS_CONFIG,
    DEFAULT_SOURCES_CONFIG,
    _build_parser,
)
from ib_stream.config import (
    ConnectionSettings,
    load_config,
    load_pipelines_config,
    load_sinks_config,
)
from ib_stream.gateway import IBGateway
from ib_stream.ingestor import Ingestor
from ib_stream.pipeline import build_pipelines
from market_data_consumer import SQSConsumer, load_sources_config

logger = logging.getLogger(__name__)

LOG_FORMAT = (
    "%(asctime)s %(levelname)-8s %(filename)s:%(lineno)d %(name)s: %(message)s"
)

# Re-exported for backwards compatibility / convenience.
__all__ = [
    "DEFAULT_CONTRACTS_CONFIG",
    "DEFAULT_SINKS_CONFIG",
    "DEFAULT_PIPELINES_CONFIG",
    "DEFAULT_SOURCES_CONFIG",
    "LOG_FORMAT",
    "main",
]


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv if argv is not None else [])

    logging.basicConfig(level=args.log_level, format=LOG_FORMAT)

    conn = ConnectionSettings()
    app_cfg = load_config(args.contracts)
    sinks_cfg = load_sinks_config(args.sinks)
    pipelines_cfg = load_pipelines_config(args.pipelines)
    sources_cfg = load_sources_config(args.sources)

    # Start the consumer first so it's ready to drain the queue as soon as
    # the ingester begins publishing. SQSConsumer.from_source validates that
    # the selected source is of type 'sqs'.
    consumer = SQSConsumer.from_source(sources_cfg.sources[0])
    consumer.start()

    pipelines = build_pipelines(app_cfg, sinks_cfg, pipelines_cfg)

    gateway = IBGateway.from_config(conn)
    ingestor = Ingestor(gateway, pipelines)
    gateway.on_reconnected = ingestor.restart

    gateway.connect()
    ingestor.start()

    try:
        util.run(ingestor.watchdog())
    except KeyboardInterrupt:
        logger.info("Shutting down …")
    finally:
        ingestor.stop()
        gateway.disconnect()
        consumer.stop()


if __name__ == "__main__":
    main(sys.argv[1:])

"""CLI entry point for ib-stream-hub.

Parses command-line arguments and delegates to main.main().
All contract definitions come from the config file.
All connection settings come from environment variables / .env file.
"""

import argparse
import logging
import sys
from pathlib import Path

from main import DEFAULT_CONFIG, main


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ib-stream",
        description="Stream real-time market data from Interactive Brokers Gateway.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config",
        metavar="PATH",
        default=str(DEFAULT_CONFIG),
        help="Path to the contracts YAML configuration file.",
    )
    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    return parser


def cli(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    main(config_path=args.config)


if __name__ == "__main__":
    cli(sys.argv[1:])

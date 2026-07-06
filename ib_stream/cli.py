"""Command-line argument definitions for ib-stream-hub.

This module only defines the argument parser and the default config paths.
Argument parsing, logger setup, and orchestration live in ``main.main()``.
"""

import argparse
from pathlib import Path

# Config paths are resolved relative to the project root (the parent of the
# ib_stream package directory), so they are independent of the caller's CWD.
_ROOT = Path(__file__).parent.parent
_INGESTOR_CONFIG_DIR = _ROOT / "ib_stream" / "config"
_CONSUMER_CONFIG_DIR = _ROOT / "market_data_consumer" / "config"

DEFAULT_CONTRACTS_CONFIG = _INGESTOR_CONFIG_DIR / "contracts.yaml"
DEFAULT_SINKS_CONFIG = _INGESTOR_CONFIG_DIR / "sinks.yaml"
DEFAULT_PIPELINES_CONFIG = _INGESTOR_CONFIG_DIR / "pipelines.yaml"
DEFAULT_SOURCES_CONFIG = _CONSUMER_CONFIG_DIR / "sources.yaml"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ib-stream",
        description=(
            "Stream real-time market data from Interactive Brokers Gateway "
            "through configurable pipelines."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--contracts",
        metavar="PATH",
        default=str(DEFAULT_CONTRACTS_CONFIG),
        help="Path to the contracts YAML configuration file.",
    )
    parser.add_argument(
        "-s", "--sinks",
        metavar="PATH",
        default=str(DEFAULT_SINKS_CONFIG),
        help="Path to the sinks YAML configuration file.",
    )
    parser.add_argument(
        "-p", "--pipelines",
        metavar="PATH",
        default=str(DEFAULT_PIPELINES_CONFIG),
        help="Path to the pipelines YAML configuration file.",
    )
    parser.add_argument(
        "--sources",
        metavar="PATH",
        default=str(DEFAULT_SOURCES_CONFIG),
        help="Path to the consumer sources YAML configuration file.",
    )
    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity.",
    )
    return parser

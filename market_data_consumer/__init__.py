"""Market data consumer subpackage.

Standalone consumer that polls an SQS queue and logs each received message
to stdout. Intentionally decoupled from the ``ib_stream`` ingester package
so it can be lifted into its own repo later; the only shared contract is
the JSON payload published to the queue.
"""

from .config import SourceConfig, SourcesConfig, SQSSourceOptions, load_sources_config
from .sqs_consumer import SQSConsumer

__all__ = [
    "SQSConsumer",
    "SourceConfig",
    "SourcesConfig",
    "SQSSourceOptions",
    "load_sources_config",
]

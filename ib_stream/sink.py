"""Sink layer: pluggable destinations for market data messages.

A :class:`Sink` is any object that can ``publish(key, payload)``.  Concrete
implementations wrap a provider (AWS SQS, AWS Kinesis, an in-memory thread
queue, …).  :func:`create_sink` builds the right implementation from a
:class:`~ib_stream.config.SinkConfig`, so the rest of the system can switch
providers purely through configuration — handy for swapping a real cloud
queue for an in-memory ``thread`` sink during tests or local runs.
"""

import json
import logging
import queue
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import KinesisConfig, SinkConfig, SQSConfig

logger = logging.getLogger(__name__)


class Sink(ABC):
    """Abstract destination for serialised market data payloads."""

    @classmethod
    @abstractmethod
    def from_options(cls, options: Dict[str, Any]) -> "Sink":
        """Build a sink instance from a provider-specific options dict."""

    @abstractmethod
    def publish(self, key: str, payload: dict) -> None:
        """Publish *payload* routed/partitioned by *key*."""


class SQSSink(Sink):
    """Publishes market data payloads to an AWS SQS queue.

    Credentials are resolved by boto3 in the standard order:
    env vars → ~/.aws/credentials → IAM instance role.

    FIFO queues (queue_url ending in ``.fifo``) automatically receive a
    ``MessageGroupId`` equal to the partition key and a UUID-based
    ``MessageDeduplicationId`` so every tick is delivered even when the
    content is identical to a previous one.
    """

    def __init__(self, region: str, queue_url: str) -> None:
        self._queue_url = queue_url
        self._is_fifo = queue_url.endswith(".fifo")
        self._client = boto3.client("sqs", region_name=region)

    @classmethod
    def from_options(cls, options: Dict[str, Any]) -> "SQSSink":
        return cls.from_config(SQSConfig(**options))

    @classmethod
    def from_config(cls, cfg: SQSConfig) -> "SQSSink":
        return cls(region=cfg.region, queue_url=cfg.queue_url)

    def publish(self, key: str, payload: dict) -> None:
        """Send *payload* to SQS, keyed by *key*.

        Logs a WARNING and returns without raising if the send fails so that
        a transient SQS outage does not kill the IB data feed.
        """
        body = json.dumps(payload, default=str)
        kwargs: dict = {"QueueUrl": self._queue_url, "MessageBody": body}
        if self._is_fifo:
            kwargs["MessageGroupId"] = key
            kwargs["MessageDeduplicationId"] = str(uuid.uuid4())
        try:
            self._client.send_message(**kwargs)
            logger.debug("[%s] published to SQS: %s", key, body)
        except (BotoCoreError, ClientError) as exc:
            logger.warning("[%s] SQS publish failed: %s", key, exc)


class KinesisSink(Sink):
    """Publishes market data payloads to an AWS Kinesis Data Stream.

    The partition *key* is used as the Kinesis ``PartitionKey`` so all records
    for a given contract land on the same shard (preserving per-key ordering).
    """

    def __init__(self, region: str, stream_name: str) -> None:
        self._stream_name = stream_name
        self._client = boto3.client("kinesis", region_name=region)

    @classmethod
    def from_options(cls, options: Dict[str, Any]) -> "KinesisSink":
        cfg = KinesisConfig(**options)
        return cls(region=cfg.region, stream_name=cfg.stream_name)

    def publish(self, key: str, payload: dict) -> None:
        body = json.dumps(payload, default=str)
        try:
            self._client.put_record(
                StreamName=self._stream_name,
                Data=body.encode("utf-8"),
                PartitionKey=key,
            )
            logger.debug("[%s] published to Kinesis: %s", key, body)
        except (BotoCoreError, ClientError) as exc:
            logger.warning("[%s] Kinesis publish failed: %s", key, exc)


class ThreadSink(Sink):
    """In-process sink backed by a thread-safe queue.

    No external provider — messages are kept in memory and can be drained via
    :meth:`get_nowait` or inspected through :attr:`messages`.  Ideal for tests
    and local development where standing up a real cloud queue is overkill.
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: "queue.Queue[tuple[str, dict]]" = queue.Queue(maxsize=maxsize)

    @classmethod
    def from_options(cls, options: Dict[str, Any]) -> "ThreadSink":
        return cls(maxsize=int(options.get("maxsize", 0)))

    def publish(self, key: str, payload: dict) -> None:
        try:
            self._queue.put_nowait((key, payload))
            logger.debug("[%s] published to thread queue", key)
        except queue.Full:
            logger.warning("[%s] thread queue full — dropping message", key)

    def get_nowait(self) -> "tuple[str, dict]":
        return self._queue.get_nowait()

    @property
    def messages(self) -> List["tuple[str, dict]"]:
        """Snapshot of currently queued (key, payload) pairs."""
        return list(self._queue.queue)


# Registry mapping a sink ``type`` string to its implementation class.
# Add new providers (e.g. an "mq" RabbitMQ/Kafka sink) here.
SINK_REGISTRY: Dict[str, type[Sink]] = {
    "sqs": SQSSink,
    "kinesis": KinesisSink,
    "thread": ThreadSink,
}


def create_sink(cfg: SinkConfig) -> Sink:
    """Instantiate the sink implementation selected by ``cfg.type``."""
    try:
        sink_cls = SINK_REGISTRY[cfg.type]
    except KeyError:
        available = ", ".join(sorted(SINK_REGISTRY))
        raise ValueError(
            f"Unknown sink type {cfg.type!r} for sink {cfg.id!r}. "
            f"Available types: {available}."
        ) from None
    return sink_cls.from_options(cfg.options)

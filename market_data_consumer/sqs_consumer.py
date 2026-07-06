"""SQS consumer.

Runs a background thread that long-polls an AWS SQS queue and logs every
received message to stdout, then deletes it. Scaffolding for the hybrid
end-to-end test: ingester (local) → SQS (cloud) → consumer (local).
"""

import json
import logging
import threading
from typing import Any, Dict

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from .config import SourceConfig, SQSSourceOptions

logger = logging.getLogger(__name__)


class SQSConsumer:
    """Polls an SQS queue on a daemon thread and logs each message body."""

    def __init__(
        self,
        region: str,
        queue_url: str,
        wait_time_seconds: int = 20,
        max_messages: int = 10,
    ) -> None:
        self._queue_url = queue_url
        self._wait_time_seconds = wait_time_seconds
        self._max_messages = max_messages
        self._client = boto3.client("sqs", region_name=region)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    @classmethod
    def from_source(cls, source: SourceConfig) -> "SQSConsumer":
        if source.type != "sqs":
            raise ValueError(
                f"SQSConsumer requires source type 'sqs', got {source.type!r} "
                f"for source {source.id!r}"
            )
        opts = SQSSourceOptions(**source.options)
        return cls(region=opts.region, queue_url=opts.queue_url)

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("SQSConsumer already started")
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="sqs-consumer", daemon=True
        )
        self._thread.start()
        logger.info("SQS consumer started (queue=%s)", self._queue_url)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        logger.info("SQS consumer stopped")

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                resp = self._client.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=self._max_messages,
                    WaitTimeSeconds=self._wait_time_seconds,
                )
            except (BotoCoreError, ClientError) as exc:
                logger.warning("SQS receive failed: %s", exc)
                # Back off briefly on error so we don't hot-spin on repeated failures.
                self._stop.wait(1.0)
                continue

            for msg in resp.get("Messages", []):
                self._handle(msg)
                try:
                    self._client.delete_message(
                        QueueUrl=self._queue_url,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                except (BotoCoreError, ClientError) as exc:
                    logger.warning("SQS delete failed: %s", exc)

    def _handle(self, msg: Dict[str, Any]) -> None:
        body = msg.get("Body", "")
        try:
            payload = json.loads(body)
            logger.info("consumed: %s", payload)
        except (json.JSONDecodeError, TypeError):
            logger.info("consumed (raw): %s", body)

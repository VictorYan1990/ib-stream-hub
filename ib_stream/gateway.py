"""IB Gateway connection manager."""

import asyncio
import logging
import random
import time
from typing import Callable

from ib_insync import IB

from .config import ConnectionSettings

logger = logging.getLogger(__name__)

_RECONNECT_DELAY = 5.0
_RECONNECT_JITTER = 2.0


class IBGateway:
    """Manages an IB Gateway / TWS connection with automatic reconnection.

    Usage::

        gw = IBGateway.from_config(cfg.connection)
        gw.connect()
        # use gw.ib for all ib_insync calls
        gw.disconnect()
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        timeout: float = 10.0,
        readonly: bool = True,
    ) -> None:
        self.ib = IB()
        self._host = host
        self._port = port
        self._client_id = client_id
        self._timeout = timeout
        self._readonly = readonly
        self._reconnect_task: asyncio.Task | None = None
        self._disconnect_time: float = 0.0
        self.on_reconnected: Callable[[], None] | None = None

        self.ib.disconnectedEvent += self._on_disconnected
        self.ib.errorEvent += self._on_error

    @classmethod
    def from_config(cls, cfg: ConnectionSettings) -> "IBGateway":
        return cls(
            host=cfg.host,
            port=cfg.port,
            client_id=cfg.client_id,
            timeout=cfg.timeout,
            readonly=cfg.readonly,
        )

    def connect(self) -> None:
        self.ib.connect(
            host=self._host,
            port=self._port,
            clientId=self._client_id,
            timeout=self._timeout,
            readonly=self._readonly,
        )
        logger.info("Connected to IB Gateway at %s:%d", self._host, self._port)

    def disconnect(self) -> None:
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        self.ib.disconnect()
        logger.info("Disconnected from IB Gateway")

    @property
    def is_connected(self) -> bool:
        return self.ib.isConnected()

    def _on_disconnected(self) -> None:
        self._disconnect_time = time.monotonic()
        logger.warning("Disconnected from IB Gateway — scheduling reconnect")
        self._reconnect_task = asyncio.ensure_future(self._reconnect_loop())

    def _on_error(self, req_id: int, error_code: int, error_string: str, contract) -> None:
        # Codes < 2000 are warnings/informational; >= 2000 are real errors.
        if error_code >= 2000:
            logger.error(
                "IB error %d (reqId=%d): %s [contract=%s]",
                error_code, req_id, error_string, contract,
            )
        else:
            logger.debug(
                "IB notice %d (reqId=%d): %s",
                error_code, req_id, error_string,
            )

    async def _reconnect_loop(self) -> None:
        delay = _RECONNECT_DELAY
        attempt = 0
        while not self.ib.isConnected():
            attempt += 1
            sleep_for = delay + random.uniform(0, _RECONNECT_JITTER)
            logger.info("Reconnect attempt %d in %.1fs …", attempt, sleep_for)
            await asyncio.sleep(sleep_for)
            try:
                await self.ib.connectAsync(
                    host=self._host,
                    port=self._port,
                    clientId=self._client_id,
                    timeout=self._timeout,
                    readonly=self._readonly,
                )
                elapsed = time.monotonic() - self._disconnect_time
                logger.info(
                    "Reconnected to IB Gateway after %.1fs (%d attempt(s))",
                    elapsed, attempt,
                )
                if self.on_reconnected:
                    self.on_reconnected()
                return
            except Exception as exc:
                logger.error("Reconnect attempt %d failed: %s", attempt, exc)
                delay = min(delay * 2, 60.0)

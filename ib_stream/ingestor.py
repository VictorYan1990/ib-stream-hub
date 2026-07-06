"""Market data ingestor: subscribes to IB contracts and publishes to a configured sink."""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from ib_insync import IB, Contract
from ib_insync.contract import CFD, Forex, Future, Index, Stock
from ib_insync.objects import BarData, RealTimeBar

from .config import ContractConfig, DataType
from .gateway import IBGateway
from .pipeline import ResolvedPipeline
from .sink import Sink

logger = logging.getLogger(__name__)

_REALTIME_BAR_SIZE = "5 secs"
_STALENESS_TIMEOUT = 360.0


def _partition_key(cfg: ContractConfig) -> str:
    """Build a stable routing key from contract identity.

    Format: ``SYMBOL:SEC_TYPE`` or ``SYMBOL:SEC_TYPE:EXPIRY`` when a
    last_trade_date is set, so two futures with different expirations land
    in separate FIFO message groups.
    """
    parts = [cfg.symbol, cfg.sec_type]
    if cfg.last_trade_date:
        parts.append(cfg.last_trade_date)
    return ":".join(parts)


def _build_contract(cfg: ContractConfig) -> Contract:
    kwargs: Dict[str, Any] = {
        "exchange": cfg.exchange,
        "currency": cfg.currency,
    }
    if cfg.primary_exchange:
        kwargs["primaryExchange"] = cfg.primary_exchange
    if cfg.last_trade_date:
        kwargs["lastTradeDateOrContractMonth"] = cfg.last_trade_date

    sec_type = cfg.sec_type.upper()

    if sec_type == "STK":
        return Stock(symbol=cfg.symbol, **kwargs)
    if sec_type == "FUT":
        return Future(symbol=cfg.symbol, **kwargs)
    if sec_type == "CASH":
        return Forex(pair=cfg.symbol, exchange=cfg.exchange)
    if sec_type == "IND":
        return Index(symbol=cfg.symbol, **kwargs)
    if sec_type == "CFD":
        return CFD(symbol=cfg.symbol, **kwargs)

    return Contract(secType=sec_type, symbol=cfg.symbol, **kwargs)


def _tick_payload(cfg: ContractConfig, ticker) -> dict:
    return {
        "type": "tick",
        "symbol": cfg.symbol,
        "sec_type": cfg.sec_type,
        "exchange": cfg.exchange,
        "currency": cfg.currency,
        "bid": ticker.bid,
        "ask": ticker.ask,
        "last": ticker.last,
        "volume": ticker.volume,
        "time": str(ticker.time),
    }


def _realtime_bar_payload(cfg: ContractConfig, bar: RealTimeBar) -> dict:
    return {
        "type": "realtime_bar",
        "symbol": cfg.symbol,
        "sec_type": cfg.sec_type,
        "exchange": cfg.exchange,
        "currency": cfg.currency,
        "time": str(bar.time),
        "open": bar.open_,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "wap": bar.wap,
    }


def _hist_bar_payload(cfg: ContractConfig, bar: BarData) -> dict:
    return {
        "type": "hist_bar",
        "symbol": cfg.symbol,
        "sec_type": cfg.sec_type,
        "exchange": cfg.exchange,
        "currency": cfg.currency,
        "date": str(bar.date),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }


@dataclass
class _Subscription:
    symbol: str
    sub_type: str          # "tick" | "realtime_bar" | "hist_bar"
    contract: Contract
    handle: Any            # Ticker | RealTimeBarList | BarDataList
    handler: Any           # the callable added to handle.updateEvent


class Ingestor:
    """Subscribes to IB market data and publishes each event to a pipeline's sink.

    The ingestor is driven by a list of :class:`~ib_stream.pipeline.ResolvedPipeline`
    objects, each pairing a contract with the sink it should feed.  Each incoming
    tick or bar is serialised to a dict and forwarded to ``sink.publish(key, payload)``.
    The partition *key* encodes the full contract identity (symbol, sec_type, and
    expiry when present) so that futures with different expirations are routed to
    separate FIFO groups.
    """

    def __init__(
        self,
        gateway: IBGateway,
        pipelines: List[ResolvedPipeline],
    ) -> None:
        self._gateway = gateway
        self._pipelines = pipelines
        self._subscriptions: List[_Subscription] = []
        self._last_publish: Dict[str, float] = {}

    @classmethod
    def from_contracts(
        cls,
        gateway: IBGateway,
        contracts: List[ContractConfig],
        sink: Sink,
    ) -> "Ingestor":
        """Build an ingestor that routes every contract to a single shared sink."""
        pipelines = [
            ResolvedPipeline(id=cfg.id, contract=cfg, sink=sink) for cfg in contracts
        ]
        return cls(gateway, pipelines)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Subscribe to all configured pipelines. Must be called after gateway.connect()."""
        for pipeline in self._pipelines:
            self._subscribe(pipeline)

    def restart(self) -> None:
        """Re-subscribe to all pipelines after a reconnect."""
        logger.info("Reconnected — re-subscribing %d pipeline(s) …", len(self._pipelines))
        self.stop()
        self.start()

    def stop(self) -> None:
        """Cancel all active subscriptions."""
        ib = self._gateway.ib
        for sub in self._subscriptions:
            try:
                sub.handle.updateEvent -= sub.handler
                if sub.sub_type == "tick":
                    ib.cancelMktData(sub.contract)
                elif sub.sub_type == "realtime_bar":
                    ib.cancelRealTimeBars(sub.handle)
                elif sub.sub_type == "hist_bar":
                    ib.cancelHistoricalData(sub.handle)
            except Exception as exc:
                logger.warning("Error cancelling subscription for %s: %s", sub.symbol, exc)
        self._subscriptions.clear()
        logger.info("All subscriptions cancelled")

    async def watchdog(self, timeout: float = _STALENESS_TIMEOUT) -> None:
        """Keep the process alive and warn when a symbol goes silent."""
        while True:
            await asyncio.sleep(timeout)
            now = time.monotonic()
            for key, last in self._last_publish.items():
                age = now - last
                if age > timeout:
                    logger.warning("[%s] No data published in %.0fs", key, age)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _subscribe(self, pipeline: ResolvedPipeline) -> None:
        ib: IB = self._gateway.ib
        cfg = pipeline.contract
        sink = pipeline.sink
        contract = _build_contract(cfg)
        key = _partition_key(cfg)
        self._last_publish[key] = time.monotonic()

        if cfg.data_type == DataType.TICK:
            sub = self._subscribe_tick(ib, cfg, sink, contract, key)
        else:
            if cfg.bar_size == _REALTIME_BAR_SIZE:
                sub = self._subscribe_realtime_bar(ib, cfg, sink, contract, key)
            else:
                sub = self._subscribe_hist_bar(ib, cfg, sink, contract, key)

        self._subscriptions.append(sub)
        logger.info("Subscribed [%s] %s (%s)", sub.sub_type, cfg.symbol, cfg.sec_type)

    def _subscribe_tick(self, ib: IB, cfg: ContractConfig, sink: Sink, contract: Contract, key: str) -> _Subscription:
        ticker = ib.reqMktData(contract, genericTickList=cfg.generic_tick_list, snapshot=False)

        def on_tick(t, _cfg=cfg, _key=key, _sink=sink):
            self._last_publish[_key] = time.monotonic()
            _sink.publish(_key, _tick_payload(_cfg, t))

        ticker.updateEvent += on_tick
        return _Subscription(cfg.symbol, "tick", contract, ticker, on_tick)

    def _subscribe_realtime_bar(self, ib: IB, cfg: ContractConfig, sink: Sink, contract: Contract, key: str) -> _Subscription:
        bars = ib.reqRealTimeBars(
            contract,
            barSize=5,
            whatToShow=cfg.what_to_show,
            useRTH=cfg.use_rth,
        )

        def on_bar(bar_list, has_new_bar, _cfg=cfg, _key=key, _sink=sink):
            if has_new_bar and bar_list:
                self._last_publish[_key] = time.monotonic()
                _sink.publish(_key, _realtime_bar_payload(_cfg, bar_list[-1]))

        bars.updateEvent += on_bar
        return _Subscription(cfg.symbol, "realtime_bar", contract, bars, on_bar)

    def _subscribe_hist_bar(self, ib: IB, cfg: ContractConfig, sink: Sink, contract: Contract, key: str) -> _Subscription:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=cfg.history_duration,
            barSizeSetting=cfg.bar_size,
            whatToShow=cfg.what_to_show,
            useRTH=cfg.use_rth,
            formatDate=2,
            keepUpToDate=True,
        )

        def on_bar(bar_list, has_new_bar, _cfg=cfg, _key=key, _sink=sink):
            if has_new_bar and bar_list:
                self._last_publish[_key] = time.monotonic()
                _sink.publish(_key, _hist_bar_payload(_cfg, bar_list[-1]))

        bars.updateEvent += on_bar
        return _Subscription(cfg.symbol, "hist_bar", contract, bars, on_bar)

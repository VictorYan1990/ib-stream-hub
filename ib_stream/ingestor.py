"""Market data ingestor: subscribes to IB contracts and feeds asyncio queues."""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from ib_insync import IB, Contract
from ib_insync.contract import CFD, Forex, Future, Index, Stock
from ib_insync.objects import BarData, RealTimeBar

from .config import ContractConfig, DataType
from .gateway import IBGateway

logger = logging.getLogger(__name__)

_REALTIME_BAR_SIZE = "5 secs"


@dataclass
class _Subscription:
    symbol: str
    sub_type: str          # "tick" | "realtime_bar" | "hist_bar"
    contract: Contract
    handle: Any            # Ticker | RealTimeBarList | BarDataList
    handler: Any           # the callable added to handle.updateEvent


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
        # For Forex, symbol is a 6-char pair like "EURUSD".
        return Forex(pair=cfg.symbol, exchange=cfg.exchange)
    if sec_type == "IND":
        return Index(symbol=cfg.symbol, **kwargs)
    if sec_type == "CFD":
        return CFD(symbol=cfg.symbol, **kwargs)

    # Fall back to a generic contract for less-common types (OPT, FOP, …).
    return Contract(secType=sec_type, symbol=cfg.symbol, **kwargs)


class Ingestor:
    """Subscribes to market data for configured contracts and exposes per-symbol queues.

    Each queue contains heterogeneous items depending on the subscription type:

    * ``tick``: :class:`ib_insync.Ticker` — snapshot of the full market data
      state after each update.
    * ``realtime_bar`` (bar_size ``"5 secs"``): :class:`ib_insync.objects.RealTimeBar`
    * ``hist_bar`` (all other bar sizes): :class:`ib_insync.objects.BarData`
    """

    def __init__(self, gateway: IBGateway, contracts: List[ContractConfig]) -> None:
        self._gateway = gateway
        self._contracts = contracts
        self._queues: Dict[str, asyncio.Queue] = {}
        self._subscriptions: List[_Subscription] = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Subscribe to all configured contracts. Must be called after gateway.connect()."""
        for cfg in self._contracts:
            self._subscribe(cfg)

    def restart(self) -> None:
        """Re-subscribe to all contracts after a reconnect."""
        logger.info("Reconnected — re-subscribing %d contract(s) …", len(self._contracts))
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

    def get_queue(self, symbol: str) -> asyncio.Queue:
        """Return the data queue for *symbol*. Raises KeyError if not subscribed."""
        return self._queues[symbol]

    @property
    def queues(self) -> Dict[str, asyncio.Queue]:
        return dict(self._queues)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _subscribe(self, cfg: ContractConfig) -> None:
        ib: IB = self._gateway.ib
        contract = _build_contract(cfg)
        queue: asyncio.Queue = asyncio.Queue()
        self._queues[cfg.symbol] = queue

        if cfg.data_type == DataType.TICK:
            sub = self._subscribe_tick(ib, cfg, contract, queue)
        else:
            if cfg.bar_size == _REALTIME_BAR_SIZE:
                sub = self._subscribe_realtime_bar(ib, cfg, contract, queue)
            else:
                sub = self._subscribe_hist_bar(ib, cfg, contract, queue)

        self._subscriptions.append(sub)
        logger.info(
            "Subscribed [%s] %s (%s)", sub.sub_type, cfg.symbol, cfg.sec_type
        )

    def _subscribe_tick(
        self,
        ib: IB,
        cfg: ContractConfig,
        contract: Contract,
        queue: asyncio.Queue,
    ) -> _Subscription:
        ticker = ib.reqMktData(
            contract,
            genericTickList=cfg.generic_tick_list,
            snapshot=False,
        )

        def on_tick(t, _symbol=cfg.symbol, _queue=queue):
            _queue.put_nowait(t)

        ticker.updateEvent += on_tick
        return _Subscription(cfg.symbol, "tick", contract, ticker, on_tick)

    def _subscribe_realtime_bar(
        self,
        ib: IB,
        cfg: ContractConfig,
        contract: Contract,
        queue: asyncio.Queue,
    ) -> _Subscription:
        bars = ib.reqRealTimeBars(
            contract,
            barSize=5,   # IB only accepts 5 here
            whatToShow=cfg.what_to_show,
            useRTH=cfg.use_rth,
        )

        def on_bar(bar_list, has_new_bar, _queue=queue):
            if has_new_bar and bar_list:
                _queue.put_nowait(bar_list[-1])

        bars.updateEvent += on_bar
        return _Subscription(cfg.symbol, "realtime_bar", contract, bars, on_bar)

    def _subscribe_hist_bar(
        self,
        ib: IB,
        cfg: ContractConfig,
        contract: Contract,
        queue: asyncio.Queue,
    ) -> _Subscription:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",              # empty = now
            durationStr=cfg.history_duration,
            barSizeSetting=cfg.bar_size,
            whatToShow=cfg.what_to_show,
            useRTH=cfg.use_rth,
            formatDate=2,               # UTC datetime objects
            keepUpToDate=True,
        )

        def on_bar(bar_list, has_new_bar, _queue=queue):
            if has_new_bar and bar_list:
                _queue.put_nowait(bar_list[-1])

        bars.updateEvent += on_bar
        return _Subscription(cfg.symbol, "hist_bar", contract, bars, on_bar)

import asyncio
import pytest
from unittest.mock import MagicMock, patch

from ib_insync import Contract
from ib_insync.contract import CFD, Forex, Future, Index, Stock

from ib_stream.config import ContractConfig, DataType
from ib_stream.gateway import IBGateway
from ib_stream.ingestor import Ingestor, _build_contract, _REALTIME_BAR_SIZE


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_gateway():
    gw = MagicMock(spec=IBGateway)
    gw.ib = MagicMock()
    return gw


@pytest.fixture
def tick_config():
    return ContractConfig(symbol="AAPL", sec_type="STK", data_type=DataType.TICK)


@pytest.fixture
def ingestor(mock_gateway, tick_config):
    return Ingestor(mock_gateway, [tick_config])


# ---------------------------------------------------------------------------
# _build_contract
# ---------------------------------------------------------------------------

class TestBuildContract:
    def test_stock(self):
        cfg = ContractConfig(symbol="AAPL", sec_type="STK")
        assert isinstance(_build_contract(cfg), Stock)

    def test_future(self):
        cfg = ContractConfig(symbol="ES", sec_type="FUT", exchange="CME")
        assert isinstance(_build_contract(cfg), Future)

    def test_forex(self):
        cfg = ContractConfig(symbol="EURUSD", sec_type="CASH", exchange="IDEALPRO")
        assert isinstance(_build_contract(cfg), Forex)

    def test_index(self):
        cfg = ContractConfig(symbol="SPX", sec_type="IND", exchange="CBOE")
        assert isinstance(_build_contract(cfg), Index)

    def test_cfd(self):
        cfg = ContractConfig(symbol="IBUS30", sec_type="CFD")
        assert isinstance(_build_contract(cfg), CFD)

    def test_unknown_sec_type_returns_generic_contract(self):
        cfg = ContractConfig(symbol="TEST", sec_type="OPT")
        c = _build_contract(cfg)
        assert isinstance(c, Contract)

    def test_sec_type_case_insensitive(self):
        cfg = ContractConfig(symbol="AAPL", sec_type="stk")
        assert isinstance(_build_contract(cfg), Stock)

    def test_last_trade_date_applied(self):
        cfg = ContractConfig(symbol="ES", sec_type="FUT", exchange="CME", last_trade_date="202509")
        c = _build_contract(cfg)
        assert c.lastTradeDateOrContractMonth == "202509"


# ---------------------------------------------------------------------------
# Ingestor queues
# ---------------------------------------------------------------------------

class TestIngestorQueues:
    def test_get_queue_unknown_symbol_raises_key_error(self, ingestor):
        with pytest.raises(KeyError):
            ingestor.get_queue("UNKNOWN")

    def test_queues_returns_a_copy(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert ingestor.queues is not ingestor.queues

    def test_queues_contains_subscribed_symbols(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert "AAPL" in ingestor.queues


# ---------------------------------------------------------------------------
# Ingestor.start
# ---------------------------------------------------------------------------

class TestIngestorStart:
    def test_creates_queue_per_contract(self, mock_gateway):
        configs = [ContractConfig(symbol="AAPL"), ContractConfig(symbol="MSFT")]
        ingestor = Ingestor(mock_gateway, configs)
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert "AAPL" in ingestor.queues
        assert "MSFT" in ingestor.queues

    def test_calls_req_mkt_data_for_tick_contracts(self, mock_gateway):
        configs = [ContractConfig(symbol="AAPL"), ContractConfig(symbol="MSFT")]
        ingestor = Ingestor(mock_gateway, configs)
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert mock_gateway.ib.reqMktData.call_count == 2

    def test_start_with_no_contracts_is_safe(self, mock_gateway):
        ingestor = Ingestor(mock_gateway, [])
        ingestor.start()  # must not raise
        assert ingestor.queues == {}


# ---------------------------------------------------------------------------
# Ingestor.stop
# ---------------------------------------------------------------------------

class TestIngestorStop:
    def test_clears_subscription_list(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert len(ingestor._subscriptions) == 1
        ingestor.stop()
        assert len(ingestor._subscriptions) == 0

    def test_calls_cancel_mkt_data_for_tick(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        ingestor.stop()
        mock_gateway.ib.cancelMktData.assert_called_once()

    def test_cancel_exception_does_not_propagate(self, ingestor, mock_gateway):
        ticker = MagicMock()
        ticker.updateEvent.__isub__ = MagicMock(side_effect=Exception("boom"))
        mock_gateway.ib.reqMktData.return_value = ticker
        ingestor.start()
        ingestor.stop()  # must not raise

    def test_stop_without_start_is_safe(self, ingestor):
        ingestor.stop()  # must not raise


# ---------------------------------------------------------------------------
# Ingestor.restart
# ---------------------------------------------------------------------------

class TestIngestorRestart:
    def test_resubscribes_after_restart(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        ingestor.restart()
        assert mock_gateway.ib.reqMktData.call_count == 2

    def test_subscription_count_stays_at_one(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        ingestor.restart()
        assert len(ingestor._subscriptions) == 1

    def test_queues_still_accessible_after_restart(self, ingestor, mock_gateway):
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        q_before = ingestor.get_queue("AAPL")
        ingestor.restart()
        q_after = ingestor.get_queue("AAPL")
        assert q_before is not q_after  # new queue after restart


# ---------------------------------------------------------------------------
# Tick subscription event handler
# ---------------------------------------------------------------------------

class TestTickHandler:
    def test_handler_enqueues_tick(self, mock_gateway):
        cfg = ContractConfig(symbol="AAPL", data_type=DataType.TICK)
        ingestor = Ingestor(mock_gateway, [cfg])
        ticker = MagicMock()
        # Save ref before start() reassigns ticker.updateEvent via +=
        event = ticker.updateEvent
        mock_gateway.ib.reqMktData.return_value = ticker
        ingestor.start()

        on_tick = event.__iadd__.call_args[0][0]
        fake_tick = MagicMock()
        on_tick(fake_tick)

        assert ingestor.get_queue("AAPL").get_nowait() is fake_tick

    def test_handler_uses_correct_generic_tick_list(self, mock_gateway):
        cfg = ContractConfig(symbol="AAPL", generic_tick_list="231,232")
        ingestor = Ingestor(mock_gateway, [cfg])
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        _, kwargs = mock_gateway.ib.reqMktData.call_args
        assert kwargs["genericTickList"] == "231,232"


# ---------------------------------------------------------------------------
# Real-time bar subscription event handler
# ---------------------------------------------------------------------------

class TestRealtimeBarHandler:
    def _start_realtime(self, mock_gateway, symbol="SPY"):
        cfg = ContractConfig(symbol=symbol, data_type=DataType.BAR, bar_size=_REALTIME_BAR_SIZE)
        ingestor = Ingestor(mock_gateway, [cfg])
        bars = MagicMock()
        event = bars.updateEvent  # save ref before start() reassigns via +=
        mock_gateway.ib.reqRealTimeBars.return_value = bars
        ingestor.start()
        handler = event.__iadd__.call_args[0][0]
        return ingestor, handler

    def test_enqueues_last_bar_when_new_bar(self, mock_gateway):
        ingestor, handler = self._start_realtime(mock_gateway)
        fake_bar = MagicMock()
        handler([fake_bar], True)
        assert ingestor.get_queue("SPY").get_nowait() is fake_bar

    def test_skips_when_no_new_bar(self, mock_gateway):
        ingestor, handler = self._start_realtime(mock_gateway)
        handler([MagicMock()], False)
        assert ingestor.get_queue("SPY").qsize() == 0

    def test_skips_when_bar_list_empty(self, mock_gateway):
        ingestor, handler = self._start_realtime(mock_gateway)
        handler([], True)
        assert ingestor.get_queue("SPY").qsize() == 0


# ---------------------------------------------------------------------------
# Historical bar subscription
# ---------------------------------------------------------------------------

class TestHistBarHandler:
    def _start_hist(self, mock_gateway, symbol="QQQ", bar_size="1 min"):
        cfg = ContractConfig(symbol=symbol, data_type=DataType.BAR, bar_size=bar_size)
        ingestor = Ingestor(mock_gateway, [cfg])
        bars = MagicMock()
        event = bars.updateEvent  # save ref before start() reassigns via +=
        mock_gateway.ib.reqHistoricalData.return_value = bars
        ingestor.start()
        handler = event.__iadd__.call_args[0][0]
        return ingestor, handler

    def test_uses_req_historical_data(self, mock_gateway):
        self._start_hist(mock_gateway)
        mock_gateway.ib.reqHistoricalData.assert_called_once()
        mock_gateway.ib.reqRealTimeBars.assert_not_called()

    def test_enqueues_last_bar_when_new_bar(self, mock_gateway):
        ingestor, handler = self._start_hist(mock_gateway)
        fake_bar = MagicMock()
        handler([fake_bar], True)
        assert ingestor.get_queue("QQQ").get_nowait() is fake_bar

    def test_skips_when_no_new_bar(self, mock_gateway):
        ingestor, handler = self._start_hist(mock_gateway)
        handler([MagicMock()], False)
        assert ingestor.get_queue("QQQ").qsize() == 0

    def test_keep_up_to_date_is_true(self, mock_gateway):
        self._start_hist(mock_gateway)
        _, kwargs = mock_gateway.ib.reqHistoricalData.call_args
        assert kwargs["keepUpToDate"] is True

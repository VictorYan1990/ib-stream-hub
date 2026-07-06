import pytest
from unittest.mock import MagicMock, patch

from ib_insync import Contract
from ib_insync.contract import CFD, Forex, Future, Index, Stock

from ib_stream.config import ContractConfig, DataType
from ib_stream.gateway import IBGateway
from ib_stream.ingestor import (
    Ingestor,
    _build_contract,
    _partition_key,
    _REALTIME_BAR_SIZE,
    _STALENESS_TIMEOUT,
)
from ib_stream.sink import SQSSink


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_gateway():
    gw = MagicMock(spec=IBGateway)
    gw.ib = MagicMock()
    return gw


@pytest.fixture
def mock_sink():
    return MagicMock(spec=SQSSink)


@pytest.fixture
def tick_config():
    return ContractConfig(symbol="AAPL", sec_type="STK", data_type=DataType.TICK)


@pytest.fixture
def ingestor(mock_gateway, mock_sink, tick_config):
    return Ingestor.from_contracts(mock_gateway, [tick_config], mock_sink)


# ---------------------------------------------------------------------------
# _partition_key
# ---------------------------------------------------------------------------

class TestPartitionKey:
    def test_stock_key(self):
        cfg = ContractConfig(symbol="AAPL", sec_type="STK")
        assert _partition_key(cfg) == "AAPL:STK"

    def test_future_with_expiry(self):
        cfg = ContractConfig(symbol="ES", sec_type="FUT", last_trade_date="202509")
        assert _partition_key(cfg) == "ES:FUT:202509"

    def test_future_without_expiry(self):
        cfg = ContractConfig(symbol="ES", sec_type="FUT")
        assert _partition_key(cfg) == "ES:FUT"

    def test_forex(self):
        cfg = ContractConfig(symbol="EURUSD", sec_type="CASH")
        assert _partition_key(cfg) == "EURUSD:CASH"

    def test_two_expirations_differ(self):
        cfg_sep = ContractConfig(symbol="ES", sec_type="FUT", last_trade_date="202509")
        cfg_dec = ContractConfig(symbol="ES", sec_type="FUT", last_trade_date="202512")
        assert _partition_key(cfg_sep) != _partition_key(cfg_dec)


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
        assert isinstance(_build_contract(cfg), Contract)

    def test_sec_type_case_insensitive(self):
        cfg = ContractConfig(symbol="AAPL", sec_type="stk")
        assert isinstance(_build_contract(cfg), Stock)

    def test_last_trade_date_applied(self):
        cfg = ContractConfig(symbol="ES", sec_type="FUT", exchange="CME", last_trade_date="202509")
        c = _build_contract(cfg)
        assert c.lastTradeDateOrContractMonth == "202509"


# ---------------------------------------------------------------------------
# Ingestor.start
# ---------------------------------------------------------------------------

class TestIngestorStart:
    def test_registers_one_subscription_per_contract(self, mock_gateway, mock_sink):
        configs = [ContractConfig(symbol="AAPL"), ContractConfig(symbol="MSFT")]
        ingestor = Ingestor.from_contracts(mock_gateway, configs, mock_sink)
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert len(ingestor._subscriptions) == 2

    def test_initialises_last_publish_per_key(self, mock_gateway, mock_sink):
        cfg = ContractConfig(symbol="AAPL", sec_type="STK")
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg], mock_sink)
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert "AAPL:STK" in ingestor._last_publish

    def test_start_with_no_contracts_is_safe(self, mock_gateway, mock_sink):
        ingestor = Ingestor.from_contracts(mock_gateway, [], mock_sink)
        ingestor.start()
        assert ingestor._subscriptions == []

    def test_uses_req_mkt_data_for_tick(self, mock_gateway, mock_sink):
        configs = [ContractConfig(symbol="AAPL"), ContractConfig(symbol="MSFT")]
        ingestor = Ingestor.from_contracts(mock_gateway, configs, mock_sink)
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        assert mock_gateway.ib.reqMktData.call_count == 2


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


# ---------------------------------------------------------------------------
# Ingestor.watchdog
# ---------------------------------------------------------------------------

class TestWatchdog:
    async def test_warns_when_symbol_goes_silent(self, ingestor, monkeypatch):
        import time
        monkeypatch.setattr("ib_stream.ingestor._STALENESS_TIMEOUT", 0.05)
        ingestor._last_publish["AAPL:STK"] = time.monotonic() - 120.0  # stale

        with patch("ib_stream.ingestor.logger") as mock_log:
            with pytest.raises(Exception):
                import asyncio
                await asyncio.wait_for(ingestor.watchdog(timeout=0.05), timeout=0.2)

        mock_log.warning.assert_called()
        assert any("AAPL:STK" in str(a) for a in mock_log.warning.call_args[0])

    async def test_no_warning_when_data_is_fresh(self, ingestor, monkeypatch):
        # Pin monotonic so age = 0 regardless of wall time
        monkeypatch.setattr("ib_stream.ingestor.time", MagicMock(monotonic=lambda: 1000.0))
        ingestor._last_publish["AAPL:STK"] = 1000.0

        with patch("ib_stream.ingestor.logger") as mock_log:
            with pytest.raises(Exception):
                import asyncio
                await asyncio.wait_for(ingestor.watchdog(timeout=0.05), timeout=0.2)

        mock_log.warning.assert_not_called()

    def test_staleness_timeout_constant_is_positive(self):
        assert _STALENESS_TIMEOUT > 0


# ---------------------------------------------------------------------------
# Tick subscription event handler
# ---------------------------------------------------------------------------

class TestTickHandler:
    def test_handler_calls_sink_publish(self, mock_gateway, mock_sink):
        cfg = ContractConfig(symbol="AAPL", sec_type="STK", data_type=DataType.TICK)
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg], mock_sink)
        ticker = MagicMock()
        event = ticker.updateEvent  # save ref before start() reassigns via +=
        mock_gateway.ib.reqMktData.return_value = ticker
        ingestor.start()

        on_tick = event.__iadd__.call_args[0][0]
        fake_ticker = MagicMock()
        on_tick(fake_ticker)

        mock_sink.publish.assert_called_once()
        key, payload = mock_sink.publish.call_args[0]
        assert key == "AAPL:STK"
        assert payload["type"] == "tick"
        assert payload["symbol"] == "AAPL"

    def test_handler_updates_last_publish(self, mock_gateway, mock_sink):
        cfg = ContractConfig(symbol="AAPL", sec_type="STK", data_type=DataType.TICK)
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg], mock_sink)
        ticker = MagicMock()
        event = ticker.updateEvent
        mock_gateway.ib.reqMktData.return_value = ticker
        ingestor.start()

        import time
        before = time.monotonic()
        on_tick = event.__iadd__.call_args[0][0]
        on_tick(MagicMock())
        assert ingestor._last_publish["AAPL:STK"] >= before

    def test_handler_uses_correct_generic_tick_list(self, mock_gateway, mock_sink):
        cfg = ContractConfig(symbol="AAPL", generic_tick_list="231,232")
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg], mock_sink)
        mock_gateway.ib.reqMktData.return_value = MagicMock()
        ingestor.start()
        _, kwargs = mock_gateway.ib.reqMktData.call_args
        assert kwargs["genericTickList"] == "231,232"


# ---------------------------------------------------------------------------
# Real-time bar subscription event handler
# ---------------------------------------------------------------------------

class TestRealtimeBarHandler:
    def _start_realtime(self, mock_gateway, mock_sink, symbol="SPY"):
        cfg = ContractConfig(symbol=symbol, data_type=DataType.BAR, bar_size=_REALTIME_BAR_SIZE)
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg], mock_sink)
        bars = MagicMock()
        event = bars.updateEvent  # save ref before start() reassigns via +=
        mock_gateway.ib.reqRealTimeBars.return_value = bars
        ingestor.start()
        handler = event.__iadd__.call_args[0][0]
        return ingestor, handler

    def test_publishes_on_new_bar(self, mock_gateway, mock_sink):
        ingestor, handler = self._start_realtime(mock_gateway, mock_sink)
        fake_bar = MagicMock()
        handler([fake_bar], True)
        mock_sink.publish.assert_called_once()
        key, payload = mock_sink.publish.call_args[0]
        assert key == "SPY:STK"
        assert payload["type"] == "realtime_bar"

    def test_skips_when_no_new_bar(self, mock_gateway, mock_sink):
        _, handler = self._start_realtime(mock_gateway, mock_sink)
        handler([MagicMock()], False)
        mock_sink.publish.assert_not_called()

    def test_skips_when_bar_list_empty(self, mock_gateway, mock_sink):
        _, handler = self._start_realtime(mock_gateway, mock_sink)
        handler([], True)
        mock_sink.publish.assert_not_called()


# ---------------------------------------------------------------------------
# Historical bar subscription
# ---------------------------------------------------------------------------

class TestHistBarHandler:
    def _start_hist(self, mock_gateway, mock_sink, symbol="QQQ", bar_size="1 min"):
        cfg = ContractConfig(symbol=symbol, data_type=DataType.BAR, bar_size=bar_size)
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg], mock_sink)
        bars = MagicMock()
        event = bars.updateEvent  # save ref before start() reassigns via +=
        mock_gateway.ib.reqHistoricalData.return_value = bars
        ingestor.start()
        handler = event.__iadd__.call_args[0][0]
        return ingestor, handler

    def test_uses_req_historical_data(self, mock_gateway, mock_sink):
        self._start_hist(mock_gateway, mock_sink)
        mock_gateway.ib.reqHistoricalData.assert_called_once()
        mock_gateway.ib.reqRealTimeBars.assert_not_called()

    def test_publishes_on_new_bar(self, mock_gateway, mock_sink):
        ingestor, handler = self._start_hist(mock_gateway, mock_sink)
        fake_bar = MagicMock()
        handler([fake_bar], True)
        mock_sink.publish.assert_called_once()
        key, payload = mock_sink.publish.call_args[0]
        assert key == "QQQ:STK"
        assert payload["type"] == "hist_bar"

    def test_skips_when_no_new_bar(self, mock_gateway, mock_sink):
        _, handler = self._start_hist(mock_gateway, mock_sink)
        handler([MagicMock()], False)
        mock_sink.publish.assert_not_called()

    def test_keep_up_to_date_is_true(self, mock_gateway, mock_sink):
        self._start_hist(mock_gateway, mock_sink)
        _, kwargs = mock_gateway.ib.reqHistoricalData.call_args
        assert kwargs["keepUpToDate"] is True


# ---------------------------------------------------------------------------
# Future contract with two expirations
# ---------------------------------------------------------------------------

class TestFutureExpirationRouting:
    def test_different_expirations_get_different_keys(self, mock_gateway, mock_sink):
        cfg_sep = ContractConfig(symbol="ES", sec_type="FUT", exchange="CME", last_trade_date="202509")
        cfg_dec = ContractConfig(symbol="ES", sec_type="FUT", exchange="CME", last_trade_date="202512")
        ingestor = Ingestor.from_contracts(mock_gateway, [cfg_sep, cfg_dec], mock_sink)
        mock_gateway.ib.reqMktData.return_value = MagicMock()

        cfg_sep_bar = ContractConfig(
            symbol="ES", sec_type="FUT", exchange="CME",
            last_trade_date="202509", data_type=DataType.BAR, bar_size="1 min",
        )
        cfg_dec_bar = ContractConfig(
            symbol="ES", sec_type="FUT", exchange="CME",
            last_trade_date="202512", data_type=DataType.BAR, bar_size="1 min",
        )
        ingestor2 = Ingestor.from_contracts(mock_gateway, [cfg_sep_bar, cfg_dec_bar], mock_sink)
        mock_gateway.ib.reqHistoricalData.return_value = MagicMock()
        ingestor2.start()

        keys = list(ingestor2._last_publish.keys())
        assert "ES:FUT:202509" in keys
        assert "ES:FUT:202512" in keys
        assert len(keys) == 2

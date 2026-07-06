import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from ib_stream.config import ConnectionSettings
from ib_stream.gateway import IBGateway, _RECONNECT_DELAY, _RECONNECT_JITTER


@pytest.fixture
def mock_ib():
    with patch("ib_stream.gateway.IB") as MockIB:
        instance = MagicMock()
        instance.isConnected.return_value = True
        MockIB.return_value = instance
        yield instance


@pytest.fixture
def gateway(mock_ib):
    return IBGateway(host="127.0.0.1", port=7497, client_id=1)


class TestInit:
    def test_default_params(self, mock_ib):
        gw = IBGateway()
        assert gw._host == "127.0.0.1"
        assert gw._port == 7497
        assert gw._client_id == 1
        assert gw._timeout == 10.0
        assert gw._readonly is True

    def test_custom_params(self, mock_ib):
        gw = IBGateway(host="192.168.1.1", port=4001, client_id=5, timeout=30.0, readonly=False)
        assert gw._host == "192.168.1.1"
        assert gw._port == 4001
        assert gw._client_id == 5
        assert gw._timeout == 30.0
        assert gw._readonly is False

    def test_initial_sentinel_values(self, mock_ib):
        gw = IBGateway()
        assert gw._reconnect_task is None
        assert gw._disconnect_time == 0.0
        assert gw.on_reconnected is None

    def test_event_handlers_registered(self, mock_ib):
        # Save refs before IBGateway.__init__ reassigns them via +=
        disconnected_ev = mock_ib.disconnectedEvent
        error_ev = mock_ib.errorEvent
        gw = IBGateway()
        disconnected_ev.__iadd__.assert_called_once_with(gw._on_disconnected)
        error_ev.__iadd__.assert_called_once_with(gw._on_error)


class TestFromConfig:
    def test_maps_all_fields(self, mock_ib):
        cfg = MagicMock(spec=ConnectionSettings)
        cfg.host = "10.0.0.1"
        cfg.port = 4001
        cfg.client_id = 3
        cfg.timeout = 20.0
        cfg.readonly = False

        gw = IBGateway.from_config(cfg)
        assert gw._host == "10.0.0.1"
        assert gw._port == 4001
        assert gw._client_id == 3
        assert gw._timeout == 20.0
        assert gw._readonly is False


class TestConnect:
    def test_passes_correct_args_to_ib(self, gateway, mock_ib):
        gateway.connect()
        mock_ib.connect.assert_called_once_with(
            host="127.0.0.1",
            port=7497,
            clientId=1,
            timeout=10.0,
            readonly=True,
        )


class TestDisconnect:
    def test_calls_ib_disconnect(self, gateway, mock_ib):
        gateway.disconnect()
        mock_ib.disconnect.assert_called_once()

    def test_cancels_pending_reconnect_task(self, gateway):
        task = MagicMock()
        task.done.return_value = False
        gateway._reconnect_task = task
        gateway.disconnect()
        task.cancel.assert_called_once()

    def test_does_not_cancel_already_done_task(self, gateway):
        task = MagicMock()
        task.done.return_value = True
        gateway._reconnect_task = task
        gateway.disconnect()
        task.cancel.assert_not_called()

    def test_no_task_is_safe(self, gateway):
        gateway._reconnect_task = None
        gateway.disconnect()  # must not raise


class TestIsConnected:
    def test_reflects_ib_state(self, gateway, mock_ib):
        mock_ib.isConnected.return_value = True
        assert gateway.is_connected is True
        mock_ib.isConnected.return_value = False
        assert gateway.is_connected is False


class TestOnError:
    def test_code_gte_2000_logs_error(self, gateway):
        with patch("ib_stream.gateway.logger") as mock_log:
            gateway._on_error(1, 2000, "broken", None)
            mock_log.error.assert_called_once()
            mock_log.debug.assert_not_called()

    def test_code_lt_2000_logs_debug(self, gateway):
        with patch("ib_stream.gateway.logger") as mock_log:
            gateway._on_error(0, 1999, "notice", None)
            mock_log.debug.assert_called_once()
            mock_log.error.assert_not_called()

    def test_boundary_exactly_2000_is_error(self, gateway):
        with patch("ib_stream.gateway.logger") as mock_log:
            gateway._on_error(0, 2000, "boundary", None)
            mock_log.error.assert_called_once()

    def test_1100_logs_warning_without_firing_reconnect(self, gateway):
        callback = MagicMock()
        gateway.on_reconnected = callback
        with patch("ib_stream.gateway.logger") as mock_log:
            gateway._on_error(-1, 1100, "connectivity lost", None)
            mock_log.warning.assert_called_once()
        callback.assert_not_called()

    def test_1101_fires_reconnected_callback(self, gateway):
        callback = MagicMock()
        gateway.on_reconnected = callback
        gateway._on_error(-1, 1101, "restored, data lost", None)
        callback.assert_called_once()

    def test_1102_fires_reconnected_callback(self, gateway):
        callback = MagicMock()
        gateway.on_reconnected = callback
        gateway._on_error(-1, 1102, "restored, data maintained", None)
        callback.assert_called_once()

    def test_1102_without_callback_does_not_raise(self, gateway):
        gateway.on_reconnected = None
        gateway._on_error(-1, 1102, "restored", None)  # must not raise


class TestOnDisconnected:
    def test_records_disconnect_timestamp(self, gateway):
        before = time.monotonic()
        with patch("asyncio.ensure_future"):
            gateway._on_disconnected()
        assert gateway._disconnect_time >= before

    def test_schedules_reconnect_coroutine(self, gateway):
        with patch("asyncio.ensure_future") as mock_ensure:
            gateway._on_disconnected()
            mock_ensure.assert_called_once()


class TestReconnectLoop:
    async def test_connects_on_first_attempt(self, gateway, mock_ib):
        mock_ib.isConnected.side_effect = [False, True]
        mock_ib.connectAsync = AsyncMock()
        gateway._disconnect_time = time.monotonic()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await gateway._reconnect_loop()

        mock_ib.connectAsync.assert_called_once_with(
            host="127.0.0.1",
            port=7497,
            clientId=1,
            timeout=10.0,
            readonly=True,
        )

    async def test_fires_on_reconnected_callback(self, gateway, mock_ib):
        mock_ib.isConnected.side_effect = [False, True]
        mock_ib.connectAsync = AsyncMock()
        callback = MagicMock()
        gateway.on_reconnected = callback
        gateway._disconnect_time = time.monotonic()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await gateway._reconnect_loop()

        callback.assert_called_once()

    async def test_no_error_when_callback_is_none(self, gateway, mock_ib):
        mock_ib.isConnected.side_effect = [False, True]
        mock_ib.connectAsync = AsyncMock()
        gateway.on_reconnected = None
        gateway._disconnect_time = time.monotonic()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await gateway._reconnect_loop()  # must not raise

    async def test_retries_after_failure(self, gateway, mock_ib):
        mock_ib.isConnected.side_effect = [False, False, True]
        mock_ib.connectAsync = AsyncMock(side_effect=[Exception("timeout"), None])
        gateway._disconnect_time = time.monotonic()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await gateway._reconnect_loop()

        assert mock_ib.connectAsync.call_count == 2

    async def test_jitter_added_to_sleep(self, gateway, mock_ib):
        mock_ib.isConnected.side_effect = [False, True]
        mock_ib.connectAsync = AsyncMock()
        gateway._disconnect_time = time.monotonic()

        sleep_args = []

        async def capture_sleep(t):
            sleep_args.append(t)

        with patch("asyncio.sleep", side_effect=capture_sleep):
            await gateway._reconnect_loop()

        assert len(sleep_args) == 1
        assert _RECONNECT_DELAY <= sleep_args[0] <= _RECONNECT_DELAY + _RECONNECT_JITTER

    async def test_backoff_grows_on_repeated_failure(self, gateway, mock_ib):
        mock_ib.isConnected.side_effect = [False, False, False, True]
        mock_ib.connectAsync = AsyncMock(side_effect=[Exception(), Exception(), None])
        gateway._disconnect_time = time.monotonic()

        sleep_args = []

        async def capture_sleep(t):
            sleep_args.append(t)

        with patch("asyncio.sleep", side_effect=capture_sleep):
            await gateway._reconnect_loop()

        # Second sleep must be longer than first (backoff applied).
        assert sleep_args[1] > sleep_args[0]

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, call, patch

import main as main_module
from main import _drain_queues, _STALENESS_TIMEOUT, DEFAULT_CONFIG


class TestDrainQueues:
    async def test_logs_each_item_from_queue(self):
        q = asyncio.Queue()
        q.put_nowait("tick_a")

        ingestor = MagicMock()
        ingestor.queues = {"AAPL": q}
        ingestor.get_queue.return_value = q

        with patch("main.logger") as mock_log:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_drain_queues(ingestor), timeout=0.2)

        logged_args = [call_args[0] for call_args in mock_log.info.call_args_list]
        assert any("AAPL" in str(a) for a in logged_args)

    async def test_warns_when_queue_silent_beyond_timeout(self, monkeypatch):
        monkeypatch.setattr(main_module, "_STALENESS_TIMEOUT", 0.05)

        q = asyncio.Queue()  # intentionally empty

        ingestor = MagicMock()
        ingestor.queues = {"MSFT": q}
        ingestor.get_queue.return_value = q

        with patch("main.logger") as mock_log:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_drain_queues(ingestor), timeout=0.3)

        mock_log.warning.assert_called()
        # call_args[0] is the positional args tuple: (fmt, symbol, timeout)
        assert any("MSFT" in str(a) for a in mock_log.warning.call_args[0])

    async def test_drains_multiple_symbols_concurrently(self):
        q_a, q_b = asyncio.Queue(), asyncio.Queue()
        q_a.put_nowait("data_a")
        q_b.put_nowait("data_b")

        ingestor = MagicMock()
        ingestor.queues = {"AAPL": q_a, "MSFT": q_b}
        ingestor.get_queue.side_effect = lambda sym: {"AAPL": q_a, "MSFT": q_b}[sym]

        seen = []
        original_info = lambda fmt, *args: seen.append(fmt % args)

        with patch("main.logger") as mock_log:
            mock_log.info.side_effect = original_info
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(_drain_queues(ingestor), timeout=0.2)

        assert any("AAPL" in s for s in seen)
        assert any("MSFT" in s for s in seen)


class TestMain:
    def test_wires_on_reconnected_before_connect(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.IBGateway") as MockGW,
            patch("main.Ingestor") as MockIngestor,
            patch("main.util"),
        ):
            gw = MockGW.from_config.return_value
            ingestor = MockIngestor.return_value

            main_module.main()

            assert gw.on_reconnected is ingestor.restart

    def test_connect_called_before_start(self):
        call_order = []

        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.IBGateway") as MockGW,
            patch("main.Ingestor") as MockIngestor,
            patch("main.util"),
        ):
            gw = MockGW.from_config.return_value
            ingestor = MockIngestor.return_value
            gw.connect.side_effect = lambda: call_order.append("connect")
            ingestor.start.side_effect = lambda: call_order.append("start")

            main_module.main()

        assert call_order == ["connect", "start"]

    def test_stop_and_disconnect_called_on_exit(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.IBGateway") as MockGW,
            patch("main.Ingestor") as MockIngestor,
            patch("main.util") as mock_util,
        ):
            gw = MockGW.from_config.return_value
            ingestor = MockIngestor.return_value
            mock_util.run.side_effect = KeyboardInterrupt

            main_module.main()

        ingestor.stop.assert_called_once()
        gw.disconnect.assert_called_once()

    def test_uses_default_config_when_none_given(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config") as mock_load,
            patch("main.IBGateway"),
            patch("main.Ingestor"),
            patch("main.util"),
        ):
            main_module.main()
            mock_load.assert_called_once_with(DEFAULT_CONFIG)

    def test_uses_provided_config_path(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config") as mock_load,
            patch("main.IBGateway"),
            patch("main.Ingestor"),
            patch("main.util"),
        ):
            main_module.main(config_path="/custom/path.yaml")
            mock_load.assert_called_once_with("/custom/path.yaml")


class TestDefaultConfigPath:
    def test_points_to_ib_stream_contracts_yaml(self):
        assert "ib_stream" in str(DEFAULT_CONFIG)
        assert DEFAULT_CONFIG.name == "contracts.yaml"

    def test_staleness_timeout_is_positive(self):
        assert _STALENESS_TIMEOUT > 0

from unittest.mock import patch

import main as main_module
from main import (
    DEFAULT_CONTRACTS_CONFIG,
    DEFAULT_PIPELINES_CONFIG,
    DEFAULT_SINKS_CONFIG,
)

class TestMain:
    def test_wires_on_reconnected_before_connect(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.load_sinks_config"),
            patch("main.load_pipelines_config"),
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
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
            patch("main.load_sinks_config"),
            patch("main.load_pipelines_config"),
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
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
            patch("main.load_sinks_config"),
            patch("main.load_pipelines_config"),
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
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

    def test_util_run_receives_watchdog_coroutine(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.load_sinks_config"),
            patch("main.load_pipelines_config"),
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
            patch("main.IBGateway"),
            patch("main.Ingestor") as MockIngestor,
            patch("main.util") as mock_util,
        ):
            ingestor = MockIngestor.return_value
            main_module.main()
            mock_util.run.assert_called_once_with(ingestor.watchdog())

    def test_uses_default_config_paths_when_none_given(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config") as mock_contracts,
            patch("main.load_sinks_config") as mock_sinks,
            patch("main.load_pipelines_config") as mock_pipelines,
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
            patch("main.IBGateway"),
            patch("main.Ingestor"),
            patch("main.util"),
        ):
            main_module.main()
            mock_contracts.assert_called_once_with(str(DEFAULT_CONTRACTS_CONFIG))
            mock_sinks.assert_called_once_with(str(DEFAULT_SINKS_CONFIG))
            mock_pipelines.assert_called_once_with(str(DEFAULT_PIPELINES_CONFIG))

    def test_uses_provided_config_paths(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config") as mock_contracts,
            patch("main.load_sinks_config") as mock_sinks,
            patch("main.load_pipelines_config") as mock_pipelines,
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
            patch("main.IBGateway"),
            patch("main.Ingestor"),
            patch("main.util"),
        ):
            main_module.main(["-c", "/c.yaml", "-s", "/s.yaml", "-p", "/p.yaml"])
            mock_contracts.assert_called_once_with("/c.yaml")
            mock_sinks.assert_called_once_with("/s.yaml")
            mock_pipelines.assert_called_once_with("/p.yaml")

    def test_pipelines_passed_to_ingestor(self):
        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.load_sinks_config"),
            patch("main.load_pipelines_config"),
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines") as mock_build,
            patch("main.IBGateway") as MockGW,
            patch("main.Ingestor") as MockIngestor,
            patch("main.util"),
        ):
            pipelines = mock_build.return_value
            gw = MockGW.from_config.return_value
            main_module.main()
            MockIngestor.assert_called_once_with(gw, pipelines)


class TestDefaultPaths:
    def test_default_contracts_config_points_to_contracts_yaml(self):
        assert DEFAULT_CONTRACTS_CONFIG.name == "contracts.yaml"
        assert "ib_stream" in str(DEFAULT_CONTRACTS_CONFIG)

    def test_default_sinks_config_points_to_sinks_yaml(self):
        assert DEFAULT_SINKS_CONFIG.name == "sinks.yaml"
        assert "ib_stream" in str(DEFAULT_SINKS_CONFIG)

    def test_default_pipelines_config_points_to_pipelines_yaml(self):
        assert DEFAULT_PIPELINES_CONFIG.name == "pipelines.yaml"
        assert "ib_stream" in str(DEFAULT_PIPELINES_CONFIG)

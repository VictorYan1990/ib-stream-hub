import pytest
from unittest.mock import patch

from main import (
    DEFAULT_CONTRACTS_CONFIG,
    DEFAULT_PIPELINES_CONFIG,
    DEFAULT_SINKS_CONFIG,
    LOG_FORMAT,
    _build_parser,
    main,
)


class TestBuildParser:
    def test_prog_name(self):
        assert _build_parser().prog == "ib-stream"

    def test_default_log_level_is_info(self):
        args = _build_parser().parse_args([])
        assert args.log_level == "INFO"

    def test_all_log_level_choices_accepted(self):
        parser = _build_parser()
        for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            args = parser.parse_args(["--log-level", level])
            assert args.log_level == level

    def test_invalid_log_level_exits(self):
        with pytest.raises(SystemExit):
            _build_parser().parse_args(["--log-level", "VERBOSE"])

    def test_contracts_short_flag(self, tmp_path):
        f = tmp_path / "c.yaml"
        f.touch()
        args = _build_parser().parse_args(["-c", str(f)])
        assert args.contracts == str(f)

    def test_sinks_short_flag(self, tmp_path):
        f = tmp_path / "s.yaml"
        f.touch()
        args = _build_parser().parse_args(["-s", str(f)])
        assert args.sinks == str(f)

    def test_pipelines_short_flag(self, tmp_path):
        f = tmp_path / "p.yaml"
        f.touch()
        args = _build_parser().parse_args(["-p", str(f)])
        assert args.pipelines == str(f)

    def test_default_contracts_is_contracts_yaml(self):
        args = _build_parser().parse_args([])
        assert args.contracts.endswith("contracts.yaml")

    def test_default_sinks_is_sinks_yaml(self):
        args = _build_parser().parse_args([])
        assert args.sinks.endswith("sinks.yaml")

    def test_default_pipelines_is_pipelines_yaml(self):
        args = _build_parser().parse_args([])
        assert args.pipelines.endswith("pipelines.yaml")


class TestMainCli:
    def test_loads_config_paths_from_argv(self, tmp_path):
        c = tmp_path / "contracts.yaml"
        s = tmp_path / "sinks.yaml"
        p = tmp_path / "pipelines.yaml"
        src = tmp_path / "sources.yaml"
        c.write_text("contracts: []")
        s.write_text("sinks: []")
        p.write_text("pipelines: []")
        src.write_text("sources: []")

        with (
            patch("main.ConnectionSettings"),
            patch("main.load_config") as mock_contracts,
            patch("main.load_sinks_config") as mock_sinks,
            patch("main.load_pipelines_config") as mock_pipelines,
            patch("main.load_sources_config") as mock_sources,
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
            patch("main.IBGateway"),
            patch("main.Ingestor"),
            patch("main.util"),
        ):
            main(["-c", str(c), "-s", str(s), "-p", str(p), "--sources", str(src)])
            mock_contracts.assert_called_once_with(str(c))
            mock_sinks.assert_called_once_with(str(s))
            mock_pipelines.assert_called_once_with(str(p))
            mock_sources.assert_called_once_with(str(src))

    def test_no_args_uses_default_paths(self):
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
            main([])
            mock_contracts.assert_called_once_with(str(DEFAULT_CONTRACTS_CONFIG))
            mock_sinks.assert_called_once_with(str(DEFAULT_SINKS_CONFIG))
            mock_pipelines.assert_called_once_with(str(DEFAULT_PIPELINES_CONFIG))

    def test_sets_log_level_and_format(self):
        with (
            patch("main.logging") as mock_logging,
            patch("main.ConnectionSettings"),
            patch("main.load_config"),
            patch("main.load_sinks_config"),
            patch("main.load_pipelines_config"),
            patch("main.load_sources_config"),
            patch("main.SQSConsumer"),
            patch("main.build_pipelines"),
            patch("main.IBGateway"),
            patch("main.Ingestor"),
            patch("main.util"),
        ):
            main(["--log-level", "DEBUG"])
            mock_logging.basicConfig.assert_called_once_with(
                level="DEBUG",
                format=LOG_FORMAT,
            )

    def test_log_format_includes_filename_and_line_number(self):
        assert "%(filename)s" in LOG_FORMAT
        assert "%(lineno)d" in LOG_FORMAT

    def test_main_with_no_argv_uses_default_paths(self):
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
            main()
            mock_contracts.assert_called_once_with(str(DEFAULT_CONTRACTS_CONFIG))
            mock_sinks.assert_called_once_with(str(DEFAULT_SINKS_CONFIG))
            mock_pipelines.assert_called_once_with(str(DEFAULT_PIPELINES_CONFIG))

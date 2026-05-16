import pytest
from unittest.mock import MagicMock, patch

from ib_stream.cli import _build_parser, cli


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

    def test_config_short_flag(self, tmp_path):
        f = tmp_path / "c.yaml"
        f.touch()
        args = _build_parser().parse_args(["-c", str(f)])
        assert args.config == str(f)

    def test_config_long_flag(self, tmp_path):
        f = tmp_path / "c.yaml"
        f.touch()
        args = _build_parser().parse_args(["--config", str(f)])
        assert args.config == str(f)

    def test_default_config_is_contracts_yaml(self):
        args = _build_parser().parse_args([])
        assert args.config.endswith("contracts.yaml")


class TestCli:
    def test_delegates_to_main_with_config_path(self, tmp_path):
        f = tmp_path / "contracts.yaml"
        f.write_text("contracts: []")
        with patch("ib_stream.cli.main") as mock_main:
            cli(["-c", str(f)])
            mock_main.assert_called_once_with(config_path=str(f))

    def test_no_args_passes_default_config(self):
        from ib_stream.cli import DEFAULT_CONFIG
        with patch("ib_stream.cli.main") as mock_main:
            cli([])
            mock_main.assert_called_once_with(config_path=str(DEFAULT_CONFIG))

    def test_sets_log_level(self):
        with patch("ib_stream.cli.main"), patch("ib_stream.cli.logging") as mock_logging:
            cli(["--log-level", "DEBUG"])
            mock_logging.basicConfig.assert_called_once()
            _, kwargs = mock_logging.basicConfig.call_args
            assert kwargs["level"] == "DEBUG"

    def test_called_with_none_argv_uses_sys_argv(self):
        with patch("ib_stream.cli.main"), patch("sys.argv", ["ib-stream"]):
            cli(None)  # must not raise; argparse reads sys.argv[1:] → []

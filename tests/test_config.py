import pytest
import yaml
from pathlib import Path

from pydantic import ValidationError

from ib_stream.config import (
    AppConfig,
    ConnectionSettings,
    ContractConfig,
    DataType,
    load_config,
)


class TestDataType:
    def test_values(self):
        assert DataType.TICK == "tick"
        assert DataType.BAR == "bar"

    def test_is_str_subclass(self):
        assert isinstance(DataType.TICK, str)
        assert isinstance(DataType.BAR, str)


class TestContractConfig:
    def test_symbol_required(self):
        with pytest.raises(ValidationError):
            ContractConfig()

    def test_defaults(self):
        c = ContractConfig(symbol="AAPL")
        assert c.sec_type == "STK"
        assert c.exchange == "SMART"
        assert c.currency == "USD"
        assert c.primary_exchange == ""
        assert c.last_trade_date == ""
        assert c.data_type == DataType.TICK
        assert c.generic_tick_list == "233"
        assert c.bar_size == "1 min"
        assert c.what_to_show == "TRADES"
        assert c.use_rth is True
        assert c.history_duration == "1 D"

    def test_custom_values(self):
        c = ContractConfig(
            symbol="ES",
            sec_type="FUT",
            exchange="CME",
            currency="USD",
            data_type=DataType.BAR,
            bar_size="5 secs",
            use_rth=False,
            history_duration="2 D",
        )
        assert c.symbol == "ES"
        assert c.sec_type == "FUT"
        assert c.exchange == "CME"
        assert c.data_type == DataType.BAR
        assert c.bar_size == "5 secs"
        assert c.use_rth is False
        assert c.history_duration == "2 D"

    def test_data_type_from_string(self):
        c = ContractConfig(symbol="SPY", data_type="bar")
        assert c.data_type == DataType.BAR

    def test_last_trade_date(self):
        c = ContractConfig(symbol="ES", last_trade_date="202509")
        assert c.last_trade_date == "202509"


class TestConnectionSettings:
    def test_loads_from_env(self, monkeypatch):
        monkeypatch.setenv("IB_HOST", "10.0.0.1")
        monkeypatch.setenv("IB_PORT", "4001")
        monkeypatch.setenv("IB_CLIENT_ID", "7")
        s = ConnectionSettings()
        assert s.host == "10.0.0.1"
        assert s.port == 4001
        assert s.client_id == 7

    def test_optional_defaults(self, monkeypatch):
        monkeypatch.setenv("IB_HOST", "127.0.0.1")
        monkeypatch.setenv("IB_PORT", "7497")
        monkeypatch.setenv("IB_CLIENT_ID", "1")
        s = ConnectionSettings()
        assert s.timeout == 10.0
        assert s.readonly is True

    def test_timeout_override(self, monkeypatch):
        monkeypatch.setenv("IB_HOST", "127.0.0.1")
        monkeypatch.setenv("IB_PORT", "7497")
        monkeypatch.setenv("IB_CLIENT_ID", "1")
        monkeypatch.setenv("IB_TIMEOUT", "30.0")
        s = ConnectionSettings()
        assert s.timeout == 30.0

    def test_readonly_override(self, monkeypatch):
        monkeypatch.setenv("IB_HOST", "127.0.0.1")
        monkeypatch.setenv("IB_PORT", "7497")
        monkeypatch.setenv("IB_CLIENT_ID", "1")
        monkeypatch.setenv("IB_READONLY", "false")
        s = ConnectionSettings()
        assert s.readonly is False

    def test_missing_required_fields_raises(self, monkeypatch):
        monkeypatch.delenv("IB_HOST", raising=False)
        monkeypatch.delenv("IB_PORT", raising=False)
        monkeypatch.delenv("IB_CLIENT_ID", raising=False)
        with pytest.raises(Exception):
            ConnectionSettings(_env_file=None)


class TestAppConfig:
    def test_empty_by_default(self):
        cfg = AppConfig()
        assert cfg.contracts == []

    def test_with_contracts(self):
        contracts = [ContractConfig(symbol="AAPL"), ContractConfig(symbol="MSFT")]
        cfg = AppConfig(contracts=contracts)
        assert len(cfg.contracts) == 2
        assert cfg.contracts[0].symbol == "AAPL"


class TestLoadConfig:
    def test_valid_yaml(self, tmp_path):
        data = {"contracts": [{"symbol": "AAPL"}, {"symbol": "MSFT"}]}
        f = tmp_path / "contracts.yaml"
        f.write_text(yaml.dump(data))
        cfg = load_config(f)
        assert len(cfg.contracts) == 2
        assert cfg.contracts[0].symbol == "AAPL"
        assert cfg.contracts[1].symbol == "MSFT"

    def test_empty_contracts_list(self, tmp_path):
        f = tmp_path / "contracts.yaml"
        f.write_text(yaml.dump({"contracts": []}))
        cfg = load_config(f)
        assert cfg.contracts == []

    def test_accepts_path_as_string(self, tmp_path):
        f = tmp_path / "contracts.yaml"
        f.write_text(yaml.dump({"contracts": [{"symbol": "SPY"}]}))
        cfg = load_config(str(f))
        assert cfg.contracts[0].symbol == "SPY"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(tmp_path / "nonexistent.yaml")

    def test_real_contracts_yaml_is_valid(self):
        path = Path(__file__).parent.parent / "ib_stream" / "config" / "contracts.yaml"
        cfg = load_config(path)
        assert len(cfg.contracts) > 0
        symbols = [c.symbol for c in cfg.contracts]
        assert "AAPL" in symbols
        assert "EURUSD" in symbols

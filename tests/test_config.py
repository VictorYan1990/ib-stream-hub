import pytest
import yaml
from pathlib import Path

from pydantic import ValidationError

from ib_stream.config import (
    AppConfig,
    ConnectionSettings,
    ContractConfig,
    DataType,
    KinesisConfig,
    PipelineConfig,
    PipelinesConfig,
    SinkConfig,
    SinksConfig,
    SQSConfig,
    load_config,
    load_pipelines_config,
    load_sinks_config,
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

    def test_explicit_id_preserved(self):
        c = ContractConfig(id="my_aapl", symbol="AAPL")
        assert c.id == "my_aapl"

    def test_id_auto_derived_when_missing(self):
        c = ContractConfig(symbol="AAPL", sec_type="STK")
        assert c.id == "aapl_stk"

    def test_id_auto_derived_includes_expiry(self):
        c = ContractConfig(symbol="ES", sec_type="FUT", last_trade_date="202509")
        assert c.id == "es_fut_202509"


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


class TestSQSConfig:
    def test_required_fields(self):
        cfg = SQSConfig(region="us-east-1", queue_url="https://sqs.us-east-1.amazonaws.com/123/q")
        assert cfg.region == "us-east-1"
        assert cfg.queue_url == "https://sqs.us-east-1.amazonaws.com/123/q"

    def test_missing_region_raises(self):
        with pytest.raises(Exception):
            SQSConfig(queue_url="https://...")

    def test_missing_queue_url_raises(self):
        with pytest.raises(Exception):
            SQSConfig(region="us-east-1")


class TestKinesisConfig:
    def test_required_fields(self):
        cfg = KinesisConfig(region="us-east-1", stream_name="ib-data")
        assert cfg.region == "us-east-1"
        assert cfg.stream_name == "ib-data"

    def test_missing_stream_name_raises(self):
        with pytest.raises(Exception):
            KinesisConfig(region="us-east-1")


class TestSinkConfig:
    def test_fields(self):
        cfg = SinkConfig(id="s1", type="sqs", options={"region": "us-east-1"})
        assert cfg.id == "s1"
        assert cfg.type == "sqs"
        assert cfg.options == {"region": "us-east-1"}

    def test_options_default_empty(self):
        cfg = SinkConfig(id="local", type="thread")
        assert cfg.options == {}

    def test_missing_id_raises(self):
        with pytest.raises(Exception):
            SinkConfig(type="sqs")

    def test_missing_type_raises(self):
        with pytest.raises(Exception):
            SinkConfig(id="s1")


class TestSinksConfig:
    def test_empty_by_default(self):
        assert SinksConfig().sinks == []

    def test_duplicate_ids_raise(self):
        with pytest.raises(ValidationError):
            SinksConfig(sinks=[
                {"id": "dup", "type": "thread"},
                {"id": "dup", "type": "sqs", "options": {"region": "x", "queue_url": "y"}},
            ])


class TestPipelineConfig:
    def test_fields_and_default_enabled(self):
        p = PipelineConfig(id="p1", contract="aapl", sink="s1")
        assert p.id == "p1"
        assert p.contract == "aapl"
        assert p.sink == "s1"
        assert p.enabled is True

    def test_enabled_can_be_false(self):
        p = PipelineConfig(id="p1", contract="aapl", sink="s1", enabled=False)
        assert p.enabled is False


class TestPipelinesConfig:
    def test_duplicate_ids_raise(self):
        with pytest.raises(ValidationError):
            PipelinesConfig(pipelines=[
                {"id": "dup", "contract": "a", "sink": "s"},
                {"id": "dup", "contract": "b", "sink": "s"},
            ])


class TestLoadSinksConfig:
    def test_valid_yaml(self, tmp_path):
        data = {"sinks": [
            {"id": "q", "type": "sqs", "options": {"region": "us-east-1", "queue_url": "https://sqs/q"}},
            {"id": "local", "type": "thread"},
        ]}
        f = tmp_path / "sinks.yaml"
        f.write_text(yaml.dump(data))
        cfg = load_sinks_config(f)
        assert len(cfg.sinks) == 2
        assert cfg.sinks[0].type == "sqs"
        assert cfg.sinks[1].type == "thread"

    def test_accepts_path_as_string(self, tmp_path):
        data = {"sinks": [{"id": "local", "type": "thread"}]}
        f = tmp_path / "sinks.yaml"
        f.write_text(yaml.dump(data))
        cfg = load_sinks_config(str(f))
        assert cfg.sinks[0].id == "local"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_sinks_config(tmp_path / "nonexistent.yaml")

    def test_real_sinks_yaml_is_valid(self):
        path = Path(__file__).parent.parent / "ib_stream" / "config" / "sinks.yaml"
        cfg = load_sinks_config(path)
        assert len(cfg.sinks) > 0
        ids = [s.id for s in cfg.sinks]
        assert "market_data_sqs" in ids


class TestLoadPipelinesConfig:
    def test_valid_yaml(self, tmp_path):
        data = {"pipelines": [
            {"id": "p1", "contract": "aapl", "sink": "q", "enabled": True},
            {"id": "p2", "contract": "msft", "sink": "q", "enabled": False},
        ]}
        f = tmp_path / "pipelines.yaml"
        f.write_text(yaml.dump(data))
        cfg = load_pipelines_config(f)
        assert len(cfg.pipelines) == 2
        assert cfg.pipelines[0].enabled is True
        assert cfg.pipelines[1].enabled is False

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_pipelines_config(tmp_path / "nonexistent.yaml")

    def test_real_pipelines_yaml_is_valid(self):
        path = Path(__file__).parent.parent / "ib_stream" / "config" / "pipelines.yaml"
        cfg = load_pipelines_config(path)
        assert len(cfg.pipelines) > 0

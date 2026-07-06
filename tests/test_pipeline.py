import pytest

from ib_stream.config import (
    AppConfig,
    ContractConfig,
    PipelineConfig,
    PipelinesConfig,
    SinkConfig,
    SinksConfig,
)
from ib_stream.pipeline import build_pipelines
from ib_stream.sink import ThreadSink


def _app():
    return AppConfig(contracts=[
        ContractConfig(id="aapl", symbol="AAPL"),
        ContractConfig(id="msft", symbol="MSFT"),
    ])


def _sinks():
    return SinksConfig(sinks=[
        SinkConfig(id="local", type="thread"),
        SinkConfig(id="local2", type="thread"),
    ])


class TestBuildPipelines:
    def test_resolves_contract_and_sink(self):
        pipelines = PipelinesConfig(pipelines=[
            PipelineConfig(id="p1", contract="aapl", sink="local"),
        ])
        resolved = build_pipelines(_app(), _sinks(), pipelines)
        assert len(resolved) == 1
        assert resolved[0].id == "p1"
        assert resolved[0].contract.symbol == "AAPL"
        assert isinstance(resolved[0].sink, ThreadSink)

    def test_skips_disabled_pipelines(self):
        pipelines = PipelinesConfig(pipelines=[
            PipelineConfig(id="p1", contract="aapl", sink="local", enabled=False),
            PipelineConfig(id="p2", contract="msft", sink="local"),
        ])
        resolved = build_pipelines(_app(), _sinks(), pipelines)
        assert [p.id for p in resolved] == ["p2"]

    def test_shared_sink_instantiated_once(self):
        pipelines = PipelinesConfig(pipelines=[
            PipelineConfig(id="p1", contract="aapl", sink="local"),
            PipelineConfig(id="p2", contract="msft", sink="local"),
        ])
        resolved = build_pipelines(_app(), _sinks(), pipelines)
        assert resolved[0].sink is resolved[1].sink

    def test_distinct_sinks_are_different_instances(self):
        pipelines = PipelinesConfig(pipelines=[
            PipelineConfig(id="p1", contract="aapl", sink="local"),
            PipelineConfig(id="p2", contract="msft", sink="local2"),
        ])
        resolved = build_pipelines(_app(), _sinks(), pipelines)
        assert resolved[0].sink is not resolved[1].sink

    def test_unknown_contract_raises(self):
        pipelines = PipelinesConfig(pipelines=[
            PipelineConfig(id="p1", contract="nope", sink="local"),
        ])
        with pytest.raises(ValueError, match="unknown contract"):
            build_pipelines(_app(), _sinks(), pipelines)

    def test_unknown_sink_raises(self):
        pipelines = PipelinesConfig(pipelines=[
            PipelineConfig(id="p1", contract="aapl", sink="nope"),
        ])
        with pytest.raises(ValueError, match="unknown sink"):
            build_pipelines(_app(), _sinks(), pipelines)

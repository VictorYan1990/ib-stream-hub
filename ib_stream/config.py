"""Configuration models and loaders for ib-stream-hub.

Three configuration files drive the system:

* ``contracts.yaml``  — what instruments to subscribe to (each has an ``id``).
* ``sinks.yaml``      — where data can go: a list of typed sinks (each has an
  ``id`` and a ``type`` such as ``sqs``, ``kinesis``, or ``thread``).
* ``pipelines.yaml``  — which contract feeds which sink, by ``id``.
"""

from enum import Enum
from pathlib import Path
from typing import Any, Dict, List

import yaml
from pydantic import BaseModel, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_DOTENV_PATH = Path(__file__).parent.parent / ".env"


class DataType(str, Enum):
    TICK = "tick"
    BAR = "bar"


class ContractConfig(BaseModel):
    # Stable identifier referenced by pipelines.yaml. Auto-derived from the
    # contract identity when omitted (see _default_id below).
    id: str = ""

    symbol: str
    sec_type: str = "STK"
    exchange: str = "SMART"
    currency: str = "USD"
    primary_exchange: str = ""
    last_trade_date: str = ""

    data_type: DataType = DataType.TICK

    # Tick-specific
    # IB generic tick IDs joined by comma; "233" = last/rtVolume/rtTime/vwap.
    generic_tick_list: str = "233"

    # Bar-specific
    # "5 secs" uses reqRealTimeBars; all other IB bar size strings
    # (e.g. "1 min", "5 mins", "1 hour") use reqHistoricalData(keepUpToDate=True).
    bar_size: str = "1 min"
    what_to_show: str = "TRADES"
    use_rth: bool = True
    # Duration string passed to reqHistoricalData for the initial backfill.
    history_duration: str = "1 D"

    @model_validator(mode="after")
    def _default_id(self) -> "ContractConfig":
        if not self.id:
            parts = [self.symbol, self.sec_type]
            if self.last_trade_date:
                parts.append(self.last_trade_date)
            self.id = "_".join(parts).lower()
        return self


class ConnectionSettings(BaseSettings):
    """IB Gateway connection parameters sourced from environment variables.

    Priority (highest → lowest): env vars → .env file → defaults.

    Required variables (no defaults — must be set explicitly):
        IB_HOST, IB_PORT, IB_CLIENT_ID

    Optional variables:
        IB_TIMEOUT    (default: 10.0)
        IB_READONLY   (default: true)
    """

    host: str
    port: int
    client_id: int
    timeout: float = 10.0
    readonly: bool = True

    model_config = SettingsConfigDict(
        env_prefix="IB_",
        env_file=str(_DOTENV_PATH),
        env_file_encoding="utf-8",
    )


class AppConfig(BaseModel):
    contracts: List[ContractConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self) -> "AppConfig":
        _require_unique_ids(self.contracts, "contract")
        return self


# ---------------------------------------------------------------------------
# Sink configuration
# ---------------------------------------------------------------------------

# Type-specific option models. Each sink class validates its own options via
# the matching model, but they are also exposed here for documentation/tests.
class SQSConfig(BaseModel):
    region: str
    queue_url: str


class KinesisConfig(BaseModel):
    region: str
    stream_name: str


class SinkConfig(BaseModel):
    """A single sink declaration.

    ``type`` selects the provider implementation (see ib_stream.sink), and
    ``options`` carries the provider-specific settings validated by that
    implementation.
    """

    id: str
    type: str
    options: Dict[str, Any] = Field(default_factory=dict)


class SinksConfig(BaseModel):
    sinks: List[SinkConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self) -> "SinksConfig":
        _require_unique_ids(self.sinks, "sink")
        return self


# ---------------------------------------------------------------------------
# Pipeline configuration
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    """Maps a contract id to a sink id. Disabled pipelines are skipped."""

    id: str
    contract: str
    sink: str
    enabled: bool = True


class PipelinesConfig(BaseModel):
    pipelines: List[PipelineConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self) -> "PipelinesConfig":
        _require_unique_ids(self.pipelines, "pipeline")
        return self


# ---------------------------------------------------------------------------
# Helpers & loaders
# ---------------------------------------------------------------------------

def _require_unique_ids(items: List[Any], label: str) -> None:
    seen: set[str] = set()
    for item in items:
        if item.id in seen:
            raise ValueError(f"Duplicate {label} id: {item.id!r}")
        seen.add(item.id)


def _read_yaml(path: str | Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def load_config(path: str | Path) -> AppConfig:
    return AppConfig(**_read_yaml(path))


def load_sinks_config(path: str | Path) -> SinksConfig:
    return SinksConfig(**_read_yaml(path))


def load_pipelines_config(path: str | Path) -> PipelinesConfig:
    return PipelinesConfig(**_read_yaml(path))

"""Configuration models and loader for ib-stream-hub."""

from enum import Enum
from pathlib import Path
from typing import List

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_DOTENV_PATH = Path(__file__).parent.parent / ".env"


class DataType(str, Enum):
    TICK = "tick"
    BAR = "bar"


class ContractConfig(BaseModel):
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


def load_config(path: str | Path) -> AppConfig:
    with open(path) as f:
        data = yaml.safe_load(f)
    return AppConfig(**data)

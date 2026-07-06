# ib-stream-hub

A lightweight Python service that connects to an Interactive Brokers Gateway or TWS instance and streams real-time market data — ticks and OHLCV bars — and publishes each update to configurable **sinks** (AWS SQS, AWS Kinesis, or an in-process queue) through declarative **pipelines**.

## Features

- **Tick streaming** — via `reqMktData` (last, size, RT volume, VWAP, …)
- **5-second real-time bars** — via `reqRealTimeBars`
- **Historical bars kept up to date** — via `reqHistoricalData(keepUpToDate=True)` for any bar size other than 5 secs (e.g. `1 min`, `5 mins`, `1 hour`)
- Supports **STK, FUT, CASH (Forex), IND, CFD** contract types
- **Pluggable sinks** — `sqs`, `kinesis`, or `thread` (in-memory), selected purely by config; easy to add more
- **Declarative pipelines** — map any contract to any sink by id
- Connection settings loaded from **environment variables / `.env`** — no secrets in code or config
- Automatic **re-subscription** on reconnect, plus a **watchdog** that warns when a symbol goes silent

## Requirements

- Python 3.11+
- A running [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) or TWS instance with the API enabled
- An Interactive Brokers account (a paper-trading account works fine, though market-data permissions vary)
- For cloud sinks: AWS credentials with permission to write to the target SQS queue / Kinesis stream

## Installation

```bash
# 1. Clone the repo
git clone <repo-url>
cd ib-stream-hub

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

## Configuration

The system is driven by **one `.env` file** (connection secrets) and **three YAML files** under `ib_stream/config/`:

| File | Purpose |
|---|---|
| `.env` | IB Gateway connection settings (secrets) |
| `contracts.yaml` | What instruments to subscribe to |
| `sinks.yaml` | Where data can go (typed destinations) |
| `pipelines.yaml` | Which contract feeds which sink |

### Connection settings (`.env`)

Copy `.env.example` to `.env` and fill in your IB Gateway details:

```bash
cp .env.example .env
```

```ini
IB_HOST=127.0.0.1
IB_PORT=7497        # TWS paper: 7497 | TWS live: 7496 | Gateway paper: 4002 | Gateway live: 4001
IB_CLIENT_ID=1
IB_TIMEOUT=10.0
IB_READONLY=true
```

> `.env` is gitignored and must never be committed.

### Contracts (`ib_stream/config/contracts.yaml`)

Each entry maps to one IB subscription and carries a unique `id` referenced by pipelines. If `id` is omitted it is auto-derived as `symbol_sectype[_expiry]` (lowercased).

```yaml
contracts:
  # Tick-level market data
  - id: aapl
    symbol: AAPL
    sec_type: STK
    exchange: SMART
    primary_exchange: NASDAQ
    currency: USD
    data_type: tick
    generic_tick_list: "233"

  # 5-second real-time bars
  - id: spy
    symbol: SPY
    sec_type: STK
    exchange: SMART
    primary_exchange: ARCA
    currency: USD
    data_type: bar
    bar_size: "5 secs"
    what_to_show: TRADES
    use_rth: true

  # Minute bars via historical-kept-up-to-date (futures example)
  - id: es_fut
    symbol: ES
    sec_type: FUT
    exchange: CME
    currency: USD
    last_trade_date: "202607"   # YYYYMM — must be a listed expiry
    data_type: bar
    bar_size: "1 min"
    what_to_show: TRADES
    use_rth: false
    history_duration: "1 D"
```

#### Contract fields reference

| Field | Required | Default | Description |
|---|---|---|---|
| `id` | no | auto | Stable id referenced by pipelines; derived from symbol/sec_type/expiry if omitted |
| `symbol` | yes | — | IB symbol (or 6-char Forex pair) |
| `sec_type` | no | `STK` | `STK`, `FUT`, `CASH`, `IND`, `CFD` |
| `exchange` | no | `SMART` | IB exchange routing |
| `primary_exchange` | no | `""` | Disambiguates SMART routing for stocks |
| `currency` | no | `USD` | Contract currency |
| `last_trade_date` | no | `""` | Futures expiry in `YYYYMM` format |
| `data_type` | no | `tick` | `tick` or `bar` |
| `generic_tick_list` | no | `"233"` | Comma-separated IB tick IDs (tick subscriptions) |
| `bar_size` | no | `"1 min"` | IB bar size string; `"5 secs"` uses `reqRealTimeBars`, all others use `reqHistoricalData` |
| `what_to_show` | no | `TRADES` | `TRADES`, `MIDPOINT`, `BID`, `ASK`, … |
| `use_rth` | no | `true` | Regular trading hours only |
| `history_duration` | no | `"1 D"` | Initial backfill window for historical bars |

### Sinks (`ib_stream/config/sinks.yaml`)

A list of typed destinations. Each has a unique `id`, a `type` selecting the provider implementation, and an `options` block with provider-specific settings.

```yaml
sinks:
  - id: market_data_sqs
    type: sqs
    options:
      region: us-east-1
      queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/ib-market-data.fifo

  - id: market_data_kinesis
    type: kinesis
    options:
      region: us-east-1
      stream_name: ib-market-data

  - id: local_debug
    type: thread
    options: {}
```

#### Sink types

| `type` | Provider | `options` | Notes |
|---|---|---|---|
| `sqs` | AWS SQS | `region`, `queue_url` | FIFO queues (`.fifo`) get `MessageGroupId` = partition key + a UUID `MessageDeduplicationId` |
| `kinesis` | AWS Kinesis | `region`, `stream_name` | Partition key is used as the Kinesis `PartitionKey` |
| `thread` | In-process queue | `maxsize` (optional) | No external provider; ideal for local runs and tests |

> AWS credentials are resolved by boto3 in the standard order: environment variables → `~/.aws/credentials` → IAM instance role. Prefer an IAM instance role in production.

### Pipelines (`ib_stream/config/pipelines.yaml`)

Each pipeline wires one contract (by id) to one sink (by id). Set `enabled: false` to keep a pipeline declared but inactive. A sink referenced by several pipelines is instantiated only once.

```yaml
pipelines:
  - id: aapl_to_sqs
    contract: aapl
    sink: market_data_sqs
    enabled: true

  - id: es_to_sqs
    contract: es_fut
    sink: market_data_sqs
    enabled: true

  - id: eurusd_to_kinesis
    contract: eurusd
    sink: market_data_kinesis
    enabled: false
```

## Usage

```bash
# Stream with all defaults (.env + the three YAML files in ib_stream/config/)
python main.py

# Point to custom config files
python main.py -c my_contracts.yaml -s my_sinks.yaml -p my_pipelines.yaml

# Raise logging verbosity
python main.py --log-level DEBUG
```

### CLI options

| Option | Default | Description |
|---|---|---|
| `-c`, `--contracts` | `ib_stream/config/contracts.yaml` | Path to contracts YAML |
| `-s`, `--sinks` | `ib_stream/config/sinks.yaml` | Path to sinks YAML |
| `-p`, `--pipelines` | `ib_stream/config/pipelines.yaml` | Path to pipelines YAML |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

## Project layout

```
ib-stream-hub/
├── main.py                         # Root entry point: loads config, builds pipelines, runs
├── ib_stream/
│   ├── __init__.py
│   ├── cli.py                      # Argument parser + default config paths
│   ├── config.py                   # Pydantic config models + YAML loaders
│   ├── gateway.py                  # IB Gateway connection manager (auto-reconnect)
│   ├── ingestor.py                 # Subscriptions → sink.publish()
│   ├── pipeline.py                 # Resolves contracts + sinks into runtime pipelines
│   ├── sink.py                     # Sink ABC, SQS/Kinesis/Thread sinks, create_sink() factory
│   └── config/
│       ├── contracts.yaml          # Instrument definitions
│       ├── sinks.yaml              # Sink (destination) definitions
│       └── pipelines.yaml          # contract → sink mappings
├── tests/
├── .env.example                    # Connection settings template
├── requirements.txt
└── pyproject.toml
```

## How it works

1. **`ConnectionSettings`** reads `IB_*` environment variables (or `.env`) and connects to IB Gateway via `ib_insync`.
2. **`IBGateway`** manages the connection lifecycle and triggers re-subscription on reconnect.
3. **`build_pipelines`** resolves `contracts.yaml`, `sinks.yaml`, and `pipelines.yaml` by id into runtime `ResolvedPipeline(contract, sink)` objects, instantiating each sink via the `create_sink()` factory.
4. **`Ingestor`** subscribes each pipeline's contract (`reqMktData` / `reqRealTimeBars` / `reqHistoricalData`), serialises every update to a dict, and forwards it to that pipeline's `sink.publish(key, payload)`. The partition `key` encodes the contract identity (symbol, sec_type, expiry) so futures with different expirations stay ordered separately.

## Extending

### Add a new sink type

Implement the `Sink` interface and register it — no changes needed elsewhere:

```python
from ib_stream.sink import Sink, SINK_REGISTRY

class MyBrokerSink(Sink):
    @classmethod
    def from_options(cls, options: dict) -> "MyBrokerSink":
        return cls(**options)

    def publish(self, key: str, payload: dict) -> None:
        ...  # send payload keyed by `key`

SINK_REGISTRY["mybroker"] = MyBrokerSink
```

Then reference it from `sinks.yaml` with `type: mybroker`.

### Test / local runs without AWS

Use the `thread` sink — messages stay in an in-memory queue you can drain or inspect:

```python
from ib_stream.sink import ThreadSink

sink = ThreadSink()
sink.publish("AAPL:STK", {"last": 150.2})
key, payload = sink.get_nowait()
```

## Development

```bash
# Run tests
pytest

# Lint (bug & security rules) and type-check
ruff check .
mypy ib_stream/

# Run with verbose logging against a paper account
IB_PORT=7497 python main.py --log-level DEBUG
```

## License

MIT

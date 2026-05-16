# ib-stream-hub

A lightweight Python service that connects to an Interactive Brokers Gateway or TWS instance and streams real-time market data — ticks and OHLCV bars — into per-symbol asyncio queues for downstream processing.

## Features

- **Tick streaming** — via `reqMktData` (last, size, RT volume, VWAP, …)
- **5-second real-time bars** — via `reqRealTimeBars`
- **Historical bars kept up to date** — via `reqHistoricalData(keepUpToDate=True)` for any bar size other than 5 secs (e.g. `1 min`, `5 mins`, `1 hour`)
- Supports **STK, FUT, CASH (Forex), IND, CFD** contract types
- Contract definitions live entirely in a **YAML config file** — no code changes to add/remove symbols
- Connection settings loaded from **environment variables / `.env`** — no secrets in code or config
- Automatic **re-subscription** on reconnect

## Requirements

- Python 3.11+
- A running [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) or TWS instance with the API enabled
- An Interactive Brokers account (paper trading account works fine, however the market data permissions can vary)

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

All instruments to stream are declared in YAML. Each entry maps to one IB subscription:

```yaml
contracts:
  # Tick-level market data (last, size, RT volume, VWAP)
  - symbol: AAPL
    sec_type: STK
    exchange: SMART
    primary_exchange: NASDAQ
    currency: USD
    data_type: tick
    generic_tick_list: "233"

  # 5-second real-time bars
  - symbol: SPY
    sec_type: STK
    exchange: SMART
    primary_exchange: ARCA
    currency: USD
    data_type: bar
    bar_size: "5 secs"
    what_to_show: TRADES
    use_rth: true

  # Minute bars via historical-kept-up-to-date (futures example)
  - symbol: ES
    sec_type: FUT
    exchange: CME
    currency: USD
    last_trade_date: "202509"   # YYYYMM — must be a listed expiry
    data_type: bar
    bar_size: "1 min"
    what_to_show: TRADES
    use_rth: false
    history_duration: "1 D"

  # Forex pair
  - symbol: EURUSD
    sec_type: CASH
    exchange: IDEALPRO
    currency: USD
    data_type: tick
    generic_tick_list: ""
```

#### Contract fields reference

| Field | Required | Default | Description |
|---|---|---|---|
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

## Usage

```bash
# Stream with defaults (.env + ib_stream/config/contracts.yaml)
python main.py

# Point to a different contracts file
python main.py --config path/to/my_contracts.yaml

# Raise logging verbosity
python main.py --log-level DEBUG
```

### CLI options

| Option | Default | Description |
|---|---|---|
| `-c`, `--config` | `ib_stream/config/contracts.yaml` | Path to contracts YAML |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` |

## Project layout

```
ib-stream-hub/
├── main.py                         # Root entry point
├── ib_stream/
│   ├── __init__.py
│   ├── cli.py                      # Argument parsing + main() orchestration
│   ├── config.py                   # Pydantic config models and YAML loader
│   ├── gateway.py                  # IB Gateway connection manager (auto-reconnect)
│   ├── ingestor.py                 # Contract subscriptions → asyncio queues
│   └── config/
│       └── contracts.yaml          # Default instrument definitions
├── tests/
├── .env.example                    # Connection settings template
├── requirements.txt
└── pyproject.toml
```

## How it works

1. **`ConnectionSettings`** reads `IB_*` environment variables (or `.env`) and connects to IB Gateway via `ib_insync`.
2. **`IBGateway`** manages the connection lifecycle and triggers re-subscription callbacks on reconnect.
3. **`Ingestor`** iterates over the contracts YAML, creates the appropriate IB subscription (`reqMktData` / `reqRealTimeBars` / `reqHistoricalData`), and fans out each update into a per-symbol `asyncio.Queue`.
4. **Your code** consumes those queues — write to a database, publish to a message broker, forward to a WebSocket, etc. The default `_drain_queues` in `cli.py` simply logs each update and serves as a starting template.

## Extending

To consume data in your own way, replace or extend `_drain_queues` in `ib_stream/cli.py`:

```python
async def _drain_queues(ingestor: Ingestor) -> None:
    async def drain(symbol: str) -> None:
        q = ingestor.get_queue(symbol)
        while True:
            item = await q.get()
            # write to DB, publish to Kafka, push to WebSocket, …
            await my_handler(symbol, item)

    await asyncio.gather(*(drain(sym) for sym in ingestor.queues))
```

## Development

```bash
# Run tests
pytest

# Run with verbose logging against a paper account
IB_PORT=7497 python main.py --log-level DEBUG
```

## License

MIT

# AGENTS.md

Canonical, vendor-neutral guidance for AI coding agents working in this
repository. 

## Project overview

`ib-stream-hub` connects to an Interactive Brokers Gateway / TWS instance,
streams real-time market data (ticks and OHLCV bars), and publishes each
update to configurable **sinks** (AWS SQS, Kinesis, or an in-process queue)
through declarative **pipelines**. A companion `market_data_consumer` submodule 
now serves as a testing consumer thatdrains a queue on the other side.

## Architecture (configuration-driven)

Nothing about *what* to stream or *where* it goes is hard-coded. Four inputs
drive the system:

| Input | Location | Purpose |
|---|---|---|
| Connection secrets | `.env` (`IB_*` env vars) | IB Gateway host/port/client id |
| Contracts | `ib_stream/config/contracts.yaml` | Instruments to subscribe to (each has an `id`) |
| Sinks | `ib_stream/config/sinks.yaml` | Typed destinations (`sqs`, `kinesis`, `thread`) |
| Pipelines | `ib_stream/config/pipelines.yaml` | Map a contract `id` → a sink `id` |
| Consumer sources | `market_data_consumer/config/sources.yaml` | Where the consumer reads from |

Runtime flow: `ConnectionSettings` → `IBGateway` → `build_pipelines()` →
`Ingestor` → `sink.publish(key, payload)`.

## Conventions (how we like changes made here)

These reflect decisions made over the life of the project — follow them:

- **Configuration over flags.** To add/remove instruments or destinations,
  edit the YAML files — do not hard-code them or add narrow CLI flags. The CLI
  stays minimal (config paths + `--log-level`).
- **IB connection is env-only.** Credentials/endpoint come from `IB_*` env
  vars / `.env`, resolved by `ConnectionSettings`. No `--host`/`--port` flags.
- **Entry point.** `main()` lives in `main.py` and owns argument parsing and
  logger setup. `ib_stream/cli.py` only defines the argument parser
  (`_build_parser`) and default config paths — it has no `cli()` wrapper.
- **Logging format** includes source location:
  `%(asctime)s %(levelname)-8s %(filename)s:%(lineno)d %(name)s: %(message)s`.
- **Sinks are pluggable.** Add a provider by implementing the `Sink` ABC and
  registering it in `SINK_REGISTRY` (`ib_stream/sink.py`); select it by `type`
  in `sinks.yaml`. Use the `thread` sink for local/test runs without AWS.
- **Prefer editing existing files** over creating new ones. Keep comments
  minimal — explain *why*, never narrate *what*.
- **Secrets never get committed** (`.env` and any credential-bearing config).

## Quality gates (keep these green)

Run before proposing changes; CI enforces them on every push:

```bash
pytest                 # keep the suite passing
ruff check .           # bugs & security only (rules E, B, S)
mypy ib_stream/        # type check
```

- **ruff** is intentionally scoped to real problems (`E`, `B`, `S`).
  Stylistic/aspirational rules are ignored (`E501`, `B017`, `S101`) — do not
  reintroduce them as failures.
- **mypy**: third-party libs without stubs (`boto3`, `botocore`) are ignored
  via `pyproject.toml`; `PyYAML` uses real stubs (`types-PyYAML`).
- Coverage gate is 70% (`--cov-fail-under=70`).

## Run

```bash
python main.py                       # all defaults
python main.py --log-level DEBUG
python main.py -c <contracts> -s <sinks> -p <pipelines> --sources <sources>
```

## Personas

Task-oriented personas live in `.agents/personas/`. Load the one that fits the
task to adopt its mindset and checklist:

- `market-data-engineer` — day-to-day feature/bugfix work on the pipeline.
- `pipeline-reviewer` — reviewing changes for correctness and conventions.

## Agent config layout (source of truth + import pattern)

Canonical, committed, vendor-neutral files:

```
AGENTS.md                     # this file — instructions (cross-tool standard)
.agents/
├── personas/                 # reusable personas (source of truth)
└── skills/                   # reusable skills (Agent Skills standard)
```

Tool-specific paths import or symlink to the canonical copies so each agent
auto-discovers them at its required fixed path:

```
CLAUDE.md            ->  imports AGENTS.md via "@AGENTS.md"
.cursor/skills       ->  symlink to ../.agents/skills
.claude/skills       ->  symlink to ../.agents/skills
.cursor/agents       ->  symlink to ../.agents/personas
```

Edit the canonical files under `.agents/` (and this `AGENTS.md`); never edit
the symlinked copies directly.

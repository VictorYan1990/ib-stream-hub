"""Pipeline resolution: ties contracts, sinks, and pipeline declarations together.

A *pipeline* in ``pipelines.yaml`` references a contract id and a sink id.
:func:`build_pipelines` resolves those ids against the loaded contract and sink
configs, instantiates each referenced sink exactly once, and returns the list
of runtime :class:`ResolvedPipeline` objects the ingestor consumes.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List

from .config import AppConfig, ContractConfig, PipelinesConfig, SinksConfig
from .sink import Sink, create_sink

logger = logging.getLogger(__name__)


@dataclass
class ResolvedPipeline:
    """A pipeline with its contract and sink resolved to live objects."""

    id: str
    contract: ContractConfig
    sink: Sink


def build_pipelines(
    app_cfg: AppConfig,
    sinks_cfg: SinksConfig,
    pipelines_cfg: PipelinesConfig,
) -> List[ResolvedPipeline]:
    """Resolve enabled pipelines into (contract, sink) runtime objects.

    Sinks are instantiated lazily and shared: a sink referenced by several
    pipelines is created only once.

    Raises:
        ValueError: if a pipeline references an unknown contract or sink id.
    """
    contracts_by_id: Dict[str, ContractConfig] = {c.id: c for c in app_cfg.contracts}
    sink_cfgs_by_id = {s.id: s for s in sinks_cfg.sinks}

    sink_cache: Dict[str, Sink] = {}
    resolved: List[ResolvedPipeline] = []

    for p in pipelines_cfg.pipelines:
        if not p.enabled:
            logger.info("Pipeline %r disabled — skipping", p.id)
            continue

        if p.contract not in contracts_by_id:
            raise ValueError(
                f"Pipeline {p.id!r} references unknown contract id {p.contract!r}"
            )
        if p.sink not in sink_cfgs_by_id:
            raise ValueError(
                f"Pipeline {p.id!r} references unknown sink id {p.sink!r}"
            )

        if p.sink not in sink_cache:
            sink_cache[p.sink] = create_sink(sink_cfgs_by_id[p.sink])

        resolved.append(
            ResolvedPipeline(
                id=p.id,
                contract=contracts_by_id[p.contract],
                sink=sink_cache[p.sink],
            )
        )
        logger.info(
            "Initialized Pipeline %r: contract %r → sink %r (%s)",
            p.id, p.contract, p.sink, sink_cfgs_by_id[p.sink].type,
        )

    return resolved

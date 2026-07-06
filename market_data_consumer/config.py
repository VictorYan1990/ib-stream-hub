"""Consumer-side configuration models and loader.

Deliberately does not import from ``ib_stream.config`` so this package stays
standalone. The schema mirrors ``sinks.yaml`` on purpose: from the consumer's
point of view, a sink on the ingester side is a *source* on this side.
"""

from pathlib import Path
from typing import Any, Dict, List

import yaml
from pydantic import BaseModel, Field, model_validator


class SQSSourceOptions(BaseModel):
    region: str
    queue_url: str


class SourceConfig(BaseModel):
    """A single source declaration.

    ``type`` selects the provider implementation (currently only ``sqs``);
    ``options`` carries the provider-specific settings.
    """

    id: str
    type: str
    options: Dict[str, Any] = Field(default_factory=dict)


class SourcesConfig(BaseModel):
    sources: List[SourceConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self) -> "SourcesConfig":
        seen: set[str] = set()
        for s in self.sources:
            if s.id in seen:
                raise ValueError(f"Duplicate source id: {s.id!r}")
            seen.add(s.id)
        return self


def load_sources_config(path: str | Path) -> SourcesConfig:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return SourcesConfig(**data)

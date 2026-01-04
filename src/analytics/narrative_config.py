from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

DEFAULT_NARRATIVE_CONFIG = Path("config/narratives.json")


@dataclass(frozen=True)
class NarrativeTerms:
    comparative_terms: Tuple[str, ...]
    relationship_patterns: Mapping[str, Tuple[str, ...]]
    risk_terms: Tuple[str, ...]
    study_context_terms: Tuple[str, ...]
    trial_phase_patterns: Tuple[str, ...]
    endpoint_terms: Tuple[str, ...]


@dataclass(frozen=True)
class NarrativeRule:
    name: str
    narrative_type: str
    narrative_subtype: Optional[str]
    confidence: float
    priority: int
    requires: Mapping[str, Tuple[str, ...]]
    requires_sentiment: Tuple[str, ...]


@dataclass(frozen=True)
class NarrativeSchema:
    terms: NarrativeTerms
    rules: Tuple[NarrativeRule, ...]


def _resolve_config_path(path: Path | str | None) -> Path:
    config_path = Path(path) if path is not None else DEFAULT_NARRATIVE_CONFIG
    if not config_path.exists():
        raise FileNotFoundError(f"Narrative config not found at {config_path}")
    return config_path.resolve()


def _normalize_strings(values: Iterable[str]) -> Tuple[str, ...]:
    return tuple(sorted({value.strip() for value in values if value and value.strip()}))


def _load_schema_from_disk(path: Path) -> NarrativeSchema:
    data = json.loads(path.read_text(encoding="utf-8"))
    terms_data = data.get("terms") or {}
    narratives_data = data.get("narratives") or []

    if not narratives_data:
        raise ValueError("Narrative config must define at least one narrative rule.")

    relationship_patterns = {
        label: _normalize_strings(patterns)
        for label, patterns in (terms_data.get("relationship_patterns") or {}).items()
    }

    terms = NarrativeTerms(
        comparative_terms=_normalize_strings(terms_data.get("comparative_terms", [])),
        relationship_patterns=relationship_patterns,
        risk_terms=_normalize_strings(terms_data.get("risk_terms", [])),
        study_context_terms=_normalize_strings(terms_data.get("study_context_terms", [])),
        trial_phase_patterns=tuple(terms_data.get("trial_phase_patterns", [])),
        endpoint_terms=_normalize_strings(terms_data.get("endpoint_terms", [])),
    )

    rules: List[NarrativeRule] = []
    for raw in narratives_data:
        name = raw.get("name")
        narrative_type = raw.get("type")
        if not name or not narrative_type:
            raise ValueError("Narrative rules must include 'name' and 'type' fields.")

        requires = {
            key: _normalize_strings(values)
            for key, values in (raw.get("requires") or {}).items()
        }
        rule = NarrativeRule(
            name=name,
            narrative_type=narrative_type,
            narrative_subtype=raw.get("subtype"),
            confidence=float(raw.get("confidence", 0.5)),
            priority=int(raw.get("priority", 0)),
            requires=requires,
            requires_sentiment=_normalize_strings(raw.get("requires_sentiment", [])),
        )
        rules.append(rule)

    if not rules:
        raise ValueError("Narrative config must include at least one rule.")

    rules.sort(key=lambda r: r.priority, reverse=True)
    return NarrativeSchema(terms=terms, rules=tuple(rules))


@lru_cache(maxsize=4)
def _load_schema_cached(resolved_path: str) -> NarrativeSchema:
    return _load_schema_from_disk(Path(resolved_path))


def load_narrative_schema(path: Path | str | None = None) -> NarrativeSchema:
    resolved = _resolve_config_path(path)
    return _load_schema_cached(str(resolved))


def load_narrative_terms(path: Path | str | None = None) -> NarrativeTerms:
    return load_narrative_schema(path).terms


def load_narrative_rules(path: Path | str | None = None) -> Tuple[NarrativeRule, ...]:
    return load_narrative_schema(path).rules


def reset_narrative_schema_cache() -> None:
    _load_schema_cached.cache_clear()

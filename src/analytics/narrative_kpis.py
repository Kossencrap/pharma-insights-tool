from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Tuple

DEFAULT_KPI_CONFIG = Path("config/narratives_kpis.json")


@dataclass(frozen=True)
class LabelPrecisionSpec:
    sample_size: int
    min_precision: float
    high_risk_min_precision: float
    high_risk_types: Tuple[str, ...]
    sample_export: str


@dataclass(frozen=True)
class ConfidenceBreakdownSpec:
    min_top_gap: float
    min_sentence_confidence: float
    require_sum_to_one: bool
    low_confidence_flag: str


@dataclass(frozen=True)
class ChangeSignificanceSpec:
    min_relative_delta: float
    min_sentence_count: int
    status_field: str
    export_glob: str


@dataclass(frozen=True)
class CompetitiveDirectionSpec:
    sample_size: int
    min_accuracy: float
    reference_file: str


@dataclass(frozen=True)
class NarrativeInvariantSpec:
    min_pass_rate: float


@dataclass(frozen=True)
class NarrativeKPISpec:
    label_precision: LabelPrecisionSpec
    confidence_breakdown: ConfidenceBreakdownSpec
    change_significance: ChangeSignificanceSpec
    competitive_direction: CompetitiveDirectionSpec
    narrative_invariants: NarrativeInvariantSpec


def _resolve_config_path(path: Path | str | None) -> Path:
    config_path = Path(path) if path is not None else DEFAULT_KPI_CONFIG
    if not config_path.exists():
        raise FileNotFoundError(f"Narrative KPI config not found at {config_path}")
    return config_path.resolve()


def _normalize_strings(values) -> Tuple[str, ...]:
    cleaned = {str(value).strip() for value in values or [] if str(value).strip()}
    return tuple(sorted(cleaned))


def _load_kpis_from_disk(path: Path) -> NarrativeKPISpec:
    data = json.loads(path.read_text(encoding="utf-8"))

    label_precision_data = data.get("label_precision")
    confidence_data = data.get("confidence_breakdown")
    change_data = data.get("change_significance")
    competitive_data = data.get("competitive_direction")

    if not all([label_precision_data, confidence_data, change_data, competitive_data]):
        raise ValueError("Narrative KPI config must include all KPI sections.")

    label_precision = LabelPrecisionSpec(
        sample_size=int(label_precision_data["sample_size"]),
        min_precision=float(label_precision_data["min_precision"]),
        high_risk_min_precision=float(label_precision_data["high_risk_min_precision"]),
        high_risk_types=_normalize_strings(label_precision_data.get("high_risk_types", [])),
        sample_export=str(label_precision_data["sample_export"]),
    )

    confidence_breakdown = ConfidenceBreakdownSpec(
        min_top_gap=float(confidence_data["min_top_gap"]),
        min_sentence_confidence=float(confidence_data["min_sentence_confidence"]),
        require_sum_to_one=bool(confidence_data.get("require_sum_to_one", True)),
        low_confidence_flag=str(confidence_data.get("low_confidence_flag", "low_confidence")),
    )

    change_significance = ChangeSignificanceSpec(
        min_relative_delta=float(change_data["min_relative_delta"]),
        min_sentence_count=int(change_data["min_sentence_count"]),
        status_field=str(change_data["status_field"]),
        export_glob=str(change_data["export_glob"]),
    )

    competitive_direction = CompetitiveDirectionSpec(
        sample_size=int(competitive_data["sample_size"]),
        min_accuracy=float(competitive_data["min_accuracy"]),
        reference_file=str(competitive_data["reference_file"]),
    )

    invariants_data = data.get("narrative_invariants", {})
    if "min_pass_rate" not in invariants_data:
        raise ValueError("Narrative KPI config must include narrative_invariants.min_pass_rate.")
    narrative_invariants = NarrativeInvariantSpec(
        min_pass_rate=float(invariants_data["min_pass_rate"])
    )

    return NarrativeKPISpec(
        label_precision=label_precision,
        confidence_breakdown=confidence_breakdown,
        change_significance=change_significance,
        competitive_direction=competitive_direction,
        narrative_invariants=narrative_invariants,
    )


@lru_cache(maxsize=4)
def _load_kpis_cached(resolved_path: str) -> NarrativeKPISpec:
    return _load_kpis_from_disk(Path(resolved_path))


def load_narrative_kpis(path: Path | str | None = None) -> NarrativeKPISpec:
    resolved = _resolve_config_path(path)
    return _load_kpis_cached(str(resolved))


def reset_narrative_kpi_cache() -> None:
    _load_kpis_cached.cache_clear()

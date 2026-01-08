import json
import sqlite3
from pathlib import Path

import pytest

from scripts import check_narrative_kpis as validator
from src.analytics.narrative_kpis import (
    ChangeSignificanceSpec,
    CompetitiveDirectionSpec,
    ConfidenceBreakdownSpec,
    LabelPrecisionSpec,
    NarrativeKPISpec,
    NarrativeInvariantSpec,
)


def make_spec(
    *,
    sample_size: int = 2,
    min_precision: float = 0.5,
    high_risk_min: float = 0.8,
    high_risk_types: tuple[str, ...] = ("safety",),
    min_top_gap: float = 0.3,
    min_sentence_confidence: float = 0.6,
    require_sum_to_one: bool = True,
    min_delta: float = 0.2,
    min_count: int = 10,
    status_field: str = "change_status",
    export_glob: str = "metrics/narratives_change_*.parquet",
    competitive_sample_size: int = 1,
    min_accuracy: float = 0.5,
    reference_file: str = "ref.json",
    sample_export: str = "sample.csv",
) -> NarrativeKPISpec:
    return NarrativeKPISpec(
        label_precision=LabelPrecisionSpec(
            sample_size=sample_size,
            min_precision=min_precision,
            high_risk_min_precision=high_risk_min,
            high_risk_types=high_risk_types,
            sample_export=sample_export,
        ),
        confidence_breakdown=ConfidenceBreakdownSpec(
            min_top_gap=min_top_gap,
            min_sentence_confidence=min_sentence_confidence,
            require_sum_to_one=require_sum_to_one,
            low_confidence_flag="low_confidence",
        ),
        change_significance=ChangeSignificanceSpec(
            min_relative_delta=min_delta,
            min_sentence_count=min_count,
            status_field=status_field,
            export_glob=export_glob,
        ),
        competitive_direction=CompetitiveDirectionSpec(
            sample_size=competitive_sample_size,
            min_accuracy=min_accuracy,
            reference_file=reference_file,
        ),
        narrative_invariants=NarrativeInvariantSpec(min_pass_rate=0.8),
    )


def test_label_precision_validator_enforces_high_risk(tmp_path: Path) -> None:
    spec = make_spec(sample_export="sample.csv")
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text(
        "sentence_id,narrative_type,is_correct\n"
        "s1,safety,false\n"
        "s2,efficacy,true\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit):
        validator._ensure_label_precision_sample(tmp_path, "sample.csv", spec)


def test_change_export_validator_requires_threshold(tmp_path: Path) -> None:
    spec = make_spec()
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    payload = [
        {
            "narrative_type": "safety",
            "change_status": "significant_increase",
            "count": 5,
            "delta_ratio": 0.5,
        }
    ]
    (metrics_dir / "narratives_change_w.parquet").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(SystemExit):
        validator._ensure_change_exports(tmp_path, "metrics/narratives_change_*.parquet", spec)


def test_confidence_validator_flags_small_gap(tmp_path: Path) -> None:
    spec = make_spec()
    db_path = tmp_path / "test.sqlite"
    con = sqlite3.connect(db_path)
    con.execute("CREATE TABLE sentence_events (narrative_type TEXT, narrative_subtype TEXT, narrative_confidence REAL)")
    con.executemany(
        "INSERT INTO sentence_events VALUES (?, ?, ?)",
        [
            ("safety", "safety_reassurance", 0.7),
            ("safety", "safety_signal", 0.55),
            ("efficacy", "positive_signal", 0.95),
        ],
    )
    con.commit()
    con.close()

    with pytest.raises(SystemExit):
        validator._ensure_confidence_metrics(db_path, spec)


def test_competitive_accuracy_validator_passes(tmp_path: Path) -> None:
    spec = make_spec(reference_file="ref.json", competitive_sample_size=1, min_accuracy=1.0)
    reference_path = tmp_path / "ref.json"
    reference_path.write_text(
        json.dumps([{"product_pair": "drug_alpha vs drug_beta", "expected_role": "favored"}]),
        encoding="utf-8",
    )
    metrics_dir = tmp_path / "metrics"
    metrics_dir.mkdir()
    (metrics_dir / "directional_w.parquet").write_text(
        json.dumps([{"product": "drug_alpha", "partner": "drug_beta", "role": "favored"}]),
        encoding="utf-8",
    )

    validator._ensure_competitive_accuracy(metrics_dir, tmp_path, spec)


def test_narrative_invariants_enforce_ratio(tmp_path: Path) -> None:
    spec = make_spec()
    db_path = tmp_path / "inv.sqlite"
    con = sqlite3.connect(db_path)
    con.execute(
        "CREATE TABLE sentence_events (narrative_type TEXT, narrative_invariant_ok INTEGER)"
    )
    con.executemany(
        "INSERT INTO sentence_events VALUES (?, ?)",
        [
            ("comparative", 1),
            ("comparative", 0),
            ("safety", 1),
        ],
    )
    con.commit()
    con.close()

    with pytest.raises(SystemExit):
        validator._ensure_narrative_invariants(db_path, spec)


def test_narrative_invariants_missing_flags(tmp_path: Path) -> None:
    spec = make_spec()
    db_path = tmp_path / "inv_missing.sqlite"
    con = sqlite3.connect(db_path)
    con.execute(
        "CREATE TABLE sentence_events (narrative_type TEXT, narrative_invariant_ok INTEGER)"
    )
    con.executemany(
        "INSERT INTO sentence_events VALUES (?, ?)",
        [
            ("comparative", None),
            ("safety", 1),
        ],
    )
    con.commit()
    con.close()

    with pytest.raises(SystemExit):
        validator._ensure_narrative_invariants(db_path, spec)

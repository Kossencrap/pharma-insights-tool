from pathlib import Path

import pytest

from src.analytics.narrative_kpis import load_narrative_kpis, reset_narrative_kpi_cache


def test_load_narrative_kpis_reads_config(tmp_path: Path) -> None:
    config_path = tmp_path / "narratives_kpis.json"
    config_path.write_text(
        """
        {
          "label_precision": {
            "sample_size": 25,
            "min_precision": 0.9,
            "high_risk_min_precision": 0.96,
            "high_risk_types": ["safety", "access"],
            "sample_export": "artifacts/kpi/sample.csv"
          },
          "confidence_breakdown": {
            "min_top_gap": 0.25,
            "min_sentence_confidence": 0.5,
            "require_sum_to_one": true,
            "low_confidence_flag": "low_confidence"
          },
          "change_significance": {
            "min_relative_delta": 0.15,
            "min_sentence_count": 12,
            "status_field": "change_status",
            "export_glob": "artifacts/aggregates/narratives_change_*.parquet"
          },
          "competitive_direction": {
            "sample_size": 40,
            "min_accuracy": 0.9,
            "reference_file": "tests/data/competitive_kpi.json"
          }
        }
        """,
        encoding="utf-8",
    )

    reset_narrative_kpi_cache()
    spec = load_narrative_kpis(config_path)

    assert spec.label_precision.sample_size == 25
    assert spec.label_precision.high_risk_types == ("access", "safety")
    assert spec.confidence_breakdown.min_top_gap == 0.25
    assert spec.change_significance.min_sentence_count == 12
    assert spec.competitive_direction.reference_file.endswith("competitive_kpi.json")


def test_load_narrative_kpis_requires_all_sections(tmp_path: Path) -> None:
    config_path = tmp_path / "bad_kpis.json"
    config_path.write_text(
        """
        {
          "label_precision": {
            "sample_size": 10,
            "min_precision": 0.8,
            "high_risk_min_precision": 0.9,
            "high_risk_types": [],
            "sample_export": "sample.csv"
          }
        }
        """,
        encoding="utf-8",
    )

    reset_narrative_kpi_cache()
    with pytest.raises(ValueError):
        load_narrative_kpis(config_path)

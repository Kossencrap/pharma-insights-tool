import csv
import sqlite3
import sys
from pathlib import Path

import pytest

from scripts import export_label_precision_sample as exporter
from src.analytics.narrative_kpis import reset_narrative_kpi_cache


def _write_config(path: Path) -> None:
    path.write_text(
        """
        {
          "label_precision": {
            "sample_size": 2,
            "min_precision": 0.8,
            "high_risk_min_precision": 0.9,
            "high_risk_types": ["safety"],
            "sample_export": "sample.csv"
          },
          "confidence_breakdown": {
            "min_top_gap": 0.2,
            "min_sentence_confidence": 0.6,
            "require_sum_to_one": true,
            "low_confidence_flag": "low_confidence"
          },
          "change_significance": {
            "min_relative_delta": 0.2,
            "min_sentence_count": 5,
            "status_field": "change_status",
            "export_glob": "metrics/narratives_change_*.parquet"
          },
          "competitive_direction": {
            "sample_size": 1,
            "min_accuracy": 0.8,
            "reference_file": "ref.json"
          },
          "narrative_invariants": {
            "min_pass_rate": 0.5
          }
        }
        """,
        encoding="utf-8",
    )


def _seed_db(path: Path) -> None:
    con = sqlite3.connect(path)
    con.execute(
        """
        CREATE TABLE sentence_events (
            doc_id TEXT,
            sentence_id TEXT,
            product_a TEXT,
            product_b TEXT,
            narrative_type TEXT,
            narrative_subtype TEXT,
            narrative_confidence REAL,
            section TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE sentences (
            doc_id TEXT,
            sentence_id TEXT,
            text TEXT,
            section TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE documents (
            doc_id TEXT,
            publication_date TEXT
        )
        """
    )

    rows = [
        (
            "doc1",
            "s:1",
            "enalapril",
            "placebo",
            "safety",
            "safety_reassurance",
            0.92,
            "Safety looks good.",
            "Results",
        ),
        (
            "doc2",
            "s:2",
            "enalapril",
            "placebo",
            "efficacy",
            "positive_signal",
            0.85,
            "Conclusion: efficacy is strong.",
            "Conclusion",
        ),
        (
            "doc3",
            "s:3",
            "drug_c",
            "drug_d",
            "safety",
            "safety_signal",
            0.88,
            "Methods: safety caution statement.",
            "methods",
        ),
        (
            "doc4",
            "s:4",
            "drug_e",
            "drug_f",
            "evidence",
            "clinical_trial",
            0.81,
            "Abstract: trial overview text.",
            "abstract",
        ),
    ]
    for doc_id, sentence_id, prod_a, prod_b, ntype, subtype, conf, text, section in rows:
        con.execute(
            "INSERT INTO sentence_events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (doc_id, sentence_id, prod_a, prod_b, ntype, subtype, conf, section),
        )
        con.execute(
            "INSERT INTO sentences VALUES (?, ?, ?, ?)",
            (doc_id, sentence_id, text, section),
        )
        con.execute(
            "INSERT INTO documents VALUES (?, ?)",
            (doc_id, "2025-01-01"),
        )
    con.commit()
    con.close()


def test_export_label_precision_sample_creates_csv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "sample.sqlite"
    _seed_db(db_path)

    config_path = tmp_path / "narratives_kpis.json"
    _write_config(config_path)
    reset_narrative_kpi_cache()

    output_path = tmp_path / "artifacts" / "kpi" / "sample.csv"
    args = [
        "export_label_precision_sample.py",
        "--db",
        str(db_path),
        "--output",
        str(output_path),
        "--sample-size",
        "2",
        "--seed",
        "7",
        "--kpi-config",
        str(config_path),
    ]
    monkeypatch.setattr(sys, "argv", args)

    exporter.main()

    assert output_path.exists()
    with output_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 2
    types = {row["narrative_type"] for row in rows}
    # High-risk safety narrative must always be present.
    assert "safety" in types
    assert all(row.get("section") for row in rows)
    sections = {row["section"] for row in rows}
    assert sections <= {"results", "conclusion"}


def test_export_label_precision_sample_allows_all_sections(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db_path = tmp_path / "sample.sqlite"
    _seed_db(db_path)

    config_path = tmp_path / "narratives_kpis.json"
    _write_config(config_path)
    reset_narrative_kpi_cache()

    output_path = tmp_path / "artifacts" / "kpi" / "sample_all.csv"
    args = [
        "export_label_precision_sample.py",
        "--db",
        str(db_path),
        "--output",
        str(output_path),
        "--sample-size",
        "4",
        "--seed",
        "11",
        "--kpi-config",
        str(config_path),
        "--allowed-sections",
        "all",
    ]
    monkeypatch.setattr(sys, "argv", args)

    exporter.main()

    with output_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert any(row["section"] == "methods" for row in rows)
    assert any(row["section"] == "abstract" for row in rows)

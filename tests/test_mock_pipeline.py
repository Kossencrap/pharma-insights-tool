import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest


def _run_module(repo_root: Path, env: dict[str, str], module: str, *args: str) -> None:
    cmd = [sys.executable, "-m", module, *args]
    result = subprocess.run(cmd, cwd=repo_root, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command {' '.join(cmd)} failed with code {result.returncode}:\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )


@pytest.mark.slow
def test_mock_data_pipeline(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    db_path = tmp_path / "mock.sqlite"
    metrics_dir = tmp_path / "metrics"
    sentiment_jsonl = tmp_path / "mock_sentences.jsonl"

    env = os.environ.copy()
    env["PYTHONPATH"] = str(repo_root)

    _run_module(
        repo_root,
        env,
        "scripts.load_mock_data",
        "--mock-file",
        str(repo_root / "data" / "mock" / "mock_documents.json"),
        "--db",
        str(db_path),
        "--products",
        str(repo_root / "config" / "products.json"),
        "--study-weights",
        str(repo_root / "config" / "study_type_weights.json"),
        "--sentiment-output",
        str(sentiment_jsonl),
    )

    _run_module(
        repo_root,
        env,
        "scripts.label_sentence_events",
        "--db",
        str(db_path),
        "--limit",
        "1000",
        "--only-missing",
    )

    _run_module(
        repo_root,
        env,
        "scripts.label_sentence_sentiment",
        "--input",
        str(sentiment_jsonl),
        "--db",
        str(db_path),
    )

    _run_module(
        repo_root,
        env,
        "scripts.aggregate_metrics",
        "--db",
        str(db_path),
        "--outdir",
        str(metrics_dir),
    )

    _run_module(
        repo_root,
        env,
        "scripts.export_sentiment_metrics",
        "--db",
        str(db_path),
        "--outdir",
        str(metrics_dir),
    )

    conn = sqlite3.connect(db_path)
    with conn:
        doc_count = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        assert doc_count == 5

        indication_count = conn.execute("SELECT COUNT(*) FROM sentence_indications").fetchone()[0]
        assert indication_count > 0

        narratives = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT narrative_type FROM sentence_events WHERE narrative_type IS NOT NULL"
            )
        }
        assert {"comparative", "positioning", "safety"}.issubset(narratives)

        sentiments = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT sentiment_label FROM sentence_events WHERE sentiment_label IS NOT NULL"
            )
        }
        assert sentiments == {"POS", "NEG", "NEU"}

    expected_files = [
        "documents_w.parquet",
        "documents_m.parquet",
        "mentions_w.parquet",
        "mentions_m.parquet",
        "co_mentions_w.parquet",
        "co_mentions_m.parquet",
        "co_mentions_weighted_w.parquet",
        "co_mentions_weighted_m.parquet",
        "narratives_w.parquet",
        "narratives_m.parquet",
        "sentiment_w.parquet",
        "sentiment_m.parquet",
        "validation_metrics.json",
    ]

    for name in expected_files:
        assert (metrics_dir / name).exists(), f"Missing metrics artifact: {name}"

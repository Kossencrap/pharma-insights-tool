import json
import os
from datetime import datetime, timezone
from pathlib import Path

from scripts.export_batch import run_export
from src.storage import init_db


def _seed_db(db_path: Path) -> None:
    con = init_db(db_path)
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json) VALUES (?, ?, ?, ?, ?)",
        ("doc-1", "Doc One", "2024-01-10", 2024, json.dumps({"id": 1})),
    )
    con.execute(
        "INSERT INTO documents (doc_id, title, publication_date, pub_year, raw_json) VALUES (?, ?, ?, ?, ?)",
        ("doc-2", "Doc Two", "2024-01-12", 2024, json.dumps({"id": 2})),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        ("sent-1", "doc-1", "abstract", 0, "Sentence one."),
    )
    con.execute(
        "INSERT INTO sentences (sentence_id, doc_id, section, sent_index, text) VALUES (?, ?, ?, ?, ?)",
        ("sent-2", "doc-2", "abstract", 0, "Sentence two."),
    )
    con.execute(
        "INSERT INTO product_mentions (mention_id, doc_id, sentence_id, product_canonical) VALUES (?, ?, ?, ?)",
        ("m-1", "doc-1", "sent-1", "product-a"),
    )
    con.execute(
        "INSERT INTO product_mentions (mention_id, doc_id, sentence_id, product_canonical) VALUES (?, ?, ?, ?)",
        ("m-2", "doc-2", "sent-2", "product-b"),
    )
    con.execute(
        "INSERT INTO co_mentions (doc_id, product_a, product_b, count) VALUES (?, ?, ?, ?)",
        ("doc-1", "product-a", "product-b", 1),
    )
    con.execute(
        "INSERT INTO co_mentions_sentences (doc_id, sentence_id, product_a, product_b, count) VALUES (?, ?, ?, ?, ?)",
        ("doc-1", "sent-1", "product-a", "product-b", 1),
    )
    con.commit()
    con.close()


def test_export_creates_outputs_and_prunes_old_runs(tmp_path: Path) -> None:
    db_path = tmp_path / "sample.sqlite"
    _seed_db(db_path)

    export_root = tmp_path / "exports"
    raw_ingest_dir = tmp_path / "raw_ingest"
    raw_ingest_dir.mkdir()

    old_run = export_root / "runs" / "run_20230101"
    old_run.mkdir(parents=True)
    (old_run / "manifest.json").write_text("{}", encoding="utf-8")

    old_file = raw_ingest_dir / "stale.json"
    old_file.write_text("old", encoding="utf-8")

    ten_days_ago = datetime.now().timestamp() - 10 * 24 * 3600
    os.utime(old_run, (ten_days_ago, ten_days_ago))
    os.utime(old_file, (ten_days_ago, ten_days_ago))

    run_time = datetime(2024, 1, 15, tzinfo=timezone.utc)
    manifest = run_export(
        db_path,
        export_root,
        freqs=("W",),
        raw_retention_days=1,
        ingest_retention_days=1,
        raw_ingest_dir=raw_ingest_dir,
        now=run_time,
    )

    run_slug = manifest["run_id"]
    aggregates_dir = export_root / "aggregates" / run_slug
    raw_dir = export_root / "runs" / run_slug / "raw"

    documents_csv = aggregates_dir / f"documents_w_{run_slug}.csv"
    mentions_csv = raw_dir / "product_mentions.csv"

    assert documents_csv.exists()
    assert mentions_csv.exists()

    with documents_csv.open("r", encoding="utf-8") as f:
        header = f.readline().strip()
    assert "bucket_start" in header
    assert "count" in header

    with mentions_csv.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    assert len(lines) == 3  # header + two mentions

    manifest_path = export_root / "runs" / run_slug / "manifest.json"
    saved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert any(check["consistent"] for check in saved_manifest["consistency"])

    assert not old_run.exists()
    assert not old_file.exists()

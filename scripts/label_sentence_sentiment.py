"""Label sentence-level sentiment from structured sentence JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.analytics.sentiment import classify_batch
from src.storage import init_db, update_sentence_event_sentiment

DEFAULT_INPUT_DIR = Path("data/processed")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to sentence-level JSONL input (from data/processed).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Path for sentiment-labeled JSONL output. Defaults to <input>_sentiment.jsonl."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        help=(
            "Optional SQLite database to update sentence_events sentiment fields."
        ),
    )
    return parser.parse_args()


def _load_sentence_records(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            records.append(json.loads(line))
    return records


def _write_sentence_records(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def _build_sentiment_updates(records: list[dict]) -> tuple[list[tuple], int]:
    updates: list[tuple] = []
    skipped = 0
    for record in records:
        doc_id = record.get("doc_id")
        sentence_id = record.get("sentence_id")
        product_a = record.get("product_a")
        product_b = record.get("product_b")
        if not all([doc_id, sentence_id, product_a, product_b]):
            skipped += 1
            continue
        updates.append(
            (
                record.get("sentiment_label"),
                record.get("sentiment_score"),
                record.get("sentiment_model"),
                record.get("sentiment_inference_ts"),
                doc_id,
                sentence_id,
                product_a,
                product_b,
            )
        )
    return updates, skipped


def _update_sentiment_in_db(db_path: Path, records: list[dict]) -> None:
    if not db_path.exists():
        raise SystemExit(
            f"SQLite database not found at {db_path}. Run ingestion with --db first."
        )

    conn = init_db(db_path)
    updates, skipped = _build_sentiment_updates(records)
    if not updates:
        print("No sentiment records contained DB keys to update.")
        return

    before = conn.total_changes
    update_sentence_event_sentiment(conn, updates)
    conn.commit()
    updated = conn.total_changes - before

    print(f"Updated {updated} sentence_events rows with sentiment labels.")
    if skipped:
        print(f"Skipped {skipped} records missing DB keys.")


def main() -> None:
    args = parse_args()
    input_path = args.input
    output_path = args.output or input_path.with_name(f"{input_path.stem}_sentiment.jsonl")

    if not input_path.exists():
        raise SystemExit(f"Input JSONL not found at {input_path}")

    records = _load_sentence_records(input_path)
    if not records:
        print("No sentence records found to label.")
        return

    labeled = classify_batch(records)
    _write_sentence_records(output_path, labeled)
    print(f"Wrote {len(labeled)} sentiment-labeled sentences to {output_path}")

    if args.db:
        _update_sentiment_in_db(args.db, labeled)


if __name__ == "__main__":
    main()

"""Export sentence_events rows (with product pairs) to JSONL for sentiment labeling."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_OUTPUT = Path("data/processed/sentence_events_for_sentiment.jsonl")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="SQLite database containing sentence_events (default: data/europepmc.sqlite).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination JSONL path (default: data/processed/sentence_events_for_sentiment.jsonl).",
    )
    return parser.parse_args()


def export_sentence_events(db_path: Path, output_path: Path) -> int:
    if not db_path.exists():
        raise SystemExit(f"SQLite database not found at {db_path}")

    conn = sqlite3.connect(db_path)
    query = (
        "SELECT se.doc_id, se.sentence_id, se.product_a, se.product_b, s.text "
        "FROM sentence_events se "
        "JOIN sentences s ON se.sentence_id = s.sentence_id "
        "WHERE se.doc_id IS NOT NULL"
    )
    rows = conn.execute(query).fetchall()
    conn.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        for doc_id, sentence_id, product_a, product_b, text in rows:
            payload = {
                "doc_id": doc_id,
                "sentence_id": sentence_id,
                "product_a": product_a,
                "product_b": product_b,
                "sentence_text": text,
            }
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")

    return len(rows)


def main() -> None:
    args = parse_args()
    count = export_sentence_events(args.db, args.output)
    print(f"Wrote {count} rows to {args.output}")


if __name__ == "__main__":
    main()

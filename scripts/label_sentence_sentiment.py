"""Label sentence-level sentiment from structured sentence JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics.sentiment import classify_batch
from src.storage import init_db, update_sentence_event_sentiment
from src.utils.identifiers import build_sentence_id

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
            raw = json.loads(line)
            if "sections" in raw and raw.get("doc_id"):
                doc_id = raw["doc_id"]
                for section in raw.get("sections", []):
                    section_name = section.get("name")
                    for sentence in section.get("sentences", []):
                        text = sentence.get("text")
                        if not text:
                            continue
                        sentence_id = build_sentence_id(
                            doc_id,
                            section_name or sentence.get("section") or "body",
                            sentence.get("index", 0),
                        )
                        records.append(
                            {
                                "doc_id": doc_id,
                                "sentence_id": sentence_id,
                                "section": section_name,
                                "sentence_text": text,
                            }
                        )
            else:
                records.append(raw)
    return records


def _write_sentence_records(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, default=str) + "\n")


def _resolve_pairs(conn, doc_id: str, sentence_id: str) -> list[tuple[str, str]]:
    cur = conn.execute(
        """
        SELECT product_a, product_b
        FROM sentence_events
        WHERE doc_id = ? AND sentence_id = ?
        """,
        (doc_id, sentence_id),
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def _build_sentiment_updates(records: list[dict], conn=None) -> tuple[list[tuple], int]:
    updates: list[tuple] = []
    skipped = 0
    for record in records:
        doc_id = record.get("doc_id")
        sentence_id = record.get("sentence_id")
        product_a = record.get("product_a")
        product_b = record.get("product_b")
        pairs: list[tuple[str | None, str | None]] = []
        if all([doc_id, sentence_id, product_a, product_b]):
            pairs = [(product_a, product_b)]
        elif conn and doc_id and sentence_id:
            resolved = _resolve_pairs(conn, doc_id, sentence_id)
            if not resolved:
                skipped += 1
                continue
            pairs = resolved
        else:
            skipped += 1
            continue
        for prod_a, prod_b in pairs:
            updates.append(
                (
                    record.get("sentiment_label"),
                    record.get("sentiment_score"),
                    record.get("sentiment_model"),
                    record.get("sentiment_inference_ts"),
                    doc_id,
                    sentence_id,
                    prod_a,
                    prod_b,
                )
            )
    return updates, skipped


def _update_sentiment_in_db(db_path: Path, records: list[dict]) -> None:
    if not db_path.exists():
        raise SystemExit(
            f"SQLite database not found at {db_path}. Run ingestion with --db first."
        )

    conn = init_db(db_path)
    updates, skipped = _build_sentiment_updates(records, conn)
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

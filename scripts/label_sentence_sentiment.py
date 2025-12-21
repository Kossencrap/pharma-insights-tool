"""Label sentence-level sentiment from structured sentence JSONL."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.analytics.sentiment import classify_batch

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


if __name__ == "__main__":
    main()

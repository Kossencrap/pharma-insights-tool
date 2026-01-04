"""Aggregate sentiment ratios and export them to parquet files."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- AUTO: ensure repo root on sys.path (PowerShell patch) ---
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# --- END AUTO ---

from src.analytics.time_series import sentiment_bucket_counts

DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_OUTDIR = Path("data/processed/metrics")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database populated by the ingestion runner.",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=DEFAULT_OUTDIR,
        help="Directory for parquet outputs (default: data/processed/metrics).",
    )
    parser.add_argument(
        "--freq",
        choices=["W", "M"],
        nargs="+",
        default=["W", "M"],
        help="Time buckets to compute: weekly (W) and/or monthly (M).",
    )
    return parser.parse_args()


def _load_rows(con: sqlite3.Connection) -> List[dict]:
    con.row_factory = sqlite3.Row
    cur = con.execute(
        """
        SELECT d.publication_date,
               se.product_a,
               se.product_b,
               se.sentiment_label
        FROM sentence_events se
        JOIN documents d ON se.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
          AND se.sentiment_label IS NOT NULL
        """
    )
    return [dict(row) for row in cur.fetchall()]


def _aggregate_sentiment(rows: List[dict], freq: str) -> List[dict]:
    return sentiment_bucket_counts(
        rows,
        timestamp_column="publication_date",
        label_column="sentiment_label",
        freq=freq,
        group_columns=["product_a", "product_b"],
    )


def _write_rows(outdir: Path, name: str, frames: Dict[str, List[dict]]) -> None:
    """Write aggregated rows to disk.

    Attempts to use pandas/pyarrow when available; otherwise falls back to JSON
    with a parquet extension so downstream tooling can still locate the export.
    """

    try:
        import pandas as pd  # type: ignore
    except ImportError:
        pd = None  # type: ignore

    outdir.mkdir(parents=True, exist_ok=True)
    for freq, rows in frames.items():
        outfile = outdir / f"{name}_{freq.lower()}.parquet"
        if pd is None:
            with outfile.open("w", encoding="utf-8") as f:
                json.dump(rows, f, default=str)
        else:
            df = pd.DataFrame(rows)
            df.to_parquet(outfile, index=False)
        print(f"Wrote {outfile} ({len(rows)} rows)")


def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise SystemExit(
            f"SQLite database not found at {args.db}. Run ingestion with --db first."
        )

    con = sqlite3.connect(args.db)
    rows = _load_rows(con)
    if not rows:
        print(
            "Warning: no sentiment rows found. Ensure ingestion stored sentiment labels.",
            file=sys.stderr,
        )

    sentiment: Dict[str, List[dict]] = {}
    for freq in args.freq:
        sentiment[freq] = _aggregate_sentiment(rows, freq)

    _write_rows(args.outdir, "sentiment", sentiment)


if __name__ == "__main__":
    main()

"""Aggregate weekly/monthly metrics and export them to parquet files."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Dict, List

from src.analytics.time_series import TimeSeriesConfig, add_change_metrics, bucket_counts

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


def _load_rows(con: sqlite3.Connection, query: str) -> List[dict]:
    con.row_factory = sqlite3.Row
    cur = con.execute(query)
    return [dict(row) for row in cur.fetchall()]


def _aggregate_documents(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        "SELECT publication_date FROM documents WHERE publication_date IS NOT NULL",
    )
    config = TimeSeriesConfig(timestamp_column="publication_date", freq=freq)
    agg = bucket_counts(config, rows)
    return add_change_metrics(agg)


def _aggregate_mentions(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        """
        SELECT d.publication_date, pm.product_canonical
        FROM product_mentions pm
        JOIN documents d ON pm.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
        """,
    )

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["product_canonical"],
    )
    agg = bucket_counts(config, rows)
    return add_change_metrics(agg, group_columns=["product_canonical"])


def _aggregate_co_mentions(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        """
        SELECT d.publication_date, cm.product_a, cm.product_b, cm.count
        FROM co_mentions cm
        JOIN documents d ON cm.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
        """,
    )

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["product_a", "product_b"],
        value_column="count",
        sum_value=True,
    )
    agg = bucket_counts(config, rows)
    return add_change_metrics(agg, group_columns=["product_a", "product_b"])


def _aggregate_weighted_co_mentions(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        """
        SELECT d.publication_date, cm.product_a, cm.product_b, cm.weighted_count
        FROM co_mentions_weighted cm
        JOIN documents d ON cm.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
        """,
    )

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["product_a", "product_b"],
        value_column="weighted_count",
        sum_value=True,
    )
    agg = bucket_counts(config, rows)
    return add_change_metrics(
        agg, group_columns=["product_a", "product_b"], value_column="count"
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

    documents: Dict[str, List[dict]] = {}
    mentions: Dict[str, List[dict]] = {}
    co_mentions: Dict[str, List[dict]] = {}
    weighted_co_mentions: Dict[str, List[dict]] = {}

    for freq in args.freq:
        documents[freq] = _aggregate_documents(con, freq)
        mentions[freq] = _aggregate_mentions(con, freq)
        co_mentions[freq] = _aggregate_co_mentions(con, freq)
        weighted_co_mentions[freq] = _aggregate_weighted_co_mentions(con, freq)

    _write_rows(args.outdir, "documents", documents)
    _write_rows(args.outdir, "mentions", mentions)
    _write_rows(args.outdir, "co_mentions", co_mentions)
    _write_rows(args.outdir, "co_mentions_weighted", weighted_co_mentions)


if __name__ == "__main__":
    main()

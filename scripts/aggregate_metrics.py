"""Aggregate weekly/monthly metrics and export them to parquet files."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from src.analytics.narrative_kpis import load_narrative_kpis
from src.analytics.time_series import (
    TimeSeriesConfig,
    add_change_metrics,
    bucket_counts,
    compute_change_status,
)

ALL_PRODUCTS_LABEL = "(all)"

DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_OUTDIR = Path("data/processed/metrics")
DEFAULT_KPI_CONFIG = Path("config/narratives_kpis.json")


@dataclass
class NarrativeChangeConfig:
    lookback: int = 4
    min_ratio: float = 0.4
    min_count: float = 3.0


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
    parser.add_argument(
        "--change-lookback",
        type=int,
        default=4,
        help="Number of prior buckets to average when computing narrative change status.",
    )
    parser.add_argument(
        "--change-min-ratio",
        type=float,
        default=None,
        help="Override the KPI-configured minimum ratio delta for significant change.",
    )
    parser.add_argument(
        "--change-min-count",
        type=float,
        default=None,
        help="Override the KPI-configured minimum absolute delta for significant change.",
    )
    parser.add_argument(
        "--kpi-config",
        type=Path,
        default=DEFAULT_KPI_CONFIG,
        help="Path to the narrative KPI configuration (default: config/narratives_kpis.json).",
    )
    return parser.parse_args()


def _load_rows(con: sqlite3.Connection, query: str) -> List[dict]:
    con.row_factory = sqlite3.Row
    cur = con.execute(query)
    return [dict(row) for row in cur.fetchall()]


def _aggregate_documents(con: sqlite3.Connection, freq: str) -> List[dict]:
    total_rows = _load_rows(
        con,
        "SELECT publication_date FROM documents WHERE publication_date IS NOT NULL",
    )
    total_config = TimeSeriesConfig(timestamp_column="publication_date", freq=freq)
    total_agg = add_change_metrics(bucket_counts(total_config, total_rows))
    for row in total_agg:
        row["product_canonical"] = ALL_PRODUCTS_LABEL

    product_rows = _load_rows(
        con,
        """
        SELECT DISTINCT pm.doc_id, pm.product_canonical, d.publication_date
        FROM product_mentions pm
        JOIN documents d ON pm.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
          AND pm.product_canonical IS NOT NULL
        """
    )
    product_config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["product_canonical"],
    )
    product_agg = add_change_metrics(
        bucket_counts(product_config, product_rows),
        group_columns=["product_canonical"],
    )
    return total_agg + product_agg


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


def _validation_metrics(con: sqlite3.Connection) -> dict:
    con.row_factory = sqlite3.Row
    row = con.execute(
        """
        SELECT
            COUNT(*) AS total_documents,
            COUNT(DISTINCT pmid) AS distinct_pmid,
            COUNT(DISTINCT pmcid) AS distinct_pmcid,
            COUNT(DISTINCT doi) AS distinct_doi,
            COUNT(DISTINCT COALESCE(NULLIF(pmid, ''), NULLIF(doi, ''), NULLIF(pmcid, ''), doc_id)) AS canonical_ids
        FROM documents
        """
    ).fetchone()

    total = row["total_documents"] or 0
    canonical_ids = row["canonical_ids"] or 0
    dedup_ratio = canonical_ids / total if total else 1.0

    return {
        "total_documents": total,
        "distinct_pmid": row["distinct_pmid"] or 0,
        "distinct_pmcid": row["distinct_pmcid"] or 0,
        "distinct_doi": row["distinct_doi"] or 0,
        "dedup_ratio": dedup_ratio,
    }


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


def _aggregate_directional_events(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        """
        SELECT
            d.publication_date,
            se.product_a,
            se.product_b,
            se.direction_type,
            se.product_a_role,
            se.product_b_role
        FROM sentence_events se
        JOIN documents d ON se.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
          AND se.direction_type IS NOT NULL
        """,
    )

    expanded: List[dict] = []
    for row in rows:
        for product_key, role_key, partner_key in (
            ("product_a", "product_a_role", "product_b"),
            ("product_b", "product_b_role", "product_a"),
        ):
            role_value = row.get(role_key)
            if not role_value:
                continue
            expanded.append(
                {
                    "publication_date": row["publication_date"],
                    "product": row[product_key],
                    "partner": row[partner_key],
                    "direction_type": row["direction_type"],
                    "role": role_value,
                }
            )

    if not expanded:
        return []

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["product", "partner", "direction_type", "role"],
    )
    agg = bucket_counts(config, expanded)
    return add_change_metrics(
        agg, group_columns=["product", "partner", "direction_type", "role"]
    )


def _aggregate_narratives(
    con: sqlite3.Connection, freq: str, change_config: NarrativeChangeConfig
) -> Tuple[List[dict], List[dict]]:
    rows = _load_rows(
        con,
        """
        SELECT d.publication_date,
               se.narrative_type,
               se.narrative_subtype,
               se.narrative_confidence
        FROM sentence_events se
        JOIN documents d ON se.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
          AND se.narrative_type IS NOT NULL
        """,
    )

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["narrative_type", "narrative_subtype"],
    )
    agg = bucket_counts(config, rows)
    change_rows = compute_change_status(
        agg,
        group_columns=["narrative_type", "narrative_subtype"],
        lookback=change_config.lookback,
        min_ratio=change_config.min_ratio,
        min_count=change_config.min_count,
    )
    enriched = add_change_metrics(
        agg, group_columns=["narrative_type", "narrative_subtype"]
    )
    return enriched, change_rows


def _aggregate_weighted_narratives(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        """
        SELECT d.publication_date,
               se.narrative_type,
               se.narrative_subtype,
               COALESCE(dw.combined_weight, dw.study_type_weight, 1.0) AS weight
        FROM sentence_events se
        JOIN documents d ON se.doc_id = d.doc_id
        LEFT JOIN document_weights dw ON se.doc_id = dw.doc_id
        WHERE d.publication_date IS NOT NULL
          AND se.narrative_type IS NOT NULL
        """,
    )
    for row in rows:
        row["weight"] = row.get("weight") or 1.0

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["narrative_type", "narrative_subtype"],
        value_column="weight",
        sum_value=True,
    )
    agg = bucket_counts(config, rows)
    for row in agg:
        row["weighted_count"] = row.pop("count")
    return add_change_metrics(
        agg,
        group_columns=["narrative_type", "narrative_subtype"],
        value_column="weighted_count",
    )


def _aggregate_narrative_dimensions(con: sqlite3.Connection, freq: str) -> List[dict]:
    rows = _load_rows(
        con,
        """
        SELECT d.publication_date,
               se.narrative_type,
               se.narrative_subtype,
               se.claim_strength,
               se.risk_posture
        FROM sentence_events se
        JOIN documents d ON se.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
          AND se.narrative_type IS NOT NULL
        """,
    )

    for row in rows:
        row["claim_strength"] = row.get("claim_strength") or "unlabeled"
        row["risk_posture"] = row.get("risk_posture") or "unlabeled"

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["narrative_type", "narrative_subtype", "claim_strength", "risk_posture"],
    )
    agg = bucket_counts(config, rows)
    return add_change_metrics(
        agg,
        group_columns=["narrative_type", "narrative_subtype", "claim_strength", "risk_posture"],
    )


def _aggregate_risk_signals(con: sqlite3.Connection, freq: str) -> List[dict]:
    base_rows = _load_rows(
        con,
        """
        SELECT d.publication_date,
               se.product_a,
               se.product_b,
               se.narrative_type,
               se.risk_posture
        FROM sentence_events se
        JOIN documents d ON se.doc_id = d.doc_id
        WHERE d.publication_date IS NOT NULL
          AND se.product_a IS NOT NULL
          AND se.product_b IS NOT NULL
        """,
    )
    if not base_rows:
        return []

    config = TimeSeriesConfig(
        timestamp_column="publication_date",
        freq=freq,
        group_columns=["product_a", "product_b"],
    )
    total = bucket_counts(config, base_rows)
    safety = bucket_counts(
        config, [row for row in base_rows if row.get("narrative_type") == "safety"]
    )
    concern = bucket_counts(
        config, [row for row in base_rows if row.get("narrative_type") == "concern"]
    )
    reassurance = bucket_counts(
        config,
        [
            row
            for row in base_rows
            if row.get("narrative_type") == "safety"
            and row.get("risk_posture") == "reassurance"
        ],
    )

    def _index(rows: List[dict]) -> Dict[tuple, float]:
        return {
            (row["product_a"], row["product_b"], row["bucket_start"]): row["count"]
            for row in rows
        }

    total_index = _index(total)
    safety_index = _index(safety)
    concern_index = _index(concern)
    reassurance_index = _index(reassurance)

    results: List[dict] = []
    for key, total_count in total_index.items():
        product_a, product_b, bucket = key
        safety_count = safety_index.get(key, 0.0)
        concern_count = concern_index.get(key, 0.0)
        reassurance_count = reassurance_index.get(key, 0.0)
        entry = {
            "product_a": product_a,
            "product_b": product_b,
            "bucket_start": bucket,
            "total_count": total_count,
            "safety_count": safety_count,
            "concern_count": concern_count,
            "reassurance_count": reassurance_count,
            "safety_ratio": (safety_count / total_count) if total_count else None,
            "concern_ratio": (concern_count / total_count) if total_count else None,
        }
        results.append(entry)

    results.sort(key=lambda r: (r["product_a"], r["product_b"], r["bucket_start"]))
    return results


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
    narratives: Dict[str, List[dict]] = {}
    narrative_changes: Dict[str, List[dict]] = {}
    weighted_narratives: Dict[str, List[dict]] = {}
    narrative_dimensions: Dict[str, List[dict]] = {}
    directional: Dict[str, List[dict]] = {}
    risk_signals: Dict[str, List[dict]] = {}
    validation: dict = {}

    kpi_spec = load_narrative_kpis(args.kpi_config)
    change_min_ratio = (
        args.change_min_ratio
        if args.change_min_ratio is not None
        else kpi_spec.change_significance.min_relative_delta
    )
    change_min_count = (
        args.change_min_count
        if args.change_min_count is not None
        else kpi_spec.change_significance.min_sentence_count
    )

    change_config = NarrativeChangeConfig(
        lookback=args.change_lookback,
        min_ratio=change_min_ratio,
        min_count=change_min_count,
    )

    for freq in args.freq:
        documents[freq] = _aggregate_documents(con, freq)
        mentions[freq] = _aggregate_mentions(con, freq)
        co_mentions[freq] = _aggregate_co_mentions(con, freq)
        weighted_co_mentions[freq] = _aggregate_weighted_co_mentions(con, freq)
        narratives[freq], narrative_changes[freq] = _aggregate_narratives(con, freq, change_config)
        weighted_narratives[freq] = _aggregate_weighted_narratives(con, freq)
        narrative_dimensions[freq] = _aggregate_narrative_dimensions(con, freq)
        directional[freq] = _aggregate_directional_events(con, freq)
        risk_signals[freq] = _aggregate_risk_signals(con, freq)
    validation = _validation_metrics(con)

    _write_rows(args.outdir, "documents", documents)
    _write_rows(args.outdir, "mentions", mentions)
    _write_rows(args.outdir, "co_mentions", co_mentions)
    _write_rows(args.outdir, "co_mentions_weighted", weighted_co_mentions)
    _write_rows(args.outdir, "narratives", narratives)
    _write_rows(args.outdir, "narratives_change", narrative_changes)
    _write_rows(args.outdir, "narratives_weighted", weighted_narratives)
    _write_rows(args.outdir, "narratives_dimensions", narrative_dimensions)
    _write_rows(args.outdir, "directional", directional)
    _write_rows(args.outdir, "risk_signals", risk_signals)
    with (args.outdir / "validation_metrics.json").open("w", encoding="utf-8") as f:
        json.dump(validation, f, indent=2)
    print(f"Wrote validation metrics to {args.outdir / 'validation_metrics.json'}")


if __name__ == "__main__":
    main()

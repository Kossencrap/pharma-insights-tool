"""Scheduler-friendly batch exporter for weekly aggregates and raw snapshots.

Exports SQLite-backed ingestion results to ``data/exports/`` as both CSV and
Parquet (when pandas/pyarrow are available). Raw snapshots are trimmed via a
retention window while aggregated metrics are retained for longer-term trend
analysis. Evidence exports include sentiment fields when available.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # Optional dependency
    import pandas as pd  # type: ignore
except ImportError:  # pragma: no cover - handled in code paths
    pd = None  # type: ignore

from scripts import aggregate_metrics as aggregator
from src.analytics import fetch_sentence_evidence, serialize_sentence_evidence
from src.analytics.weights import load_study_type_weights

DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_EXPORT_ROOT = Path("data/exports")
DEFAULT_RAW_RETENTION_DAYS = 30
DEFAULT_INGEST_RETENTION_DAYS = 14
DEFAULT_EVIDENCE_LIMIT = 500
DEFAULT_STUDY_WEIGHT_CONFIG = ROOT / "config" / "study_type_weights.json"

RAW_TABLES = [
    "documents",
    "document_weights",
    "sentences",
    "product_mentions",
    "co_mentions",
    "co_mentions_sentences",
    "sentence_events",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite store")
    parser.add_argument(
        "--export-root",
        type=Path,
        default=DEFAULT_EXPORT_ROOT,
        help="Root directory for all exports (default: data/exports)",
    )
    parser.add_argument(
        "--freq",
        choices=["W", "M"],
        nargs="+",
        default=["W"],
        help="Time bucket frequencies for aggregates (default: weekly only)",
    )
    parser.add_argument(
        "--raw-retention-days",
        type=int,
        default=DEFAULT_RAW_RETENTION_DAYS,
        help="Retention window for run-level raw exports",
    )
    parser.add_argument(
        "--ingest-retention-days",
        type=int,
        default=DEFAULT_INGEST_RETENTION_DAYS,
        help="Retention window for data/raw ingestion artifacts",
    )
    parser.add_argument(
        "--evidence-limit",
        type=int,
        default=DEFAULT_EVIDENCE_LIMIT,
        help="Maximum number of sentence-level evidence rows to export (default: 500)",
    )
    parser.add_argument(
        "--study-weight-config",
        type=Path,
        default=DEFAULT_STUDY_WEIGHT_CONFIG,
        help="Path to study-type weight configuration for confidence breakdowns.",
    )
    parser.add_argument(
        "--run-slug",
        help="Optional run identifier to make manifests deterministic across runs.",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        help="Optional explicit manifest path (defaults to <export-root>/runs/<slug>/manifest.json).",
    )
    return parser.parse_args()


def _rows_from_cursor(cursor: sqlite3.Cursor) -> List[dict]:
    columns: Sequence[str] = [col[0] for col in cursor.description or []]
    rows = []
    for raw in cursor.fetchall():
        row = {col: raw[idx] for idx, col in enumerate(columns)}
        rows.append(row)
    return rows


def _write_csv(rows: Iterable[dict], outfile: Path) -> None:
    rows_list = list(rows)
    outfile.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: Sequence[str] = rows_list[0].keys() if rows_list else []
    with outfile.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows_list:
            serialized = {k: str(v) if isinstance(v, datetime) else v for k, v in row.items()}
            writer.writerow(serialized)


def _write_parquet(rows: List[dict], outfile: Path) -> None:
    outfile.parent.mkdir(parents=True, exist_ok=True)
    if pd is None:
        with outfile.open("w", encoding="utf-8") as f:
            json.dump(rows, f, default=str)
        print(f"Pandas not available; wrote JSON payload to {outfile}")
        return

    df = pd.DataFrame(rows)
    df.to_parquet(outfile, index=False)


def _write_jsonl(rows: List[dict], outfile: Path) -> None:
    outfile.parent.mkdir(parents=True, exist_ok=True)
    with outfile.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")


def _export_query(con: sqlite3.Connection, query: str, base_name: str, outdir: Path) -> dict:
    cur = con.execute(query)
    rows = _rows_from_cursor(cur)
    csv_path = outdir / f"{base_name}.csv"
    parquet_path = outdir / f"{base_name}.parquet"
    _write_csv(rows, csv_path)
    _write_parquet(rows, parquet_path)
    print(f"Exported {len(rows)} rows to {csv_path} and {parquet_path}")
    return {"csv": str(csv_path), "parquet": str(parquet_path), "rows": len(rows)}


def _aggregate_frames(
    con: sqlite3.Connection,
    freqs: Sequence[str],
    *,
    change_config: aggregator.NarrativeChangeConfig | None = None,
) -> Dict[str, Dict[str, List[dict]]]:
    aggregates: Dict[str, Dict[str, List[dict]]] = {
        "documents": {},
        "mentions": {},
        "co_mentions": {},
        "co_mentions_weighted": {},
        "narratives": {},
        "narratives_change": {},
        "directional": {},
    }
    config = change_config or aggregator.NarrativeChangeConfig()
    for freq in freqs:
        aggregates["documents"][freq] = aggregator._aggregate_documents(con, freq)
        aggregates["mentions"][freq] = aggregator._aggregate_mentions(con, freq)
        aggregates["co_mentions"][freq] = aggregator._aggregate_co_mentions(con, freq)
        aggregates["co_mentions_weighted"][freq] = aggregator._aggregate_weighted_co_mentions(
            con, freq
        )
        narrative_rows, change_rows = aggregator._aggregate_narratives(con, freq, config)
        aggregates["narratives"][freq] = narrative_rows
        aggregates["narratives_change"][freq] = change_rows
        aggregates["directional"][freq] = aggregator._aggregate_directional_events(con, freq)
    return aggregates


def _export_aggregates(
    aggregates: Dict[str, Dict[str, List[dict]]], outdir: Path, run_slug: str
) -> Dict[str, Dict[str, dict]]:
    summary: Dict[str, Dict[str, dict]] = {}
    for dataset, frames in aggregates.items():
        summary[dataset] = {}
        for freq, rows in frames.items():
            base_name = f"{dataset}_{freq.lower()}_{run_slug}"
            csv_path = outdir / f"{base_name}.csv"
            parquet_path = outdir / f"{base_name}.parquet"
            _write_csv(rows, csv_path)
            _write_parquet(rows, parquet_path)
            summary[dataset][freq] = {
                "csv": str(csv_path),
                "parquet": str(parquet_path),
                "rows": len(rows),
            }
            print(f"Aggregated {dataset} ({freq}) -> {csv_path} ({len(rows)} rows)")
    return summary


def _export_evidence(
    con: sqlite3.Connection,
    outdir: Path,
    *,
    run_slug: str,
    limit: int,
    study_weight_lookup: Dict[str, float] | None,
) -> dict:
    outdir.mkdir(parents=True, exist_ok=True)
    evidence_rows = fetch_sentence_evidence(con, limit=limit)
    serialized = serialize_sentence_evidence(
        evidence_rows,
        study_weight_lookup=study_weight_lookup,
        include_confidence=True,
    )

    base_name = f"sentence_evidence_{run_slug}"
    csv_path = outdir / f"{base_name}.csv"
    parquet_path = outdir / f"{base_name}.parquet"
    jsonl_path = outdir / f"{base_name}.jsonl"

    _write_csv(serialized, csv_path)
    _write_parquet(serialized, parquet_path)
    _write_jsonl(serialized, jsonl_path)

    print(f"Exported {len(serialized)} evidence rows -> {csv_path}")
    return {
        "csv": str(csv_path),
        "parquet": str(parquet_path),
        "jsonl": str(jsonl_path),
        "rows": len(serialized),
    }


def _table_count(con: sqlite3.Connection, table: str, where: str | None = None) -> int:
    query = f"SELECT COUNT(*) FROM {table}"
    if where:
        query += f" WHERE {where}"
    cur = con.execute(query)
    return int(cur.fetchone()[0])


def _validate_consistency(con: sqlite3.Connection, aggregates: Dict[str, Dict[str, List[dict]]]) -> List[dict]:
    checks = []
    doc_total = _table_count(con, "documents", "publication_date IS NOT NULL")
    mention_total = _table_count(con, "product_mentions")
    sentence_total = _table_count(con, "sentences")

    for freq, rows in aggregates.get("documents", {}).items():
        agg_total = int(
            sum(
                row.get("count", 0) or 0
                for row in rows
                if not row.get("product_canonical")
                or str(row.get("product_canonical")) == aggregator.ALL_PRODUCTS_LABEL
            )
        )
        checks.append(
            {
                "metric": f"documents_{freq}",
                "table_count": doc_total,
                "aggregated_count": agg_total,
                "consistent": agg_total == doc_total,
            }
        )

    for freq, rows in aggregates.get("mentions", {}).items():
        agg_total = int(sum(row.get("count", 0) or 0 for row in rows))
        checks.append(
            {
                "metric": f"mentions_{freq}",
                "table_count": mention_total,
                "aggregated_count": agg_total,
                "consistent": agg_total == mention_total,
            }
        )

    checks.append(
        {
            "metric": "sentences",
            "table_count": sentence_total,
            "aggregated_count": None,
            "consistent": True,
        }
    )
    return checks


def _prune_retention(target: Path, retention_days: int) -> List[Path]:
    if retention_days <= 0 or not target.exists():
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    removed: List[Path] = []
    for child in target.iterdir():
        mtime = datetime.fromtimestamp(child.stat().st_mtime, tz=timezone.utc)
        if mtime < cutoff:
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
            removed.append(child)
            print(f"Pruned old artifact: {child}")
    return removed


def run_export(
    db_path: Path,
    export_root: Path,
    *,
    freqs: Sequence[str] = ("W",),
    raw_retention_days: int = DEFAULT_RAW_RETENTION_DAYS,
    ingest_retention_days: int = DEFAULT_INGEST_RETENTION_DAYS,
    raw_ingest_dir: Path = Path("data/raw"),
    now: datetime | None = None,
    run_slug: str | None = None,
    manifest_path: Path | None = None,
    evidence_limit: int = DEFAULT_EVIDENCE_LIMIT,
    study_weight_lookup: Dict[str, float] | None = None,
) -> dict:
    run_ts = now or datetime.now(timezone.utc)
    resolved_slug = run_slug or run_ts.strftime("run_%Y%m%d")

    if not db_path.exists():
        raise SystemExit(f"SQLite database not found at {db_path}. Run ingestion first.")

    run_dir = export_root / "runs" / resolved_slug
    aggregates_dir = export_root / "aggregates" / resolved_slug
    evidence_dir = run_dir / "evidence"
    raw_export_dir = run_dir / "raw"

    run_dir.mkdir(parents=True, exist_ok=True)
    aggregates_dir.mkdir(parents=True, exist_ok=True)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    aggregates = _aggregate_frames(con, freqs)
    aggregate_exports = _export_aggregates(aggregates, aggregates_dir, resolved_slug)
    evidence_exports = _export_evidence(
        con,
        evidence_dir,
        run_slug=resolved_slug,
        limit=evidence_limit,
        study_weight_lookup=study_weight_lookup,
    )

    raw_exports: Dict[str, dict] = {}
    for table in RAW_TABLES:
        raw_exports[table] = _export_query(
            con, f"SELECT * FROM {table}", base_name=table, outdir=raw_export_dir
        )

    consistency = _validate_consistency(con, aggregates)
    manifest = {
        "run_id": resolved_slug,
        "run_at": run_ts.isoformat(),
        "db_path": str(db_path),
        "export_root": str(export_root),
        "aggregate_exports": aggregate_exports,
        "evidence_export": evidence_exports,
        "raw_exports": raw_exports,
        "consistency": consistency,
    }

    manifest_target = manifest_path or (run_dir / "manifest.json")
    with manifest_target.open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"Wrote manifest to {manifest_target}")
    manifest["manifest_path"] = str(manifest_target)

    _prune_retention(run_dir.parent, raw_retention_days)
    _prune_retention(raw_ingest_dir, ingest_retention_days)

    return manifest


def main() -> None:
    args = parse_args()
    study_weight_lookup: Dict[str, float] | None = None
    if args.study_weight_config and args.study_weight_config.exists():
        study_weight_lookup = load_study_type_weights(args.study_weight_config)
    run_export(
        db_path=args.db,
        export_root=args.export_root,
        freqs=args.freq,
        raw_retention_days=args.raw_retention_days,
        ingest_retention_days=args.ingest_retention_days,
        run_slug=args.run_slug,
        manifest_path=args.manifest_path,
        evidence_limit=args.evidence_limit,
        study_weight_lookup=study_weight_lookup,
    )


if __name__ == "__main__":
    main()

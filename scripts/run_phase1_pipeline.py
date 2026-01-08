"""One-command orchestrator for the Phase 1 pipeline.

This script stitches together ingestion, labeling, metrics aggregation,
exports, and dashboards artifacts to satisfy the "one blessed path" acceptance
criterion for Phase 1. It shells out to the existing CLI entry points so each
stage remains independently testable.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List

from src.analytics import load_product_config
from scripts.export_batch import run_export

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_PRODUCTS = ROOT / "config" / "products.json"
DEFAULT_STUDY_WEIGHTS = ROOT / "config" / "study_type_weights.json"
DEFAULT_ARTIFACTS = Path("data/artifacts/phase1")
DEFAULT_MAX_RECORDS = 250
DEFAULT_EVENT_LIMIT = 7500
DEFAULT_EVIDENCE_LIMIT = 300


def _slug(text: str) -> str:
    return "_".join(text.lower().split())


def _run_command(cmd: Iterable[str], description: str) -> None:
    printable = " ".join(str(part) for part in cmd)
    print(f"\n[phase1] {description}\n  $ {printable}")
    subprocess.run(list(cmd), check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--products",
        type=Path,
        default=DEFAULT_PRODUCTS,
        help="Path to product dictionary JSON for ingestion and mention extraction.",
    )
    parser.add_argument(
        "--product",
        "-p",
        action="append",
        help="Optional subset of products to ingest (defaults to all canonical entries).",
    )
    parser.add_argument("--from-date", type=str, help="Lower bound publication date (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, help="Upper bound publication date (YYYY-MM-DD)")
    parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help=f"Cap ingestion for reproducibility (default: {DEFAULT_MAX_RECORDS})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="SQLite database path for persistence (default: data/europepmc.sqlite)",
    )
    parser.add_argument(
        "--output-prefix",
        help="Optional filename prefix for structured artifacts (defaults to first product).",
    )
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=DEFAULT_ARTIFACTS,
        help="Root directory for metrics, exports, and evidence artifacts.",
    )
    parser.add_argument(
        "--include-reviews", dest="include_reviews", action="store_true", default=True
    )
    parser.add_argument(
        "--exclude-reviews", dest="include_reviews", action="store_false"
    )
    parser.add_argument(
        "--include-trials", dest="include_trials", action="store_true", default=True
    )
    parser.add_argument(
        "--exclude-trials", dest="include_trials", action="store_false"
    )
    parser.add_argument(
        "--study-weight-config",
        type=Path,
        default=DEFAULT_STUDY_WEIGHTS,
        help="Path to study-type weight configuration (default: config/study_type_weights.json)",
    )
    parser.add_argument(
        "--event-limit",
        type=int,
        default=DEFAULT_EVENT_LIMIT,
        help="Maximum number of sentence pairs to label with context (default: 7500)",
    )
    parser.add_argument(
        "--evidence-limit",
        type=int,
        default=DEFAULT_EVIDENCE_LIMIT,
        help="Evidence rows to include in batch exports (default: 300)",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=DEFAULT_ARTIFACTS / "phase1_run_manifest.json",
        help="Where to write the Phase 1 manifest (default: data/artifacts/phase1/phase1_run_manifest.json)",
    )
    parser.add_argument(
        "--run-slug",
        help="Optional run identifier to pass through to exports for determinism.",
    )
    parser.add_argument(
        "--max-sentences-per-doc",
        type=int,
        default=400,
        help="Soft cap on sentences stored per document (default: 400)",
    )
    parser.add_argument(
        "--max-co-mentions-per-sentence",
        type=int,
        default=50,
        help="Soft cap on co-mention pairs per sentence (default: 50)",
    )
    parser.add_argument(
        "--db-size-warn-mb",
        type=int,
        default=250,
        help="Warn when the SQLite DB exceeds this size after ingestion (default: 250MB)",
    )
    parser.add_argument(
        "--no-query-aliases",
        dest="expand_query_aliases",
        action="store_false",
        help="Disable expanding ingestion queries to include product aliases from config.",
    )
    parser.set_defaults(expand_query_aliases=True)
    parser.add_argument(
        "--require-all-products",
        action="store_true",
        help="Require every selected product (and its aliases) to appear in the ingestion query (AND).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    product_config = load_product_config(args.products)
    product_names: List[str] = args.product or list(product_config.keys())
    if not product_names:
        raise SystemExit("No products provided; update config/products.json or pass --product.")

    artifacts_dir = args.artifacts_dir
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    prefix = args.output_prefix or _slug(product_names[0])
    structured_path = Path("data/processed") / f"{prefix}_structured.jsonl"
    sentiment_events_path = Path("data/processed") / f"{prefix}_sentence_events_for_sentiment.jsonl"

    ingestion_cmd = [
        sys.executable,
        "-m",
        "scripts.ingest_europe_pmc",
    ]
    for name in product_names:
        ingestion_cmd.extend(["-p", name])
    ingestion_cmd.extend(
        [
            "--from-date",
            args.from_date,
        ]
        if args.from_date
        else []
    )
    if args.to_date:
        ingestion_cmd.extend(["--to-date", args.to_date])
    ingestion_cmd.extend(
        [
            "--max-records",
            str(args.max_records),
            "--db",
            str(args.db),
            "--product-config",
            str(args.products),
            "--study-weight-config",
            str(args.study_weight_config),
            "--output-prefix",
            prefix,
            "--max-sentences-per-doc",
            str(args.max_sentences_per_doc),
            "--max-co-mentions-per-sentence",
            str(args.max_co_mentions_per_sentence),
            "--db-size-warn-mb",
            str(args.db_size_warn_mb),
        ]
    )
    if args.expand_query_aliases:
        ingestion_cmd.append("--expand-query-aliases")
    if args.require_all_products:
        ingestion_cmd.append("--require-all-products")
    if not args.include_reviews:
        ingestion_cmd.append("--exclude-reviews")
    if not args.include_trials:
        ingestion_cmd.append("--exclude-trials")

    _run_command(ingestion_cmd, "1/5 Ingest Europe PMC and structure documents")

    label_events_cmd = [
        sys.executable,
        "-m",
        "scripts.label_sentence_events",
        "--db",
        str(args.db),
        "--limit",
        str(args.event_limit),
        "--only-missing",
    ]
    if args.from_date:
        label_events_cmd.extend(["--since-publication", args.from_date])
    _run_command(label_events_cmd, "2/5 Label co-mention sentence contexts")

    export_sentiment_input_cmd = [
        sys.executable,
        "-m",
        "scripts.export_sentence_events_jsonl",
        "--db",
        str(args.db),
        "--output",
        str(sentiment_events_path),
    ]
    _run_command(export_sentiment_input_cmd, "2b/5 Export sentence events for sentiment labeling")

    sentiment_cmd = [
        sys.executable,
        "-m",
        "scripts.label_sentence_sentiment",
        "--input",
        str(sentiment_events_path),
        "--db",
        str(args.db),
    ]
    _run_command(sentiment_cmd, "3/5 Apply heuristic sentiment to sentences")

    metrics_outdir = artifacts_dir / "metrics"
    metrics_outdir.mkdir(parents=True, exist_ok=True)

    aggregate_cmd = [
        sys.executable,
        "-m",
        "scripts.aggregate_metrics",
        "--db",
        str(args.db),
        "--outdir",
        str(metrics_outdir),
    ]
    _run_command(aggregate_cmd, "4/5 Aggregate publication and mention metrics")

    sentiment_metrics_cmd = [
        sys.executable,
        "-m",
        "scripts.export_sentiment_metrics",
        "--db",
        str(args.db),
        "--outdir",
        str(metrics_outdir),
    ]
    _run_command(sentiment_metrics_cmd, "4b/5 Export sentiment ratios for dashboards")

    print("\n[phase1] 5/5 Export evidence, aggregates, and manifest")
    export_manifest = run_export(
        db_path=args.db,
        export_root=artifacts_dir / "exports",
        freqs=["W", "M"],
        evidence_limit=args.evidence_limit,
        run_slug=args.run_slug,
    )

    phase_manifest = {
        "phase": "phase1",
        "products": product_names,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "db": str(args.db),
        "artifacts_dir": str(artifacts_dir),
        "structured_path": str(structured_path),
        "sentiment_events_path": str(sentiment_events_path),
        "metrics_dir": str(metrics_outdir),
        "export_manifest": export_manifest,
        "export_manifest_path": export_manifest.get("manifest_path"),
        "limits": {
            "max_records": args.max_records,
            "event_limit": args.event_limit,
            "evidence_limit": args.evidence_limit,
            "max_sentences_per_doc": args.max_sentences_per_doc,
            "max_co_mentions_per_sentence": args.max_co_mentions_per_sentence,
            "db_size_warn_mb": args.db_size_warn_mb,
        },
    }

    args.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    args.manifest_path.write_text(
        __import__("json").dumps(phase_manifest, indent=2), encoding="utf-8"
    )
    print(f"[phase1] Wrote pipeline manifest to {args.manifest_path}")
    print("\n[phase1] Pipeline complete. Review artifacts under", artifacts_dir)


if __name__ == "__main__":
    main()

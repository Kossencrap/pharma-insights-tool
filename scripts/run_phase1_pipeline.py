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

    ingestion_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "ingest_europe_pmc.py"),
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
        ]
    )
    if not args.include_reviews:
        ingestion_cmd.append("--exclude-reviews")
    if not args.include_trials:
        ingestion_cmd.append("--exclude-trials")

    _run_command(ingestion_cmd, "1/5 Ingest Europe PMC and structure documents")

    label_events_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "label_sentence_events.py"),
        "--db",
        str(args.db),
        "--limit",
        str(args.event_limit),
        "--only-missing",
    ]
    if args.from_date:
        label_events_cmd.extend(["--since-publication", args.from_date])
    _run_command(label_events_cmd, "2/5 Label co-mention sentence contexts")

    sentiment_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "label_sentence_sentiment.py"),
        "--input",
        str(structured_path),
        "--db",
        str(args.db),
    ]
    _run_command(sentiment_cmd, "3/5 Apply heuristic sentiment to sentences")

    metrics_outdir = artifacts_dir / "metrics"
    metrics_outdir.mkdir(parents=True, exist_ok=True)

    aggregate_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "aggregate_metrics.py"),
        "--db",
        str(args.db),
        "--outdir",
        str(metrics_outdir),
    ]
    _run_command(aggregate_cmd, "4/5 Aggregate publication and mention metrics")

    sentiment_metrics_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "export_sentiment_metrics.py"),
        "--db",
        str(args.db),
        "--outdir",
        str(metrics_outdir),
    ]
    _run_command(sentiment_metrics_cmd, "4b/5 Export sentiment ratios for dashboards")

    batch_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "export_batch.py"),
        "--db",
        str(args.db),
        "--export-root",
        str(artifacts_dir / "exports"),
        "--evidence-limit",
        str(args.evidence_limit),
        "--study-weight-config",
        str(args.study_weight_config),
    ]
    _run_command(batch_cmd, "5/5 Export evidence, aggregates, and manifest")

    print("\n[phase1] Pipeline complete. Review artifacts under", artifacts_dir)


if __name__ == "__main__":
    main()

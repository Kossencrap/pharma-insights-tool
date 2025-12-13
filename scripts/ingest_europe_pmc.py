"""CLI ingestion runner for Europe PMC -> structured documents."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.analytics import mean_sentence_length, sentence_counts_by_section
from src.ingestion.europe_pmc_client import EuropePMCClient, EuropePMCQuery
from src.structuring.sentence_splitter import SentenceSplitter

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def _slug(text: str) -> str:
    return "_".join(text.lower().split())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--product",
        "-p",
        action="append",
        required=True,
        help="Product names/aliases to search (repeatable)",
    )
    parser.add_argument(
        "--from-date",
        type=date.fromisoformat,
        help="Lower bound publication date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date",
        type=date.fromisoformat,
        help="Upper bound publication date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=100,
        help="Cap ingestion for quick runs (default: 100)",
    )
    parser.add_argument(
        "--page-size", type=int, default=100, help="Europe PMC page size (default: 100)"
    )
    parser.add_argument(
        "--include-reviews",
        dest="include_reviews",
        action="store_true",
        default=True,
        help="Include review articles (default: on)",
    )
    parser.add_argument(
        "--exclude-reviews",
        dest="include_reviews",
        action="store_false",
        help="Exclude review articles",
    )
    parser.add_argument(
        "--include-trials",
        dest="include_trials",
        action="store_true",
        default=True,
        help="Include clinical trial language (default: on)",
    )
    parser.add_argument(
        "--exclude-trials",
        dest="include_trials",
        action="store_false",
        help="Exclude clinical trial language",
    )
    parser.add_argument(
        "--output-prefix",
        help="Optional filename prefix; defaults to first product name",
    )
    parser.add_argument(
        "--polite-delay",
        type=float,
        default=0.1,
        help="Seconds to sleep between Europe PMC requests",
    )
    return parser.parse_args()


def run_ingestion(
    product_names: List[str],
    args: argparse.Namespace,
    *,
    client: EuropePMCClient | None = None,
    splitter: SentenceSplitter | None = None,
    raw_dir: Path = RAW_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    client = client or EuropePMCClient(polite_delay_s=args.polite_delay)
    splitter = splitter or SentenceSplitter()

    query_str = client.build_drug_query(
        product_names=product_names,
        require_abstract=True,
        from_date=args.from_date,
        to_date=args.to_date,
        include_reviews=args.include_reviews,
        include_trials=args.include_trials,
    )

    query = EuropePMCQuery(query=query_str, page_size=args.page_size)

    try:
        first_page = client.fetch_search_page(query, page=1)
    except RuntimeError as exc:
        print(
            "Europe PMC search request failed (often caused by blocked proxies or network restrictions):",
            exc,
            file=sys.stderr,
        )
        raise

    hit_count = int(first_page.get("hitCount") or 0)

    prefix = args.output_prefix or _slug(product_names[0])
    raw_path = raw_dir / f"{prefix}_raw.json"
    structured_path = processed_dir / f"{prefix}_structured.jsonl"

    print(f"Europe PMC reported hitCount={hit_count} for the query.")

    if hit_count == 0:
        print(f"No Europe PMC results returned (hitCount=0) for query: {query.query}")
        print("Check product spelling, relax date filters, or rerun without --exclude flags.")

        with raw_path.open("w", encoding="utf-8") as f:
            json.dump([], f, indent=2)

        structured_path.touch()
        print(f"Raw records written to: {raw_path}")
        print(f"Structured documents written to: {structured_path}")
        return

    results = list(
        client.search(query, max_records=args.max_records, initial_payload=first_page)
    )

    with raw_path.open("w", encoding="utf-8") as f:
        json.dump([r.raw for r in results], f, indent=2)

    documents = []
    with structured_path.open("w", encoding="utf-8") as f:
        for record in results:
            doc = splitter.split_document(record)
            documents.append(doc)
            f.write(json.dumps(doc.to_dict()) + "\n")

    print(f"Ingested {len(results)} documents for query: {query.query}")
    if documents:
        section_counts = sentence_counts_by_section(documents[0])
        mean_len = mean_sentence_length(documents[0])
        print(f"Example sentence counts: {section_counts}")
        print(f"Mean sentence length (first doc): {mean_len:.1f} chars")
    print(f"Raw records written to: {raw_path}")
    print(f"Structured documents written to: {structured_path}")


def main() -> None:
    args = parse_args()
    run_ingestion(product_names=args.product, args=args)


if __name__ == "__main__":
    main()

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from src.analytics import fetch_sentence_evidence

DEFAULT_DB = Path("data/europepmc.sqlite")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database populated by ingestion.",
    )
    parser.add_argument("--product-a", type=str, help="Filter by the first product in a pair.")
    parser.add_argument("--product-b", type=str, help="Filter by the second product in a pair.")
    parser.add_argument(
        "--pub-after",
        type=str,
        help="Only include documents with publication_date on or after this ISO date.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of evidence sentences to display (default: 50).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise SystemExit(
            f"SQLite database not found at {args.db}. Run ingestion and labeling scripts first."
        )

    conn = sqlite3.connect(args.db)
    evidence_rows = fetch_sentence_evidence(
        conn,
        product_a=args.product_a,
        product_b=args.product_b,
        pub_after=args.pub_after,
        limit=args.limit,
    )

    if not evidence_rows:
        print("No evidence sentences found for the given filters.")
        return

    for evidence in evidence_rows:
        header = f"{evidence.doc_id} | {evidence.product_a} vs {evidence.product_b}"
        if evidence.publication_date:
            header += f" | pub: {evidence.publication_date}"
        if evidence.journal:
            header += f" | journal: {evidence.journal}"
        print("-" * len(header))
        print(header)
        print(evidence.sentence_text.strip())
        if evidence.labels:
            print(f"Labels: {', '.join(evidence.labels)}")
        if evidence.evidence_weight:
            print(f"Weight: {evidence.evidence_weight:.3f}")
        print()


if __name__ == "__main__":
    main()

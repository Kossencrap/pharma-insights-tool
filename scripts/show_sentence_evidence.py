"""Inspect evidence sentences for product pairs with optional context labels."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import List, Sequence, Tuple

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


def fetch_evidence(
    conn: sqlite3.Connection,
    *,
    product_a: str | None,
    product_b: str | None,
    pub_after: str | None,
    limit: int,
) -> List[Tuple[str, str, str, str, str, str | None, Sequence[str]]]:
    query = [
        """
        SELECT cms.doc_id,
               cms.sentence_id,
               cms.product_a,
               cms.product_b,
               s.text,
               d.publication_date,
               COALESCE(se.comparative_terms, ''),
               COALESCE(se.relationship_types, ''),
               COALESCE(se.risk_terms, ''),
               COALESCE(se.study_context, '')
        FROM co_mentions_sentences cms
        JOIN sentences s ON cms.sentence_id = s.sentence_id
        JOIN documents d ON cms.doc_id = d.doc_id
        LEFT JOIN sentence_events se
          ON cms.doc_id = se.doc_id
         AND cms.sentence_id = se.sentence_id
         AND cms.product_a = se.product_a
         AND cms.product_b = se.product_b
        WHERE 1=1
        """
    ]
    params: list[object] = []

    if product_a:
        query.append("AND lower(cms.product_a) = lower(?)")
        params.append(product_a)

    if product_b:
        query.append("AND lower(cms.product_b) = lower(?)")
        params.append(product_b)

    if pub_after:
        query.append("AND d.publication_date >= ?")
        params.append(pub_after)

    query.append(
        "ORDER BY d.publication_date DESC, cms.doc_id, cms.sentence_id LIMIT ?"
    )
    params.append(limit)

    cur = conn.execute("\n".join(query), params)
    return [
        (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            [item for item in [row[6], row[7], row[8], row[9]] if item],
        )
        for row in cur.fetchall()
    ]


def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise SystemExit(
            f"SQLite database not found at {args.db}. Run ingestion and labeling scripts first."
        )

    conn = sqlite3.connect(args.db)
    evidence_rows = fetch_evidence(
        conn,
        product_a=args.product_a,
        product_b=args.product_b,
        pub_after=args.pub_after,
        limit=args.limit,
    )

    if not evidence_rows:
        print("No evidence sentences found for the given filters.")
        return

    for doc_id, sentence_id, product_a, product_b, text, pub_date, labels in evidence_rows:
        header = f"{doc_id} | {product_a} vs {product_b}"
        if pub_date:
            header += f" | pub: {pub_date}"
        print("-" * len(header))
        print(header)
        print(text.strip())
        if labels:
            print(f"Labels: {', '.join(labels)}")
        print()


if __name__ == "__main__":
    main()

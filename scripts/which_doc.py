"""Identify documents that mention both target products."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = Path("data/europepmc.sqlite")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "product",
        nargs=2,
        metavar=("PRODUCT_A", "PRODUCT_B"),
        help="Two canonical product names to search for in combination.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database populated by the ingestion runner.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of doc_ids to show (default: 10).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    product_a, product_b = args.product

    if not args.db.exists():
        raise SystemExit(f"SQLite database not found at {args.db}. Run ingestion with --db first.")

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT m1.doc_id
        FROM product_mentions m1
        JOIN product_mentions m2 ON m1.doc_id = m2.doc_id
        WHERE m1.product_canonical = ?
          AND m2.product_canonical = ?
        GROUP BY m1.doc_id
        LIMIT ?
        """,
        (product_a, product_b, args.limit),
    ).fetchall()

    if not rows:
        print(f"No documents found with both {product_a} and {product_b}.")
        return

    doc_id = rows[0][0]
    print("doc_ids with both:", rows)

    details = cur.execute(
        "SELECT pmid, title, publication_date FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()

    if details:
        pmid, title, publication_date = details
        print("\nExample doc:")
        print(f"PMID: {pmid}")
        print(f"Title: {title}")
        print(f"Publication date: {publication_date}")


if __name__ == "__main__":
    main()

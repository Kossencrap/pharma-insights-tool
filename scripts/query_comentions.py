"""Display top document-level co-mentions derived from stored product mentions."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


DEFAULT_DB = Path("data/europepmc.sqlite")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database populated by the ingestion runner.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum number of co-mention pairs to display (default: 50).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise SystemExit(f"SQLite database not found at {args.db}. Run ingestion with --db first.")

    con = sqlite3.connect(args.db)
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT m1.product_canonical AS a,
               m2.product_canonical AS b,
               COUNT(DISTINCT m1.doc_id) AS n_docs
        FROM product_mentions m1
        JOIN product_mentions m2
          ON m1.doc_id = m2.doc_id
         AND m1.product_canonical < m2.product_canonical
        GROUP BY 1,2
        ORDER BY n_docs DESC, a, b
        LIMIT ?
        """,
        (args.limit,),
    ).fetchall()

    if not rows:
        print("No co-mentions found. Ensure the database contains product mentions.")
        return

    print("Top co-mentions (doc-level):")
    for a, b, count in rows:
        print(f"{a} | {b} | {count}")


if __name__ == "__main__":
    main()

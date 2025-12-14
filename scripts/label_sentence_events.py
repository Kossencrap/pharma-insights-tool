"""Annotate sentence-level co-mentions with lightweight context labels."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List, Tuple

from src.analytics.context_labels import classify_sentence_context, labels_to_columns
from src.storage import init_db, insert_sentence_events

DEFAULT_DB = Path("data/europepmc.sqlite")


Row = Tuple[str, str, str, str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB,
        help="Path to the SQLite database populated by ingestion.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Maximum number of sentence pairs to label (default: 5000).",
    )
    parser.add_argument(
        "--since-publication",
        type=str,
        help="Only process documents with publication_date on or after this ISO date.",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip pairs that already have sentence_events records.",
    )
    return parser.parse_args()


def fetch_pairs(conn, *, limit: int, since_publication: str | None, only_missing: bool) -> List[Row]:
    query = [
        """
        SELECT cms.doc_id, cms.sentence_id, cms.product_a, cms.product_b, s.text
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

    if since_publication:
        query.append("AND d.publication_date >= ?")
        params.append(since_publication)

    if only_missing:
        query.append("AND se.doc_id IS NULL")

    query.append("ORDER BY d.publication_date DESC, cms.doc_id, cms.sentence_id LIMIT ?")
    params.append(limit)

    cur = conn.execute("\n".join(query), params)
    return [(row[0], row[1], row[2], row[3], row[4]) for row in cur.fetchall()]


def main() -> None:
    args = parse_args()

    conn = init_db(args.db)
    rows = fetch_pairs(
        conn,
        limit=args.limit,
        since_publication=args.since_publication,
        only_missing=args.only_missing,
    )

    if not rows:
        print("No sentence pairs found to label.")
        return

    events = []
    for doc_id, sentence_id, product_a, product_b, text in rows:
        labels = classify_sentence_context(text)
        columns = labels_to_columns(labels)
        events.append(
            (
                doc_id,
                sentence_id,
                product_a,
                product_b,
                columns[0],
                columns[1],
                columns[2],
                columns[3],
                columns[4],
            )
        )

    insert_sentence_events(conn, events)
    conn.commit()

    print(f"Labeled {len(events)} sentence co-mention pairs into sentence_events.")


if __name__ == "__main__":
    main()

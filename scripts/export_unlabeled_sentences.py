"""Export co-mention sentences that lack narrative labels or fail invariants."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

from src.analytics.sections import normalize_section

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("data/europepmc.sqlite"),
        help="Path to the ingestion SQLite database (default: data/europepmc.sqlite).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/artifacts/kpi/narratives_unlabeled.csv"),
        help="Destination CSV for unlabeled or failing sentences.",
    )
    return parser.parse_args()


ALLOWED_SECTIONS = {"results", "result", "conclusion", "conclusions", "discussion"}


def export_unlabeled(db_path: Path, output_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    query = """
        SELECT
            cms.doc_id,
            cms.sentence_id,
            cms.product_a,
            cms.product_b,
            s.section,
            s.text AS sentence_text,
            se.narrative_type,
            se.narrative_subtype,
            se.narrative_confidence,
            se.narrative_invariant_ok,
            se.narrative_invariant_reason
        FROM co_mentions_sentences AS cms
        LEFT JOIN sentence_events AS se
               ON cms.doc_id = se.doc_id
              AND cms.sentence_id = se.sentence_id
              AND cms.product_a = se.product_a
              AND cms.product_b = se.product_b
        LEFT JOIN sentences AS s
               ON cms.doc_id = s.doc_id
              AND cms.sentence_id = s.sentence_id
        WHERE se.narrative_type IS NULL
           OR se.narrative_invariant_ok = 0
        ORDER BY cms.doc_id, cms.sentence_id
    """
    rows = conn.execute(query).fetchall()
    conn.close()

    filtered_rows = []
    for (
        doc_id,
        sentence_id,
        product_a,
        product_b,
        section,
        sentence_text,
        narrative_type,
        narrative_subtype,
        narrative_confidence,
        narrative_invariant_ok,
        narrative_invariant_reason,
    ) in rows:
        canonical_section, _, _ = normalize_section(section, sentence_text)
        normalized = (canonical_section or "").lower()
        if normalized not in ALLOWED_SECTIONS:
            continue
        filtered_rows.append(
            (
                doc_id,
                sentence_id,
                product_a,
                product_b,
                canonical_section,
                sentence_text,
                narrative_type,
                narrative_subtype,
                narrative_confidence,
                narrative_invariant_ok,
                narrative_invariant_reason,
            )
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "doc_id",
        "sentence_id",
        "product_a",
        "product_b",
        "section",
        "sentence_text",
        "narrative_type",
        "narrative_subtype",
        "narrative_confidence",
        "narrative_invariant_ok",
        "narrative_invariant_reason",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(filtered_rows)
    return len(filtered_rows)


def main() -> None:
    args = parse_args()
    count = export_unlabeled(args.db, args.output)
    print(f"Wrote {count} sentences to {args.output}")


if __name__ == "__main__":
    main()

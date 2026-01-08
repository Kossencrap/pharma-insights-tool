"""Export a stratified narrative sample for the manual KPI review."""

from __future__ import annotations

import argparse
import csv
import random
import sqlite3
from pathlib import Path
from typing import List, Sequence

from src.analytics.narrative_kpis import load_narrative_kpis
from src.analytics.sections import normalize_section


def _normalize_allowed_sections(values: Sequence[str] | None) -> set[str] | None:
    if not values:
        return None
    normalized: set[str] = set()
    for value in values:
        text = (value or "").strip().lower()
        if not text:
            continue
        if text in {"all", "any", "*"}:
            return None
        normalized.add(text)
    return normalized or None


def _has_sentence_event_section(conn: sqlite3.Connection) -> bool:
    columns = conn.execute("PRAGMA table_info(sentence_events)").fetchall()
    return any((col[1] or "").lower() == "section" for col in columns)


def _fetch_candidates(conn: sqlite3.Connection, allowed_sections: set[str] | None = None) -> List[dict]:
    se_has_section = _has_sentence_event_section(conn)
    section_expr = "se.section" if se_has_section else "NULL"
    rows = conn.execute(
        f"""
        SELECT se.sentence_id,
               se.narrative_type,
               se.narrative_subtype,
               se.narrative_confidence,
               se.product_a,
               se.product_b,
               s.text AS sentence_text,
               d.publication_date,
               COALESCE({section_expr}, s.section) AS section_label
        FROM sentence_events se
        LEFT JOIN sentences s
               ON se.doc_id = s.doc_id
              AND se.sentence_id = s.sentence_id
        LEFT JOIN documents d
               ON se.doc_id = d.doc_id
        WHERE se.narrative_type IS NOT NULL
        """
    ).fetchall()

    results = []
    for row in rows:
        (
            sentence_id,
            narrative_type,
            narrative_subtype,
            narrative_confidence,
            product_a,
            product_b,
            sentence_text,
            publication_date,
            section_label,
        ) = row
        canonical_section, cleaned_text, _ = normalize_section(section_label, sentence_text)
        normalized_section = canonical_section or (section_label.strip().lower() if section_label else None)
        if allowed_sections is not None and (normalized_section or "") not in allowed_sections:
            continue
        if (narrative_type or "").strip().lower() == "evidence":
            continue
        results.append(
            {
                "sentence_id": sentence_id,
                "narrative_type": narrative_type,
                "narrative_subtype": narrative_subtype,
                "narrative_confidence": narrative_confidence,
                "product_a": product_a,
                "product_b": product_b,
                "sentence_text": cleaned_text if canonical_section else sentence_text,
                "publication_date": publication_date,
                "section": canonical_section or section_label,
            }
        )
    return results


def _stratified_sample(
    rows: Sequence[dict], sample_size: int, required_types: Sequence[str], rng: random.Random
) -> List[dict]:
    if sample_size <= 0 or sample_size >= len(rows):
        return list(rows)
    if len(rows) < sample_size:
        raise SystemExit(f"Only {len(rows)} labeled sentences available; need {sample_size}.")

    buckets: dict[str, List[dict]] = {}
    for row in rows:
        narrative_type = (row.get("narrative_type") or "").strip().lower()
        if not narrative_type:
            continue
        buckets.setdefault(narrative_type, []).append(row)

    for bucket in buckets.values():
        rng.shuffle(bucket)

    sample: List[dict] = []
    for required in required_types:
        key = required.lower()
        bucket = buckets.get(key, [])
        if not bucket:
            continue
        sample.append(bucket.pop())

    if len(sample) > sample_size:
        return sample[:sample_size]

    remaining = [
        row for bucket_rows in buckets.values() for row in bucket_rows if row not in sample
    ]
    rng.shuffle(remaining)
    needed = sample_size - len(sample)
    sample.extend(remaining[:needed])
    return sample


def _write_sample(path: Path, rows: Sequence[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "sentence_id",
        "narrative_type",
        "narrative_subtype",
        "product_a",
        "product_b",
        "sentence_text",
        "section",
        "publication_date",
        "narrative_confidence",
        "is_correct",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    **{name: row.get(name) for name in fieldnames if name in row},
                    "is_correct": "",
                }
            )


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
        default=Path("artifacts/kpi/narratives_label_kpi.csv"),
        help="Destination CSV for the KPI sample.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Override the desired sample size (default: use config/narratives_kpis.json).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=13,
        help="Random seed for deterministic sampling (default: 13).",
    )
    parser.add_argument(
        "--kpi-config",
        type=Path,
        default=Path("config/narratives_kpis.json"),
        help="Path to the KPI specification.",
    )
    parser.add_argument(
        "--allowed-sections",
        nargs="+",
        default=("results", "conclusion"),
        metavar="SECTION",
        help=(
            "Canonical sections to include in the sample (default: results conclusion). "
            "Specify 'all' to disable filtering."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.db.exists():
        raise SystemExit(f"SQLite database not found at {args.db}. Run ingestion before sampling.")

    kpi_spec = load_narrative_kpis(args.kpi_config)
    desired_sample_size = (
        args.sample_size if args.sample_size is not None else kpi_spec.label_precision.sample_size
    )
    rng = random.Random(args.seed)

    allowed_sections = _normalize_allowed_sections(args.allowed_sections)
    conn = sqlite3.connect(args.db)
    try:
        rows = _fetch_candidates(conn, allowed_sections=allowed_sections)
    finally:
        conn.close()

    if not rows:
        raise SystemExit("sentence_events table has no labeled rows; run label_sentence_events.py first.")

    sample = _stratified_sample(
        rows, desired_sample_size, kpi_spec.label_precision.high_risk_types, rng
    )
    _write_sample(args.output, sample)
    print(f"Wrote {len(sample)} KPI sample rows to {args.output}")


if __name__ == "__main__":
    main()

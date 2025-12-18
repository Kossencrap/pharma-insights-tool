"""Display top document-level co-mentions with confidence and study weighting."""

from __future__ import annotations

import argparse
import sqlite3
from collections import defaultdict
from pathlib import Path

from src.analytics.weights import STUDY_TYPE_ALIASES, load_study_type_weights

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
    parser.add_argument(
        "--study-weight-config",
        type=Path,
        default=Path("config/study_type_weights.json"),
        help="Path to study-type weight configuration (default: config/study_type_weights.json)",
    )
    return parser.parse_args()


def _resolve_weight(study_type: str | None, weight_lookup: dict[str, float]) -> float:
    if not study_type:
        return weight_lookup.get("other", 1.0)
    normalized = study_type.strip().lower()
    canonical = STUDY_TYPE_ALIASES.get(normalized, normalized)
    return weight_lookup.get(canonical, weight_lookup.get("other", 1.0))


def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise SystemExit(f"SQLite database not found at {args.db}. Run ingestion with --db first.")

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    study_weight_lookup = load_study_type_weights(args.study_weight_config)

    rows = con.execute(
        """
        SELECT cm.product_a AS a,
               cm.product_b AS b,
               cm.count AS pair_count,
               cm.doc_id,
               dw.recency_weight,
               dw.study_type,
               dw.study_type_weight,
               dw.combined_weight
        FROM co_mentions cm
        LEFT JOIN document_weights dw ON cm.doc_id = dw.doc_id
        """
    ).fetchall()

    if not rows:
        print("No co-mentions found. Ensure the database contains product mentions.")
        return

    stats: dict[tuple[str, str], dict[str, object]] = defaultdict(
        lambda: {
            "doc_ids": set(),
            "raw_count": 0,
            "confidence": 0.0,
            "study_weights": [],
        }
    )

    for row in rows:
        pair = (row["a"], row["b"])
        pair_stats = stats[pair]
        pair_stats["doc_ids"].add(row["doc_id"])
        pair_stats["raw_count"] += int(row["pair_count"] or 0)

        study_weight = row["study_type_weight"]
        resolved_weight = study_weight or _resolve_weight(row["study_type"], study_weight_lookup)
        pair_stats["study_weights"].append(resolved_weight)

        recency = row["recency_weight"] or 1.0
        combined = row["combined_weight"] or (recency * (resolved_weight or 1.0))
        pair_stats["confidence"] += combined * max(int(row["pair_count"] or 0), 1)

    ranked = []
    for (a, b), values in stats.items():
        doc_count = len(values["doc_ids"])
        avg_study_weight = sum(values["study_weights"]) / max(len(values["study_weights"]), 1)
        ranked.append(
            {
                "product_a": a,
                "product_b": b,
                "doc_count": doc_count,
                "raw_count": values["raw_count"],
                "avg_study_weight": avg_study_weight,
                "confidence": values["confidence"],
            }
        )

    ranked.sort(key=lambda r: (-r["confidence"], -r["doc_count"], r["product_a"], r["product_b"]))

    print("Top co-mentions (doc-level):")
    for entry in ranked[: args.limit]:
        print(
            f"{entry['product_a']} | {entry['product_b']} | docs: {entry['doc_count']} "
            f"| confidence: {entry['confidence']:.3f} | avg study weight: {entry['avg_study_weight']:.2f}"
        )


if __name__ == "__main__":
    main()

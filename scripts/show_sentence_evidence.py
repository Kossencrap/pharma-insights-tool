from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from src.analytics import fetch_sentence_evidence
from src.analytics.weights import STUDY_TYPE_ALIASES, load_study_type_weights

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
    parser.add_argument(
        "--study-weight-config",
        type=Path,
        default=Path("config/study_type_weights.json"),
        help="Path to study-type weight configuration (default: config/study_type_weights.json)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.db.exists():
        raise SystemExit(
            f"SQLite database not found at {args.db}. Run ingestion and labeling scripts first."
        )

    conn = sqlite3.connect(args.db)
    study_weight_lookup = load_study_type_weights(args.study_weight_config)

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

    def _resolve_weight(study_type: str | None) -> float | None:
        if not study_type:
            return study_weight_lookup.get("other")
        normalized = study_type.strip().lower()
        canonical = STUDY_TYPE_ALIASES.get(normalized, normalized)
        return study_weight_lookup.get(canonical, study_weight_lookup.get("other"))

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
        study_weight = evidence.study_type_weight or _resolve_weight(evidence.study_type)
        base_weight = evidence.recency_weight or 1.0
        combined_weight = evidence.combined_weight or (
            base_weight * (study_weight or 1.0)
        )
        confidence = combined_weight * max(evidence.count, 1)
        weight_msg = {
            "recency_weight": evidence.recency_weight,
            "study_type": evidence.study_type,
            "study_type_weight": study_weight,
            "combined_weight": combined_weight,
            "count": evidence.count,
            "confidence": confidence,
        }
        print(f"Weights: {json.dumps(weight_msg, default=float)}")
        print()


if __name__ == "__main__":
    main()

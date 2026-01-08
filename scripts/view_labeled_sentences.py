"""Lightweight Streamlit browser for labeled sentence evidence.

The app allows filtering by product pairs and inspecting matched terms,
weights, and labels without needing to write SQL queries manually.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Optional

from src.analytics import fetch_sentence_evidence
from src.analytics.weights import STUDY_TYPE_ALIASES, load_study_type_weights

DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_WEIGHT_CONFIG = Path("config/study_type_weights.json")


def _resolve_weight(study_type: str | None, lookup: dict[str, float]) -> float:
    if not study_type:
        return lookup.get("other", 1.0)
    normalized = study_type.strip().lower()
    canonical = STUDY_TYPE_ALIASES.get(normalized, normalized)
    return lookup.get(canonical, lookup.get("other", 1.0))


def _load_evidence(
    db_path: Path,
    weight_lookup: dict[str, float],
    *,
    product_a: str | None,
    product_b: str | None,
    narrative_type: Optional[str],
    narrative_subtype: Optional[str],
) -> Iterable[dict]:
    conn = sqlite3.connect(db_path)
    rows = fetch_sentence_evidence(
        conn,
        product_a=product_a,
        product_b=product_b,
        narrative_type=narrative_type,
        narrative_subtype=narrative_subtype,
    )
    for row in rows:
        study_weight = row.study_type_weight or _resolve_weight(row.study_type, weight_lookup)
        base = row.recency_weight or 1.0
        combined = row.combined_weight or (base * (study_weight or 1.0))
        confidence = combined * max(row.count, 1)
        record = row.to_dict()
        record["confidence"] = confidence
        record["study_type_weight_resolved"] = study_weight
        yield record


def main() -> None:
    try:
        import streamlit as st
    except ImportError:  # pragma: no cover - optional dependency
        print("Install streamlit to launch the viewer: pip install streamlit")
        return

    st.set_page_config(page_title="Sentence Evidence Browser", layout="wide")
    st.title("Sentence Evidence Browser")

    db_path = Path(st.sidebar.text_input("SQLite DB path", str(DEFAULT_DB)))
    weight_config = Path(
        st.sidebar.text_input("Study weight config", str(DEFAULT_WEIGHT_CONFIG))
    )
    weight_lookup = load_study_type_weights(weight_config) if weight_config.exists() else {}

    col1, col2 = st.columns(2)
    product_a = col1.text_input("Product A filter") or None
    product_b = col2.text_input("Product B filter") or None
    narrative_type = st.sidebar.text_input("Narrative type filter").strip() or None
    narrative_subtype = st.sidebar.text_input("Narrative subtype filter").strip() or None

    if st.button("Load evidence"):
        if not db_path.exists():
            st.error(f"SQLite database not found at {db_path}")
            return

        evidence = list(
            _load_evidence(
                db_path,
                weight_lookup,
                product_a=product_a,
                product_b=product_b,
                narrative_type=narrative_type,
                narrative_subtype=narrative_subtype,
            )
        )
        if not evidence:
            st.info("No evidence found for the selected filters.")
            return

        st.caption(f"Showing {len(evidence)} sentences")
        grouped = {}
        for record in evidence:
            key = record.get("narrative_type") or "(no narrative)"
            grouped.setdefault(key, []).append(record)

        for narrative_key in sorted(grouped.keys()):
            st.subheader(f"Narrative: {narrative_key}")
            for record in grouped[narrative_key]:
                with st.expander(
                    f"{record['doc_id']} | {record['product_a']} vs {record['product_b']} (confidence: {record['confidence']:.3f})"
                ):
                    st.write(record["sentence_text"])
                    meta_cols = st.columns(4)
                    meta_cols[0].metric("Recency", f"{record.get('recency_weight', 1.0):.2f}")
                    meta_cols[1].metric(
                        "Study weight", f"{record.get('study_type_weight_resolved', 1.0):.2f}"
                    )
                    meta_cols[2].metric("Count", str(record.get("count", 1)))
                    meta_cols[3].metric("Confidence", f"{record['confidence']:.2f}")

                    if record.get("labels"):
                        st.write("**Labels:**", ", ".join(record["labels"]))
                    if record.get("matched_terms"):
                        st.write("**Matched terms:**", record["matched_terms"])
                    if record.get("context_rule_hits"):
                        st.write("**Context rules:**", ", ".join(record["context_rule_hits"]))
                    if record.get("narrative_type"):
                        narrative = record["narrative_type"]
                        if record.get("narrative_subtype"):
                            narrative += f" ({record['narrative_subtype']})"
                        if record.get("narrative_confidence") is not None:
                            narrative += f" | conf={record['narrative_confidence']:.2f}"
                        st.caption(f"Narrative: {narrative}")
                    st.json({k: v for k, v in record.items() if k not in {"sentence_text"}})


if __name__ == "__main__":
    main()

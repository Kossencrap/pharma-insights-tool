"""Streamlit dashboard for aggregated pharma insight metrics."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

from src.analytics import explain_confidence, fetch_sentence_evidence
from src.analytics.weights import load_study_type_weights

DEFAULT_DB = Path("data/europepmc.sqlite")
DEFAULT_METRICS_DIR = Path("data/processed/metrics")


def _read_metrics(path: Path):
    try:
        import pandas as pd  # type: ignore
    except ImportError:
        pd = None  # type: ignore

    if not path.exists():
        return None

    if pd is None:
        with path.open("r", encoding="utf-8") as f:
            try:
                rows = json.load(f)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    "pandas is required to read parquet metrics; re-export metrics as JSON to continue."
                ) from exc
        return rows

    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    with path.open("r", encoding="utf-8") as f:
        return pd.DataFrame(json.load(f))


def _ensure_datetime(frame):
    if frame is None:
        return frame
    try:
        import pandas as pd  # type: ignore
    except ImportError:
        return frame
    if "bucket_start" in frame.columns:
        frame["bucket_start"] = pd.to_datetime(frame["bucket_start"])
    return frame


def _load_products(mentions_frame) -> list[str]:
    if mentions_frame is None:
        return []
    if hasattr(mentions_frame, "dropna"):
        products = (
            mentions_frame["product_canonical"].dropna().astype(str).unique().tolist()
        )
        return sorted(products)
    products = sorted({row.get("product_canonical") for row in mentions_frame if row.get("product_canonical")})
    return products


def _co_mentions_for_product(co_mentions_frame, product: str):
    if co_mentions_frame is None:
        return None
    if hasattr(co_mentions_frame, "loc"):
        frame = co_mentions_frame
        mask = (
            frame["product_a"].str.lower().eq(product.lower())
            | frame["product_b"].str.lower().eq(product.lower())
        )
        return frame.loc[mask].copy()
    return [
        row
        for row in co_mentions_frame
        if row.get("product_a", "").lower() == product.lower()
        or row.get("product_b", "").lower() == product.lower()
    ]


def _partner_options(co_mentions_frame, product: str) -> list[str]:
    if co_mentions_frame is None:
        return []
    if hasattr(co_mentions_frame, "iterrows"):
        partners = set()
        for _, row in co_mentions_frame.iterrows():
            if str(row["product_a"]).lower() == product.lower():
                partners.add(str(row["product_b"]))
            elif str(row["product_b"]).lower() == product.lower():
                partners.add(str(row["product_a"]))
        return sorted(partners)
    partners = set()
    for row in co_mentions_frame:
        if row.get("product_a", "").lower() == product.lower():
            partners.add(row.get("product_b"))
        elif row.get("product_b", "").lower() == product.lower():
            partners.add(row.get("product_a"))
    return sorted({p for p in partners if p})


def _unique_values(frame, column: str) -> list[str]:
    if frame is None:
        return []
    if hasattr(frame, "columns") and column in frame.columns:
        series = frame[column].dropna()
        return sorted(series.astype(str).unique().tolist())
    rows = frame or []
    return sorted({str(row.get(column)) for row in rows if row.get(column)})


def _has_column(frame, column: str) -> bool:
    if frame is None:
        return False
    if hasattr(frame, "columns"):
        return column in frame.columns
    return any(column in row for row in frame or [])


def _render_evidence(
    st,
    *,
    db_path: Path,
    product_a: Optional[str],
    product_b: Optional[str],
    header: str,
    limit: int = 50,
    study_weight_lookup: Optional[dict] = None,
    narrative_type: Optional[str] = None,
    narrative_subtype: Optional[str] = None,
) -> None:
    with st.expander(header):
        if st.button("Load evidence", key=f"evidence-{header}"):
            if not db_path.exists():
                st.error(f"SQLite database not found at {db_path}")
                return
            conn = sqlite3.connect(db_path)
            evidence = fetch_sentence_evidence(
                conn,
                product_a=product_a,
                product_b=product_b,
                narrative_type=narrative_type,
                narrative_subtype=narrative_subtype,
                limit=limit,
            )
            if not evidence:
                st.info("No evidence found for the selected filters.")
                return
            st.caption(f"Showing {len(evidence)} sentences")
            for row in evidence:
                record = row.to_dict(include_confidence=bool(study_weight_lookup))
                with st.container():
                    st.markdown(
                        f"**{record['doc_id']}** | {record['product_a']} vs {record['product_b']}"
                    )
                    st.write(record["sentence_text"])
                    meta_cols = st.columns(4)
                    meta_cols[0].metric("Date", record.get("publication_date") or "n/a")
                    meta_cols[1].metric("Count", str(record.get("count", 1)))
                    meta_cols[2].metric(
                        "Sentiment", record.get("sentiment_label") or "n/a"
                    )
                    meta_cols[3].metric(
                        "Confidence", f"{record.get('evidence_weight', 1.0):.2f}"
                    )
                    aliases = []
                    if record.get("product_a_alias"):
                        aliases.append(
                            f"{record['product_a_alias']} → {record['product_a']}"
                        )
                    if record.get("product_b_alias"):
                        aliases.append(
                            f"{record['product_b_alias']} → {record['product_b']}"
                        )
                    if aliases:
                        st.caption("Aliases matched: " + ", ".join(aliases))
                    indications = record.get("indications") or []
                    if indications:
                        st.caption("Indications: " + ", ".join(indications))
                    if record.get("matched_terms"):
                        st.caption(f"Matched terms: {record['matched_terms']}")
                    context_rules = record.get("context_rule_hits") or []
                    if context_rules:
                        if isinstance(context_rules, str):
                            try:
                                parsed_rules = json.loads(context_rules)
                                if isinstance(parsed_rules, list):
                                    context_rules = parsed_rules
                            except Exception:
                                pass
                        if isinstance(context_rules, list):
                            st.caption(
                                "Context rules: " + ", ".join(str(rule) for rule in context_rules)
                            )
                        else:
                            st.caption(f"Context rules: {context_rules}")
                    if record.get("narrative_type"):
                        narrative = record["narrative_type"]
                        if record.get("narrative_subtype"):
                            narrative += f" ({record['narrative_subtype']})"
                        if record.get("narrative_confidence") is not None:
                            narrative += f" | conf={record['narrative_confidence']:.2f}"
                        st.caption(f"Narrative: {narrative}")
                    if study_weight_lookup:
                        st.json(
                            explain_confidence(row, study_weight_lookup),
                            expanded=False,
                        )


def _render_chart(st, frame, title: str, *, group: Optional[str] = None, value: str = "count"):
    if frame is None or (hasattr(frame, "empty") and frame.empty):
        st.info(f"No data available for {title.lower()}.")
        return

    try:
        import altair as alt  # type: ignore
    except ImportError:
        alt = None  # type: ignore

    if alt is None:
        st.line_chart(frame, x="bucket_start", y=value)
        return

    if group:
        chart = (
            alt.Chart(frame)
            .mark_line()
            .encode(
                x="bucket_start:T",
                y=alt.Y(f"{value}:Q"),
                color=alt.Color(f"{group}:N"),
                tooltip=["bucket_start:T", value, group],
            )
            .properties(height=300)
        )
    else:
        chart = (
            alt.Chart(frame)
            .mark_line()
            .encode(
                x="bucket_start:T",
                y=alt.Y(f"{value}:Q"),
                tooltip=["bucket_start:T", value],
            )
            .properties(height=300)
        )
    st.altair_chart(chart, use_container_width=True)


def _with_partner_column(frame, product: str):
    if frame is None:
        return frame
    if hasattr(frame, "loc"):
        partner = frame["product_b"].where(
            frame["product_a"].str.lower() == product.lower(), frame["product_a"]
        )
        updated = frame.copy()
        updated["partner"] = partner
        return updated
    updated_rows = []
    for row in frame:
        row_copy = dict(row)
        if row.get("product_a", "").lower() == product.lower():
            row_copy["partner"] = row.get("product_b")
        else:
            row_copy["partner"] = row.get("product_a")
        updated_rows.append(row_copy)
    return updated_rows


def _filter_narratives_by_type(frame, narrative_type: str | None, narrative_subtype: str | None):
    if frame is None:
        return frame
    if hasattr(frame, "loc"):
        filtered = frame
        if narrative_type and "narrative_type" in frame.columns:
            filtered = filtered.loc[
                filtered["narrative_type"].str.lower() == narrative_type.lower()
            ]
        if narrative_subtype and "narrative_subtype" in filtered.columns:
            filtered = filtered.loc[
                filtered["narrative_subtype"].str.lower() == narrative_subtype.lower()
            ]
        return filtered
    rows = frame or []
    if narrative_type:
        rows = [
            row for row in rows if str(row.get("narrative_type", "")).lower() == narrative_type.lower()
        ]
    if narrative_subtype:
        rows = [
            row for row in rows if str(row.get("narrative_subtype", "")).lower() == narrative_subtype.lower()
        ]
    return rows


def _filter_change_by_status(frame, status: str | None):
    if frame is None or not status:
        return frame
    status_lower = status.lower()
    if hasattr(frame, "loc"):
        if "status" not in frame.columns:
            return frame
        return frame.loc[frame["status"].str.lower() == status_lower]
    rows = frame or []
    return [row for row in rows if str(row.get("status", "")).lower() == status_lower]


def _render_change_table(st, frame):
    columns = [
        "narrative_type",
        "narrative_subtype",
        "bucket_start",
        "status",
        "count",
        "reference_avg",
        "delta_count",
        "delta_ratio",
    ]
    if frame is None:
        st.info("No narrative change rows for the selected filters.")
        return
    if hasattr(frame, "loc"):
        available = [col for col in columns if col in frame.columns]
        if not available:
            st.dataframe(frame)
        else:
            st.dataframe(frame[available])
    else:
        rows = frame or []
        if not rows:
            st.info("No narrative change rows for the selected filters.")
            return
        display = [
            {col: row.get(col) for col in columns if col in row}
            for row in rows
        ]
        st.table(display)


def _extract_change_thresholds(frame):
    if frame is None:
        return None
    if hasattr(frame, "head"):
        if frame.empty:
            return None
        row = frame.iloc[0]
        return {
            "lookback": int(row.get("lookback_used") or 0),
            "min_ratio": row.get("min_ratio"),
            "min_count": row.get("min_count"),
        }
    rows = frame or []
    if not rows:
        return None
    row = rows[0]
    return {
        "lookback": int(row.get("lookback_used") or 0),
        "min_ratio": row.get("min_ratio"),
        "min_count": row.get("min_count"),
    }


def main() -> None:
    try:
        import streamlit as st
    except ImportError:  # pragma: no cover - optional dependency
        print("Install streamlit to launch the dashboard: pip install streamlit")
        return

    st.set_page_config(page_title="Pharma Insights Metrics", layout="wide")
    st.title("Pharma Insights Metrics Dashboard")

    metrics_dir = Path(st.sidebar.text_input("Metrics directory", str(DEFAULT_METRICS_DIR)))
    db_path = Path(st.sidebar.text_input("SQLite DB path", str(DEFAULT_DB)))
    weight_path = Path(
        st.sidebar.text_input("Study weight config", "config/study_type_weights.json")
    )
    freq_label = st.sidebar.selectbox("Time frequency", ["Weekly", "Monthly"])
    freq = "w" if freq_label == "Weekly" else "m"

    study_weight_lookup: dict | None = None
    if weight_path.exists():
        study_weight_lookup = load_study_type_weights(weight_path)
    else:
        st.sidebar.warning("Study weight config not found; confidence shown without weights.")

    try:
        documents_frame = _ensure_datetime(
            _read_metrics(metrics_dir / f"documents_{freq}.parquet")
        )
        mentions_frame = _ensure_datetime(
            _read_metrics(metrics_dir / f"mentions_{freq}.parquet")
        )
        co_mentions_frame = _ensure_datetime(
            _read_metrics(metrics_dir / f"co_mentions_{freq}.parquet")
        )
        sentiment_frame = _ensure_datetime(
            _read_metrics(metrics_dir / f"sentiment_{freq}.parquet")
        )
        narratives_frame = _ensure_datetime(
            _read_metrics(metrics_dir / f"narratives_{freq}.parquet")
        )
        narrative_change_frame = _ensure_datetime(
            _read_metrics(metrics_dir / f"narratives_change_{freq}.parquet")
        )
    except RuntimeError as exc:
        st.error(str(exc))
        return

    st.info(
        "This dashboard is the primary Phase 1 artifact. Co-mentions represent sentences "
        "where two products co-occur; confidence combines recency, study-type weights, and "
        "repeat mentions. Sentiment is lexicon-based and heuristic."
    )

    products = _load_products(mentions_frame)
    selected_product = st.sidebar.selectbox(
        "Product filter", options=["(all)"] + products
    )
    product_filter = None if selected_product == "(all)" else selected_product

    partner_filter = None
    if product_filter and co_mentions_frame is not None:
        partners = _partner_options(co_mentions_frame, product_filter)
        partner_filter = st.sidebar.selectbox(
            "Co-mention partner", options=["(any)"] + partners
        )
        if partner_filter == "(any)":
            partner_filter = None

    st.subheader("Publication volume")
    _render_chart(st, documents_frame, "Publication volume")
    _render_evidence(
        st,
        db_path=db_path,
        product_a=product_filter,
        product_b=partner_filter,
        header="Evidence for publication volume",
        study_weight_lookup=study_weight_lookup,
    )

    st.subheader("Product mentions trend")
    if product_filter and hasattr(mentions_frame, "loc"):
        mentions_filtered = mentions_frame.loc[
            mentions_frame["product_canonical"].str.lower() == product_filter.lower()
        ]
    elif product_filter:
        mentions_filtered = [
            row
            for row in mentions_frame or []
            if row.get("product_canonical", "").lower() == product_filter.lower()
        ]
    else:
        mentions_filtered = mentions_frame
    _render_chart(
        st,
        mentions_filtered,
        "Product mentions trend",
        group=None if product_filter else "product_canonical",
    )
    _render_evidence(
        st,
        db_path=db_path,
        product_a=product_filter,
        product_b=None,
        header="Evidence for product mentions",
        study_weight_lookup=study_weight_lookup,
    )

    st.subheader("Co-mentions trend")
    if product_filter:
        co_mentions_filtered = _co_mentions_for_product(co_mentions_frame, product_filter)
    else:
        co_mentions_filtered = co_mentions_frame
    if partner_filter and hasattr(co_mentions_filtered, "loc"):
        co_mentions_filtered = co_mentions_filtered.loc[
            (co_mentions_filtered["product_a"].str.lower() == partner_filter.lower())
            | (co_mentions_filtered["product_b"].str.lower() == partner_filter.lower())
        ]
    if product_filter:
        co_mentions_filtered = _with_partner_column(co_mentions_filtered, product_filter)
    _render_chart(
        st,
        co_mentions_filtered,
        "Co-mentions trend",
        group="partner" if product_filter else "product_a",
    )
    _render_evidence(
        st,
        db_path=db_path,
        product_a=product_filter,
        product_b=partner_filter,
        header="Evidence for co-mentions",
        study_weight_lookup=study_weight_lookup,
    )

    narrative_type_value = None
    narrative_subtype_value = None

    st.subheader("Narrative trend")
    if narratives_frame is None or not _has_column(narratives_frame, "narrative_type"):
        st.info(
            "No narrative metrics available; re-run aggregate_metrics to generate narratives_*.parquet."
        )
    else:
        narrative_types = ["(all)"] + _unique_values(narratives_frame, "narrative_type")
        narrative_type_filter = st.sidebar.selectbox(
            "Narrative type", options=narrative_types
        )
        narrative_type_value = (
            None if narrative_type_filter == "(all)" else narrative_type_filter
        )
        narrative_subtypes = ["(all)"] + _unique_values(
            narratives_frame, "narrative_subtype"
        )
        narrative_subtype_filter = st.sidebar.selectbox(
            "Narrative subtype", options=narrative_subtypes
        )
        narrative_subtype_value = (
            None if narrative_subtype_filter == "(all)" else narrative_subtype_filter
        )
        narratives_filtered = _filter_narratives_by_type(
            narratives_frame, narrative_type_value, narrative_subtype_value
        )
        _render_chart(
            st,
            narratives_filtered,
            "Narrative trend",
            group="narrative_subtype" if narrative_type_value else "narrative_type",
        )
        _render_evidence(
            st,
            db_path=db_path,
            product_a=product_filter,
            product_b=partner_filter,
            header="Evidence for narrative trend",
            study_weight_lookup=study_weight_lookup,
            narrative_type=narrative_type_value,
            narrative_subtype=narrative_subtype_value,
        )

    st.subheader("Narrative change since last review")
    if narrative_change_frame is None or not _has_column(narrative_change_frame, "status"):
        st.info(
            "No narrative change metrics available; re-run aggregate_metrics to generate narratives_change_*.parquet."
        )
    else:
        change_status_options = ["(all)"] + _unique_values(narrative_change_frame, "status")
        change_status_filter = st.sidebar.selectbox(
            "Narrative change status", options=change_status_options
        )
        change_status_value = (
            None if change_status_filter == "(all)" else change_status_filter
        )
        changes_filtered = _filter_narratives_by_type(
            narrative_change_frame, narrative_type_value, narrative_subtype_value
        )
        changes_filtered = _filter_change_by_status(changes_filtered, change_status_value)
        _render_change_table(st, changes_filtered)
        thresholds = _extract_change_thresholds(changes_filtered)
        if thresholds:
            st.caption(
                f"Status compares the latest bucket to the average of the previous {thresholds['lookback']} buckets "
                f"(min ratio {thresholds['min_ratio']}, min delta {thresholds['min_count']})."
            )
        _render_evidence(
            st,
            db_path=db_path,
            product_a=product_filter,
            product_b=partner_filter,
            header="Evidence for narrative changes",
            study_weight_lookup=study_weight_lookup,
            narrative_type=narrative_type_value,
            narrative_subtype=narrative_subtype_value,
        )

    st.subheader("Sentiment ratios")
    sentiment_filtered = sentiment_frame
    if product_filter and hasattr(sentiment_frame, "loc"):
        sentiment_filtered = sentiment_frame.loc[
            (sentiment_frame["product_a"].str.lower() == product_filter.lower())
            | (sentiment_frame["product_b"].str.lower() == product_filter.lower())
        ]
    if partner_filter and hasattr(sentiment_filtered, "loc"):
        sentiment_filtered = sentiment_filtered.loc[
            (sentiment_filtered["product_a"].str.lower() == partner_filter.lower())
            | (sentiment_filtered["product_b"].str.lower() == partner_filter.lower())
        ]
    _render_chart(
        st,
        sentiment_filtered,
        "Sentiment ratios",
        group="sentiment_label",
        value="ratio",
    )
    _render_evidence(
        st,
        db_path=db_path,
        product_a=product_filter,
        product_b=partner_filter,
        header="Evidence for sentiment ratios",
        study_weight_lookup=study_weight_lookup,
    )


if __name__ == "__main__":
    main()
def _filter_narratives_by_type(frame, narrative_type: str | None, narrative_subtype: str | None):
    if frame is None:
        return frame
    if hasattr(frame, "loc"):
        filtered = frame
        if narrative_type and "narrative_type" in frame.columns:
            filtered = filtered.loc[
                filtered["narrative_type"].str.lower() == narrative_type.lower()
            ]
        if narrative_subtype and "narrative_subtype" in filtered.columns:
            filtered = filtered.loc[
                filtered["narrative_subtype"].str.lower() == narrative_subtype.lower()
            ]
        return filtered
    rows = frame or []
    if narrative_type:
        rows = [
            row for row in rows if str(row.get("narrative_type", "")).lower() == narrative_type.lower()
        ]
    if narrative_subtype:
        rows = [
            row for row in rows if str(row.get("narrative_subtype", "")).lower() == narrative_subtype.lower()
        ]
    return rows

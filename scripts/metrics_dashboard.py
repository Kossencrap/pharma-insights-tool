"""Streamlit dashboard for aggregated pharma insight metrics."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

from src.analytics import fetch_sentence_evidence

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
            rows = json.load(f)
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


def _render_evidence(
    st,
    *,
    db_path: Path,
    product_a: Optional[str],
    product_b: Optional[str],
    header: str,
    limit: int = 50,
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
                limit=limit,
            )
            if not evidence:
                st.info("No evidence found for the selected filters.")
                return
            st.caption(f"Showing {len(evidence)} sentences")
            for row in evidence:
                record = row.to_dict()
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
    freq_label = st.sidebar.selectbox("Time frequency", ["Weekly", "Monthly"])
    freq = "w" if freq_label == "Weekly" else "m"

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
    _render_chart(st, mentions_filtered, "Product mentions trend")
    _render_evidence(
        st,
        db_path=db_path,
        product_a=product_filter,
        product_b=None,
        header="Evidence for product mentions",
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
    _render_chart(
        st,
        co_mentions_filtered,
        "Co-mentions trend",
        group="product_b" if product_filter else "product_a",
    )
    _render_evidence(
        st,
        db_path=db_path,
        product_a=product_filter,
        product_b=partner_filter,
        header="Evidence for co-mentions",
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
    )


if __name__ == "__main__":
    main()

from datetime import date, timedelta

import pytest

from src.analytics.weights import compute_document_weight, compute_recency_weight, map_study_type


def test_recency_weight_half_life(execution_log):
    today = date(2024, 1, 1)
    half_life_days = 365
    half_life_date = today - timedelta(days=half_life_days)

    recent_weight = compute_recency_weight(today, reference_date=today, half_life_days=half_life_days)
    half_life_weight = compute_recency_weight(
        half_life_date, reference_date=today, half_life_days=half_life_days
    )

    assert recent_weight == pytest.approx(1.0)
    assert half_life_weight == pytest.approx(0.5, rel=1e-3)
    execution_log.record(
        "Recency weighting",
        "Today vs 1-year-old publication yields weights 1.0 -> 0.5",
    )


def test_map_study_type_prefers_highest_weight(execution_log):
    weights = {
        "clinical trial": 1.4,
        "randomized controlled trial": 1.6,
        "review": 0.9,
        "other": 1.0,
    }
    study_type, weight = map_study_type(
        ["Journal Article", "Randomised Controlled Trial", "Systematic Review"], weights
    )

    assert study_type == "randomized controlled trial"
    assert weight == 1.6
    execution_log.record(
        "Study type mapping",
        "Randomized controlled trial selected with weight 1.6 over review/other",
    )


def test_compute_document_weight_extracts_from_pubtypelist(execution_log):
    raw_metadata = {"pubTypeList": {"pubType": ["Randomized Controlled Trial"]}}
    weights = {"clinical trial": 1.4, "randomized controlled trial": 1.6, "other": 1.0}
    weight = compute_document_weight(
        doc_id="doc-1",
        publication_date=date(2024, 1, 1),
        raw_metadata=raw_metadata,
        weight_lookup=weights,
        reference_date=date(2024, 1, 1),
    )

    assert weight.study_type == "randomized controlled trial"
    assert weight.study_type_weight == pytest.approx(1.6)
    assert weight.recency_weight == pytest.approx(1.0)
    assert weight.combined_weight == pytest.approx(1.6)
    execution_log.record(
        "Document weight",
        "doc-1 combines study-type 1.6 with recency 1.0 -> combined 1.6",
    )

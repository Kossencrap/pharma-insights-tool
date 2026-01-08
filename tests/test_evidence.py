from datetime import datetime

from src.analytics.evidence import SentenceEvidence, build_narrative_card


def _make_sentence(sentence_id: str, *, count: int, recency_weight: float, weight: float | None = None):
    return SentenceEvidence(
        doc_id="doc-1",
        sentence_id=sentence_id,
        product_a="DrugA",
        product_a_alias=None,
        product_b="DrugB",
        product_b_alias=None,
        sentence_text=f"{sentence_id} text",
        publication_date="2024-05-01",
        journal="Journal A",
        section="abstract",
        sent_index=0,
        count=count,
        recency_weight=recency_weight,
        study_type="randomized controlled trial",
        study_type_weight=1.2,
        combined_weight=weight,
        labels=["efficacy"],
        matched_terms=None,
        context_rule_hits=(),
        indications=(),
        direction_type=None,
        product_a_role=None,
        product_b_role=None,
        direction_triggers=(),
        narrative_type="efficacy",
        narrative_subtype="positive_signal",
        narrative_confidence=0.75,
        sentiment_label="POS",
        sentiment_score=None,
        sentiment_model=None,
        sentiment_inference_ts=None,
    )


def test_build_narrative_card_serializes_top_sentence():
    evidence_rows = [
        _make_sentence("s-low", count=1, recency_weight=0.5),
        _make_sentence("s-top", count=2, recency_weight=1.0),
    ]
    metrics_row = {
        "bucket_start": datetime(2024, 5, 6),
        "count": 5,
        "wow_change": 0.25,
        "z_score": 1.5,
    }
    change_row = {
        "status": "significant_increase",
        "delta_count": 3,
        "delta_ratio": 0.6,
        "reference_avg": 2,
    }

    card = build_narrative_card(
        narrative_type="efficacy",
        narrative_subtype="positive_signal",
        metrics_row=metrics_row,
        change_row=change_row,
        evidence_rows=evidence_rows,
        max_sentences=1,
    )

    payload = card.to_dict(study_weight_lookup={"randomized controlled trial": 1.3, "other": 1.0})

    assert payload["narrative_type"] == "efficacy"
    assert payload["bucket_start"].startswith("2024-05-06")
    assert payload["current_count"] == 5.0
    assert payload["change_status"] == "significant_increase"
    assert payload["evidence_total_weight"] > 0
    assert len(payload["evidence"]) == 1
    assert payload["evidence"][0]["sentence_id"] == "s-top"


def test_build_narrative_card_infers_type_from_evidence():
    evidence_rows = [
        _make_sentence("s-1", count=1, recency_weight=0.8),
    ]

    card = build_narrative_card(
        narrative_type=None,
        narrative_subtype=None,
        metrics_row=None,
        change_row=None,
        evidence_rows=evidence_rows,
        max_sentences=2,
    )

    payload = card.to_dict(study_weight_lookup=None)

    assert payload["narrative_type"] == "efficacy"
    assert payload["narrative_subtype"] == "positive_signal"
    assert payload["current_count"] is None
    assert len(payload["evidence"]) == 1

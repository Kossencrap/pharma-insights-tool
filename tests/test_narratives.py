from src.analytics.context_labels import SentenceContextLabels
from src.analytics.narratives import classify_narrative
from src.analytics.sentiment import SentimentLabel


def test_risk_terms_map_to_safety():
    labels = SentenceContextLabels(risk_terms={"Adverse events"}, matched_terms={})
    result = classify_narrative(labels, sentiment_label=SentimentLabel.NEGATIVE.value)

    assert result.narrative_type == "safety"
    assert result.narrative_subtype == "safety_reassurance"
    assert result.confidence == 0.9


def test_comparative_with_positive_sentiment():
    labels = SentenceContextLabels(comparative_terms={"superior"})
    result = classify_narrative(labels, sentiment_label=SentimentLabel.POSITIVE.value)

    assert result.narrative_type == "comparative"
    assert result.narrative_subtype == "comparative_advantage"
    assert result.confidence == 0.8


def test_study_context_defaults():
    labels = SentenceContextLabels(study_context={"Phase III trial"})
    result = classify_narrative(labels)

    assert result.narrative_type == "evidence"
    assert result.narrative_subtype == "clinical_trial"
    assert result.confidence == 0.7


def test_positioning_combination_priority():
    labels = SentenceContextLabels(relationship_types={"combination"})
    result = classify_narrative(labels)

    assert result.narrative_type == "positioning"
    assert result.narrative_subtype == "combination"
    assert result.confidence == 0.85


def test_positive_sentiment_without_other_signals_maps_to_efficacy():
    labels = SentenceContextLabels()
    result = classify_narrative(labels, sentiment_label=SentimentLabel.POSITIVE.value)

    assert result.narrative_type == "efficacy"
    assert result.narrative_subtype == "positive_signal"
    assert result.confidence == 0.6


def test_safety_rule_wins_when_multiple_signals_present():
    labels = SentenceContextLabels(
        risk_terms={"adverse events"}, comparative_terms={"superior"}
    )
    result = classify_narrative(labels, sentiment_label=SentimentLabel.POSITIVE.value)

    assert result.narrative_type == "safety"
    assert result.narrative_subtype == "safety_reassurance"

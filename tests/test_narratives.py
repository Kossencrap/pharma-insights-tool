from src.analytics.context_labels import SentenceContextLabels
from src.analytics.narratives import (
    ProductRoleContext,
    classify_directional_roles,
    classify_narrative,
)
from src.analytics.sentiment import SentimentLabel


def test_risk_terms_map_to_safety():
    labels = SentenceContextLabels(risk_terms={"Adverse events"}, matched_terms={})
    text = "Adverse events were comparable between groups."
    result = classify_narrative(labels, sentiment_label=SentimentLabel.NEGATIVE.value, text=text)

    assert result.narrative_type == "safety"
    assert result.narrative_subtype == "safety_acknowledgment"
    assert result.risk_posture == "acknowledgment"
    assert result.confidence == 0.9


def test_comparative_with_positive_sentiment():
    labels = SentenceContextLabels(comparative_terms={"superior"})
    text = "DrugX was superior to DrugY for reducing mortality."
    result = classify_narrative(labels, sentiment_label=SentimentLabel.POSITIVE.value, text=text)

    assert result.narrative_type == "comparative"
    assert result.narrative_subtype == "comparative_efficacy_advantage"
    assert result.confidence == 0.7


def test_comparative_tradeoff_requires_risk_terms():
    labels = SentenceContextLabels(comparative_terms={"versus"}, risk_terms={"risk"})
    text = "DrugX versus DrugY showed similar mortality but higher risk of adverse events."
    result = classify_narrative(labels, sentiment_label=SentimentLabel.NEGATIVE.value, text=text)

    assert result.narrative_type == "comparative"
    assert result.narrative_subtype == "comparative_efficacy_disadvantage"
    assert result.confidence == 0.8


def test_study_context_defaults():
    labels = SentenceContextLabels(study_context={"Phase III trial"})
    result = classify_narrative(labels, text="This Phase III trial enrolled 1000 patients.")

    assert result.narrative_type == "evidence"
    assert result.narrative_subtype == "clinical_trial"
    assert result.confidence == 0.7


def test_positioning_combination_priority():
    labels = SentenceContextLabels(relationship_types={"combination"})
    result = classify_narrative(labels, text="DrugX combined with DrugY improved outcomes.")

    assert result.narrative_type == "positioning"
    assert result.narrative_subtype == "combination"
    assert result.confidence == 0.85


def test_positive_sentiment_without_other_signals_maps_to_efficacy():
    labels = SentenceContextLabels()
    result = classify_narrative(
        labels,
        sentiment_label=SentimentLabel.POSITIVE.value,
        text="Patients experienced improved response.",
    )

    assert result.narrative_type == "efficacy"
    assert result.narrative_subtype == "positive_signal"
    assert result.confidence == 0.6


def test_safety_rule_wins_with_risk_only():
    labels = SentenceContextLabels(risk_terms={"adverse events"})
    text = "No increase in adverse events was observed."
    result = classify_narrative(labels, sentiment_label=SentimentLabel.POSITIVE.value, text=text)

    assert result.narrative_type == "safety"
    assert result.narrative_subtype == "safety_acknowledgment"
    assert result.risk_posture == "acknowledgment"


def test_positioning_line_of_therapy_rule():
    labels = SentenceContextLabels(line_of_therapy_terms={"first-line"})
    result = classify_narrative(labels, text="Recommended as first-line therapy.")

    assert result.narrative_type == "positioning"
    assert result.narrative_subtype == "line_of_therapy"
    assert result.confidence == 0.83


def test_real_world_evidence_rule():
    labels = SentenceContextLabels(real_world_terms={"real-world"})
    result = classify_narrative(labels, text="Real-world data supported effectiveness.")

    assert result.narrative_type == "evidence"
    assert result.narrative_subtype == "real_world"
    assert result.confidence == 0.72


def test_access_barrier_rule_maps_to_access_family():
    labels = SentenceContextLabels(access_terms={"coverage"})
    result = classify_narrative(labels, text="Coverage barriers limited access.")

    assert result.narrative_type == "access"
    assert result.narrative_subtype == "coverage_access"


def test_directional_roles_preferred_over():
    text = "Semaglutide was preferred over insulin for most patients."
    result = classify_directional_roles(
        text,
        ProductRoleContext(canonical="semaglutide"),
        ProductRoleContext(canonical="insulin"),
    )
    assert result.direction_type == "alternative"
    assert result.product_a_role == "favored"
    assert result.product_b_role == "disfavored"


def test_directional_roles_switch_to():
    text = "Participants switched to semaglutide from insulin after 12 weeks."
    result = classify_directional_roles(
        text,
        ProductRoleContext(canonical="insulin"),
        ProductRoleContext(canonical="semaglutide"),
    )
    assert result.direction_type == "switch"
    assert result.product_a_role == "switch_source"
    assert result.product_b_role == "switch_destination"


def test_methods_section_blocks_safety_classification():
    labels = SentenceContextLabels(risk_terms={"adverse events"})
    text = "No increase in adverse events was observed."
    result = classify_narrative(labels, section="methods", text=text)

    assert result.narrative_type is None
    assert result.narrative_subtype is None


def test_results_section_allows_safety_classification():
    labels = SentenceContextLabels(risk_terms={"adverse events"})
    text = "No increase in adverse events was observed."
    result = classify_narrative(labels, section="results", text=text)

    assert result.narrative_type == "safety"


def test_risk_posture_reassurance_overrides_subtype():
    labels = SentenceContextLabels(
        risk_terms={"adverse events"},
        risk_posture_labels={"reassurance"},
    )
    text = "No increase in adverse events was observed."
    result = classify_narrative(labels, text=text)

    assert result.narrative_subtype == "safety_reassurance"
    assert result.risk_posture == "reassurance"


def test_claim_strength_inferred_from_terms():
    labels = SentenceContextLabels(
        comparative_terms={"better than"},
        claim_strength_labels={"confirmatory"},
    )
    text = "DrugX was superior to DrugY."
    result = classify_narrative(labels, sentiment_label=SentimentLabel.POSITIVE.value, text=text)

    assert result.claim_strength == "confirmatory"

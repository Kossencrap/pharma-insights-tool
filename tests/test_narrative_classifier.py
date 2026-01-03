from src.analytics.context_labels import SentenceContextLabels
from src.analytics.narrative_classifier import NarrativePrediction, classify_narrative


def test_risk_drives_safety_label() -> None:
    labels = SentenceContextLabels(risk_terms={"risk"}, study_context={"trial"})
    prediction = classify_narrative(
        "Serious adverse events were reported in the trial.", labels
    )

    assert prediction == NarrativePrediction(
        narrative_type="safety", narrative_subtype="risk_signal", confidence=0.8
    )


def test_comparative_and_endpoint_map_to_positioning() -> None:
    labels = SentenceContextLabels(comparative_terms={"vs"}, endpoint_terms={"pfs"})
    prediction = classify_narrative("Drug A vs Drug B improved PFS.", labels)

    assert prediction.narrative_type == "comparative_positioning"
    assert prediction.narrative_subtype == "head_to_head"
    assert prediction.confidence >= 0.7


def test_trial_context_without_other_signals() -> None:
    labels = SentenceContextLabels(study_context={"trial"}, trial_phase_terms={"phase 3"})
    prediction = classify_narrative("Phase 3 trial enrolled patients.", labels)

    assert prediction.narrative_type == "study_context"
    assert prediction.narrative_subtype == "trial_design"
    assert prediction.confidence == 0.7

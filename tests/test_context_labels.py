import json

from src.analytics.context_labels import classify_sentence_context, labels_to_columns


def test_comparative_label_detected(execution_log):
    sentence = "Semaglutide was compared with insulin and found superior in outcomes."

    labels = classify_sentence_context(sentence)
    columns = labels_to_columns(labels)

    assert "compared with" in labels.comparative_terms
    assert columns[0] is not None
    assert "superior" in columns[0]
    execution_log.record(
        "Context labels",
        "Identified comparative terms (compared with/superior) in semaglutide vs insulin sentence",
    )


def test_trial_phase_and_study_context():
    sentence = "The phase IIb randomized trial enrolled patients with type 2 diabetes."

    labels = classify_sentence_context(sentence)
    columns = labels_to_columns(labels)

    assert "phase iib" in {t.lower() for t in labels.trial_phase_terms}
    assert "randomized" in labels.study_context
    assert columns[3] is not None


def test_endpoint_and_adverse_event_terms():
    sentence = (
        "Primary endpoint was change in HbA1c; adverse events included hypoglycemia and nausea."
    )

    labels = classify_sentence_context(sentence)
    columns = labels_to_columns(labels)

    assert "primary endpoint" in labels.endpoint_terms
    assert "hba1c" in labels.endpoint_terms or "hba1c" in labels.matched_terms.get(
        "endpoint_terms", []
    )
    assert "adverse events" in labels.risk_terms
    assert columns[2] is not None


def test_matched_terms_serialization_includes_new_keys():
    sentence = "The phase 3 study reported improved overall survival and fewer adverse events."

    labels = classify_sentence_context(sentence)
    values = labels_to_columns(labels)
    matched = values[4]
    triggers = values[5]

    assert matched is not None
    assert "endpoint_terms" in matched
    assert "trial_phase_terms" in matched
    assert triggers is not None
    trigger_list = json.loads(triggers)
    assert "study_context" in trigger_list


def test_line_of_therapy_and_real_world_terms_detected():
    sentence = "First-line therapy decisions now rely on real-world registry data for coverage."

    labels = classify_sentence_context(sentence)

    assert "first-line" in {term.lower() for term in labels.line_of_therapy_terms}
    assert "real-world" in {term.lower() for term in labels.real_world_terms}
    assert "coverage" in {term.lower() for term in labels.access_terms}
    assert "line_of_therapy_terms" in labels.matched_terms
    assert "real_world_terms" in labels.matched_terms
    assert "access_terms" in labels.matched_terms

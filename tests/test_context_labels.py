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

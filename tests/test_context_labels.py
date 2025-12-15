from src.analytics.context_labels import classify_sentence_context, labels_to_columns


def test_comparative_label_detected():
    sentence = "Semaglutide was compared with insulin and found superior in outcomes."

    labels = classify_sentence_context(sentence)
    columns = labels_to_columns(labels)

    assert "compared with" in labels.comparative_terms
    assert columns[0] is not None
    assert "superior" in columns[0]

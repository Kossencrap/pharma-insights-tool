from src.analytics.sentiment import SentimentLabel, classify_sentence


def test_negation_flips_to_negative(execution_log):
    sentence = "The therapy was not improved versus placebo."
    result = classify_sentence(sentence)

    assert result.label == SentimentLabel.NEGATIVE.value
    execution_log.record(
        "Sentiment negation",
        "'not improved' classified as NEG sentiment",
    )


def test_hedge_softens_to_neutral():
    sentence = "The therapy may improve glycemic control."
    result = classify_sentence(sentence)

    assert result.label == SentimentLabel.NEUTRAL.value


def test_mixed_sentiment_returns_neutral():
    sentence = "Outcomes improved in cohort A but worsened in cohort B."
    result = classify_sentence(sentence)

    assert result.label == SentimentLabel.NEUTRAL.value

from src.analytics.mention_extractor import MentionExtractor


PRODUCT_ALIASES = {
    "semaglutide": ["Ozempic", "semaglutide"],
    "tirzepatide": ["Mounjaro", "tirzepatide"],
}


def test_regex_extraction_handles_possessives():
    extractor = MentionExtractor(PRODUCT_ALIASES)
    text = "Patients on Ozempic's formulation were compared to tirzepatide."

    mentions = extractor.extract(text)

    assert any(m.alias_matched.lower().startswith("ozempic") for m in mentions)
    assert any(m.product_canonical == "tirzepatide" for m in mentions)


def test_model_assisted_extraction_uses_entity_ruler():
    extractor = MentionExtractor(PRODUCT_ALIASES, use_model_assisted=True)
    text = "The trial switched patients to Mounjaro after Ozempic."

    mentions = extractor.extract(text)
    canonical_labels = {m.product_canonical for m in mentions}

    assert "semaglutide" in canonical_labels
    assert "tirzepatide" in canonical_labels
    assert any(m.match_method == "nlp" for m in mentions)
    assert any(m.match_method == "regex" for m in mentions)

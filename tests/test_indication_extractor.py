from src.analytics.indication_extractor import IndicationExtractor, load_indication_config


def test_indication_extractor_matches_aliases(tmp_path):
    config_path = tmp_path / "indications.json"
    config_path.write_text('{"type 2 diabetes": ["t2d"], "obesity": ["obesity"]}', encoding="utf-8")

    config = load_indication_config(config_path)
    extractor = IndicationExtractor(config)
    text = "Patients with T2D and obesity were enrolled."

    mentions = extractor.extract(text)

    canonicals = {mention.indication_canonical for mention in mentions}
    assert canonicals == {"type 2 diabetes", "obesity"}


def test_indication_extractor_avoids_duplicate_spans():
    aliases = {"type 2 diabetes": ["type 2 diabetes", "T2D"]}
    extractor = IndicationExtractor(aliases)
    text = "Type 2 diabetes (T2D) complications differ."

    mentions = extractor.extract(text)

    assert len(mentions) == 1
    assert mentions[0].alias_matched.lower().startswith("type 2")

from src.analytics.sections import normalize_section


def test_normalize_section_alias_from_metadata():
    section, cleaned, derived = normalize_section("Results")
    assert section == "results"
    assert cleaned is None
    assert derived is False


def test_normalize_section_from_heading_strips_prefix():
    section, cleaned, derived = normalize_section(None, "Methods: safety outcomes were recorded.")
    assert section == "methods"
    assert cleaned == "safety outcomes were recorded."
    assert derived is True


def test_normalize_section_from_html_heading():
    text = "Background text.<h4>Results</h4>Significant improvements observed."
    section, cleaned, derived = normalize_section("abstract", text)
    assert section == "results"
    assert "Significant improvements observed." in cleaned
    assert derived is True


def test_normalize_section_from_slash_heading():
    text = "Background/Objectives: Urgent hospitalization requires action."
    section, cleaned, derived = normalize_section("abstract", text)
    assert section == "introduction"
    assert cleaned.startswith("Urgent hospitalization")
    assert derived is True


def test_normalize_section_from_run_on_heading():
    text = "BackgroundThe Heart Health Hub study examined titration."
    section, cleaned, derived = normalize_section("abstract", text)
    assert section == "introduction"
    assert cleaned.startswith("The Heart Health Hub")
    assert derived is True


def test_normalize_section_maps_interventions_to_methods():
    text = "<h4>Interventions</h4>Participants received sacubitril/valsartan."
    section, cleaned, derived = normalize_section("abstract", text)
    assert section == "methods"
    assert cleaned.startswith("Participants received")
    assert derived is True

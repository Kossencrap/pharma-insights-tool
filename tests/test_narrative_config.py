import json
from pathlib import Path

import pytest

from src.analytics.narrative_config import (
    load_narrative_schema,
    reset_narrative_schema_cache,
)


def test_load_narrative_schema_parses_rules(tmp_path: Path) -> None:
    config_path = tmp_path / "narratives.json"
    config_path.write_text(
        json.dumps(
            {
                "terms": {
                    "comparative_terms": ["vs"],
                    "relationship_patterns": {"combination": ["and"]},
                    "risk_terms": ["risk"],
                    "study_context_terms": ["trial"],
                },
                "narratives": [
                    {
                        "name": "test_safety",
                        "type": "safety",
                        "subtype": "reassurance",
                        "confidence": 0.9,
                        "priority": 10,
                        "requires": {"risk_terms": ["*"]},
                    }
                ],
                "directional_patterns": [
                    {
                        "name": "test_direction",
                        "direction_type": "alternative",
                        "subject_role": "favored",
                        "object_role": "disfavored",
                        "phrases": ["better than"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    reset_narrative_schema_cache()

    schema = load_narrative_schema(config_path)
    assert schema.rules[0].name == "test_safety"
    assert schema.terms.comparative_terms == ("vs",)
    assert schema.terms.directional_patterns[0].name == "test_direction"


def test_invalid_config_raises(tmp_path: Path) -> None:
    config_path = tmp_path / "bad_narratives.json"
    config_path.write_text(json.dumps({"terms": {}}), encoding="utf-8")
    reset_narrative_schema_cache()

    with pytest.raises(ValueError):
        load_narrative_schema(config_path)

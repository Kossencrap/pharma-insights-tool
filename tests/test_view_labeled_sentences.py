import sys
import types
from types import SimpleNamespace

import scripts.view_labeled_sentences as viewer


class DummyEvidence(SimpleNamespace):
    def to_dict(self):
        return {
            "doc_id": "doc-1",
            "product_a": "DrugA",
            "product_b": "DrugB",
            "sentence_text": "DrugA outperformed DrugB.",
            "recency_weight": self.recency_weight,
            "count": self.count,
            "narrative_type": "comparative",
        }


def test_load_evidence_smoke(monkeypatch, tmp_path):
    db_path = tmp_path / "dummy.sqlite"
    db_path.touch()

    def fake_fetch(conn, **kwargs):
        return [
            DummyEvidence(
                study_type_weight=None,
                study_type="randomized controlled trial",
                recency_weight=0.5,
                combined_weight=None,
                count=2,
            )
        ]

    monkeypatch.setattr(viewer, "fetch_sentence_evidence", fake_fetch)

    weight_lookup = {"randomized controlled trial": 1.4, "other": 1.0}
    records = list(
        viewer._load_evidence(
            db_path=db_path,
            weight_lookup=weight_lookup,
            product_a=None,
            product_b=None,
            narrative_type=None,
            narrative_subtype=None,
        )
    )

    assert len(records) == 1
    record = records[0]
    assert record["confidence"] == 0.5 * 1.4 * 2
    assert record["study_type_weight_resolved"] == 1.4
    assert record["doc_id"] == "doc-1"


def test_main_runs_with_streamlit_stub(monkeypatch, tmp_path):
    db_path = tmp_path / "viewer.sqlite"
    db_path.touch()
    weight_path = tmp_path / "weights.json"
    weight_path.write_text("{}", encoding="utf-8")

    evidence_rows = [
        {
            "doc_id": "doc-1",
            "product_a": "DrugA",
            "product_b": "DrugB",
            "sentence_text": "DrugA outperformed DrugB.",
            "recency_weight": 0.5,
            "study_type_weight_resolved": 1.2,
            "count": 2,
            "confidence": 1.0,
            "labels": [],
            "matched_terms": None,
            "context_rule_hits": [],
            "narrative_type": "comparative",
            "narrative_subtype": "advantage",
            "narrative_confidence": 0.9,
        }
    ]

    monkeypatch.setattr(
        viewer,
        "_load_evidence",
        lambda *args, **kwargs: evidence_rows,
    )

    responses = iter([str(db_path), str(weight_path), "", ""])

    class SidebarStub:
        def text_input(self, *args, **kwargs):
            try:
                return next(responses)
            except StopIteration:
                return ""

    class ColumnStub:
        def text_input(self, *args, **kwargs):
            return ""

        def metric(self, *args, **kwargs):
            return None

    class ExpanderStub:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    module = types.ModuleType("streamlit")
    module.sidebar = SidebarStub()
    module.captions = []
    module.errors = []

    module.set_page_config = lambda **kwargs: None
    module.title = lambda *args, **kwargs: None
    module.columns = lambda count: [ColumnStub() for _ in range(count)]
    module.button = lambda label: True
    module.error = lambda message: module.errors.append(message)
    module.info = lambda message: None
    module.caption = lambda message: module.captions.append(message)
    module.subheader = lambda message: None
    module.write = lambda *args, **kwargs: None
    module.json = lambda *args, **kwargs: None
    module.expander = lambda *args, **kwargs: ExpanderStub()

    monkeypatch.setitem(sys.modules, "streamlit", module)

    viewer.main()

    assert any("Showing 1 sentences" in text for text in module.captions)
    assert not module.errors

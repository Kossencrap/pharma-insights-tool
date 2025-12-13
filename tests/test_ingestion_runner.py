import argparse
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import ingest_europe_pmc as runner
from src.ingestion.models import EuropePMCSearchResult


def test_run_ingestion_writes_outputs_and_uses_query_params(tmp_path, monkeypatch, capsys):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"

    fake_results = [
        EuropePMCSearchResult(title="Title one.", abstract="Abstract sentence one.", raw={"id": 1}),
        EuropePMCSearchResult(title="Second title.", abstract="Another abstract.", raw={"id": 2}),
    ]

    class FakeClient:
        last_build_kwargs = None
        last_search_query = None
        last_max_records = None
        last_fetch_query = None

        def __init__(self, polite_delay_s: float = 0.0):
            self.polite_delay_s = polite_delay_s

        @staticmethod
        def build_drug_query(**kwargs):
            FakeClient.last_build_kwargs = kwargs
            return "mock query"

        def fetch_search_page(self, query, page: int = 1):
            FakeClient.last_fetch_query = query
            return {"hitCount": len(fake_results), "resultList": {"result": [r.raw for r in fake_results]}}

        def search(self, query, max_records=None, initial_payload=None):
            FakeClient.last_search_query = query
            FakeClient.last_max_records = max_records
            return iter(fake_results)

    monkeypatch.setattr(runner, "EuropePMCClient", FakeClient)

    args = argparse.Namespace(
        from_date=None,
        to_date=None,
        include_reviews=True,
        include_trials=True,
        output_prefix=None,
        max_records=2,
        page_size=2,
        polite_delay=0.0,
    )

    runner.run_ingestion(["MockProduct"], args, raw_dir=raw_dir, processed_dir=processed_dir)

    assert FakeClient.last_build_kwargs["product_names"] == ["MockProduct"]
    assert FakeClient.last_search_query.query == "mock query"
    assert FakeClient.last_search_query.page_size == 2
    assert FakeClient.last_max_records == 2

    raw_path = raw_dir / "mockproduct_raw.json"
    structured_path = processed_dir / "mockproduct_structured.jsonl"

    assert raw_path.exists()
    assert structured_path.exists()

    with raw_path.open("r", encoding="utf-8") as f:
        raw_records = json.load(f)
    assert [rec["id"] for rec in raw_records] == [1, 2]

    with structured_path.open("r", encoding="utf-8") as f:
        structured_records = [json.loads(line) for line in f]
    assert len(structured_records) == 2
    assert structured_records[0]["sections"][0]["sentences"][0]["text"].startswith("Title")

    output = capsys.readouterr().out
    assert "Ingested 2 documents" in output
    assert "mockproduct_raw.json" in output


def test_run_ingestion_handles_zero_results(tmp_path, monkeypatch, capsys):
    raw_dir = tmp_path / "raw"
    processed_dir = tmp_path / "processed"

    class EmptyClient:
        def __init__(self, polite_delay_s: float = 0.0):
            self.polite_delay_s = polite_delay_s

        @staticmethod
        def build_drug_query(**kwargs):
            return "mock empty query"

        @staticmethod
        def fetch_search_page(query, page: int = 1):
            return {"hitCount": 0, "resultList": {"result": []}}

    monkeypatch.setattr(runner, "EuropePMCClient", EmptyClient)

    args = argparse.Namespace(
        from_date=None,
        to_date=None,
        include_reviews=True,
        include_trials=True,
        output_prefix=None,
        max_records=5,
        page_size=5,
        polite_delay=0.0,
    )

    runner.run_ingestion(["NoResults"], args, raw_dir=raw_dir, processed_dir=processed_dir)

    raw_path = raw_dir / "noresults_raw.json"
    structured_path = processed_dir / "noresults_structured.jsonl"

    assert raw_path.exists()
    assert structured_path.exists()

    with raw_path.open("r", encoding="utf-8") as f:
        assert json.load(f) == []

    with structured_path.open("r", encoding="utf-8") as f:
        assert f.read() == ""

    output = capsys.readouterr().out
    assert "hitCount=0" in output

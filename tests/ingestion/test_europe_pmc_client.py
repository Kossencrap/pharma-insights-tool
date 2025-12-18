import pathlib
import sys
from types import SimpleNamespace
import requests

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.ingestion.europe_pmc_client import EuropePMCClient, EuropePMCQuery


def test_search_page_includes_result_type(monkeypatch):
    client = EuropePMCClient()
    captured = {}

    def fake_get(url, *, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return SimpleNamespace(status_code=200, json=lambda: {})

    monkeypatch.setattr(client.session, "get", fake_get)

    q = EuropePMCQuery(query="example", result_type="core")
    client._search_page(q, cursor_mark="*")

    assert captured["params"]["resultType"] == "core"


def test_search_page_legacy_includes_result_type(monkeypatch):
    client = EuropePMCClient()
    captured = {}

    def fake_get(url, *, params=None, timeout=None):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return SimpleNamespace(status_code=200, json=lambda: {})

    monkeypatch.setattr(client.session, "get", fake_get)

    q = EuropePMCQuery(query="example", result_type="core")
    client._search_page_legacy(q, page=1)

    assert captured["params"]["resultType"] == "core"


def test_search_stops_on_empty_page(monkeypatch):
    client = EuropePMCClient()
    payloads = [
        {"resultList": {"result": [
            {"pmid": "1", "title": "t1"}
        ]}, "nextCursorMark": "abc"},
        {"resultList": {"result": []}, "nextCursorMark": "abc"},
    ]

    def fake_fetch(q, cursor_mode, cursor_mark, page, allow_version_stub_fallback):
        return payloads.pop(0), cursor_mode

    monkeypatch.setattr(client, "_fetch_search_payload", fake_fetch)

    results = list(client.search(EuropePMCQuery(query="example")))
    assert len(results) == 1


def test_search_retries_transient_failure(monkeypatch):
    calls = {"count": 0}

    class FlakySession:
        def __init__(self):
            self.headers = {}
            self.trust_env = True
            self.proxies = {}

        def get(self, url, params=None, timeout=None):
            calls["count"] += 1
            if calls["count"] == 1:
                raise requests.exceptions.ConnectionError("transient")
            return SimpleNamespace(status_code=200, json=lambda: {"resultList": {"result": []}})

        def mount(self, *_args, **_kwargs):
            return None

    client = EuropePMCClient(session=FlakySession(), max_retries=2, backoff_factor=0)
    payload, _ = client.fetch_search_page(EuropePMCQuery(query="example"))
    assert payload == {"resultList": {"result": []}}
    assert calls["count"] == 2

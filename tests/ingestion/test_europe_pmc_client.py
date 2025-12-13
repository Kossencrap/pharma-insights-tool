import pathlib
import sys
from types import SimpleNamespace

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

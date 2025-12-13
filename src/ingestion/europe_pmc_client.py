from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .models import EuropePMCSearchResult


EUROPE_PMC_SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
EUROPE_PMC_FULLTEXT_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/{id}/fullTextXML"
EUROPE_PMC_METADATA_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/{id}"  # returns XML/JSON depending


@dataclass(frozen=True)
class EuropePMCQuery:
    """
    A structured query representation you can store/log for reproducibility.
    """
    query: str
    page_size: int = 100
    format: str = "json"  # Europe PMC supports json, xml
    sort: str = "P_PDATE_D desc"  # publication date descending (Europe PMC requires order)


class EuropePMCClient:
    """
    Europe PMC REST client for ingestion.

    Goals:
    - deterministic pagination
    - retry/backoff
    - normalized output models
    - optional raw record retention for auditability
    """

    def __init__(
        self,
        timeout_s: int = 30,
        max_retries: int = 5,
        backoff_factor: float = 0.5,
        user_agent: str = "pharma-insights-tool/0.1 (+https://github.com/Kossencrap/pharma-insights-tool)",
        polite_delay_s: float = 0.0,
        *,
        trust_env: bool = True,
        proxies: Optional[Dict[str, str]] = None,
    ) -> None:
        self.timeout_s = timeout_s
        self.polite_delay_s = polite_delay_s

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})
        # Allow callers to bypass environment proxy variables when local proxies block access.
        self.session.trust_env = trust_env
        if proxies:
            self.session.proxies.update(proxies)

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    # --------------------------
    # Query helpers (drug/product)
    # --------------------------

    @staticmethod
    def build_drug_query(
        *,
        product_names: List[str],
        require_abstract: bool = True,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
        include_reviews: bool = True,
        include_trials: bool = True,
        additional_terms: Optional[List[str]] = None,
    ) -> str:
        """
        Build a pragmatic Europe PMC query for product-centric monitoring.

        Notes:
        - Europe PMC query syntax supports fielded queries like TITLE:"..." ABSTRACT:"..."
        - We usually start broad: TITLE/ABSTRACT mention of product names.
        - You can tighten later with (INDICATION terms, "trial", "safety", etc.)

        Example output:
        (TITLE:"dupilumab" OR ABSTRACT:"dupilumab" OR TITLE:"Dupixent" OR ABSTRACT:"Dupixent")
        AND FIRST_PDATE:[2024-01-01 TO 2025-12-31]
        """
        if not product_names:
            raise ValueError("product_names must be non-empty")

        # Search in title/abstract for each name (case-insensitive on backend).
        name_clauses: List[str] = []
        for name in product_names:
            safe = name.replace('"', '\\"')
            name_clauses.append(f'TITLE:"{safe}"')
            name_clauses.append(f'ABSTRACT:"{safe}"')

        q = "(" + " OR ".join(name_clauses) + ")"

        # Optional date range
        if from_date or to_date:
            start = (from_date or date(1900, 1, 1)).isoformat()
            end = (to_date or date.today()).isoformat()
            q += f" AND FIRST_PDATE:[{start} TO {end}]"

        # Optional “must have abstract”
        if require_abstract:
            q += " AND HAS_ABSTRACT:Y"

        # Optional high-level type filters (kept loose; refine later)
        # Europe PMC has PUB_TYPE and/or field terms depending on record;
        # simplest is to add keywords in title/abstract when needed.
        type_terms: List[str] = []
        if include_trials:
            type_terms.append('(TITLE:"trial" OR ABSTRACT:"trial" OR TITLE:"randomized" OR ABSTRACT:"randomized")')
        if include_reviews:
            type_terms.append('(TITLE:"review" OR ABSTRACT:"review" OR PUB_TYPE:"Review")')

        # If you want to restrict to either trials or reviews, add additional_terms or tighten logic in your calling layer.
        # Here we DON'T force type_terms, because it can drop relevant papers.
        # We'll leave this off by default for recall and let downstream classifiers filter.

        if additional_terms:
            # Free-form terms, ORed inside a group
            safe_terms = []
            for t in additional_terms:
                t = t.replace('"', '\\"')
                safe_terms.append(f'"{t}"')
            q += " AND (" + " OR ".join(safe_terms) + ")"

        return q

    # --------------------------
    # Search & pagination
    # --------------------------

    def search(
        self,
        q: EuropePMCQuery,
        *,
        max_records: Optional[int] = None,
        initial_payload: Optional[Dict[str, Any]] = None,
        use_cursor: bool = True,
        allow_version_stub_fallback: bool = True,
    ) -> Iterable[EuropePMCSearchResult]:
        """
        Stream normalized search results.

        Pagination strategy:
        - cursorMark-based pagination (required when using certain sort orders)
        - stop when we have no results or max_records reached
        """
        cursor = "*"
        page = 1
        yielded = 0

        payload = initial_payload
        cursor_mode = use_cursor

        while True:
            if payload is None:
                if self.polite_delay_s > 0:
                    time.sleep(self.polite_delay_s)

                payload, cursor_mode = self._fetch_search_payload(
                    q,
                    cursor_mode=cursor_mode,
                    cursor_mark=cursor,
                    page=page,
                    allow_version_stub_fallback=allow_version_stub_fallback,
                )
            elif self._is_version_stub(payload):
                if cursor_mode and cursor == "*" and allow_version_stub_fallback:
                    page = 1
                    payload, cursor_mode = self._fetch_search_payload(
                        q,
                        cursor_mode=False,
                        cursor_mark=cursor,
                        page=page,
                        allow_version_stub_fallback=allow_version_stub_fallback,
                    )
                else:
                    self._raise_version_stub_error()

            if self._is_version_stub(payload):
                self._raise_version_stub_error()
            hits = payload.get("resultList", {}).get("result", []) or []

            if not hits:
                break

            for rec in hits:
                yield self._normalize_record(rec)
                yielded += 1
                if max_records is not None and yielded >= max_records:
                    return

            next_cursor = payload.get("nextCursorMark")
            if cursor_mode:
                if not next_cursor or next_cursor == cursor:
                    break
                cursor = next_cursor
            else:
                page += 1
            payload = None

    def search_to_list(
        self,
        q: EuropePMCQuery,
        *,
        max_records: Optional[int] = None,
    ) -> List[EuropePMCSearchResult]:
        return list(self.search(q, max_records=max_records))

    def fetch_search_page(
        self,
        q: EuropePMCQuery,
        *,
        cursor_mark: str = "*",
        page: int = 1,
        use_cursor: bool = True,
        allow_version_stub_fallback: bool = True,
    ) -> Tuple[Dict[str, Any], bool]:
        """Public helper to fetch a single search page for diagnostics."""
        payload, cursor_mode = self._fetch_search_payload(
            q,
            cursor_mode=use_cursor,
            cursor_mark=cursor_mark,
            page=page,
            allow_version_stub_fallback=allow_version_stub_fallback,
        )
        if self._is_version_stub(payload):
            self._raise_version_stub_error()
        return payload, cursor_mode

    def _search_page(self, q: EuropePMCQuery, *, cursor_mark: str) -> Dict[str, Any]:
        params = {
            "query": q.query,
            "format": q.format,
            "pageSize": q.page_size,
            "cursorMark": cursor_mark,
            "sort": self._validate_sort(q.sort),
        }
        r = self.session.get(EUROPE_PMC_SEARCH_URL, params=params, timeout=self.timeout_s)
        if r.status_code != 200:
            raise RuntimeError(f"Europe PMC search failed: HTTP {r.status_code} - {r.text[:300]}")
        try:
            return r.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Europe PMC returned non-JSON response: {e}") from e

    def _search_page_legacy(self, q: EuropePMCQuery, *, page: int) -> Dict[str, Any]:
        params = {
            "query": q.query,
            "format": q.format,
            "pageSize": q.page_size,
            "page": page,
            "sort": self._validate_sort(q.sort),
        }
        r = self.session.get(EUROPE_PMC_SEARCH_URL, params=params, timeout=self.timeout_s)
        if r.status_code != 200:
            raise RuntimeError(f"Europe PMC search failed: HTTP {r.status_code} - {r.text[:300]}")
        try:
            return r.json()
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Europe PMC returned non-JSON response: {e}") from e

    def _fetch_search_payload(
        self,
        q: EuropePMCQuery,
        *,
        cursor_mode: bool,
        cursor_mark: str,
        page: int,
        allow_version_stub_fallback: bool,
    ) -> Tuple[Dict[str, Any], bool]:
        if cursor_mode:
            payload = self._search_page(q, cursor_mark=cursor_mark)
            if self._is_version_stub(payload) and cursor_mark == "*" and allow_version_stub_fallback:
                payload = self._search_page_legacy(q, page=page)
                cursor_mode = False
        else:
            payload = self._search_page_legacy(q, page=page)
        return payload, cursor_mode

    @staticmethod
    def _is_version_stub(payload: Dict[str, Any]) -> bool:
        return set(payload.keys()) == {"version"}

    @staticmethod
    def _validate_sort(sort: str) -> str:
        """Ensure a sort order is explicitly provided.

        Europe PMC requires sort parameters to include the order (``asc`` or ``desc``);
        omitting it results in a version-only payload instead of results.
        """

        normalized = sort.strip().lower()
        if normalized.endswith(" asc") or normalized.endswith(" desc"):
            return sort

        raise ValueError(
            "Europe PMC sort must include an explicit order, e.g., 'P_PDATE_D desc'. "
            f"Got: '{sort}'."
        )

    @staticmethod
    def _raise_version_stub_error() -> None:
        raise RuntimeError(
            "Europe PMC returned only a version stub. Common causes include using an invalid "
            "sort parameter (it must include an explicit order, e.g., 'P_PDATE_D desc') or a "
            "proxy/network device filtering the request. Try adding the sort order, running "
            "on a different network, adjusting proxy settings, or using --legacy-pagination "
            "to fall back to page-based requests."
        )

    # --------------------------
    # Optional full text retrieval (OA)
    # --------------------------

    def fetch_fulltext_xml(self, *, pmcid: str) -> str:
        """
        Fetch full text XML for an OA PMC record.
        Input should look like 'PMC1234567' (Europe PMC accepts various IDs).
        """
        url = EUROPE_PMC_FULLTEXT_URL.format(id=pmcid)
        r = self.session.get(url, timeout=self.timeout_s)
        if r.status_code != 200:
            raise RuntimeError(f"Full text fetch failed for {pmcid}: HTTP {r.status_code} - {r.text[:300]}")
        return r.text

    # --------------------------
    # Normalization
    # --------------------------

    @staticmethod
    def _parse_publication_date(raw: Dict[str, Any]) -> Tuple[Optional[date], Optional[int]]:
        """
        Europe PMC records sometimes have:
        - 'firstPublicationDate' (YYYY-MM-DD)
        - 'pubYear' (YYYY)
        - 'journalInfo' variants
        """
        pub_year = None
        pub_date = None

        if raw.get("pubYear"):
            try:
                pub_year = int(raw["pubYear"])
            except Exception:
                pub_year = None

        # Prefer firstPublicationDate if present
        for key in ("firstPublicationDate", "firstPublicationDateLong", "pubDate"):
            val = raw.get(key)
            if not val:
                continue
            if isinstance(val, str):
                # Expect YYYY-MM-DD or YYYY
                try:
                    if len(val) == 10:
                        pub_date = datetime.strptime(val, "%Y-%m-%d").date()
                    elif len(val) == 4:
                        pub_year = pub_year or int(val)
                except Exception:
                    pass
            break

        return pub_date, pub_year

    def _normalize_record(self, rec: Dict[str, Any]) -> EuropePMCSearchResult:
        pub_date, pub_year = self._parse_publication_date(rec)

        is_oa = rec.get("isOpenAccess")
        if isinstance(is_oa, str):
            is_oa = is_oa.upper() == "Y"

        return EuropePMCSearchResult(
            pmid=rec.get("pmid"),
            pmcid=rec.get("pmcid"),
            doi=rec.get("doi"),
            title=rec.get("title") or "",
            abstract=rec.get("abstractText"),
            journal=(rec.get("journalTitle") or rec.get("journal") or None),
            publication_date=pub_date,
            pub_year=pub_year,
            author_string=rec.get("authorString"),
            first_author=rec.get("firstAuthor"),
            is_open_access=is_oa if isinstance(is_oa, bool) else None,
            cited_by_count=int(rec["citedByCount"]) if rec.get("citedByCount") not in (None, "") else None,
            source=rec.get("source"),
            raw=rec,
        )

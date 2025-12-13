"""CLI ingestion runner for Europe PMC -> structured documents."""

from __future__ import annotations

import argparse
import inspect
import sys
from datetime import date
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import json
from src.analytics import (
    MentionExtractor,
    co_mentions_from_sentence,
    load_product_config,
    mean_sentence_length,
    sentence_counts_by_section,
)
from src.ingestion.europe_pmc_client import EuropePMCClient, EuropePMCQuery
from src.storage import init_db, insert_co_mentions, insert_mentions, insert_sentences, upsert_document
from src.structuring.sentence_splitter import SentenceSplitter
from src.utils.identifiers import build_sentence_id

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def _slug(text: str) -> str:
    return "_".join(text.lower().split())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--product",
        "-p",
        action="append",
        required=True,
        help="Product names/aliases to search (repeatable)",
    )
    parser.add_argument(
        "--from-date",
        type=date.fromisoformat,
        help="Lower bound publication date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--to-date",
        type=date.fromisoformat,
        help="Upper bound publication date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=100,
        help="Cap ingestion for quick runs (default: 100)",
    )
    parser.add_argument(
        "--page-size", type=int, default=100, help="Europe PMC page size (default: 100)"
    )
    parser.add_argument(
        "--include-reviews",
        dest="include_reviews",
        action="store_true",
        default=True,
        help="Include review articles (default: on)",
    )
    parser.add_argument(
        "--exclude-reviews",
        dest="include_reviews",
        action="store_false",
        help="Exclude review articles",
    )
    parser.add_argument(
        "--include-trials",
        dest="include_trials",
        action="store_true",
        default=True,
        help="Include clinical trial language (default: on)",
    )
    parser.add_argument(
        "--exclude-trials",
        dest="include_trials",
        action="store_false",
        help="Exclude clinical trial language",
    )
    parser.add_argument(
        "--output-prefix",
        help="Optional filename prefix; defaults to first product name",
    )
    parser.add_argument(
        "--polite-delay",
        type=float,
        default=0.1,
        help="Seconds to sleep between Europe PMC requests",
    )
    parser.add_argument(
        "--no-proxy",
        action="store_true",
        help="Ignore system HTTP(S) proxy settings for Europe PMC requests",
    )
    parser.add_argument(
        "--proxy",
        action="append",
        metavar="SCHEME=URL",
        help=(
            "Override proxy settings, e.g. https=https://user:pass@proxy:8080. "
            "Repeat the flag to set both http/https."
        ),
    )
    parser.add_argument(
        "--legacy-pagination",
        action="store_true",
        help=(
            "Use page-based pagination instead of cursorMark. Helpful when proxies "
            "strip cursor responses; equivalent to passing --legacy-pagination to the client."
        ),
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Optional SQLite database path for persistence of documents, sentences, and mentions.",
    )
    parser.add_argument(
        "--product-config",
        type=Path,
        default=ROOT / "config" / "products.json",
        help="Path to product dictionary JSON for deterministic mention extraction.",
    )
    return parser.parse_args()


def _parse_proxy_overrides(proxy_args: List[str] | None) -> dict[str, str]:
    proxies: dict[str, str] = {}
    for entry in proxy_args or []:
        if "=" not in entry:
            raise ValueError("Proxy override must be in SCHEME=URL format")
        scheme, url = entry.split("=", 1)
        scheme = scheme.strip().lower()
        url = url.strip()
        if not scheme or not url:
            raise ValueError("Proxy override requires non-empty scheme and URL")
        proxies[scheme] = url
    return proxies


def run_ingestion(
    product_names: List[str],
    args: argparse.Namespace,
    *,
    client: EuropePMCClient | None = None,
    splitter: SentenceSplitter | None = None,
    raw_dir: Path = RAW_DIR,
    processed_dir: Path = PROCESSED_DIR,
) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    db_path: Path | None = getattr(args, "db", None)
    product_config: Path | None = getattr(args, "product_config", None)

    conn = init_db(db_path) if db_path else None

    mention_extractor: MentionExtractor | None = None
    if product_config and product_config.exists():
        product_dict = load_product_config(product_config)
        mention_extractor = MentionExtractor(product_dict)
        print(
            f"Loaded {len(product_dict)} products from {product_config} for mention extraction."
        )
    elif db_path:
        print(
            f"No product config found at {product_config}; skipping mention extraction while still writing documents to DB."
        )

    proxy_args = getattr(args, "proxy", None)
    no_proxy = getattr(args, "no_proxy", False)

    try:
        proxies = _parse_proxy_overrides(proxy_args)
    except ValueError as exc:
        print(f"Invalid proxy configuration: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    if no_proxy:
        print("Proxy usage disabled for Europe PMC requests (trust_env=False).")
    if proxies:
        proxy_keys = ", ".join(sorted(proxies))
        print(f"Using custom proxy overrides for Europe PMC requests: {proxy_keys}.")

    client_kwargs = {"polite_delay_s": args.polite_delay}
    init_params = inspect.signature(EuropePMCClient.__init__).parameters
    if "trust_env" in init_params:
        client_kwargs["trust_env"] = not no_proxy
    if "proxies" in init_params:
        client_kwargs["proxies"] = proxies or None

    if args.legacy_pagination:
        print("Using legacy page-based pagination for Europe PMC requests.")

    client = client or EuropePMCClient(**client_kwargs)
    splitter = splitter or SentenceSplitter()

    query_str = client.build_drug_query(
        product_names=product_names,
        require_abstract=True,
        from_date=args.from_date,
        to_date=args.to_date,
        include_reviews=args.include_reviews,
        include_trials=args.include_trials,
    )

    query = EuropePMCQuery(query=query_str, page_size=args.page_size)

    try:
        first_page, cursor_mode = client.fetch_search_page(
            query,
            cursor_mark="*",
            use_cursor=not args.legacy_pagination,
        )
    except RuntimeError as exc:
        print(
            "Europe PMC search request failed (often caused by blocked proxies or network restrictions):",
            exc,
            file=sys.stderr,
        )
        raise

    hit_count = first_page.get("hitCount")
    first_hits = first_page.get("resultList", {}).get("result", []) or []

    prefix = args.output_prefix or _slug(product_names[0])
    raw_path = raw_dir / f"{prefix}_raw.json"
    structured_path = processed_dir / f"{prefix}_structured.jsonl"

    if hit_count is not None:
        print(f"Europe PMC reported hitCount={hit_count} for the query.")
    else:
        print("Europe PMC did not include hitCount; continuing based on returned results.")

    if not first_hits:
        print(f"No Europe PMC results returned for query: {query.query}")
        print("Check product spelling, relax date filters, or rerun without --exclude flags.")

        with raw_path.open("w", encoding="utf-8") as f:
            json.dump([], f, indent=2)

        structured_path.touch()
        print(f"Raw records written to: {raw_path}")
        print(f"Structured documents written to: {structured_path}")
        return

    results = list(
        client.search(
            query,
            max_records=args.max_records,
            initial_payload=first_page,
            use_cursor=cursor_mode,
        )
    )

    with raw_path.open("w", encoding="utf-8") as f:
        json.dump([r.raw for r in results], f, indent=2)

    documents = []
    with structured_path.open("w", encoding="utf-8") as f:
        for record in results:
            doc = splitter.split_document(record)
            documents.append(doc)
            f.write(json.dumps(doc.to_dict()) + "\n")

            if conn:
                upsert_document(conn, doc, raw_json=record.raw)

                sentence_rows = []
                mention_batches: list[tuple[str, list[tuple[str, str, str, int, int, str]]]] = []
                co_batches: list[tuple[str, list[tuple[str, str, int]]]] = []
                for sentence in doc.iter_sentences():
                    sentence_id = build_sentence_id(doc.doc_id, sentence.section, sentence.index)
                    sentence_rows.append((sentence_id, sentence))

                    if mention_extractor:
                        mentions = mention_extractor.extract(sentence.text)
                        if mentions:
                            mention_rows = [
                                (
                                    f"{sentence_id}:{m.product_canonical}:{m.start_char}-{m.end_char}",
                                    m.product_canonical,
                                    m.alias_matched,
                                    m.start_char,
                                    m.end_char,
                                    m.match_method,
                                )
                                for m in mentions
                            ]
                            mention_batches.append((sentence_id, mention_rows))

                            co_pairs = co_mentions_from_sentence(mentions)
                            if co_pairs:
                                co_batches.append((sentence_id, co_pairs))

                if sentence_rows:
                    insert_sentences(conn, doc.doc_id, sentence_rows)
                for sentence_id, mention_rows in mention_batches:
                    insert_mentions(conn, doc.doc_id, sentence_id, mention_rows)
                for sentence_id, co_pairs in co_batches:
                    insert_co_mentions(conn, doc.doc_id, sentence_id, co_pairs)

    print(f"Ingested {len(results)} documents for query: {query.query}")
    if documents:
        section_counts = sentence_counts_by_section(documents[0])
        mean_len = mean_sentence_length(documents[0])
        print(f"Example sentence counts: {section_counts}")
        print(f"Mean sentence length (first doc): {mean_len:.1f} chars")
    print(f"Raw records written to: {raw_path}")
    print(f"Structured documents written to: {structured_path}")

    if conn:
        conn.commit()
        conn.close()
        print(f"Persisted documents and mentions to SQLite at {db_path}")


def main() -> None:
    args = parse_args()
    run_ingestion(product_names=args.product, args=args)


if __name__ == "__main__":
    main()

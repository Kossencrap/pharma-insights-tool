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
    ProductMention,
    co_mentions_from_sentence,
    load_product_config,
    mean_sentence_length,
    sentence_counts_by_section,
)
from src.ingestion.europe_pmc_client import EuropePMCClient, EuropePMCQuery
from src.storage import (
    init_db,
    insert_co_mentions,
    insert_mentions,
    insert_sentences,
    get_ingest_status,
    update_ingest_status,
    upsert_document,
)
from src.structuring.sentence_splitter import SentenceSplitter
from src.utils.identifiers import build_sentence_id

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def _slug(text: str) -> str:
    return "_".join(text.lower().split())


def _default_status_key(product_names: List[str], include_reviews: bool, include_trials: bool) -> str:
    base = "-".join(sorted(p.lower() for p in product_names))
    return f"{base}|reviews={int(include_reviews)}|trials={int(include_trials)}"


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
        "--incremental",
        action="store_true",
        help="Resume from the last stored publication date for this query key.",
    )
    parser.add_argument(
        "--status-key",
        help="Override the default status key used for incremental ingestion state.",
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

    incremental = getattr(args, "incremental", False)
    status_key = getattr(args, "status_key", None)
    resolved_status_key = None
    effective_from_date = args.from_date

    if incremental:
        if conn is None:
            print("Incremental ingestion requires --db to be set.", file=sys.stderr)
            raise SystemExit(2)

        resolved_status_key = status_key or _default_status_key(
            product_names, args.include_reviews, args.include_trials
        )
        stored_status = get_ingest_status(conn, resolved_status_key)

        if stored_status:
            last_publication_date, last_pmid = stored_status
            if last_publication_date:
                if effective_from_date is None or last_publication_date > effective_from_date:
                    effective_from_date = last_publication_date
            if effective_from_date:
                print(
                    "Incremental mode active: resuming from publication date "
                    f"{effective_from_date} using status key '{resolved_status_key}'."
                )
            else:
                print(
                    "Incremental mode active but no stored publication watermark found; "
                    f"status key '{resolved_status_key}'."
                )
            if last_pmid:
                print(f"Last ingested PMID for this key: {last_pmid}")
        else:
            print(
                "Incremental mode active with no previous status; starting fresh from provided filters."
            )
    else:
        if status_key:
            resolved_status_key = status_key

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
        from_date=effective_from_date,
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
                doc_mentions: list[ProductMention] = []
                for sentence in doc.iter_sentences():
                    sentence_id = build_sentence_id(doc.doc_id, sentence.section, sentence.index)
                    sentence_rows.append((sentence_id, sentence))

                    if mention_extractor:
                        mentions = mention_extractor.extract(sentence.text)
                        if mentions:
                            doc_mentions.extend(mentions)
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

                if sentence_rows:
                    insert_sentences(conn, doc.doc_id, sentence_rows)
                for sentence_id, mention_rows in mention_batches:
                    insert_mentions(conn, doc.doc_id, sentence_id, mention_rows)
                if mention_extractor and doc_mentions:
                    co_mention_pairs = co_mentions_from_sentence(doc_mentions)
                    if co_mention_pairs:
                        insert_co_mentions(conn, doc.doc_id, co_mention_pairs)

    print(f"Ingested {len(results)} documents for query: {query.query}")
    if documents:
        section_counts = sentence_counts_by_section(documents[0])
        mean_len = mean_sentence_length(documents[0])
        print(f"Example sentence counts: {section_counts}")
        print(f"Mean sentence length (first doc): {mean_len:.1f} chars")

    latest_pub_date = max(
        (doc.publication_date for doc in documents if doc.publication_date),
        default=None,
    )
    latest_pmid = None
    if latest_pub_date:
        for doc in sorted(
            documents,
            key=lambda d: (d.publication_date or date.min, d.pmid or ""),
            reverse=True,
        ):
            if doc.publication_date == latest_pub_date:
                latest_pmid = doc.pmid
                break

    if incremental and conn:
        status_identifier = resolved_status_key or _default_status_key(
            product_names, args.include_reviews, args.include_trials
        )
        update_ingest_status(
            conn,
            status_identifier,
            last_publication_date=latest_pub_date,
            last_pmid=latest_pmid,
        )
        print(
            "Updated incremental status "
            f"({status_identifier}) to publication date {latest_pub_date}"
        )
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

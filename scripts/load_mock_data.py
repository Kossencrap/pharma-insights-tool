"""Load rule-exercising mock insulin/metformin documents into SQLite."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from src.analytics.indication_extractor import IndicationExtractor, load_indication_config
from src.analytics.mention_extractor import (
    MentionExtractor,
    ProductMention,
    co_mentions_from_sentence,
    load_product_config,
)
from src.analytics.weights import compute_document_weight, load_study_type_weights
from src.storage import (
    init_db,
    insert_co_mentions,
    insert_co_mentions_sentences,
    insert_mentions,
    insert_sentence_indications,
    insert_sentences,
    upsert_document,
    upsert_document_weight,
)
from src.utils.identifiers import build_sentence_id


@dataclass
class MockDocument:
    doc_id: str
    source: str | None
    title: str | None
    abstract: str | None
    journal: str | None
    publication_date: date | None
    pub_year: int | None
    study_design: str | None
    study_phase: str | None
    sample_size: int | None
    pmid: str | None = None
    pmcid: str | None = None
    doi: str | None = None


@dataclass
class MockSentence:
    text: str
    index: int
    start_char: int
    end_char: int
    section: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mock-file",
        type=Path,
        default=Path("data/mock/mock_documents.json"),
        help="Path to the JSON file containing mock document specs.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("data/mock/mock.sqlite"),
        help="SQLite database to seed with the mock documents.",
    )
    parser.add_argument(
        "--products",
        type=Path,
        default=Path("config/products.json"),
        help="Product alias config for mention extraction.",
    )
    parser.add_argument(
        "--study-weights",
        type=Path,
        default=Path("config/study_type_weights.json"),
        help="Study-weight config used for document weighting.",
    )
    parser.add_argument(
        "--indications",
        type=Path,
        default=Path("config/indications.json"),
        help="Indication alias config for indication extraction.",
    )
    parser.add_argument(
        "--sentiment-output",
        type=Path,
        default=Path("data/mock/mock_sentences.jsonl"),
        help="Destination JSONL for sentiment labeling input.",
    )
    return parser.parse_args()


def _load_specs(path: Path) -> List[dict]:
    if not path.exists():
        raise SystemExit(f"Mock data file not found at {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise SystemExit("Mock data file must contain a JSON array of documents.")
    return data


def _build_document(spec: dict) -> Tuple[MockDocument, List[Tuple[str, MockSentence]]]:
    pub_date: date | None = None
    if spec.get("publication_date"):
        pub_date = date.fromisoformat(spec["publication_date"])
    document = MockDocument(
        doc_id=spec["doc_id"],
        source=spec.get("source"),
        title=spec.get("title"),
        abstract=spec.get("abstract"),
        journal=spec.get("journal"),
        publication_date=pub_date,
        pub_year=pub_date.year if pub_date else None,
        study_design=spec.get("study_design"),
        study_phase=spec.get("study_phase"),
        sample_size=spec.get("sample_size"),
    )

    sentence_rows: List[Tuple[str, MockSentence]] = []
    section_texts: Dict[str, str] = {}
    sent_index = 0

    for section_spec in spec.get("sections", []):
        name = section_spec.get("name", "section")
        offset = 0
        parts: List[str] = []
        for raw_text in section_spec.get("sentences", []):
            text = raw_text.strip()
            parts.append(text)
            sentence = MockSentence(
                text=text,
                index=sent_index,
                start_char=offset,
                end_char=offset + len(text),
                section=name,
            )
            sent_id = build_sentence_id(document.doc_id, name, sent_index)
            sentence_rows.append((sent_id, sentence))
            sent_index += 1
            offset += len(text) + 1
        section_texts[name.lower()] = " ".join(parts)

    if not document.abstract and section_texts:
        for key in ("abstract", "summary", "results", "findings"):
            if key in section_texts:
                document.abstract = section_texts[key]
                break
        if not document.abstract:
            document.abstract = next(iter(section_texts.values()))

    return document, sentence_rows


def _insert_mentions_for_sentence(
    conn,
    doc_id: str,
    sentence_id: str,
    mentions: Sequence[ProductMention],
) -> None:
    if not mentions:
        return
    mention_rows = [
        (
            f"{sentence_id}:{mention.product_canonical}:{mention.start_char}-{mention.end_char}",
            mention.product_canonical,
            mention.alias_matched,
            mention.start_char,
            mention.end_char,
            mention.match_method,
        )
        for mention in mentions
    ]
    insert_mentions(conn, doc_id, sentence_id, mention_rows)


def _insert_indications_for_sentence(
    conn,
    doc_id: str,
    sentence_id: str,
    indications,
) -> None:
    if not indications:
        return
    rows = [
        (
            indication.indication_canonical,
            indication.alias_matched,
            indication.start_char,
            indication.end_char,
        )
        for indication in indications
    ]
    insert_sentence_indications(conn, doc_id, sentence_id, rows)


def _aggregate_document_comentions(
    sentences: Iterable[Tuple[str, Sequence[ProductMention]]]
) -> List[Tuple[str, str, int]]:
    doc_counts: Dict[Tuple[str, str], int] = {}
    for _, mentions in sentences:
        for a, b, count in co_mentions_from_sentence(mentions):
            key = (a, b)
            doc_counts[key] = doc_counts.get(key, 0) + count
    return [(a, b, cnt) for (a, b), cnt in doc_counts.items()]


def _write_sentiment_records(path: Path, records: Sequence[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record))
            fh.write("\n")


def main() -> None:
    args = _parse_args()
    specs = _load_specs(args.mock_file)
    product_aliases = load_product_config(args.products)
    mention_extractor = MentionExtractor(product_aliases)
    indication_aliases: Dict[str, List[str]] = {}
    if args.indications.exists():
        indication_aliases = load_indication_config(args.indications)
    indication_extractor = IndicationExtractor(indication_aliases) if indication_aliases else None
    study_weights = load_study_type_weights(args.study_weights)
    conn = init_db(args.db)

    total_docs = 0
    total_sentences = 0
    total_mentions = 0
    sentiment_records: List[dict] = []

    for spec in specs:
        document, sentence_rows = _build_document(spec)
        upsert_document(conn, document, raw_json={"study_type": spec.get("study_design")})
        weight = compute_document_weight(
            doc_id=document.doc_id,
            publication_date=document.publication_date,
            raw_metadata={"study_type": spec.get("study_design")},
            weight_lookup=study_weights,
        )
        upsert_document_weight(conn, weight)
        insert_sentences(conn, document.doc_id, sentence_rows)

        sentence_mentions: List[Tuple[str, Sequence[ProductMention]]] = []
        sentence_pair_rows: List[Tuple[str, str, str, int]] = []

        for sentence_id, sentence in sentence_rows:
            mentions = mention_extractor.extract(sentence.text)
            sentence_mentions.append((sentence_id, mentions))
            total_mentions += len(mentions)
            total_sentences += 1
            _insert_mentions_for_sentence(conn, document.doc_id, sentence_id, mentions)
            if indication_extractor:
                indications = indication_extractor.extract(sentence.text)
                _insert_indications_for_sentence(
                    conn, document.doc_id, sentence_id, indications
                )
            for a, b, count in co_mentions_from_sentence(mentions):
                sentence_pair_rows.append((sentence_id, a, b, count))
                sentiment_records.append(
                    {
                        "doc_id": document.doc_id,
                        "sentence_id": sentence_id,
                        "product_a": a,
                        "product_b": b,
                        "sentence_text": sentence.text,
                        "section": sentence.section,
                        "publication_date": document.publication_date.isoformat()
                        if document.publication_date
                        else None,
                        "count": count,
                    }
                )

        if sentence_pair_rows:
            insert_co_mentions_sentences(conn, document.doc_id, sentence_pair_rows)

        doc_pairs = _aggregate_document_comentions(sentence_mentions)
        if doc_pairs:
            insert_co_mentions(conn, document.doc_id, doc_pairs)

        total_docs += 1

    conn.commit()
    sentiment_path = args.sentiment_output
    if sentiment_records:
        _write_sentiment_records(sentiment_path, sentiment_records)
        print(f"Wrote {len(sentiment_records)} sentiment-ready rows to {sentiment_path}")
    else:
        sentiment_path.parent.mkdir(parents=True, exist_ok=True)
        sentiment_path.write_text("", encoding="utf-8")
        print(f"No sentiment-ready rows generated; created empty file at {sentiment_path}")

    print(
        f"Loaded {total_docs} mock documents, {total_sentences} sentences, and {total_mentions} mentions into {args.db}."
    )


if __name__ == "__main__":
    main()

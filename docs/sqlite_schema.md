# SQLite schema reference

This document captures the lightweight schema used by the Phase 1 pipeline. It
is intended as a quick map between pipeline stages and the tables they touch so
that new contributors can troubleshoot runs without reading the code.

## Core tables

### `documents`
Stores normalized publication-level metadata (title, abstract, identifiers).
Populated by `scripts/ingest_europe_pmc.py`.

Key columns:
- `doc_id` (primary key): Stable identifier derived from Europe PMC IDs.
- `pmid`/`pmcid`: Published identifiers when available.
- `journal`, `publication_date`: Publication context.

### `document_weights`
Holds per-document weights derived from recency and study type. Populated by
`ingest_europe_pmc.py` using `src.analytics.weights.compute_document_weight`.

Key columns:
- `doc_id` (primary key, foreign key to `documents`)
- `recency_weight`, `study_type`, `study_type_weight`, `combined_weight`

### `sentences`
Sentence-level slices for each document section. Populated during ingestion via
`SentenceSplitter`.

Key columns:
- `sentence_id` (primary key): `${doc_id}:{section}:{index}`
- `section`, `sent_index`, `text`

### `product_mentions`
Exact-product matches found in each sentence. Populated during ingestion via
`MentionExtractor.extract`.

Key columns:
- `doc_id` + `sentence_id` + positional keys
- `product_canonical`, `alias_matched`, `match_method`

### `sentence_indications`
Deterministic indication/use-case mentions detected per sentence. Populated
during ingestion via `IndicationExtractor.extract`.

Key columns:
- `doc_id`, `sentence_id`
- `indication_canonical`, `alias_matched`, `start_char`, `end_char`

### `co_mentions`
Document-level product pairs. Populated during ingestion when a document
contains multiple products.

Key columns:
- `doc_id` + `product_a` + `product_b` (sorted lexicographically)
- `count` (occurrence count within the document)

### `co_mentions_sentences`
Sentence-level product pairs derived from `product_mentions`. Populated during
ingestion.

Key columns:
- `sentence_id` + `product_a` + `product_b`
- `count` (occurrence count within the sentence)

### `sentence_events`
Context labels for each sentence co-mention (comparative terms, risk terms,
study context). Populated by `scripts/label_sentence_events.py` and later
updated with sentiment by `scripts/label_sentence_sentiment.py`.

Key columns:
- `doc_id`, `sentence_id`, `product_a`, `product_b` (composite primary key)
- Label columns: `comparative_terms`, `relationship_types`, `risk_terms`,
  `study_context`, `matched_terms`
- `section` (canonicalized abstract section per `config/section_aliases.json`)
- Narrative columns: `narrative_type`, `narrative_subtype`,
  `narrative_confidence`
- Sentiment: `sentiment_label`, `sentiment_score`, `sentiment_model`,
  `sentiment_inference_ts`

### `ingest_status`
Watermark per ingestion key for incremental runs. Populated and updated by
`ingest_europe_pmc.py`.

Key columns:
- `status_key` (primary key)
- `last_publication_date`, `last_pmid`

## Derived aggregates and exports

Aggregates written by `scripts/aggregate_metrics.py` and
`scripts/export_sentiment_metrics.py` are exported to Parquet/CSV; they are not
stored as tables in SQLite. Evidence exports are produced by
`scripts/export_batch.py` and include confidence breakdowns for each sentence
row.

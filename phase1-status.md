# Phase 1 implementation status

## Completed or in-flight capabilities
- **Europe PMC ingestion pipeline** with reproducible queries, retry/pagination, and optional proxy control is available via the `EuropePMCClient`, aligning with the scoped data source for Phase 1 ingestion. 【F:src/ingestion/europe_pmc_client.py†L23-L155】
- **Document structuring and deduplication** converts ingestion results into normalized documents with title/abstract sections and identifier cleanup, giving us sentence-level containers for later analytics. 【F:src/structuring/models.py†L11-L198】
- **Rule-based product signal extraction** already detects product mentions and co-mentions, providing the competitive context foundation without requiring heavy models. 【F:src/analytics/mention_extractor.py†L11-L154】
- **Time-series aggregation utilities** compute weekly/monthly buckets plus change metrics, covering the “volume & momentum” metric expected in early dashboards. 【F:src/analytics/time_series.py†L9-L124】
- **Metrics dashboards and sentiment exports** are now available through Streamlit and a sentiment ratios export, covering the evidence-linked visual layer for Phase 1 metrics. 【F:scripts/metrics_dashboard.py†L1-L276】【F:scripts/export_sentiment_metrics.py†L1-L111】
- **CLI runners and helper scripts** exist for ingestion, sentence splitting, labeling, export, and co-mention inspection, enabling end-to-end dry runs on small batches. 【F:README.md†L32-L166】
- **Test scaffolding** spans ingestion, structuring, analytics, and export flows, signaling that key pieces of the pipeline already have regression coverage. 【F:README.md†L65-L80】
- **Sentence-level context labeling and evidence exports** are available via `label_sentence_events.py`, `context_labels.py`, and the batch export/evidence helpers. 【F:scripts/label_sentence_events.py†L1-L112】【F:src/analytics/context_labels.py†L1-L208】【F:scripts/export_batch.py†L1-L210】

## Not yet covered for Phase 1 goals
- **Sentiment classification** is not implemented yet; current scope explicitly excludes ML-based NLP modeling, so the narrative direction layer remains to be built. 【F:README.md†L118-L121】

## Overall readout
The ingestion-to-structuring path and supporting analytics utilities are in place, so Phase 1 is well underway on data acquisition, normalization, and early competitive signals. The remaining gap to declare Phase 1 complete is adding the sentiment detection layer and exposing evidence-linked dashboards that answer the MVP questions with traceable outputs.

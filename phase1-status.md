# Phase 1 implementation status

## Completed or in-flight capabilities
- **Europe PMC ingestion pipeline** with reproducible queries, retry/pagination, and optional proxy control is available via the `EuropePMCClient`, aligning with the scoped data source for Phase 1 ingestion. 【F:src/ingestion/europe_pmc_client.py†L23-L155】
- **Document structuring and deduplication** converts ingestion results into normalized documents with title/abstract sections and identifier cleanup, giving us sentence-level containers for later analytics. 【F:src/structuring/models.py†L11-L198】
- **Rule-based product signal extraction** already detects product mentions and co-mentions, providing the competitive context foundation without requiring heavy models. 【F:src/analytics/mention_extractor.py†L11-L154】
- **Time-series aggregation utilities** compute weekly/monthly buckets plus change metrics, covering the “volume & momentum” metric expected in early dashboards. 【F:src/analytics/time_series.py†L9-L124】
- **Metrics dashboards and sentiment exports** are now available through Streamlit and a sentiment ratios export, covering the evidence-linked visual layer for Phase 1 metrics. 【F:scripts/metrics_dashboard.py†L1-L276】【F:scripts/export_sentiment_metrics.py†L1-L111】
- **Deterministic sentiment classification** is implemented via the sentence sentiment labeling pipeline, enabling narrative direction analysis with evidence-linked exports. 【F:scripts/label_sentence_sentiment.py†L1-L188】【F:src/analytics/sentiment.py†L1-L247】
- **CLI runners and helper scripts** exist for ingestion, sentence splitting, labeling, export, and co-mention inspection, enabling end-to-end dry runs on small batches. 【F:README.md†L32-L166】
- **Test scaffolding** spans ingestion, structuring, analytics, and export flows, signaling that key pieces of the pipeline already have regression coverage. 【F:README.md†L65-L80】
- **Sentence-level context labeling and evidence exports** are available via `label_sentence_events.py`, `context_labels.py`, and the batch export/evidence helpers. 【F:scripts/label_sentence_events.py†L1-L112】【F:src/analytics/context_labels.py†L1-L208】【F:scripts/export_batch.py†L1-L210】

## Phase 1 operational hardening (now wired into checks)
- **Run the full pytest suite** after installing dependencies to validate regression coverage end to end. This is the default in `powershell/functional_checks.ps1` unless `-SkipPytests` is set. 【F:README.md†L103-L112】【F:powershell/functional_checks.ps1†L33-L57】
- **Ensure sentiment labeling is always included in functional checks** by keeping `label_sentence_sentiment.py` in the default run. The functional checks now resolve the structured JSONL path automatically so sentiment labeling runs whenever the data exists. 【F:powershell/functional_checks.ps1†L90-L122】
- **Refresh Phase 1 status documentation** when new capabilities land to keep MVP readiness current (this update). 【F:phase1-status.md†L1-L14】

## Overall readout
Phase 1 MVP requirements are now functionally met: ingestion, narrative/sentiment metrics, co-mentions, and evidence-linked dashboards work together for at least one product. The remaining Phase 1 work is operational hardening (making sentiment labeling part of every workflow, running the full test suite regularly, and keeping status docs up to date) before moving on to Phase 2 data sources and richer analytics.

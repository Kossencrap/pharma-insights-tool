# Pharma Insights Tool

Product-centric scientific and narrative intelligence
based on biomedical literature.

> **Status (2026-01-05):** Phase 1 MVP remains production-ready; Phase 2 narrative/directional features are implemented in code but still tracking validation and KPI work (see `phase1-status.md` + `docs/phase2-narrative.md`).

## Project structure

```
pharma-insights-tool/
├── README.md                        # Project overview, setup, and structure
├── .env.example                     # Sample environment variables for local runs
├── .gitignore                       # Ignored files and directories for git
├── pyproject.toml                   # Core Python packaging and dependency metadata
├── setup.py                         # Packaging entrypoint delegating to pyproject
├── project-plan                     # Narrative requirements and MVP scope document
├── config/                          # Config files that drive ingestion and scoring
│   ├── products.json                # Example product list with IDs and synonyms
│   ├── narratives.json              # Narrative taxonomy (Phase 2) driving labels
│   └── study_type_weights.json      # Heuristic weights for study-type evidence
├── data/                            # Git-ignored datasets and generated outputs
│   ├── artifacts/                   # Derived assets (charts, metrics, exports)
│   │   └── .gitkeep                 # Placeholder to keep folder in repo
│   ├── processed/                   # Normalized JSONL documents from structuring
│   │   └── .gitkeep
│   └── raw/                         # Raw Europe PMC payloads
│       └── .gitkeep
├── notebooks/                       # Jupyter notebooks for exploratory analysis
│   └── exploration/                 # Sandbox for experimentation
│       └── .gitkeep
├── powershell/                      # Windows-friendly helper utilities
│   ├── README.md                    # Instructions to run PowerShell checks
│   └── functional_checks.ps1        # Convenience validation script for Windows
├── scripts/                         # CLI entry points for ingestion and analysis
│   ├── aggregate_metrics.py         # Aggregate and export run-level metrics
│   ├── export_sentiment_metrics.py  # Export sentiment ratios for dashboards
│   ├── export_batch.py              # Persist structured outputs (JSONL/SQLite)
│   ├── ingest_europe_pmc.py         # Pull Europe PMC data with product filters
│   ├── label_sentence_events.py     # Apply heuristic context labels to sentences
│   ├── query_comentions.py          # Query product co-mentions from SQLite
│   ├── show_sentence_evidence.py    # Display labeled sentence evidence
│   ├── split_sentences.py           # Deterministic sentence segmentation helper
│   ├── view_labeled_sentences.py    # Browse labeled sentences and evidence weights
│   ├── metrics_dashboard.py         # Streamlit dashboard for metrics + evidence
│   └── which_doc.py                 # Locate example documents for product pairs
├── src/                             # Application source code
│   ├── __init__.py                  # Package marker
│   ├── analytics/                   # Analytics and labeling logic
│   │   ├── __init__.py              # Analytics package marker
│   │   ├── context_labels.py        # Rule-based context labeling of sentences
│   │   ├── evidence.py              # Sentence-level evidence retrieval helpers
│   │   ├── mention_extractor.py     # Product mention extraction from sentences
│   │   ├── indication_extractor.py  # Deterministic indication/use-case tagging
│   │   ├── narratives.py            # Deterministic narrative classification helpers
│   │   ├── time_series.py           # Time-series metrics for literature trends
│   │   └── weights.py               # Weighting helpers for evidence scoring
│   ├── ingestion/                   # Europe PMC ingestion and query helpers
│   │   ├── __init__.py              # Ingestion package marker
│   │   ├── europe_pmc_client.py     # Requests client with retry/pagination logic
│   │   └── models.py                # Typed ingestion models (queries, results)
│   ├── models/                      # Shared data models
│   │   └── __init__.py              # Model package marker
│   ├── storage/                     # Persistence layer
│   │   ├── __init__.py              # Storage package marker
│   │   └── sqlite_store.py          # SQLite helpers for storing documents
│   ├── structuring/                 # Sentence and document structuring
│   │   ├── __init__.py              # Structuring package marker
│   │   ├── models.py                # Structured representations of documents
│   │   └── sentence_splitter.py     # Deterministic sentence splitter utility
│   └── utils/                       # Shared utilities
│       ├── __init__.py              # Utils package marker
│       └── identifiers.py           # Identifier normalization helpers
└── tests/                           # Automated tests covering ingestion and analytics
    ├── conftest.py                  # Pytest fixtures shared across suites
    ├── test_co_mentions_sentence_level.py  # Co-mention coverage at sentence level
    ├── test_context_labels.py       # Validation for context label heuristics
    ├── test_export_batch.py         # Export pipeline validation
    ├── test_ingestion.py            # End-to-end ingestion runner checks
    ├── test_ingestion_runner.py     # Europe PMC runner integration tests
    ├── test_mentions_and_storage.py # Mention extraction + storage integration
    ├── test_structuring.py          # Sentence structuring behaviour checks
    ├── test_time_series_metrics.py  # Time-series metric computation tests
    ├── analytics/                   # Analytics-focused test helpers
    │   └── test_weights.py          # Evidence weighting test cases
    ├── ingestion/                   # Ingestion-specific fixtures/tests
    │   └── test_europe_pmc_client.py# HTTP client, retry, and query tests
    └── structuring/                 # Structuring-specific fixtures/tests
        └── __init__.py              # Fixture namespace marker
├── README.md                     # Project overview and usage
├── project-plan                  # Narrative requirements and MVP scope
├── pyproject.toml, setup.py      # Python packaging and dependency metadata
├── config/                       # Configuration for local runs and weights
│   ├── products.json             # Sample product list for ingestion
│   └── study_type_weights.json   # Heuristics for evidence weighting
├── data/                         # Datasets and generated artifacts (gitignored)
│   ├── raw/                      # Raw Europe PMC exports
│   ├── processed/                # Structured documents (one JSONL per run)
│   └── artifacts/                # Auxiliary outputs (e.g., charts, metrics)
├── notebooks/
│   └── exploration/.gitkeep      # Placeholder for exploratory notebooks
├── scripts/                      # CLI entry points for ingestion and analysis
│   ├── aggregate_metrics.py      # Aggregate and export run metrics
│   ├── export_sentiment_metrics.py # Export sentiment ratios for dashboards
│   ├── export_batch.py           # Persist structured outputs to disk
│   ├── ingest_europe_pmc.py      # Pull Europe PMC data with product filters
│   ├── label_sentence_events.py  # Apply heuristic labels to sentences
│   ├── query_comentions.py       # Query product co-mentions from SQLite
│   ├── show_sentence_evidence.py # Display evidence for labeled sentences
│   ├── split_sentences.py        # Deterministic sentence segmentation
│   ├── view_labeled_sentences.py # Browse labeled sentences and evidence weights
│   ├── metrics_dashboard.py      # Streamlit dashboard for metrics + evidence
│   └── which_doc.py              # Find example documents for product pairs
├── powershell/                   # Windows-friendly helpers
│   ├── README.md                 # How to run PowerShell checks
│   └── functional_checks.ps1     # Convenience script for local validation
├── src/                          # Application code
│   ├── ingestion/                # Europe PMC ingestion and parsing
│   ├── structuring/              # Sentence and document structuring helpers
│   ├── analytics/                # Co-mention queries and analytics routines
│   ├── storage/                  # Persistence utilities (e.g., SQLite helpers)
│   ├── utils/                    # Shared utilities and logging helpers
│   └── models/                   # Typed data models and schemas
└── tests/                        # Unit tests for ingestion, structuring, analytics
    ├── ingestion/
    ├── structuring/
    └── analytics/
```

## Current scope
- Literature ingestion from Europe PMC
- Deterministic sentence structuring
- No ML-based NLP yet; rule-based context labeling is available

## Pipeline
Europe PMC → Document → Section → Sentence

## Testing
After installing dependencies, run the full pytest suite to validate ingestion and analytics behavior:

```bash
python -m pytest
```

For a scripted end-to-end validation (including pytest and sentiment labeling), see
`powershell/functional_checks.ps1`.

## One-command Phase 1 pipeline
Run the canonical Phase 1 flow (ingestion → labeling → exports) with a single command (see `docs/phase1.md` for acceptance criteria):

```bash
python scripts/run_phase1_pipeline.py \
  --products config/products.json \
  --from-date 2022-01-01 \
  --max-records 250 \
  --db data/europepmc.sqlite \
  --manifest-path data/artifacts/phase1/phase1_run_manifest.json
```

Artifacts (metrics, evidence exports, manifest) are written to `data/artifacts/phase1` by default. The manifest includes input flags, guardrail limits, and pointers to export folders for verification.

Guardrails:
- `--max-sentences-per-doc` (default 400) skips sentences beyond the cap per document
- `--max-co-mentions-per-sentence` (default 50) drops excessive co-mention pairs per sentence
- `--db-size-warn-mb` (default 250) emits warnings when SQLite grows too large

## Running ingestion locally
Use the CLI runner to pull a small batch of Europe PMC results and emit both raw and structured outputs:

```bash
python scripts/ingest_europe_pmc.py -p "dupilumab" -p "Dupixent" --from-date 2022-01-01 --max-records 50
```

Outputs are written to `data/raw/` (raw JSON array) and `data/processed/` (one structured document per line).

For repeat runs, you can enable incremental ingestion and persist a watermark in SQLite:

```bash
python scripts/ingest_europe_pmc.py -p "dupilumab" --db data/europepmc.sqlite --incremental
```

The default status key is derived from the product list plus review/trial filters, but you can override it with
`--status-key` when you need multiple independent watermarks.

### Inspecting co-mentions (document-level)
After running ingestion with `--db data/europepmc.sqlite`, you can query document-level co-mentions directly from SQLite without storing sentence-level pairs:

```bash
# Top pairs across documents
python scripts/query_comentions.py --db data/europepmc.sqlite --limit 25

# Find example documents that mention both products
python scripts/which_doc.py metformin insulin --db data/europepmc.sqlite
```

### Labeling sentence sentiment
Once you have sentence-level JSONL (for example, extracted into `data/processed/`), you can
annotate it with deterministic sentiment labels:

```bash
python scripts/label_sentence_sentiment.py --input data/processed/example_sentences.jsonl
```

### Narrative taxonomy (Phase 2)
Narrative labels, precedence, and the supporting term lists are defined in
`config/narratives.json`. Update this file to add new narrative families, adjust
priorities, or expand the context terms without touching code. Each rule lists
the required context signals (e.g., comparative terms, risk phrases), optional
sentiment constraints, and a confidence score. See `docs/phase2-narrative.md`
for guidance on designing new rules and how they flow through
`scripts/label_sentence_events.py`, the SQLite schema, and the dashboards.

### Narrative change view
Running `python -m scripts.aggregate_metrics` now emits
`narratives_change_{w|m}.parquet` alongside the existing aggregates. These files
flag each narrative subtype as `new`, `disappearing`, or a significant increase/
decrease based on the most recent bucket versus the prior lookback window. The
Streamlit dashboard exposes this under “Narrative change since last review,” so
re-running the aggregator is all that’s needed to refresh the change view. The
aggregator also writes `narratives_weighted_{w|m}.parquet` (study-type weighted
counts), `narratives_dimensions_{w|m}.parquet` (narrative × claim strength ×
risk posture), and `risk_signals_{w|m}.parquet` (safety/concern ratios per
product pair) so you can analyze raw, weighted, and omission-driven trends with
the same deterministic pipeline.

To persist sentiment labels back into SQLite, include `--db`:

```bash
python scripts/label_sentence_sentiment.py --input data/processed/example_sentences.jsonl --db data/europepmc.sqlite
```

The script writes a sibling JSONL with added `sentiment_label`, `sentiment_score`,
`sentiment_model`, and `sentiment_inference_ts` fields while keeping identifiers such as
`doc_id`, `sentence_id`, `date`, and `product_mentions` intact.

### Exporting metrics for dashboards
With a populated SQLite database, export the weekly/monthly metrics that back dashboards:

```bash
python scripts/aggregate_metrics.py --db data/europepmc.sqlite --outdir data/processed/metrics
python scripts/export_sentiment_metrics.py --db data/europepmc.sqlite --outdir data/processed/metrics
```

### Metrics dashboard
Launch the Streamlit dashboard to visualize publication volume, mentions, co-mentions, and
sentiment ratios with evidence drill-downs:

```bash
streamlit run scripts/metrics_dashboard.py
```

The dashboard is the primary Phase 1 artifact. It tolerates missing pandas/altair by falling back to basic charts and exposes alias provenance plus confidence breakdowns for each evidence sentence.

### Proxy troubleshooting
If your environment blocks outbound traffic via a corporate proxy, you can disable proxy usage or provide explicit proxy URLs:

```bash
# Ignore system proxy variables
python scripts/ingest_europe_pmc.py -p "aspirin" --no-proxy

# Override proxies explicitly (repeat --proxy for each scheme)
python scripts/ingest_europe_pmc.py -p "aspirin" --proxy "https=https://proxy.example:8080" --proxy "http=http://proxy.example:8080"
```

## Phase 1 known limitations
- No causal inference; outputs are descriptive only.
- No directionality detection ("A causes B" vs. "A compared to B").
- No dosage or population stratification.
- No cross-sentence inference; all analytics stay within a single sentence.
- Sentiment is lexicon/rule based and does not capture nuanced clinical outcomes.
- Product matching is alias-based with boundary enforcement; extremely short aliases are skipped to avoid false positives.

## Non-goals (for now)
- Gene or pathway extraction
- Free-text summarization
- Chat interfaces

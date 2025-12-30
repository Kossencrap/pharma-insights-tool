# Phase 1 acceptance and verification

This document defines when Phase 1 is **done**, how to run the canonical pipeline, and where to find the review artifacts.

## Acceptance checklist
- ✅ One-command pipeline completes on a fresh machine
- ✅ Manifest captured with inputs, limits, and export pointers
- ✅ Confidence is explainable sentence-by-sentence (aliases + weights visible)
- ✅ Guardrails protect product matching and SQLite growth
- ✅ Golden dataset committed for demo/regression
- ✅ Dashboard is the primary artifact and can run without pandas/altair
- ✅ Known limitations are explicit (no causal inference, no dosage, no cross-sentence reasoning)

## Canonical pipeline
Run the single entry point to stitch ingestion → labeling → exports:

```bash
python scripts/run_phase1_pipeline.py \
  --products config/products.json \
  --from-date 2022-01-01 \
  --max-records 200 \
  --db data/europepmc.sqlite \
  --manifest-path data/artifacts/phase1/phase1_run_manifest.json
```

Artifacts land under `data/artifacts/phase1/`:
- `metrics/`: time-bucketed aggregates for the dashboard
- `exports/`: raw tables, aggregates, and evidence with confidence breakdowns
- `phase1_run_manifest.json`: pipeline manifest with inputs, limits, and export paths

### Guardrails and limits
- `--max-sentences-per-doc` (default 400): skip extra sentences per document
- `--max-co-mentions-per-sentence` (default 50): drop excessive pairs per sentence
- `--db-size-warn-mb` (default 250): emit warning if SQLite grows too large
- Product alias collisions and too-short aliases warn during config load

## Explainable confidence and alias provenance
- Evidence exports include `product_a_alias` / `product_b_alias` for synonym provenance
- `confidence_breakdown` is attached per evidence row (recency, study type, mention count)
- CLI (`scripts/show_sentence_evidence.py`) and dashboard reuse the same helper for parity

## Golden dataset (regression + demo)
Example outputs live in `data/examples/`:
- `example_run_manifest.json`: snapshot of a reproducible run
- `example_top_comentions.csv`: top co-mentions for two well-known drug pairs
- `example_evidence.jsonl`: sentence-level evidence with aliases and confidence breakdowns

To regenerate, run the pipeline for the canonical pairs:
```bash
python scripts/run_phase1_pipeline.py \
  --products config/products.json \
  --product metformin --product insulin \
  --product semaglutide --product liraglutide \
  --max-records 150 \
  --from-date 2022-01-01
```
Copy the resulting `phase1_run_manifest.json`, co-mention exports, and evidence JSONL from `data/artifacts/phase1/exports` into `data/examples/` when updating the snapshots.

## Dashboard as the Phase 1 artifact
Launch the Streamlit dashboard over the exported metrics:
```bash
streamlit run scripts/metrics_dashboard.py \
  --server.port 8501
```

Notes:
- The dashboard falls back to built-in charts if pandas/altair are missing
- On first load, use the sidebar to point at the `metrics/` directory created by the pipeline
- Tooltips and captions explain co-mentions, alias matches, and confidence inputs
- Sentiment is lexicon-based and non-directional

## Known limitations (Phase 1 scope)
- No causal inference or treatment effect estimation
- No dosage or population stratification
- No cross-sentence inference; evidence is per-sentence only
- Lexicon-based sentiment; directionality is heuristic

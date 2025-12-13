# Pharma Insights Tool

Product-centric scientific and narrative intelligence
based on biomedical literature.

## Current scope
- Literature ingestion from Europe PMC
- Deterministic sentence structuring
- No NLP or modeling yet

## Pipeline
Europe PMC → Document → Section → Sentence

## Getting started
Install dependencies and run the provided ingestion script to pull a small
sample from Europe PMC:

```bash
pip install -e .
python scripts/ingest_europe_pmc.py
```

Raw records will be written to `data/raw/` and the normalized document JSONL
is written to `data/processed/`.

## Non-goals (for now)
- Gene or pathway extraction
- Free-text summarization
- Chat interfaces

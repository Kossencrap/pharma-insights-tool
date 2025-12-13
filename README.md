# Pharma Insights Tool

Product-centric scientific and narrative intelligence
based on biomedical literature.

## Current scope
- Literature ingestion from Europe PMC
- Deterministic sentence structuring
- No NLP or modeling yet

## Pipeline
Europe PMC → Document → Section → Sentence

## Running ingestion locally
Use the CLI runner to pull a small batch of Europe PMC results and emit both raw and structured outputs:

```bash
python scripts/ingest_europe_pmc.py -p "dupilumab" -p "Dupixent" --from-date 2022-01-01 --max-records 50
```

Outputs are written to `data/raw/` (raw JSON array) and `data/processed/` (one structured document per line).

## Non-goals (for now)
- Gene or pathway extraction
- Free-text summarization
- Chat interfaces

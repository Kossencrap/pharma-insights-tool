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

### Inspecting co-mentions (document-level)
After running ingestion with `--db data/europepmc.sqlite`, you can query document-level co-mentions directly from SQLite without storing sentence-level pairs:

```bash
# Top pairs across documents
python scripts/query_comentions.py --db data/europepmc.sqlite --limit 25

# Find example documents that mention both products
python scripts/which_doc.py metformin insulin --db data/europepmc.sqlite
```

### Proxy troubleshooting
If your environment blocks outbound traffic via a corporate proxy, you can disable proxy usage or provide explicit proxy URLs:

```bash
# Ignore system proxy variables
python scripts/ingest_europe_pmc.py -p "aspirin" --no-proxy

# Override proxies explicitly (repeat --proxy for each scheme)
python scripts/ingest_europe_pmc.py -p "aspirin" --proxy "https=https://proxy.example:8080" --proxy "http=http://proxy.example:8080"
```

## Non-goals (for now)
- Gene or pathway extraction
- Free-text summarization
- Chat interfaces

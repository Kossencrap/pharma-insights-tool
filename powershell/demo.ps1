# ============================================================
# Pharma Insights Tool — Demo Script (PowerShell)
# Flow: Setup -> Ingest -> Rank Co-mentions -> Drilldown -> Evidence
# ============================================================

$ErrorActionPreference = 'Stop'

# Force UTF-8 for this PowerShell session (prevents UnicodeEncodeError)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

# -----------------------------
# 0) Demo configuration
# -----------------------------
$RepoRoot   = Join-Path $HOME 'Documents\pharma-insights-tool'
$DbPath     = Join-Path $RepoRoot 'data\europepmc.sqlite'
$ExportRoot = Join-Path $RepoRoot 'data\exports'
$MetricsDir = Join-Path $RepoRoot 'data\processed\metrics'

$FromDate   = '2022-01-01'
$MaxRecords = 50

# Pair to drill down on (known to work in your dataset)
$ProductA   = 'aspirin'
$ProductB   = 'dupilumab'

# Optional: set to $true if you want to launch the Streamlit evidence browser
$LaunchStreamlit = $false

Write-Host ''
Write-Host '============================================================'
Write-Host ' Pharma Insights Tool — End-to-End Demo'
Write-Host '============================================================'
Write-Host ("Repo   : {0}" -f $RepoRoot)
Write-Host ("DB     : {0}" -f $DbPath)
Write-Host ("Ingest : dupilumab + Dupixent since {0} (max {1})" -f $FromDate, $MaxRecords)
Write-Host ("Drill  : {0} + {1}" -f $ProductA, $ProductB)
Write-Host '============================================================'
Write-Host ''

# -----------------------------
# 1) Repo + venv sanity checks
# -----------------------------
Write-Host '== 1. Repo and venv sanity checks =='

if (!(Test-Path $RepoRoot)) {
  throw ("Repo not found at: {0}" -f $RepoRoot)
}
Set-Location $RepoRoot

# Ensure repo root is on Python import path so `import src.*` works when running scripts from /scripts
$env:PYTHONPATH = $RepoRoot

if (!(Test-Path '.\.venv\Scripts\Activate.ps1')) {
  throw 'Venv not found. Create it first: python -m venv .venv'
}

Write-Host 'Activating venv...'
. .\.venv\Scripts\Activate.ps1

Write-Host 'Python interpreter:'
python -c "import sys; print(sys.executable)" | Out-Host

Write-Host 'Ensuring project is installed in editable mode (so src imports work)...'
python -m pip install -e . | Out-Host

Write-Host 'Quick import check (src):'
python -c "import src; print('import src OK')" | Out-Host

Write-Host ''

# -----------------------------
# 2) Ingest Europe PMC -> SQLite
# -----------------------------
Write-Host '== 2. Ingest Europe PMC into SQLite =='
Write-Host 'Goal: fetch records, extract mentions, persist documents + mentions to SQLite.'
Write-Host ''

if (!(Test-Path '.\data')) {
  New-Item -ItemType Directory -Path '.\data' | Out-Null
}

python scripts/ingest_europe_pmc.py `
  -p 'dupilumab' `
  -p 'Dupixent' `
  --db $DbPath `
  --from-date $FromDate `
  --max-records $MaxRecords `
  --incremental | Out-Host
if ($LASTEXITCODE -ne 0) { throw "ingest_europe_pmc failed" }

Write-Host ''

if (Test-Path $DbPath) {
  Write-Host ("SQLite DB created/updated: {0}" -f $DbPath)
} else {
  throw ("SQLite DB not found at expected path: {0}" -f $DbPath)
}

Write-Host ''

# -----------------------------
# 3) Label sentence-level co-mentions
# -----------------------------
Write-Host '== 3. Label sentence co-mentions =='
Write-Host 'Goal: enrich sentence pairs with comparative/risk/relationship context labels.'
Write-Host ''

python scripts/label_sentence_events.py `
  --db $DbPath `
  --limit 5000 `
  --only-missing | Out-Host
if ($LASTEXITCODE -ne 0) { throw "label_sentence_events failed" }

Write-Host ''

# -----------------------------
# 4) Aggregate weekly/monthly metrics
# -----------------------------
Write-Host '== 4. Aggregate time-series metrics =='
Write-Host 'Goal: compute weekly/monthly volume + change metrics for docs/mentions/co-mentions.'
Write-Host ''

if (!(Test-Path $MetricsDir)) {
  New-Item -ItemType Directory -Path $MetricsDir | Out-Null
}

python scripts/aggregate_metrics.py `
  --db $DbPath `
  --outdir $MetricsDir | Out-Host
if ($LASTEXITCODE -ne 0) { throw "aggregate_metrics failed" }

Write-Host ''

# -----------------------------
# 5) Rank co-mentions
# -----------------------------
Write-Host '== 5. Top co-mentions (doc-level) =='
Write-Host 'Goal: show highest-scoring product pairs from ingested docs.'
Write-Host ''

python scripts/query_comentions.py --db $DbPath --limit 25 | Out-Host
if ($LASTEXITCODE -ne 0) { throw "query_comentions failed" }

Write-Host ''

# -----------------------------
# 6) Drill down: which docs contain BOTH
# -----------------------------
Write-Host '== 6. Drilldown: docs containing BOTH products =='
Write-Host 'Goal: show concrete PMIDs supporting the co-mention.'
Write-Host ''

python scripts/which_doc.py $ProductA $ProductB --db $DbPath | Out-Host
if ($LASTEXITCODE -ne 0) { throw "which_doc failed" }

Write-Host ''

# -----------------------------
# 7) Evidence: sentence-level proof + weights
# -----------------------------
Write-Host '== 7. Evidence: sentence-level mentions + weights =='
Write-Host 'Goal: show exact sentences (title/abstract) and the scoring weights.'
Write-Host ''

python scripts/show_sentence_evidence.py `
  --db $DbPath `
  --product-a $ProductA `
  --product-b $ProductB | Out-Host
if ($LASTEXITCODE -ne 0) { throw "show_sentence_evidence failed" }

Write-Host ''

# -----------------------------
# 8) Export batch outputs
# -----------------------------
Write-Host '== 8. Export batch outputs =='
Write-Host 'Goal: export raw tables, aggregates, and sentence evidence for downstream use.'
Write-Host ''

if (!(Test-Path $ExportRoot)) {
  New-Item -ItemType Directory -Path $ExportRoot | Out-Null
}

python scripts/export_batch.py `
  --db $DbPath `
  --export-root $ExportRoot `
  --freq W M `
  --evidence-limit 500 | Out-Host
if ($LASTEXITCODE -ne 0) { throw "export_batch failed" }

Write-Host ''

# -----------------------------
# 9) Optional: Streamlit evidence browser
# -----------------------------
Write-Host '== 9. Optional: Streamlit evidence browser =='
Write-Host 'Goal: browse labeled sentences interactively (requires streamlit).' 

if ($LaunchStreamlit) {
  python -c "import importlib.util; import sys; sys.exit(0 if importlib.util.find_spec('streamlit') else 1)"
  if ($LASTEXITCODE -ne 0) {
    Write-Host 'Streamlit not installed. Run: pip install streamlit' -ForegroundColor Yellow
  } else {
    Write-Host 'Launching Streamlit app. Close the browser tab or CTRL+C to exit.' -ForegroundColor Yellow
    streamlit run scripts/view_labeled_sentences.py
  }
} else {
  Write-Host 'Skipping Streamlit launch (LaunchStreamlit = $false).' -ForegroundColor Yellow
}

Write-Host ''
Write-Host '============================================================'
Write-Host ' Demo complete.'
Write-Host '============================================================'
Write-Host 'Notes:'
Write-Host ' - If evidence includes HTML markup, strip tags during normalization for cleaner output.'
Write-Host ' - If incremental status prints a future date, clamp/ignore partial dates so the watermark cannot jump ahead.'
Write-Host ''

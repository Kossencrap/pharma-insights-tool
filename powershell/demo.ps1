# ============================================================
# Pharma Insights Tool — Demo Script (PowerShell)
# Flow: Setup -> Ingest -> Rank Co-mentions -> Drilldown -> Evidence
# ============================================================

$ErrorActionPreference = 'Stop'

# Force UTF-8 for this PowerShell session (prevents Unicode issues)
chcp 65001 | Out-Null
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new()
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

# -----------------------------
# 0) Demo configuration
# -----------------------------
$RepoRoot   = Join-Path $HOME 'Documents\pharma-insights-tool'
$RunDir     = Join-Path $RepoRoot 'data\demo_runs'
$RunStamp   = Get-Date -Format 'yyyyMMdd_HHmmss'
$DbPath     = Join-Path $RunDir ("europepmc_demo_{0}.sqlite" -f $RunStamp)
$ExportRoot = Join-Path $RepoRoot 'data\exports'
$MetricsDir = Join-Path $RepoRoot 'data\processed\metrics'

$FromDate   = '2022-01-01'
$MaxRecords = 5000

# Pair to ingest and drill down on (drill stays fixed; no fallback)
$ProductA   = 'aspirin'
$ProductB   = 'ibuprofen'

# Optional: set to $true if you want to launch the Streamlit evidence browser
$LaunchStreamlit = $false
# Optional: set to $true if you want to launch the Streamlit metrics dashboard
$LaunchMetricsDashboard = $false

Write-Host ''
Write-Host '============================================================'
Write-Host ' Pharma Insights Tool — End-to-End Demo'
Write-Host '============================================================'
Write-Host ("Repo   : {0}" -f $RepoRoot)
Write-Host ("DB     : {0}" -f $DbPath)
Write-Host ("Ingest : {0} + {1} since {2} (max {3})" -f $ProductA, $ProductB, $FromDate, $MaxRecords)
Write-Host ("Drill  : {0} + {1} (fixed; no fallback)" -f $ProductA, $ProductB)
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

# Make sure pip tooling is sane inside the venv
python -m pip install -U pip setuptools wheel | Out-Host

Write-Host 'Ensuring project is installed in editable mode (so src imports work)...'
python -m pip install -e . | Out-Host

Write-Host 'Quick import check (src):'
python -c "import src; print('import src OK')" | Out-Host

Write-Host ''

# -----------------------------
# 2) Ingest Europe PMC -> per-run SQLite
# -----------------------------
Write-Host '== 2. Ingest Europe PMC into SQLite =='
Write-Host 'Goal: fetch records mentioning ProductA OR ProductB (current ingest supports OR); drill stays fixed.'
Write-Host ''

if (!(Test-Path $RunDir)) {
  New-Item -ItemType Directory -Path $RunDir | Out-Null
}

# NOTE: --require-all-products is NOT supported by scripts/ingest_europe_pmc.py in your current repo.
# We therefore ingest in OR-mode and keep the drill/evidence pair fixed to ProductA+ProductB.
python scripts/ingest_europe_pmc.py `
  -p $ProductA `
  -p $ProductB `
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

# 5) Export sentiment ratios
# -----------------------------
Write-Host '== 5. Export sentiment ratios =='
Write-Host 'Goal: export weekly/monthly sentiment ratio metrics for dashboards.'
Write-Host ''

python scripts/export_sentiment_metrics.py `
  --db $DbPath `
  --outdir $MetricsDir | Out-Host
if ($LASTEXITCODE -ne 0) { throw "export_sentiment_metrics failed" }

Write-Host ''

# -----------------------------
# 6) Top co-mentions (doc-level)
# -----------------------------
Write-Host '== 6. Top co-mentions (doc-level) =='
Write-Host 'Goal: show highest-scoring product pairs (informational; drill pair stays fixed).'
Write-Host ''

python scripts/query_comentions.py --db $DbPath --limit 25 | Out-Host
if ($LASTEXITCODE -ne 0) { throw "query_comentions failed" }

Write-Host ''
Write-Host ("Selected drill pair: {0} + {1}" -f $ProductA, $ProductB) -ForegroundColor Yellow
Write-Host ''

# -----------------------------
# 7) Drill down: which docs contain BOTH (fixed pair)
# -----------------------------
Write-Host '== 7. Drilldown: docs containing BOTH products =='
Write-Host ('Goal: show concrete PMIDs supporting the co-mention ({0} + {1}).' -f $ProductA, $ProductB)
Write-Host ''

$docOutput = python scripts/which_doc.py $ProductA $ProductB --db $DbPath
if ($LASTEXITCODE -ne 0) { throw "which_doc failed" }
$docOutput | Out-Host

Write-Host ''

# -----------------------------
# 8) Evidence: sentence-level proof + weights (fixed pair)
# -----------------------------
Write-Host '== 8. Evidence: sentence-level mentions + weights =='
Write-Host ('Goal: show exact sentences (title/abstract) and the scoring weights for {0} + {1}.' -f $ProductA, $ProductB)
Write-Host ''

$evidenceOutput = python scripts/show_sentence_evidence.py `
  --db $DbPath `
  --product-a $ProductA `
  --product-b $ProductB
if ($LASTEXITCODE -ne 0) { throw "show_sentence_evidence failed" }

if ($evidenceOutput -match 'No evidence' -or $evidenceOutput -match 'No documents' -or $evidenceOutput -match 'No sentence') {
  Write-Host ("No sentence evidence found for {0} + {1} in this run." -f $ProductA, $ProductB) -ForegroundColor Yellow
  Write-Host "Tips:" -ForegroundColor Yellow
  Write-Host "  - Increase MaxRecords (e.g., 20000) and re-run." -ForegroundColor Yellow
  Write-Host "  - Broaden FromDate (e.g., 2015-01-01) for more co-mentions." -ForegroundColor Yellow
  Write-Host "  - If you truly need AND-only ingestion, add filtering logic to ingest_europe_pmc.py (new flag) and then re-enable it here." -ForegroundColor Yellow
} else {
  $evidenceOutput | Out-Host
}

Write-Host ''

# -----------------------------
# 9) Export batch outputs
# -----------------------------
Write-Host '== 9. Export batch outputs =='
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
# 10) Label sentiment on exported sentence evidence
# -----------------------------
Write-Host '== 10. Label sentiment on evidence sentences =='
Write-Host 'Goal: attach deterministic sentiment labels to exported sentence evidence JSONL.'
Write-Host ''

$RunSlug = "run_{0}" -f (Get-Date -Format 'yyyyMMdd')
$EvidenceJsonl = Join-Path $ExportRoot ("runs\{0}\evidence\sentence_evidence_{0}.jsonl" -f $RunSlug)

if (Test-Path $EvidenceJsonl) {
  python scripts/label_sentence_sentiment.py `
    --input $EvidenceJsonl | Out-Host
  if ($LASTEXITCODE -ne 0) { throw "label_sentence_sentiment failed" }
} else {
  Write-Host ("Evidence JSONL not found at {0}. Skipping sentiment labeling." -f $EvidenceJsonl) -ForegroundColor Yellow
}

Write-Host ''

# -----------------------------
# 11) Optional: Streamlit evidence browser
# -----------------------------
Write-Host '== 11. Optional: Streamlit evidence browser =='
Write-Host 'Goal: browse labeled sentences interactively (requires streamlit in the venv).'

if ($LaunchStreamlit) {
  python -c "import importlib.util; import sys; sys.exit(0 if importlib.util.find_spec('streamlit') else 1)"
  if ($LASTEXITCODE -ne 0) {
    Write-Host 'Streamlit not installed in this venv. Run:' -ForegroundColor Yellow
    Write-Host '  python -m pip install streamlit' -ForegroundColor Yellow
  } else {
    Write-Host 'Launching Streamlit app. Close the browser tab or CTRL+C to exit.' -ForegroundColor Yellow
    python -m streamlit run scripts/view_labeled_sentences.py
  }
} else {
  Write-Host 'Skipping Streamlit launch (LaunchStreamlit = $false).' -ForegroundColor Yellow
}

Write-Host ''

# -----------------------------
# 12) Optional: Streamlit metrics dashboard
# -----------------------------
Write-Host '== 12. Optional: Streamlit metrics dashboard =='
Write-Host 'Goal: visualize metrics and sentiment ratios interactively.'

if ($LaunchMetricsDashboard) {
  python -c "import importlib.util; import sys; sys.exit(0 if importlib.util.find_spec('streamlit') else 1)"
  if ($LASTEXITCODE -ne 0) {
    Write-Host 'Streamlit not installed in this venv. Run:' -ForegroundColor Yellow
    Write-Host '  python -m pip install streamlit' -ForegroundColor Yellow
  } else {
    Write-Host 'Launching Streamlit dashboard. Close the browser tab or CTRL+C to exit.' -ForegroundColor Yellow
    python -m streamlit run scripts/metrics_dashboard.py
  }
} else {
  Write-Host 'Skipping Streamlit metrics dashboard (LaunchMetricsDashboard = $false).' -ForegroundColor Yellow
}

Write-Host ''
Write-Host '============================================================'
Write-Host ' Demo complete.'
Write-Host '============================================================'
Write-Host 'Notes:'
Write-Host ' - This demo uses a per-run SQLite DB under data\demo_runs to avoid old runs contaminating results.'
Write-Host ' - The ingest step currently retrieves records matching either product (OR). Ranking may show other frequent pairs; drill/evidence stays fixed to ProductA+ProductB.'
Write-Host ' - If evidence includes HTML markup, strip tags during normalization for cleaner output.'
Write-Host ''

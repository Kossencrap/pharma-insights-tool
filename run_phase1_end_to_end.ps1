# ======================================================================================
# Phase 1 end-to-end (PowerShell/Windows)
# - Ensures venv
# - Installs editable package
# - Patches scripts to add repo-root to sys.path (durable fix)
# - Verifies ingest_europe_pmc indentation issue is resolved
# - Runs pipeline -> pytest -> streamlit dashboard
# Repo root: C:\Users\sebas\Documents\branch2-pharma-insights-tool
# ======================================================================================

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# ---- 0) Go to repo root --------------------------------------------------------------
$RepoRoot = "C:\Users\sebas\Documents\branch2-pharma-insights-tool"
Set-Location $RepoRoot

# ---- 1) Ensure venv exists + activate + install (editable) ---------------------------
if (-not (Test-Path .\.venv\Scripts\python.exe)) {
  python -m venv .venv
}
.\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip setuptools wheel | Out-Host
python -m pip install -e . | Out-Host

# Optional: install runtime/test tools if not already provided by extras
python -m pip install pytest streamlit | Out-Host

# ---- 2) Patch scripts to insert repo root into sys.path -------------------------------
function Ensure-SysPathGuard {
  param(
    [Parameter(Mandatory = $true)][string]$FilePath,
    [Parameter(Mandatory = $true)][int]$AfterLineNumber,
    [Parameter(Mandatory = $true)][string]$MarkerImport # e.g., "from src.analytics"
  )

  if (-not (Test-Path $FilePath)) {
    throw "File not found: $FilePath"
  }

  $content = Get-Content $FilePath -Raw

  # If the file already contains a sys.path insertion using ROOT, do nothing.
  if ($content -match 'sys\.path\.insert\(0,\s*str\(ROOT\)\)' -or $content -match 'if\s+str\(ROOT\)\s+not\s+in\s+sys\.path') {
    Write-Host "Sys.path guard already present: $FilePath" -ForegroundColor Green
    return
  }

  # Only patch if it imports from src (expected failure mode)
  if ($content -notmatch [regex]::Escape($MarkerImport)) {
    Write-Host "No '$MarkerImport' import found; skipping sys.path guard patch: $FilePath" -ForegroundColor Yellow
    return
  }

  # Insert guard right after ROOT is defined (preferred), otherwise near top.
  $lines = Get-Content $FilePath
  $idx = [Math]::Min($AfterLineNumber, $lines.Count) - 1

  $guard = @(
    "import sys",
    "if str(ROOT) not in sys.path:",
    "    sys.path.insert(0, str(ROOT))",
    ""
  )

  $newLines = @()
  for ($i = 0; $i -lt $lines.Count; $i++) {
    $newLines += $lines[$i]
    if ($i -eq $idx) {
      $newLines += $guard
    }
  }

  Set-Content -Path $FilePath -Value $newLines -Encoding UTF8
  Write-Host "Inserted sys.path guard into: $FilePath" -ForegroundColor Cyan
}

Write-Host "`n== Patching scripts for sys.path guard ==" -ForegroundColor Cyan

# Per your notes:
# - run_phase1_pipeline.py: insert after ROOT = Path(...).parents[1] (around line 18)
# - metrics_dashboard.py: insert after ROOT = Path(...).parents[1] (around line 10)
Ensure-SysPathGuard -FilePath ".\scripts\run_phase1_pipeline.py" -AfterLineNumber 18 -MarkerImport "from src.analytics"
Ensure-SysPathGuard -FilePath ".\scripts\metrics_dashboard.py"  -AfterLineNumber 10 -MarkerImport "from src.analytics"

# ---- 3) Check ingest_europe_pmc indentation issue ------------------------------------
Write-Host "`n== Checking ingestion runner file for indentation error ==" -ForegroundColor Cyan

$IngestPath = ".\scripts\ingest_europe_pmc.py"
if (-not (Test-Path $IngestPath)) {
  throw "Missing: $IngestPath"
}

# Lightweight heuristic:
# Find a line containing "with structured_path.open" and ensure subsequent "for record in normalized_results"
# is indented by at least 4 spaces.
$ingestLines = Get-Content $IngestPath
$withIdx = -1
for ($i = 0; $i -lt $ingestLines.Count; $i++) {
  if ($ingestLines[$i] -match 'with\s+structured_path\.open') {
    $withIdx = $i
    break
  }
}

if ($withIdx -ge 0) {
  $forIdx = -1
  for ($j = $withIdx + 1; $j -lt [Math]::Min($withIdx + 120, $ingestLines.Count); $j++) {
    if ($ingestLines[$j] -match 'for\s+record\s+in\s+normalized_results') {
      $forIdx = $j
      break
    }
  }

  if ($forIdx -ge 0) {
    $forLine = $ingestLines[$forIdx]
    $isIndented = $forLine -match '^\s{4,}for\s+record\s+in\s+normalized_results'

    if (-not $isIndented) {
      Write-Host "`nIndentation appears broken in scripts\ingest_europe_pmc.py near the with/open block." -ForegroundColor Red
      Write-Host "Fix required: indent the 'for record in normalized_results:' loop so it is inside the 'with structured_path.open(...) as f:' block." -ForegroundColor Red
      Write-Host "Open file at the problematic region and re-indent lines ~377-450." -ForegroundColor Yellow

      # Open at approximate line region if VS Code is available
      try {
        code -g "$RepoRoot\scripts\ingest_europe_pmc.py:377" | Out-Null
      } catch {
        Write-Host "Tip: open manually: $RepoRoot\scripts\ingest_europe_pmc.py around line 377" -ForegroundColor Yellow
      }

      throw "Abort: fix indentation in scripts\ingest_europe_pmc.py then rerun this script."
    } else {
      Write-Host "Indentation check looks OK (for-loop is indented under with-block)." -ForegroundColor Green
    }
  } else {
    Write-Host "Could not find 'for record in normalized_results' near the 'with structured_path.open' block; skipping indentation heuristic." -ForegroundColor Yellow
  }
} else {
  Write-Host "Could not find 'with structured_path.open' in $IngestPath; skipping indentation heuristic." -ForegroundColor Yellow
}

# ---- 4) Phase 1 pipeline -------------------------------------------------------------
Write-Host "`n== Phase 1 pipeline ==" -ForegroundColor Cyan

$StartDate = "2024-01-01"
$EndDate   = "2024-12-31"

$ProductsPath  = "config\products.json"
$DbPath        = "data\europepmc.sqlite"
$ArtifactsRoot = "data\artifacts\phase1"
$ManifestPath  = Join-Path $ArtifactsRoot "phase1_run_manifest.json"

if (-not (Test-Path $ArtifactsRoot)) {
  New-Item -ItemType Directory -Path $ArtifactsRoot | Out-Null
}

python scripts\run_phase1_pipeline.py `
  --products $ProductsPath `
  --from-date $StartDate `
  --to-date $EndDate `
  --max-records 250 `
  --db $DbPath `
  --artifacts-dir $ArtifactsRoot `
  --manifest-path $ManifestPath

if ($LASTEXITCODE -ne 0) { throw "Phase 1 pipeline failed." }
if (-not (Test-Path $ManifestPath)) { throw "Manifest not found: $ManifestPath" }

Write-Host "`nPipeline succeeded." -ForegroundColor Green
Write-Host "Artifacts : $ArtifactsRoot" -ForegroundColor Green
Write-Host "Manifest  : $ManifestPath" -ForegroundColor Green

# ---- 5) Execute tests ----------------------------------------------------------------
Write-Host "`n== Pytest suite ==" -ForegroundColor Cyan
python -m pytest -vv --maxfail=1 --color=yes
if ($LASTEXITCODE -ne 0) { throw "Pytest failed." }
Write-Host "`nTests passed." -ForegroundColor Green

# Optional: full Windows validation flow
# Write-Host "`n== Full functional checks (PowerShell) ==" -ForegroundColor Cyan
# .\powershell\functional_checks.ps1
# if ($LASTEXITCODE -ne 0) { throw "functional_checks.ps1 failed." }

# ---- 6) Launch dashboard --------------------------------------------------------------
Write-Host "`n== Streamlit dashboard ==" -ForegroundColor Cyan
Write-Host "After it starts, in the Streamlit sidebar select:" -ForegroundColor Yellow
Write-Host "  Metrics folder: data\artifacts\phase1\metrics" -ForegroundColor Yellow
Write-Host "  Evidence DB    : data\europepmc.sqlite" -ForegroundColor Yellow

streamlit run scripts\metrics_dashboard.py

# ---- 7) Manual review pointers -------------------------------------------------------
# Open these after generating artifacts:
#   docs\phase1.md
#   data\artifacts\phase1\phase1_run_manifest.json
#   data\examples\  (golden outputs / snapshots)

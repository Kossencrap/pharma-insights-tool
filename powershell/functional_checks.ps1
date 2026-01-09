[CmdletBinding()]
param(
    [string]$PythonExe = "py",
    [string]$DataRoot = "data/powershell-checks",
    [string[]]$Products = @("enalapril", "sacubitril_valsartan"),
    [int]$MaxRecords = 25,
    [switch]$SkipPytests,
    [switch]$SkipNetworkSteps,
    [switch]$SkipFunctionalQueries,
    [switch]$SkipStreamlit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$ProductConfigPath = Join-Path $RepoRoot "config/products.json"
$StudyWeightConfigPath = Join-Path $RepoRoot "config/study_type_weights.json"
$env:PYTHONPATH = if ($env:PYTHONPATH) { "$RepoRoot;$($env:PYTHONPATH)" } else { "$RepoRoot" }
$script:ProductAliasLookup = @{}

if (Test-Path $ProductConfigPath) {
    $productConfigRaw = Get-Content -Raw $ProductConfigPath | ConvertFrom-Json
    foreach ($entry in $productConfigRaw.PSObject.Properties) {
        $values = @($entry.Value)
        if (-not ($values -contains $entry.Name)) {
            $values += $entry.Name
        }
        $deduped = New-Object System.Collections.Generic.List[string]
        $seen = New-Object 'System.Collections.Generic.HashSet[string]'
        foreach ($alias in $values) {
            $text = [string]$alias
            if ([string]::IsNullOrWhiteSpace($text)) {
                continue
            }
            $clean = $text.Trim()
            $key = $clean.ToLower()
            if ($seen.Contains($key)) {
                continue
            }
            $seen.Add($key) | Out-Null
            $deduped.Add($clean) | Out-Null
        }
        $script:ProductAliasLookup[$entry.Name.ToLower()] = $deduped.ToArray()
    }
}

function Write-Heading {
    param([string]$Message)
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Invoke-ExternalCommand {
    param(
        [Parameter(Mandatory)] [string]$Executable,
        [Parameter()] [string[]]$Arguments = @()
    )

    $expandedArgs = $Arguments | ForEach-Object { $_ }
    Write-Host "-> $Executable $($expandedArgs -join ' ')" -ForegroundColor DarkGray
    $process = Start-Process -FilePath $Executable -ArgumentList $expandedArgs -Wait -PassThru -NoNewWindow
    if ($process.ExitCode -ne 0) {
        throw "Command failed with exit code $($process.ExitCode): $Executable $($expandedArgs -join ' ')"
    }
}

function Ensure-Directory {
    param([string]$Path)
    if (-not (Test-Path -Path $Path)) {
        Write-Host "Creating directory: $Path" -ForegroundColor Yellow
        New-Item -ItemType Directory -Path $Path | Out-Null
    }
}

function Get-ProductSearchTerms {
    param([string[]]$CanonicalProducts)

    $terms = New-Object System.Collections.Generic.List[string]
    $seen = New-Object 'System.Collections.Generic.HashSet[string]'

    foreach ($product in $CanonicalProducts) {
        $candidates = New-Object System.Collections.Generic.List[string]
        $candidates.Add($product) | Out-Null
        $key = $product.ToLower()
        if ($script:ProductAliasLookup.ContainsKey($key)) {
            foreach ($alias in $script:ProductAliasLookup[$key]) {
                $candidates.Add($alias) | Out-Null
            }
        }
        foreach ($candidate in $candidates) {
            if ([string]::IsNullOrWhiteSpace($candidate)) {
                continue
            }
            $clean = $candidate.Trim()
            $norm = $clean.ToLower()
            if ($seen.Contains($norm)) {
                continue
            }
            $seen.Add($norm) | Out-Null
            $terms.Add($clean) | Out-Null
        }
    }

    return $terms.ToArray()
}

function Show-ConfigSummary {
    param(
        [string[]]$ProductsToCheck,
        [string]$ProductConfig,
        [string]$WeightConfig
    )

    Write-Heading "Config snapshot"

    if (Test-Path $ProductConfig) {
        Write-Host ("Product config: {0}" -f $ProductConfig) -ForegroundColor DarkGray
        foreach ($product in $ProductsToCheck) {
            $key = $product.ToLower()
            if ($script:ProductAliasLookup.ContainsKey($key)) {
                $aliases = $script:ProductAliasLookup[$key] -join ", "
                Write-Host ("  {0}: {1}" -f $product, $aliases)
            } else {
                Write-Host ("  {0}: NOT FOUND in product config" -f $product) -ForegroundColor Yellow
            }
        }
    } else {
        Write-Host ("Product config not found at {0}" -f $ProductConfig) -ForegroundColor Yellow
    }

    if (Test-Path $WeightConfig) {
        Write-Host ("Study weight config: {0}" -f $WeightConfig) -ForegroundColor DarkGray
        $weightJson = Get-Content -Raw $WeightConfig | ConvertFrom-Json
        $weightKeys = $weightJson.PSObject.Properties.Name | Sort-Object
        Write-Host ("  Weight entries: {0}" -f $weightKeys.Count)
        Write-Host ("  Keys: {0}" -f ($weightKeys -join ", "))
    } else {
        Write-Host ("Study weight config not found at {0}" -f $WeightConfig) -ForegroundColor Yellow
    }
}

function Run-Pytests {
    if ($SkipPytests) {
        Write-Host "Skipping pytest run (SkipPytests set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Running pytest suite"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @('-m', 'pytest')
}

function Run-Ingestion {
    if ($SkipNetworkSteps) {
        Write-Host "Skipping ingestion and downstream queries (SkipNetworkSteps set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Running Europe PMC ingestion"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    Ensure-Directory -Path $DataRoot

    $ingestArgs = @('scripts/ingest_europe_pmc.py')
    $searchTerms = Get-ProductSearchTerms -CanonicalProducts $Products
    foreach ($term in $searchTerms) {
        $ingestArgs += @('-p', "`"$term`"")
    }
    $ingestArgs += @('--from-date', '2022-01-01', '--max-records', $MaxRecords, '--db', $dbPath, '--incremental')

    Invoke-ExternalCommand -Executable $PythonExe -Arguments $ingestArgs

    $prefix = ($Products[0]).ToLower() -replace '\s+', '_'
    $script:StructuredJsonl = Join-Path "data/processed" ("{0}_structured.jsonl" -f $prefix)
}

function Run-LabelSentenceEvents {
    if ($SkipNetworkSteps) {
        Write-Host "Skipping sentence-event labeling (SkipNetworkSteps set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Labeling sentence co-mentions"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/label_sentence_events.py',
        '--db', $dbPath,
        '--limit', '500',
        '--only-missing'
    )

    $script:SentenceEventsForSentiment = Join-Path $DataRoot "sentence_events_for_sentiment.jsonl"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/export_sentence_events_jsonl.py',
        '--db', $dbPath,
        '--output', $script:SentenceEventsForSentiment
    )
}

function Run-LabelSentenceSentiment {
    Write-Heading "Labeling sentence sentiment"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    if (-not $script:SentenceEventsForSentiment) {
        $script:SentenceEventsForSentiment = Join-Path $DataRoot "sentence_events_for_sentiment.jsonl"
    }
    if (-not (Test-Path $script:SentenceEventsForSentiment)) {
        Write-Host ("Sentence events JSONL not found at {0}. Run Run-LabelSentenceEvents first." -f $script:SentenceEventsForSentiment) -ForegroundColor Yellow
        return
    }
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/label_sentence_sentiment.py',
        '--input', $script:SentenceEventsForSentiment,
        '--db', $dbPath
    )
}

function Run-AggregateMetrics {
    if ($SkipNetworkSteps) {
        Write-Host "Skipping metric aggregation (SkipNetworkSteps set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Aggregating weekly/monthly metrics"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    $outDir = Join-Path $DataRoot "metrics"
    Ensure-Directory -Path $outDir
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/aggregate_metrics.py',
        '--db', $dbPath,
        '--outdir', $outDir,
        '--change-lookback', '4',
        '--change-min-ratio', '0.4',
        '--change-min-count', '3'
    )
    Write-Host ("Directional metrics: {0}" -f (Join-Path $outDir "directional_w.parquet")) -ForegroundColor DarkGray
}

function Run-SentimentMetrics {
    if ($SkipNetworkSteps) {
        Write-Host "Skipping sentiment metric export (SkipNetworkSteps set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Exporting sentiment ratio metrics"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    $outDir = Join-Path $DataRoot "metrics"
    Ensure-Directory -Path $outDir
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/export_sentiment_metrics.py',
        '--db', $dbPath,
        '--outdir', $outDir
    )
}

function Run-ExportBatch {
    if ($SkipNetworkSteps) {
        Write-Host "Skipping batch export (SkipNetworkSteps set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Exporting batch outputs"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    $exportRoot = Join-Path $DataRoot "exports"
    Ensure-Directory -Path $exportRoot
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/export_batch.py',
        '--db', $dbPath,
        '--export-root', $exportRoot,
        '--freq', 'W', 'M',
        '--evidence-limit', '500'
    )
}

function Run-Queries {
    if ($SkipNetworkSteps -or $SkipFunctionalQueries) {
        Write-Host "Skipping functional queries (SkipNetworkSteps or SkipFunctionalQueries set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Querying co-mentions"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @('scripts/query_comentions.py', '--db', $dbPath, '--limit', '25')

    Write-Heading "Inspecting example documents for two products"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @('scripts/which_doc.py', $Products[0], $Products[-1], '--db', $dbPath)

    Write-Heading "Showing labeled sentence evidence"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/show_sentence_evidence.py',
        '--db', $dbPath,
        '--product-a', $Products[0],
        '--product-b', $Products[-1],
        '--limit', '10'
    )
}

function Run-StreamlitViewer {
    if ($SkipStreamlit) {
        Write-Host "Skipping Streamlit evidence browser (SkipStreamlit set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Optional Streamlit evidence browser"
    & $PythonExe -c "import importlib.util; import sys; sys.exit(0 if importlib.util.find_spec('streamlit') else 1)"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Streamlit not installed. Run: pip install streamlit" -ForegroundColor Yellow
        return
    }

    Write-Host "Launching Streamlit evidence browser. Close the browser tab or CTRL+C to exit." -ForegroundColor Yellow
    & $PythonExe -m streamlit run scripts/view_labeled_sentences.py
}

function Run-MetricsDashboard {
    if ($SkipStreamlit) {
        Write-Host "Skipping Streamlit metrics dashboard (SkipStreamlit set)." -ForegroundColor Yellow
        return
    }

    Write-Heading "Optional Streamlit metrics dashboard"
    & $PythonExe -c "import importlib.util; import sys; sys.exit(0 if importlib.util.find_spec('streamlit') else 1)"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Streamlit not installed. Run: pip install streamlit" -ForegroundColor Yellow
        return
    }

    Write-Host "Launching Streamlit metrics dashboard. Close the browser tab or CTRL+C to exit." -ForegroundColor Yellow
    & $PythonExe -m streamlit run scripts/metrics_dashboard.py
}

function Run-NarrativeKpiChecks {
    param(
        [string]$DataRoot
    )

    Write-Heading "Validating narrative KPI artifacts"
    $scriptPath = Join-Path $RepoRoot "scripts/check_narrative_kpis.py"
    if (-not (Test-Path -Path $scriptPath)) {
        throw "KPI validation script not found at $scriptPath"
    }

    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    $metricsDir = Join-Path $DataRoot "metrics"
    $kpiDataRoot = $RepoRoot
    $kpiConfigPath = Join-Path $RepoRoot "config/narratives_kpis.json"
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        $scriptPath,
        "--db", $dbPath,
        "--data-root", $kpiDataRoot,
        "--metrics-dir", $metricsDir,
        "--repo-root", $RepoRoot,
        "--kpi-config", $kpiConfigPath
    )
}

function Assert-Phase2Metrics {
    param(
        [string]$DbPath,
        [string]$MetricsDir
    )

    Write-Heading "Validating Phase 2 narrative/directional coverage"

    if (-not (Test-Path -Path $DbPath)) {
        throw "SQLite database not found at $DbPath. Run ingestion first."
    }

    $requiredFiles = @(
        "narratives_w.parquet",
        "narratives_change_w.parquet",
        "directional_w.parquet",
        "sentiment_w.parquet"
    )

    foreach ($name in $requiredFiles) {
        $path = Join-Path $MetricsDir $name
        if (-not (Test-Path -Path $path)) {
            throw "Required metrics file missing: $path"
        }
        $fileInfo = Get-Item $path
        if ($fileInfo.Length -le 0) {
            throw "Metrics file is empty: $path"
        }
    }

    $scriptContent = @"
import json
import sqlite3
import sys

db_path = sys.argv[1]
conn = sqlite3.connect(db_path)
queries = {
    "narratives": "SELECT COUNT(*) FROM sentence_events WHERE narrative_type IS NOT NULL",
    "directional": "SELECT COUNT(*) FROM sentence_events WHERE direction_type IS NOT NULL",
    "sentiment": "SELECT COUNT(*) FROM sentence_events WHERE sentiment_label IS NOT NULL"
}
results = {}
for key, sql in queries.items():
    cur = conn.execute(sql)
    value = cur.fetchone()[0]
    results[key] = int(value or 0)
conn.close()
print(json.dumps(results))
"@

    $tempPath = Join-Path ([System.IO.Path]::GetTempPath()) ("phase2_metrics_{0}.py" -f ([System.Guid]::NewGuid().ToString("N")))
    Set-Content -Path $tempPath -Value $scriptContent -Encoding UTF8
    try {
        $jsonOutput = & $PythonExe $tempPath $DbPath
    } finally {
        Remove-Item $tempPath -ErrorAction SilentlyContinue
    }

    $counts = $jsonOutput | ConvertFrom-Json
    if ([int]$counts.narratives -le 0) {
        throw "Narrative classification produced zero rows. Phase 2 outputs unavailable."
    }
    if ([int]$counts.directional -le 0) {
        throw "Directional roles produced zero rows. Phase 2 competitive context missing."
    }
    if ([int]$counts.sentiment -le 0) {
        throw "Sentiment labels not stored in SQLite; rerun scripts.label_sentence_sentiment."
    }

    Write-Host ("Narrative rows: {0}" -f $counts.narratives) -ForegroundColor DarkGreen
    Write-Host ("Directional rows: {0}" -f $counts.directional) -ForegroundColor DarkGreen
    Write-Host ("Sentiment rows: {0}" -f $counts.sentiment) -ForegroundColor DarkGreen
}

Write-Heading "Starting functional PowerShell checks"
Write-Host "Python executable: $PythonExe" -ForegroundColor DarkGray
Write-Host "Data root: $DataRoot" -ForegroundColor DarkGray
Write-Host "Products: $($Products -join ', ')" -ForegroundColor DarkGray
Write-Host "Max records per ingestion: $MaxRecords" -ForegroundColor DarkGray

Show-ConfigSummary -ProductsToCheck $Products -ProductConfig $ProductConfigPath -WeightConfig $StudyWeightConfigPath

Run-Pytests
Run-Ingestion
Run-LabelSentenceEvents
Run-LabelSentenceSentiment
Run-AggregateMetrics
Run-SentimentMetrics
$dbPath = Join-Path $DataRoot "europepmc.sqlite"
$metricsDir = Join-Path $DataRoot "metrics"
Assert-Phase2Metrics -DbPath $dbPath -MetricsDir $metricsDir
Run-NarrativeKpiChecks -DataRoot $DataRoot
Run-ExportBatch
Run-Queries
Run-StreamlitViewer
Run-MetricsDashboard

Write-Heading "Phase 1 ops checklist (run summary)"
$dbPath = Join-Path $DataRoot "europepmc.sqlite"
$metricsDir = Join-Path $DataRoot "metrics"
$exportRoot = Join-Path $DataRoot "exports"
Write-Host ("Products: {0}" -f ($Products -join ", "))
Write-Host ("Product config: {0}" -f $ProductConfigPath)
Write-Host ("Study weight config: {0}" -f $StudyWeightConfigPath)
Write-Host ("SQLite DB: {0}" -f $dbPath)
if ($script:StructuredJsonl) {
    Write-Host ("Structured JSONL: {0}" -f $script:StructuredJsonl)
}
Write-Host ("Metrics dir: {0}" -f $metricsDir)
Write-Host ("Narrative metrics: {0}" -f (Join-Path $metricsDir "narratives_w.parquet"))
Write-Host ("Directional metrics: {0}" -f (Join-Path $metricsDir "directional_w.parquet"))
Write-Host ("Export root: {0}" -f $exportRoot)
Write-Host "Key scripts run:"
Write-Host "  - scripts/ingest_europe_pmc.py"
Write-Host "  - scripts/label_sentence_events.py"
Write-Host "  - scripts/label_sentence_sentiment.py"
Write-Host "  - scripts/aggregate_metrics.py"
Write-Host "  - scripts/export_sentiment_metrics.py"
Write-Host "  - scripts/export_batch.py"
Write-Host "  - scripts/query_comentions.py"
Write-Host "  - scripts/show_sentence_evidence.py"
Write-Host "Streamlit dashboards (optional):"
Write-Host "  - scripts/view_labeled_sentences.py"
Write-Host "  - scripts/metrics_dashboard.py"

Write-Heading "All checks completed"

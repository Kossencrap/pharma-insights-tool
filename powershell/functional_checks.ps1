[CmdletBinding()]
param(
    [string]$PythonExe = "py",
    [string]$DataRoot = "data/powershell-checks",
    [string[]]$Products = @("dupilumab", "Dupixent"),
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

function Show-ConfigSummary {
    param(
        [string[]]$ProductsToCheck,
        [string]$ProductConfig,
        [string]$WeightConfig
    )

    Write-Heading "Config snapshot"

    if (Test-Path $ProductConfig) {
        Write-Host ("Product config: {0}" -f $ProductConfig) -ForegroundColor DarkGray
        $productConfigJson = Get-Content -Raw $ProductConfig | ConvertFrom-Json
        $productLookup = @{}
        foreach ($entry in $productConfigJson.PSObject.Properties) {
            $productLookup[$entry.Name.ToLower()] = $entry.Value
        }
        foreach ($product in $ProductsToCheck) {
            $key = $product.ToLower()
            if ($productLookup.ContainsKey($key)) {
                $aliases = $productLookup[$key] -join ", "
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
    foreach ($product in $Products) {
        $ingestArgs += @('-p', $product)
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
}

function Run-LabelSentenceSentiment {
    Write-Heading "Labeling sentence sentiment"
    $dbPath = Join-Path $DataRoot "europepmc.sqlite"
    if (-not $script:StructuredJsonl) {
        $prefix = ($Products[0]).ToLower() -replace '\s+', '_'
        $script:StructuredJsonl = Join-Path "data/processed" ("{0}_structured.jsonl" -f $prefix)
    }
    if (-not (Test-Path $script:StructuredJsonl)) {
        if ($SkipNetworkSteps) {
            Write-Host ("Structured JSONL not found at {0}. Skipping sentiment labeling (SkipNetworkSteps set)." -f $script:StructuredJsonl) -ForegroundColor Yellow
            return
        }
        Write-Host ("Structured JSONL not found at {0}. Ensure ingestion runs first." -f $script:StructuredJsonl) -ForegroundColor Yellow
        return
    }
    Invoke-ExternalCommand -Executable $PythonExe -Arguments @(
        'scripts/label_sentence_sentiment.py',
        '--input', $script:StructuredJsonl,
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

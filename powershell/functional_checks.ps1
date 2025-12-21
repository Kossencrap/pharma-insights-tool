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
        '--outdir', $outDir
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

Run-Pytests
Run-Ingestion
Run-LabelSentenceEvents
Run-AggregateMetrics
Run-SentimentMetrics
Run-ExportBatch
Run-Queries
Run-StreamlitViewer
Run-MetricsDashboard

Write-Heading "All checks completed"

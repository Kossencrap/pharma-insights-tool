[CmdletBinding()]
param(
    [string]$PythonExe = "py",
    [string]$DataRoot = "data/powershell-checks",
    [string[]]$Products = @("dupilumab", "Dupixent"),
    [int]$MaxRecords = 25,
    [switch]$SkipPytests,
    [switch]$SkipNetworkSteps,
    [switch]$SkipFunctionalQueries
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
}

Write-Heading "Starting functional PowerShell checks"
Write-Host "Python executable: $PythonExe" -ForegroundColor DarkGray
Write-Host "Data root: $DataRoot" -ForegroundColor DarkGray
Write-Host "Products: $($Products -join ', ')" -ForegroundColor DarkGray
Write-Host "Max records per ingestion: $MaxRecords" -ForegroundColor DarkGray

Run-Pytests
Run-Ingestion
Run-Queries

Write-Heading "All checks completed"

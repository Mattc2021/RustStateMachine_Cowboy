param(
    [int]$ExpectedApps = 3,
    [int]$TimeoutSeconds = 20,
    [int]$HoldMilliseconds = 1000,
    [int]$FaultAfterMilliseconds = 5000,
    [int]$PowerOffMilliseconds = 2000
)

$ErrorActionPreference = "Stop"
$workspace = Split-Path -Parent $PSScriptRoot
$processes = @()

Push-Location $workspace
try {
    Write-Host "Building the complete SAM workspace..."
    cargo build --workspace
    if ($LASTEXITCODE -ne 0) { throw "cargo build failed with exit code $LASTEXITCODE" }

    $samArguments = @(
        "--auto-showcase",
        "--expected-apps", $ExpectedApps,
        "--auto-timeout-secs", $TimeoutSeconds,
        "--auto-hold-ms", $HoldMilliseconds
    )

    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host " SAM RESILIENCE SHOWCASE" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "Starting SAM, navigation, guidance, and telemetry..."
    $sam = Start-Process -FilePath ".\target\debug\sam.exe" -ArgumentList $samArguments -PassThru -NoNewWindow
    $processes += $sam
    $processes += Start-Process -FilePath ".\target\debug\navigation.exe" -PassThru -NoNewWindow
    $processes += Start-Process -FilePath ".\target\debug\guidance.exe" -PassThru -NoNewWindow
    $telemetry = Start-Process -FilePath ".\target\debug\telemetry.exe" -ArgumentList "--exit-after-ms", $FaultAfterMilliseconds -PassThru -NoNewWindow
    $processes += $telemetry

    $telemetry.WaitForExit()

    Write-Host ""
    Write-Host "[LAUNCHER] Telemetry process stopped; power is OFF for $PowerOffMilliseconds ms..." -ForegroundColor Yellow
    Start-Sleep -Milliseconds $PowerOffMilliseconds
    Write-Host "[LAUNCHER] Restoring telemetry power with one temporary safety interlock..." -ForegroundColor Green
    $telemetry = Start-Process -FilePath ".\target\debug\telemetry.exe" -ArgumentList "--reject-working-once" -PassThru -NoNewWindow
    $processes += $telemetry

    $sam.WaitForExit()
    $sam.Refresh()
    $samExitCode = $sam.ExitCode
    if ($null -ne $samExitCode -and $samExitCode -ne 0) {
        throw "SAM exited with code $samExitCode"
    }
    Write-Host ""
    Write-Host "SHOWCASE PASSED" -ForegroundColor Green
}
finally {
    foreach ($process in $processes) {
        if (-not $process.HasExited) {
            Stop-Process -Id $process.Id -Force
        }
    }
    Pop-Location
}

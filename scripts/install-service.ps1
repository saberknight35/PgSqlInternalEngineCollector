# install-service.ps1
# Publishes the collector and installs it as an auto-start Windows Service with
# restart-on-failure recovery (the recovery config is the watchdog — no extra task).
# Run from an elevated PowerShell on the collector VM.

param(
    [string]$ServiceName = "PgSqlInternalEngineCollector",
    [string]$InstallDir  = "C:\dms-metrics\service",
    [string]$ProjectPath = "..\PgSqlInternalEngineCollector.csproj"
)

$ErrorActionPreference = "Stop"

Write-Host "Publishing $ProjectPath ..."
dotnet publish $ProjectPath -c Release -r win-x64 --self-contained false -o $InstallDir

$exe = Join-Path $InstallDir "PgSqlInternalEngineCollector.Service.exe"
if (-not (Test-Path $exe)) { throw "Published exe not found at $exe" }

if (Get-Service $ServiceName -ErrorAction SilentlyContinue) {
    Write-Host "Stopping and removing existing service..."
    Stop-Service $ServiceName -ErrorAction SilentlyContinue
    sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 2
}

Write-Host "Creating service..."
New-Service -Name $ServiceName -BinaryPathName $exe -StartupType Automatic `
    -DisplayName "PostgreSQL Internal Engine Collector"

# Recovery: restart after 5s on 1st/2nd/3rd failure; reset fail count daily.
Write-Host "Configuring recovery (restart on failure)..."
sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null

Write-Host "Starting service..."
Start-Service $ServiceName
Get-Service $ServiceName
Write-Host "Done. Logs go to the Windows Event Log / your configured sink."

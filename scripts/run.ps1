param([Parameter(Mandatory=$true)][string]$Action)
. "$PSScriptRoot\environment.ps1"
& "$env:RTAP_TOOLING\venv\Scripts\python.exe" "$PSScriptRoot\lab.py" $Action
if($LASTEXITCODE -ne 0){throw "Lab action failed: $Action. Inspect logs in $env:RTAP_ROOT\logs"}

. "$PSScriptRoot\environment.ps1"
& "$env:RTAP_TOOLING\venv\Scripts\python.exe" "$PSScriptRoot\verify_kafka.py" --count 10
if($LASTEXITCODE -ne 0){throw 'Kafka consumer verification failed'}

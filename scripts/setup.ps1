. "$PSScriptRoot/environment.ps1"
$python = if($env:RTAP_PYTHON){$env:RTAP_PYTHON}else{'python'}
& $python -c "import sys; assert sys.version_info[:2]==(3,12), 'Python 3.12 required'"
if($LASTEXITCODE){throw 'Python 3.12 required; set RTAP_PYTHON'}
& $python -m venv "$env:RTAP_TOOLING/venv"
if($LASTEXITCODE){throw 'venv failed'}
& $env:PYSPARK_PYTHON -m pip install torch==2.8.0 torchvision==0.23.0 --index-url https://download.pytorch.org/whl/cpu
if($LASTEXITCODE){throw 'PyTorch failed'}
& $env:PYSPARK_PYTHON -m pip install -r requirements.txt
if($LASTEXITCODE){throw 'Dependencies failed'}
$archive="$env:RTAP_CACHE/kafka.tgz"
if(!(Test-Path "$env:KAFKA_HOME/libs/kafka_2.13-3.9.1.jar")){
 Invoke-WebRequest https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz -OutFile $archive
 Invoke-WebRequest https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz.sha512 -OutFile "$archive.sha512"
 $expected=(([IO.File]::ReadAllText("$archive.sha512")) -split ':',2)[1] -replace '\s',''
 if((Get-FileHash $archive -Algorithm SHA512).Hash -ine $expected){throw 'Kafka hash mismatch'}
 tar -xzf $archive -C $env:RTAP_TOOLING
 if($LASTEXITCODE){throw 'Kafka extraction failed'}
}
New-Item -ItemType Directory -Force .runtime/models | Out-Null
if(!(Test-Path .runtime/models/yolov8n.pt)){Invoke-WebRequest https://github.com/ultralytics/assets/releases/download/v8.3.0/yolov8n.pt -OutFile .runtime/models/yolov8n.pt}
& mvn.cmd -B -f config/spark-kafka-pom.xml "-Dmaven.repo.local=$env:RTAP_CACHE/maven" dependency:copy-dependencies "-DoutputDirectory=$env:RTAP_TOOLING/spark-kafka-jars"
if($LASTEXITCODE){throw 'Install Maven 3.9 and add mvn.cmd to PATH'}
& $env:PYSPARK_PYTHON -m pip check
if($LASTEXITCODE){throw 'Dependency check failed'}
Write-Output 'Run init_kafka_cluster.bat ONCE then START_LAB.bat'

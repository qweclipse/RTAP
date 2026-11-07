$ErrorActionPreference = 'Stop'
$env:RTAP_ROOT = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $env:RTAP_ROOT
$base = Join-Path $env:RTAP_ROOT '.runtime'
$env:RTAP_TOOLING = "$base/tooling"
$env:RTAP_DATA = "$base/data"
$env:RTAP_CACHE = "$base/cache"
$env:TEMP = "$base/temp"
$env:TMP = $env:TEMP
$env:TMPDIR = $env:TEMP
$env:PIP_CACHE_DIR = "$env:RTAP_CACHE/pip"
$env:PYTHONPYCACHEPREFIX = "$env:RTAP_CACHE/pycache"
$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUTF8 = '1'
$env:SPARK_HOME = "$env:RTAP_TOOLING/venv/Lib/site-packages/pyspark"
$env:PYSPARK_PYTHON = "$env:RTAP_TOOLING/venv/Scripts/python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:SPARK_LOCAL_IP = '127.0.0.1'
$env:SPARK_LOCAL_DIRS = "$env:RTAP_DATA/spark-temp"
$env:KAFKA_HOME = "$env:RTAP_TOOLING/kafka_2.13-3.9.1"
$env:OMP_NUM_THREADS = '2'
$env:MKL_NUM_THREADS = '2'
foreach($path in @($env:TEMP,$env:RTAP_DATA,$env:RTAP_CACHE,$env:RTAP_TOOLING,$env:PIP_CACHE_DIR,$env:PYTHONPYCACHEPREFIX)){New-Item -ItemType Directory -Force -Path $path | Out-Null}
if(!$env:JAVA_HOME -or !(Test-Path "$env:JAVA_HOME/bin/java.exe")){throw 'Set JAVA_HOME to an installed JDK 21'}
if(!$env:HADOOP_HOME -or !(Test-Path "$env:HADOOP_HOME/bin/hadoop.dll")){throw 'Set HADOOP_HOME to Hadoop 3.4.1 native Windows binaries (see README)'}
$env:JAVA_TOOL_OPTIONS = '-Djava.io.tmpdir="'+($env:TEMP -replace '\\','/')+'" -Duser.home="'+($env:RTAP_CACHE -replace '\\','/')+'"'
$env:PATH = "$env:HADOOP_HOME/bin;$env:JAVA_HOME/bin;$env:RTAP_TOOLING/venv/Scripts;$env:PATH"

$env:SPARK_CONF_DIR = "$env:RTAP_ROOT/config/spark"

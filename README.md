# RTAP — Real-Time Analytical Processing

Windows streaming analytics: webcam + microphone → YOLOv8n/WebRTC VAD → Kafka KRaft → Spark Structured Streaming → Logistic Regression + SHAP → Streamlit.

## Features
Real video/audio events; 10-second event-time windows; L1/L2/Elastic Net models; interpretable alerts; RU/RO interface; pause/resume and microphone selection; independent camera, inference, preview and Kafka rates. Preview reuses Producer detections. Missing audio remains NULL, not silence.

## Requirements
Windows 10/11 x64, Python **3.12**, JDK **21**, Maven **3.9**, webcam and microphone. CPU works; NVIDIA CUDA is optional. Tested original workstation: Ryzen 5 5600H, RTX 3050 Laptop, about 14 GiB usable RAM. Kafka/Spark/PyTorch can use several GiB RAM; allow at least 12 GiB free disk for tools/caches. No claim of support for all machines or operating systems.

**Manual prerequisite:** install/build native Windows Hadoop **3.4.1** (`bin/hadoop.dll`, `bin/winutils.exe`) from trusted Apache source, using Visual Studio C++ build tools where required. Native Hadoop compilation is not automated here. No unverified third-party native binaries are shipped. See https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/WindowsProblems.html and https://archive.apache.org/dist/hadoop/common/hadoop-3.4.1/ .

## Setup
Clone/download and open PowerShell in this folder. Set `JAVA_HOME` and `HADOOP_HOME` to your installed components, and ensure `mvn.cmd` is on PATH. If Python 3.12 is not the default, set `RTAP_PYTHON` to its executable. These settings belong to your shell, not a committed personal configuration.

Run `SETUP_LAB.bat`. It creates `.runtime/tooling/venv`, installs pinned Python dependencies (CPU PyTorch by default), downloads Kafka 3.9.1 with SHA512 verification and YOLOv8n weights, and resolves Spark 4.0.1 Kafka jars through Maven. All runtime/cache paths are local to this checkout. Then run `init_kafka_cluster.bat` once for a new empty cluster. Existing Kafka identity is never reformatted.

## Run
Double-click **START_LAB.bat**, open http://127.0.0.1:8501 and click «Начать анализ» / «Începe analiza». Opening the dashboard leaves the camera off. Pause releases capture/audio; resume uses the same Producer. To stop, use the checkbox and shutdown button, or **STOP_LAB.bat**. Run CHECK_LAB.bat for diagnostics. Preview is read-only on loopback port 8502. Ports 9092/9093 are reserved for local Kafka.

Performance sidebar selects standard (30/1/1/1) or performance (30/8/5/3): camera/YOLO/preview FPS/Kafka events per second. Actual rates depend on hardware. CPU fallback is automatic; optional GPU installation uses the official PyTorch 2.8.0 CUDA 12.6 index with torchvision 0.23.0. Stop the application before changing packages. No global CUDA Toolkit is required by those wheels.

## Models and validation
The included joblib bundle contains six models trained on **480 illustrative synthetic rows, 60 sessions**; train/test 360/120 without session overlap. Synthetic test F1 0.9739 is NOT real-world accuracy; tuning did not improve held-out F1. `scripts/train_models.py` reproduces the synthetic dataset and training; it overwrites only this checkout's model/metric outputs. Treat joblib files as trusted code artifacts.

With the environment active, run `python -m pytest -q`. Tests use temporary fixtures, not a second physical camera. The historical workstation validation included 1,075 actual events and 42 independently reconciled windows; those private recordings, logs, screenshots and academic documents are deliberately excluded. This cleaned copy's checks are documented in RELEASE_CHECKS.md.

## Layout and limitations
`kafka_trio_MD.py`: capture/features/events; `spark_trio_MD.py`: schema, validation, watermark 5 s, deduplication and atomic final CSV; `alert_trio_MD.py`: dashboard and saved ML models. Shared code is in `rtap/`; lifecycle/setup in `scripts/`; editable defaults in `config/lab.json`.

One local broker/replica is not a highly available cluster. Kafka retention is disabled to avoid Windows mapped-index rename failures: monitor `.runtime/data` disk use. Late/missing measurements are explicit; RMS is not calibrated dB SPL; VAD detects voice activity, not speech content. `bias_flag` remains NULL. Multi-day stability, diverse physical scenes, three-person group accuracy and USB unplug scenarios are not established. Academic reports and presentations are not part of this application repository.

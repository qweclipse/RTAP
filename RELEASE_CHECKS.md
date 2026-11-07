# Publication verification — 2026-11-07

- 37 pytest/AppTest checks passed in this cleaned checkout; three third-party SHAP/matplotlib deprecation warnings.
- Real Kafka KRaft and Spark Structured Streaming started successfully with isolated data and temporary ports 19092/19093; Streamlit health returned HTTP 200 on 18501. The Producer stayed stopped, so no camera or microphone was acquired.
- The smoke harness restored release configuration (9092/9093, dashboard 8501), then gracefully stopped only the processes it started.
- Installed pinned Python packages, Kafka jars, JDK and native Hadoop were reused from the workstation. This is an isolated source/runtime smoke test, not a fresh Windows installation or proof on another computer. Native Hadoop remains a documented manual prerequisite.
- Personal storage paths were replaced by checkout-local `.runtime` paths or required environment variables. Fixes in this publication copy include absolute Spark/Ivy/file-URI paths and rejection of stale component error heartbeats during restart.
- The ML metric test reconstructs the deterministic synthetic test rows and predicts with the shipped models; it does not require private experiment exports.
- The source application, models and original evidence in the development workspace were not modified. No academic documents, webcam images, hardware logs, Kafka data, checkpoints, caches, installed tools or environment secrets are included.

The historical hardware results described in README belong to the development workstation and are not presented as a fresh hardware benchmark for this release.

"""Real Spark CSV -> Logistic Regression / SHAP, with local lifecycle controls."""
from rtap.config import configure_storage
configure_storage()

import json
import subprocess
import sys
import time

import joblib
import pandas as pd
import streamlit as st
from streamlit.components.v1 import iframe
from kafka import KafkaConsumer

from rtap.config import ROOT, data_dir, load_config
from rtap.control import controller_state, request_command
from rtap.events import alert_state
from rtap.i18n import translator
from rtap.ml import build_explainers, explain, infer
from rtap.performance import PROFILES, validate_rates
from rtap.storage import read_aggregates, read_json
from rtap.timeline import local_table, tag_sessions

st.set_page_config(page_title="RTAP", page_icon="📡", layout="wide")
config = load_config()
root = data_dir()
performance_config = read_json(root / "performance.json", {})
language = st.sidebar.selectbox("Язык / Limba", ["Русский", "Română"], key="language")
t = translator(language)
st.title(t("title"))
st.caption("Webcam → YOLOv8n → Kafka KRaft → Spark · 10 s → Logistic Regression + SHAP")


@st.cache_resource
def load_models(modified):
    bundle = joblib.load(ROOT / "models/logistic_models.joblib")
    return bundle, build_explainers(bundle)


@st.cache_data(ttl=5)
def kafka_status():
    try:
        client = KafkaConsumer(bootstrap_servers=config["bootstrap_servers"], request_timeout_ms=3000,
                               session_timeout_ms=2000, heartbeat_interval_ms=600,
                               api_version_auto_timeout_ms=2000)
        try:
            return config["topic"] in client.topics()
        finally:
            client.close()
    except Exception:
        return False


@st.cache_data(ttl=30)
def audio_devices():
    # A fresh PortAudio process sees hotplug without touching the Producer stream.
    try:
        result = subprocess.run([sys.executable, "-m", "rtap.audio"], cwd=ROOT,
                                capture_output=True, text=True, encoding="utf-8", timeout=8,
                                creationflags=subprocess.CREATE_NO_WINDOW)
        return json.loads(result.stdout) if result.returncode == 0 else []
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return []


@st.cache_data(max_entries=2)
def aggregates_for(signature):
    return read_aggregates(root / "aggregated_events")


st.sidebar.header(t("settings"))
model_file = ROOT / "models/logistic_models.joblib"
if not model_file.exists():
    st.error(t("missing_models"))
    st.stop()
bundle, explainers = load_models(model_file.stat().st_mtime_ns)
format_model = lambda value: value.replace("tuned", t("tuned"))
model_keys = list(bundle["models"])
model_name = st.sidebar.selectbox(t("model"), model_keys, index=model_keys.index(st.session_state.get("selected_model", model_keys[-1])), key="model_"+language, format_func=format_model)
st.session_state.selected_model = model_name
persons = st.sidebar.number_input(t("persons_threshold"), min_value=1., value=st.session_state.get("person_setting", float(config["alert_person_threshold"])), step=1., key="persons_threshold_"+language)
noise = st.sidebar.number_input(t("noise_threshold"), min_value=0., max_value=1., value=st.session_state.get("noise_setting", float(config["alert_noise_threshold"])), step=.05, key="noise_threshold_"+language)
st.session_state.person_setting, st.session_state.noise_setting = persons, noise
st.sidebar.caption(t("rule"))
st.sidebar.info(t("synthetic"))


def component_state(state):
    if not state:
        return t("stopped")
    if state.get("status") == "running" and time.time()-state.get("updated", 0) > 15:
        return t("stale")
    return t(state.get("status", "stopped")) if state.get("status") in ("running", "starting", "paused", "stopped") else t("error")


def send(action, **payload):
    st.session_state.pending = request_command(action, **payload)
    st.session_state.pending_at = time.time()
    st.session_state.shutdown_sent = action == "shutdown"
    st.session_state.pending_action = action


@st.fragment(run_every="1s")
def controls():
    output = st.empty()
    output.empty()
    with output.container():
        if read_json(root / "performance.json", {}).get("revision") != performance_config.get("revision"):
            st.rerun()
        state = controller_state()
        producer = read_json(root / "producer.json", {})
        spark = read_json(root / "aggregated_events/spark.json", {})
        status = state.get("status", "STOPPED")
        pending = st.session_state.get("pending")
        if pending == state.get("last_command") or time.time()-st.session_state.get("pending_at", 0) > 60:
            st.session_state.pending = None
            if pending and pending == state.get("last_command") and st.session_state.get("pending_action") == "performance":
                st.rerun()
            pending = None
        busy = bool(pending) or status in ("STARTING", "STOPPING") or not state["available"]
        st.subheader(t("system"))
        cols = st.columns(3)
        cols[0].metric("Kafka / video_events", t("connected") if kafka_status() else t("unavailable"))
        cols[1].metric("Spark Structured Streaming", component_state(spark))
        cols[2].metric("Producer", status, help=t(status))
        st.caption(t(status))
        if not state["available"]:
            st.warning(t("controller_off"))
        if state.get("error"):
            st.error(t(state["error"]))
        if spark.get("error"):
            st.error(t("error") + " · Spark")
        cols = st.columns(3)
        if cols[0].button(t("start"), disabled=busy or status not in ("STOPPED", "ERROR"), key="start", width="stretch"):
            send("start")
            st.rerun(scope="fragment")
        if cols[1].button(t("pause"), disabled=busy or status != "RUNNING", key="pause", width="stretch"):
            send("pause")
            st.rerun(scope="fragment")
        if cols[2].button(t("resume"), disabled=busy or status != "PAUSED", key="resume", width="stretch"):
            send("resume")
            st.rerun(scope="fragment")
        confirm = st.checkbox(t("confirm"), key="confirm_shutdown")
        if st.button(t("shutdown"), disabled=busy or not confirm, key="shutdown"):
            send("shutdown")
            st.rerun(scope="fragment")
        if pending or st.session_state.get("shutdown_sent"):
            st.info(t("shutdown_message") if st.session_state.get("shutdown_sent") else t("queued"))
        if producer.get("audio_error") and status == "RUNNING":
            st.warning(t("audio_warning"))
        if producer.get("audio_device") and status == "RUNNING":
            st.caption(producer["audio_device"])
        if status == "RUNNING":
            fps = producer.get("actual_fps", {})
            cols = st.columns(4)
            for col, key in zip(cols, PROFILES["standard"]):
                col.metric(t(key+"_metric"), f"{fps.get(key, 0):.1f}", help=t(key+"_rate"))
            diagnostics = producer.get("diagnostics", {})
            if diagnostics.get("fallback_reason"):
                st.warning(t("cpu_fallback"))


@st.fragment(run_every="1s")
def preview():
    st.markdown("**" + t("preview") + "**")
    producer = read_json(root / "producer.json", {})
    status = producer.get("status")
    if status == "running" and time.time()-producer.get("updated", 0) < 5 and producer.get("preview_url"):
        iframe(producer["preview_url"]+"?language="+("ro" if language == "Română" else "ru"), height=250)
    elif producer.get("preview_error") and status == "running":
        st.warning(t("preview_unavailable"))
    else:
        st.info(t("camera_paused") if status == "paused" else t("waiting_frame") if status == "starting" else t("camera_off"))


def performance_settings():
    applied = read_json(root / "performance.json", {})
    current = applied.get("rates", PROFILES[config.get("performance_profile", "standard")])
    with st.sidebar.expander(t("performance")):
        if st.session_state.get("rate_revision") != applied.get("revision", "initial"):
            st.session_state.rate_profile = "standard" if current == PROFILES["standard"] else "performance"
            st.session_state.rate_revision = applied.get("revision", "initial")
        profile = st.selectbox(t("profile"), list(PROFILES), index=int(current != PROFILES["standard"]),
                               key="rate_profile", format_func=lambda key: t("standard" if key == "standard" else "performance_mode"))
        previous = st.session_state.get("edited_profile")
        defaults = PROFILES[profile] if previous is not None and previous != profile else current
        st.session_state.edited_profile = profile
        with st.form("performance_form"):
            values = {}
            limits = dict(camera=(1.,60.), inference=(.1,30.), preview=(.1,30.), kafka=(.01,5.))
            for key, (low, high) in limits.items():
                values[key] = st.number_input(t(key+"_rate"), min_value=low, max_value=high,
                    value=float(defaults[key]), step=1. if key == "camera" else .1,
                    key=f"rate_{key}_{profile}_{applied.get('revision', 'initial')}")
            if st.form_submit_button(t("apply"), key="apply_performance", disabled=not controller_state()["available"]):
                try:
                    send("performance", rates=validate_rates(values))
                    st.rerun()
                except ValueError:
                    st.error(t("invalid_rates"))
        st.caption(t("rate_help"))


def audio_settings():
    # Keep dropdown outside timed fragments so refresh cannot close an open menu.
    devices = audio_devices()
    labels = {"__default__": t("default_audio"), "__off__": t("off_audio")}
    labels.update({device["key"]: device["label"] for device in devices})
    applied = read_json(root / "audio_selection.json", {})
    selected = applied.get("key", "__default__")
    if selected not in labels:
        labels[selected] = t("missing_audio")
    with st.form("audio_form", border=False):
        keys = list(labels)
        choice = st.selectbox(t("audio_source"), keys, index=keys.index(selected), format_func=labels.get,
                              key=f"audio_source_{language}_{applied.get('revision', 'initial')}")
        if st.form_submit_button(t("apply"), disabled=not controller_state()["available"], key="apply_audio"):
            send("audio", key=choice)
            st.rerun()
    if st.button(t("refresh"), key="refresh_audio"):
        audio_devices.clear()
        st.rerun()
    st.caption(t("audio_help"))


def show_chart(data):
    trend = data[["window_end", "avg_person", "avg_objects", "line_group"]].copy()
    trend["local_time"] = pd.to_datetime(trend["window_end"], utc=True).dt.tz_convert("Europe/Chisinau").dt.strftime("%Y-%m-%d %H:%M:%S")
    trend = trend.melt(id_vars=["local_time", "line_group"], value_vars=["avg_person", "avg_objects"], var_name="feature")
    st.vega_lite_chart(spec={"data": {"values": trend.to_dict("records")}, "mark": {"type": "line", "point": True}, "encoding": {
        "x": {"field": "local_time", "type": "temporal", "title": t("time")},
        "y": {"field": "value", "type": "quantitative", "title": t("mean")},
        "color": {"field": "feature", "type": "nominal"},
        "detail": {"field": "line_group", "type": "nominal"}}}, width="stretch")


@st.fragment(run_every="1s")
def observations():
    output = st.empty()
    output.empty()
    with output.container():
        producer = read_json(root / "producer.json", {})
        state = controller_state()
        active = state.get("status") == "RUNNING" and time.time()-producer.get("updated", 0) < 5
        event = producer.get("last_event")
        st.subheader(t("observations"))
        if event:
            cols = st.columns(4)
            cols[0].metric(t("people"), event["n_person"])
            cols[1].metric(t("objects"), event["object_count"])
            cols[2].metric("noise_level · RMS", f"{event['noise_level']:.4f}" if event["noise_level"] is not None else "NULL")
            cols[3].metric(t("messages"), producer.get("messages", 0))
            cols = st.columns(2)
            cols[0].metric("voice_intensity · RMS", f"{event.get('voice_intensity'):.4f}" if event.get("voice_intensity") is not None else "NULL")
            cols[1].metric("speech_flag · VAD", event.get("speech_flag") if event.get("speech_flag") is not None else "NULL")
            local = pd.to_datetime(event["timestamp"], unit="s", utc=True).tz_convert("Europe/Chisinau")
            st.caption(f"{local} · Europe/Chisinau · UTC {pd.to_datetime(event['timestamp'], unit='s', utc=True)} · offset {producer.get('kafka_offset')}")
            if not active:
                st.warning(t("historical_observation"))
        else:
            st.info(t("waiting_observation"))


@st.fragment(run_every="5s")
def live():
    output = st.empty()
    output.empty()
    with output.container():
        producer = read_json(root / "producer.json", {})
        state = controller_state()
        active = state.get("status") == "RUNNING" and time.time()-producer.get("updated", 0) < 5
        files = sorted((root / "aggregated_events").glob("batch-*.csv"))
        data = aggregates_for(tuple((str(p), p.stat().st_mtime_ns) for p in files[-300:]))
        st.subheader(t("aggregates"))
        st.caption(t("windows"))
        if data.empty:
            st.info(t("waiting_window"))
            return
        data = tag_sessions(data, read_json(root / "sessions.json", []))
        current = data[data.session_id == state.get("session_id")]
        history = data[data.session_id != state.get("session_id")]
        tab_current, tab_history = st.tabs([t("current_session"), t("history")])
        with tab_current:
            if current.empty:
                st.info(t("session_empty"))
            else:
                st.dataframe(local_table(current.tail(30).iloc[::-1]), hide_index=True, width="stretch")
                show_chart(current)
        with tab_history:
            st.dataframe(local_table(history.tail(30).iloc[::-1]), hide_index=True, width="stretch")
            if not history.empty:
                show_chart(history)
        latest = (current if not current.empty else data).iloc[-1]
        stale = not active or current.empty or time.time()-float(latest["completed_at"]) > 30
        if stale:
            st.warning(t("historical"))
        st.subheader(t("classification"))
        result = infer(bundle, latest, model_name)
        label_key = "group" if result["probability"] >= .5 else "solo"
        st.metric(f"{format_model(model_name)} · {t('latest_window')}", t(label_key), f"P = {result['probability']:.3f}")
        if latest.avg_person == 0:
            st.info(t("no_people"))
        if result["missing_features"]:
            st.caption(t("imputed") + " " + ", ".join(result["missing_features"]))
        st.subheader(t("alerts"))
        alert = alert_state(latest, persons, noise)
        {"ALERT": st.error, "UNAVAILABLE": st.warning, "NORMAL": st.success}[alert](t({"ALERT": "alert", "UNAVAILABLE": "indeterminate", "NORMAL": "normal"}[alert]))
        recent = data.tail(20).copy()
        recent["alert_state"] = recent.apply(lambda row: t({"ALERT": "alert", "UNAVAILABLE": "indeterminate", "NORMAL": "normal"}[alert_state(row, persons, noise)]), axis=1)
        st.dataframe(local_table(recent[["window_end", "source_id", "avg_person", "avg_noise", "avg_voice", "avg_speech", "audio_samples", "event_count", "alert_state"]].iloc[::-1]).rename(columns={"alert_state": t("status")}), hide_index=True, width="stretch")
        st.subheader(t("shap"))
        shap_values, logit = explain(bundle, explainers, latest, model_name)
        st.vega_lite_chart(spec={"data": {"values": shap_values.rename(columns={"SHAP log-odds": "contribution"}).to_dict("records")},
            "mark": "bar", "height": 200, "encoding": {"y": {"field": "Feature", "type": "nominal", "title": None},
            "x": {"field": "contribution", "type": "quantitative", "title": "SHAP · log-odds"}}}, width="stretch")
        st.caption(t("shap_help") + f" logit = {logit:.4f}")


performance_settings()
left, right = st.columns([3, 1])
with left:
    controls()
with right:
    preview()
audio_settings()
observations()
live()
st.subheader(t("comparison"))
metrics = read_json(ROOT / "reports/model_metrics.json", {})
if metrics:
    st.caption(t("split"))
    table = pd.DataFrame(metrics["models"])
    table["model"] = table["model"].map(format_model)
    st.dataframe(table[["model", "cv_f1", "test_f1", "test_precision", "test_recall", "params"]], hide_index=True, width="stretch")
    selected = next(m for m in metrics["models"] if m["model"] == model_name)
    left, right = st.columns(2)
    left.write(t("coefficients"))
    left.dataframe(pd.DataFrame({"Feature": bundle["features"], t("coefficients"): selected["coefficients"]}), hide_index=True)
    right.write(t("confusion"))
    right.dataframe(pd.DataFrame(selected["confusion_matrix"], index=[t("solo"), t("group")], columns=[t("solo"), t("group")]))
st.code("C = [0.01, 0.1, 1, 10]\nl1_ratio = [0.2, 0.5, 0.8]\nmax_iter = [1000, 3000]", language="python")
st.write(t("training"))
experiments = read_json(ROOT / "reports/streaming_experiments.json")
if experiments:
    st.dataframe(pd.DataFrame(experiments["results"]), hide_index=True, width="stretch")

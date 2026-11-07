import time
from streamlit.testing.v1 import AppTest
from rtap.config import ROOT
from rtap.storage import write_csv, write_json


def test_dashboard_real_models_and_alert_states(tmp_path,monkeypatch):
    monkeypatch.setenv("RTAP_DATA",str(tmp_path))
    row=dict(window_start="2026-10-23T04:00:00.000Z",window_end="2026-10-23T04:00:10.000Z",
             source_id="TEST_FIXTURE",avg_person=4.,max_person=5,avg_noise=.15,avg_bias=None,
             avg_voice=None,avg_speech=None,avg_objects=5.,event_count=2,audio_samples=2,
             last_event_timestamp=time.time(),completed_at=time.time())
    write_csv(tmp_path/"aggregated_events/batch-000000000001.csv",[row])
    write_json(tmp_path/"producer.json",dict(status="running",updated=time.time(),messages=2))
    write_json(tmp_path/"aggregated_events/spark.json",dict(status="running",updated=time.time()))
    write_json(tmp_path/"audio_selection.json", dict(key="__off__", revision="test"))
    app=AppTest.from_file(str(ROOT/"alert_trio_MD.py"),default_timeout=30).run()
    assert not app.exception
    assert any("Предупреждение" == str(e.value) for e in app.error)
    status_table = next(e.value for e in app.dataframe if "Статус" in e.value.columns)
    assert status_table["Статус"].iloc[0] == "Предупреждение"
    assert len(app.dataframe)>=4
    app.selectbox(key="model_Русский").select("L1").run()
    assert not app.exception
    assert any("L1" in str(e.label) for e in app.metric)
    row["audio_samples"]=1
    write_csv(tmp_path/"aggregated_events/batch-000000000002.csv",[row])
    app.run()
    assert not app.exception
    assert any("Недостаточно данных" == str(e.value) for e in app.warning)
    app.selectbox(key="language").select("Română").run()
    assert not app.exception
    assert any("Date insuficiente" == str(e.value) for e in app.warning)
    assert any("Stare" in e.value.columns for e in app.dataframe)
    assert app.button(key="start").label == "Începe analiza"
    assert app.selectbox(key="model_Română").value == "L1"
    assert app.selectbox(key="audio_source_Română_test").value == "__off__"


def test_dashboard_empty_source(tmp_path,monkeypatch):
    monkeypatch.setenv("RTAP_DATA",str(tmp_path))
    app=AppTest.from_file(str(ROOT/"alert_trio_MD.py"),default_timeout=30).run()
    assert not app.exception
    assert any("первого финального окна" in str(e.value) for e in app.info)
    assert app.button(key="start").disabled
    assert app.button(key="shutdown").disabled

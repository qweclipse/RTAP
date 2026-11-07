import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import webrtcvad

from rtap.audio import Microphone, frame_features
from rtap.control import request_command, controller_state
from rtap.storage import read_json
from rtap.timeline import tag_sessions, local_table


def test_normalization_separates_voice_from_whole_signal():
    quiet = np.full(480, 1638, dtype=np.int16)
    voiced = np.full(480, 8192, dtype=np.int16)
    features = frame_features([(quiet, False), (voiced, True)])
    assert features["speech_flag"] == 1
    assert features["voice_intensity"] == .25
    assert features["noise_level"] == pytest.approx(np.sqrt((1638**2+8192**2)/2)/32768)
    assert features["noise_level"] != features["voice_intensity"]


def test_real_webrtc_vad_silence_and_stale_missing():
    audio = Microphone(enabled=False)
    assert all(value is None for value in audio.read_features().values())
    audio.rate = 16000
    audio.stream = SimpleNamespace(active=True)
    samples = np.zeros((480, 1), dtype=np.int16)
    assert webrtcvad.Vad(2).is_speech(samples.tobytes(), 16000) is False
    audio._callback(samples, 480, None, None)
    assert audio.read_features() == dict(noise_level=0., voice_intensity=0., speech_flag=0)
    audio.updated = time.monotonic()-2
    assert all(value is None for value in audio.read_features().values())
    audio.stream = None
    audio.close()


def test_disconnected_stream_property_cannot_stop_video_caller():
    import sounddevice as sd
    class Disconnected:
        @property
        def active(self):
            raise sd.PortAudioError("Device disconnected")
    audio = Microphone(enabled=False)
    audio.stream = Disconnected()
    assert all(value is None for value in audio.read_features().values())
    assert "disconnected" in audio.error


def test_command_inbox_is_unique_validated_and_does_not_start_devices(tmp_path, monkeypatch):
    monkeypatch.setenv("RTAP_DATA", str(tmp_path))
    first = request_command("start")
    second = request_command("pause")
    commands = [read_json(p) for p in (tmp_path/"commands").glob("*.json")]
    assert {c["id"] for c in commands} == {first, second}
    assert controller_state()["status"] == "STOPPED"
    assert not controller_state()["available"]
    with pytest.raises(ValueError):
        request_command("execute_shell")


def test_timeline_breaks_session_pause_and_large_gaps():
    data = pd.DataFrame(dict(window_start=pd.to_datetime([0, 10, 50, 60], unit="s", utc=True),
                             window_end=pd.to_datetime([10, 20, 60, 70], unit="s", utc=True),
                             source_id=["cam"]*4))
    sessions = [dict(session_id="one", start=0, end=20), dict(session_id="one", start=50, end=70)]
    tagged = tag_sessions(data, sessions)
    assert tagged.line_group.nunique() == 2
    assert tagged.session_id.nunique() == 1
    assert local_table(tagged).window_end.iloc[0].endswith("+0300")


def test_json_reader_retries_windows_replacement_lock(tmp_path, monkeypatch):
    from pathlib import Path
    path = tmp_path / "status.json"
    path.write_text('{"status":"RUNNING"}')
    original = Path.read_text
    attempts = []
    def temporary_lock(self, *args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise PermissionError("Windows atomic replace")
        return original(self, *args, **kwargs)
    monkeypatch.setattr(Path, "read_text", temporary_lock)
    assert read_json(path)["status"] == "RUNNING"
    assert len(attempts) == 2


def test_controller_absence_does_not_claim_active_producer_is_stopped(tmp_path, monkeypatch):
    import os
    import psutil
    from rtap.storage import write_json
    monkeypatch.setenv("RTAP_DATA", str(tmp_path))
    process = psutil.Process(os.getpid())
    write_json(tmp_path / "process-producer.json", dict(pid=process.pid, created=process.create_time(), exe=process.exe()))
    state = controller_state()
    assert state["status"] == "ERROR" and state["error"] == "controller_orphaned"
    assert not state["available"]


def test_new_controller_does_not_accept_previous_process_heartbeat(tmp_path, monkeypatch):
    import os
    import psutil
    from rtap.storage import write_json
    monkeypatch.setenv("RTAP_DATA", str(tmp_path))
    process = psutil.Process(os.getpid())
    write_json(tmp_path/"process-controller.json", dict(pid=process.pid, created=process.create_time(), exe=process.exe()))
    write_json(tmp_path/"controller.json", dict(status="STOPPED", updated=process.create_time()-1))
    assert not controller_state()["available"]
    write_json(tmp_path/"controller.json", dict(status="STOPPED", updated=time.time()))
    assert controller_state()["available"]

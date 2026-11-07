import time
from types import SimpleNamespace

import numpy as np
import pytest

from rtap.i18n import TEXT, translator
from rtap.performance import PROFILES, validate_rates, next_tick
from rtap.video import Detector, LatestCamera


def test_rate_limits_and_legacy_profiles():
    assert validate_rates(PROFILES["standard"])["kafka"] == 1
    assert validate_rates(PROFILES["performance"])["inference"] == 8
    for key, value in (("camera", float("nan")), ("preview", True), ("kafka", 6), ("inference", .5)):
        with pytest.raises(ValueError):
            validate_rates(dict(PROFILES["standard"], **{key:value}))
    with pytest.raises(ValueError):
        validate_rates({"camera":30})


def test_cadence_skips_missed_slots_without_accumulating_drift():
    assert next_tick(0, 10, 5) == 10.2
    assert next_tick(10, 10.06, 5) == 10.2
    assert next_tick(10, 11.05, 5) == 11.2


def test_capture_keeps_only_latest_frame_and_releases_on_close(monkeypatch):
    class Camera:
        released = False
        def set(self, *args): return True
        def get(self, *args): return 60
        def getBackendName(self): return "TEST"
        def read(self): return True, np.zeros((4,4,3), dtype=np.uint8)
        def release(self): self.released = True
    camera = Camera()
    monkeypatch.setattr("rtap.video.open_camera", lambda _:camera)
    capture = LatestCamera(0, 60)
    first = capture.latest
    time.sleep(.12)  # Deliberately lag inference: no FIFO to drain.
    assert capture.latest[0] > first[0]+2
    assert isinstance(capture.latest, tuple) and len(capture.latest) == 4
    assert capture.fps() > 30
    capture.close()
    assert capture.latest is None and camera.released and not capture.thread.is_alive()


def test_runtime_cuda_failure_loads_fresh_cpu_backend(monkeypatch):
    detector = Detector.__new__(Detector)
    detector.device, detector.half, detector.model = "0", True, object()
    detector.config = {}
    attempts = []
    def predict(frame):
        attempts.append(detector.device)
        if detector.device == "0":
            raise RuntimeError("simulated CUDA failure")
        return "valid CPU result"
    monkeypatch.setattr(detector, "_predict", predict)
    monkeypatch.setattr(detector, "_load", lambda:setattr(detector,"model","new CPU model"))
    monkeypatch.setattr("torch.cuda.empty_cache", lambda:None)
    assert detector.predict(None) == "valid CPU result"
    assert attempts == ["0", "cpu"]
    assert detector.model == "new CPU model" and not detector.half
    assert "simulated CUDA failure" in detector.fallback_reason


def test_public_statuses_are_short_and_no_academic_wording():
    assert [translator("Русский")(k) for k in ("normal","alert","indeterminate")] == ["Норма","Предупреждение","Недостаточно данных"]
    text = " ".join(value for translations in TEXT.values() for value in translations).lower()
    assert not any(word in text for word in ("преподавател", "профессор", "profesor", "laborator", "лабораторная", "educațional", "учебн"))


def test_preview_is_bounded_read_only_and_releases_port():
    import json
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    from rtap.preview import PreviewServer
    server = PreviewServer(port=0)
    try:
        server.publish(dict(status="running", frame_id=1, jpeg="test"))
        server.publish(dict(status="paused"))
        with urlopen(server.url+"frame") as response:
            assert json.load(response)["status"] == "paused"
            assert response.headers["Cache-Control"] == "no-store"
        with urlopen(server.url+"?language=ro") as response:
            assert "Persoane" in response.read().decode()
        with pytest.raises(HTTPError) as denied:
            urlopen(Request(server.url, headers={"Host":"untrusted.example"}))
        assert denied.value.code == 403
        with pytest.raises(HTTPError):
            urlopen(server.url+"../../config/lab.json")
    finally:
        server.close()
    assert not server.thread.is_alive()

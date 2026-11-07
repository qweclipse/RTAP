"""One physical capture owner, a bounded latest-frame slot, and CUDA fallback."""
import logging
import threading
import time
from collections import deque

import cv2
import numpy as np
import torch
from ultralytics import YOLO


def open_camera(index):
    for backend in (cv2.CAP_DSHOW, cv2.CAP_MSMF):
        camera = cv2.VideoCapture(index, backend)
        if camera.isOpened():
            camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
            camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
            camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)
            return camera
        camera.release()
    raise RuntimeError(f"Cannot open webcam {index}. Close other camera apps and check Windows privacy permissions.")


class LatestCamera:
    def __init__(self, index, fps, sequence=0):
        self.index, self.target_fps, self.sequence = index, fps, sequence
        self.latest = None
        self.error = None
        self.backend = None
        self.reported_fps = None
        self.captured = deque(maxlen=240)
        self.stop = threading.Event()
        self.ready = threading.Event()
        self.thread = threading.Thread(target=self._capture, name="camera-capture", daemon=True)
        self.thread.start()
        if not self.ready.wait(10) or self.error:
            self.close()
            raise RuntimeError(self.error or "Camera initialization timed out")

    def _capture(self):
        camera = None
        try:
            camera = open_camera(self.index)
            self.backend = camera.getBackendName()
            applied = None
            failures = 0
            while not self.stop.is_set():
                started = time.perf_counter()
                if self.target_fps != applied:
                    camera.set(cv2.CAP_PROP_FPS, self.target_fps)
                    self.reported_fps = camera.get(cv2.CAP_PROP_FPS)
                    applied = self.target_fps
                ok, frame = camera.read()
                if ok:
                    self.sequence += 1
                    captured = time.perf_counter()
                    self.latest = (self.sequence, time.time(), captured, frame)
                    self.captured.append(captured)
                    failures = 0
                    self.ready.set()
                else:
                    failures += 1
                    if failures >= 30:
                        raise RuntimeError("Camera stopped returning frames")
                    self.stop.wait(.05)
                self.stop.wait(max(0, 1/self.target_fps-(time.perf_counter()-started)))
        except Exception as exc:
            self.error = str(exc)
            self.ready.set()
        finally:
            if camera is not None:
                camera.release()

    def fps(self):
        times = list(self.captured)
        return (len(times)-1)/(times[-1]-times[0]) if len(times) > 1 else 0.

    def close(self):
        self.stop.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("Camera driver did not release capture thread")
        self.latest = None


class Detector:
    def __init__(self, config, device="auto"):
        self.config = config
        self.fallback_reason = None
        self.device = "0" if device != "cpu" and torch.cuda.is_available() else "cpu"
        self.half = self.device != "cpu" and config.get("yolo_fp16", False)
        try:
            self._load()
        except Exception as exc:
            if self.device == "cpu":
                raise
            self._fallback(exc)

    def _load(self):
        cv2.setNumThreads(1)
        self.model = YOLO(self.config["model_weights"])
        self._predict(np.zeros((480, 640, 3), dtype=np.uint8))
        # Ultralytics' CPU backend overwrites OMP_NUM_THREADS during its first call.
        torch.set_num_threads(self.config.get("torch_threads", 2))

    def _fallback(self, exc):
        self.fallback_reason = str(exc)
        logging.exception("CUDA inference failed; switching to CPU")
        self.model = None
        try:
            torch.cuda.empty_cache()
        except RuntimeError:
            pass
        self.device, self.half = "cpu", False
        self._load()

    def _predict(self, frame):
        result = self.model.predict(frame, conf=self.config["confidence"], imgsz=self.config["image_size"],
                                    device=self.device, half=self.half, verbose=False)[0]
        if not torch.isfinite(result.boxes.data).all():
            raise RuntimeError("Non-finite YOLO detection")
        return result.cpu()

    def predict(self, frame):
        try:
            return self._predict(frame)
        except Exception as exc:
            if self.device == "cpu":
                raise
            self._fallback(exc)
            return self._predict(frame)

    def diagnostics(self):
        return dict(device="cuda:0" if self.device != "cpu" else "cpu", fp16=self.half,
                    torch_threads=torch.get_num_threads(), opencv_threads=cv2.getNumThreads(),
                    fallback_reason=self.fallback_reason)

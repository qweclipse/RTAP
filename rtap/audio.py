"""Local WebRTC VAD: 30 ms PCM frames, rolling 1 s digital RMS (not dB SPL)."""
from collections import deque
import json
import logging
import threading
import time

import numpy as np
import sounddevice as sd
import webrtcvad

FIELDS = ("noise_level", "voice_intensity", "speech_flag")


def input_devices(refresh=False):
    if refresh:
        # PortAudio has no public hotplug refresh; call only after closing our sole stream.
        sd._terminate()
        sd._initialize()
    apis = sd.query_hostapis()
    result = []
    for index, device in enumerate(sd.query_devices()):
        if device["max_input_channels"] > 0:
            api = apis[device["hostapi"]]["name"]
            result.append({"key": json.dumps([api, device["name"]], ensure_ascii=False),
                           "label": f"{device['name']} · {api}", "index": index})
    return result


def frame_features(frames):
    """RMS / 32768; voice RMS uses VAD-positive frames only; valid silence is zero."""
    samples = np.concatenate([samples for samples, voiced in frames]).astype(np.float64)
    voice = [samples for samples, voiced in frames if voiced]
    return {"noise_level": min(1., float(np.sqrt(np.mean(samples ** 2)) / 32768)),
            "voice_intensity": min(1., float(np.sqrt(np.mean(np.concatenate(voice).astype(np.float64) ** 2)) / 32768)) if voice else 0.,
            "speech_flag": int(bool(voice))}


class Microphone:
    def __init__(self, enabled=True, selection="__default__", refresh=False):
        self.stream = None
        self.lock = threading.Lock()
        self.frames = deque(maxlen=34)
        self.updated = 0
        self.error = None
        self.selection = selection
        self.device_name = None
        self.vad = webrtcvad.Vad(2)
        self.callback_cpu_seconds = 0.
        self.callback_thread_id = None
        if not enabled or selection == "__off__":
            return
        try:
            devices = input_devices(refresh)
            device = None
            if selection != "__default__":
                matches = [d for d in devices if d["key"] == selection]
                if len(matches) != 1:
                    raise RuntimeError("Selected microphone is unavailable or ambiguous")
                device = matches[0]["index"]
            self.device_name = sd.query_devices(device, "input")["name"]
            for rate in (16000, 48000, 32000, 8000):
                try:
                    sd.check_input_settings(device=device, channels=1, dtype="int16", samplerate=rate)
                    break
                except sd.PortAudioError:
                    continue
            else:
                raise RuntimeError("Microphone does not support WebRTC PCM sample rates")
            self.rate = rate
            self.stream = sd.InputStream(device=device, channels=1, dtype="int16", samplerate=rate,
                                         blocksize=rate * 30 // 1000, callback=self._callback)
            self.stream.start()
        except Exception as exc:
            self.error = str(exc)
            logging.warning("Microphone unavailable: %s", exc)
            self.close()

    def _callback(self, samples, frames, timing, status):
        cpu_started = time.thread_time()
        self.callback_thread_id = threading.get_native_id()
        try:
            voiced = self.vad.is_speech(samples.tobytes(), self.rate) if not status else False
            with self.lock:
                if status:
                    self.frames.clear()
                    self.error = str(status)
                else:
                    self.frames.append((samples[:, 0].copy(), voiced))
                    self.updated = time.monotonic()
                    self.error = None
        except Exception as exc:
            with self.lock:
                self.frames.clear()
                self.error = str(exc)
        finally:
            self.callback_cpu_seconds += time.thread_time()-cpu_started

    def read_features(self):
        with self.lock:
            frames = list(self.frames)
            fresh = time.monotonic() - self.updated < 1.5
        try:
            active = self.stream is not None and self.stream.active
        except (sd.PortAudioError, RuntimeError) as exc:
            self.error = str(exc)
            active = False
        if not frames or not fresh or not active:
            if self.stream and not fresh and not self.error:
                self.error = "Microphone stopped delivering audio; select or reconnect a device"
            return dict.fromkeys(FIELDS)
        return frame_features(frames)

    def read(self):
        return self.read_features()["noise_level"]

    def close(self):
        if self.stream is not None:
            try:
                self.stream.stop()
                self.stream.close()
            except sd.PortAudioError:
                logging.exception("Audio stream close failed")
            self.stream = None
        with self.lock:
            self.frames.clear()


if __name__ == "__main__":
    print(json.dumps(input_devices(), ensure_ascii=False))

"""Etapa 1: continuous physical webcam -> YOLOv8n -> Kafka JSON events."""
from rtap.config import configure_storage
configure_storage()

import argparse
import base64
from collections import deque
import logging
import time
from pathlib import Path

import cv2
import torch
from kafka import KafkaProducer

from rtap.audio import Microphone
from rtap.config import data_dir, load_config
from rtap.events import encode_event, new_event
from rtap.performance import PROFILES, validate_rates, next_tick
from rtap.storage import read_json, write_json
from rtap.video import Detector, LatestCamera, open_camera
from rtap.preview import PreviewServer


def detection_features(result):
    classes = result.boxes.cls.cpu().numpy().astype(int)
    confidence = result.boxes.conf.cpu().numpy()
    return dict(n_person=int((classes == 0).sum()), object_count=len(classes),
                top_object=result.names[int(classes[confidence.argmax()])] if len(classes) else None,
                detection_confidence=float(confidence.mean()) if len(classes) else None)



def main():
    config = load_config()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=float, default=0, help="Analysis duration after initialization; 0 = continuous")
    parser.add_argument("--profile", choices=PROFILES, default=config.get("performance_profile", "standard"))
    parser.add_argument("--hz", type=float, help="Legacy experiments: set both inference and Kafka frequency")
    for rate in PROFILES["standard"]:
        parser.add_argument(f"--{rate}-fps" if rate != "kafka" else "--kafka-hz", type=float, dest="rate_"+rate)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--camera", type=int, default=config["camera_index"])
    parser.add_argument("--no-audio", action="store_true")
    parser.add_argument("--managed", action="store_true", help="Accept local controller pause/resume")
    args = parser.parse_args()
    rates = PROFILES[args.profile].copy()
    if args.hz is not None:
        rates.update(inference=args.hz, kafka=args.hz)
    rates.update({key: getattr(args, "rate_"+key) for key in rates if getattr(args, "rate_"+key) is not None})
    try:
        rates = validate_rates(rates)
    except ValueError as exc:
        parser.error(str(exc))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    root = data_dir()
    stop = root / "stop-producer"
    status_path = root / "producer.json"
    started = time.perf_counter()
    boot_started, duration_started = started, None
    camera = audio = producer = preview_server = None
    preview_error = None

    def write_preview(packet):
        if preview_server is not None:
            preview_server.publish(packet)
    counters = dict(camera=0, inference=0, preview=0, kafka=0, skipped_capture_frames=0)
    measured = {key: deque(maxlen=240) for key in ("inference", "preview", "kafka")}
    state = {"status": "starting", "updated": time.time(), "messages": 0}
    write_json(status_path, state)
    write_preview(dict(status="starting"))
    try:
        if not Path(config["model_weights"]).is_file():
            raise FileNotFoundError("YOLO weights missing: run SETUP_LAB.bat")
        try:
            preview_server = PreviewServer()
        except OSError as exc:
            preview_error = str(exc)
            logging.exception("Preview endpoint unavailable; Kafka analysis continues")
        detector = Detector(config, args.device)
        producer = KafkaProducer(bootstrap_servers=config["bootstrap_servers"], acks="all",
                                 enable_idempotence=True, retries=10, request_timeout_ms=10000,
                                 max_block_ms=10000, value_serializer=encode_event)
        deadlines = {key: 0. for key in ("inference", "preview", "kafka", "state", "control")}
        control, selection, desired_audio, applied_rates = {}, None, None, None
        last_seq = published_seq = previewed_seq = 0
        last_result = features = captured_frame = None
        timings = {}
        callback_seconds = 0.
        while not stop.exists() and (not args.seconds or duration_started is None or time.perf_counter()-duration_started < args.seconds):
            now = time.perf_counter()
            if duration_started is None and now-boot_started > 45:
                raise TimeoutError("Capture did not become ready within 45 seconds")
            if now >= deadlines["control"]:
                deadlines["control"] = now+.2
                control = read_json(root / "analysis.json", {}) if args.managed else {}
                desired_audio = read_json(root / "audio_selection.json", {"key": "__default__"})
                desired_rates = read_json(root / "performance.json", {}) if args.managed else {}
                if desired_rates != applied_rates:
                    if desired_rates:
                        rates = validate_rates(desired_rates["rates"])
                    applied_rates = desired_rates
                    deadlines.update(inference=0., preview=0., kafka=0.)
                    for times in measured.values():
                        times.clear()
                    if camera is not None:
                        camera.target_fps = rates["camera"]
            if control.get("paused"):
                if camera is not None:
                    camera.close()
                    counters["camera"] = camera.sequence
                    camera = None
                    callback_seconds += audio.callback_cpu_seconds
                    audio.close()
                    last_result = None
                    write_preview(dict(status="paused"))
                state.update(status="paused", updated=time.time(), camera_active=False)
                write_json(status_path, state)
                time.sleep(.1)
                continue
            if camera is not None and (camera.error or camera.latest and now-camera.latest[2] > 3):
                camera.close()
                counters["camera"] = camera.sequence
                camera = None
                callback_seconds += audio.callback_cpu_seconds
                audio.close()
            if camera is None:
                camera = LatestCamera(args.camera, rates["camera"], counters["camera"])
                selection = desired_audio
                audio = Microphone(config["audio_enabled"] and not args.no_audio, selection["key"], refresh=True)
                if duration_started is None:
                    duration_started = started = time.perf_counter()
                    state["startup_seconds"] = round(started-boot_started, 2)
                last_seq = counters["camera"]
                last_result = None
                for times in measured.values():
                    times.clear()
                deadlines.update(inference=0., preview=0., kafka=0.)
            if desired_audio != selection:
                callback_seconds += audio.callback_cpu_seconds
                audio.close()
                selection = desired_audio
                audio = Microphone(config["audio_enabled"] and not args.no_audio, selection["key"], refresh=True)
            latest = camera.latest
            now = time.perf_counter()
            if now >= deadlines["inference"] and latest and latest[0] != last_seq and now-latest[2] < 1:
                deadlines["inference"] = next_tick(deadlines["inference"], now, rates["inference"])
                counters["skipped_capture_frames"] += max(0, latest[0]-last_seq-1)
                last_seq = latest[0]
                last_result = detector.predict(latest[3])
                features = detection_features(last_result)
                captured_frame = latest
                finished = time.perf_counter()
                timings.update(inference_ms=round((finished-now)*1000, 2),
                               capture_to_inference_ms=round((finished-latest[2])*1000, 2))
                counters["inference"] += 1
                measured["inference"].append(finished)
            now = time.perf_counter()
            fresh = last_result is not None and now-captured_frame[2] < 1
            if fresh and now >= deadlines["kafka"] and published_seq != last_seq:
                deadlines["kafka"] = next_tick(deadlines["kafka"], now, rates["kafka"])
                event = new_event(config["source_id"], timestamp=captured_frame[1], **audio.read_features(),
                                  session_id=control.get("session_id"), frame_id=last_seq,
                                  inference_ms=timings["inference_ms"], **features)
                try:
                    metadata = producer.send(config["topic"], key=config["source_id"].encode(), value=event).get(timeout=15)
                except Exception:
                    logging.exception("Kafka publish failed; continuing webcam acquisition")
                    state.update(status="publish_error", updated=time.time())
                    write_json(status_path, state)
                    time.sleep(1)
                    continue
                published_seq = last_seq
                counters["kafka"] += 1
                measured["kafka"].append(time.perf_counter())
                timings["capture_to_kafka_ms"] = round((time.perf_counter()-captured_frame[2])*1000, 2)
                state.update(last_event=event, kafka_offset=metadata.offset)
                logging.info("Kafka ACK offset=%d count=%d persons=%d objects=%d noise=%s",
                             metadata.offset, counters["kafka"], event["n_person"], event["object_count"], event["noise_level"])
            if fresh and now >= deadlines["preview"] and previewed_seq != last_seq:
                deadlines["preview"] = next_tick(deadlines["preview"], now, rates["preview"])
                ok, jpeg = cv2.imencode(".jpg", last_result.plot())
                if ok:
                    write_preview(dict(jpeg=base64.b64encode(jpeg).decode("ascii"), status="running",
                               frame_id=last_seq, captured_at=captured_frame[1], preview_hz=rates["preview"],
                               n_person=features["n_person"], **detector.diagnostics()))
                    previewed_seq = last_seq
                    counters["preview"] += 1
                    measured["preview"].append(time.perf_counter())
            now = time.perf_counter()
            if now >= deadlines["state"]:
                deadlines["state"] = now+.5
                counters["camera"] = camera.sequence
                counters.update(audio_callback_cpu_seconds=callback_seconds+audio.callback_cpu_seconds,
                                audio_callback_thread_id=audio.callback_thread_id)
                fps = {key: (len(times)-1)/(times[-1]-times[0]) if len(times)>1 else 0. for key,times in measured.items()}
                fps["camera"] = camera.fps()
                state.update(status="running", updated=time.time(), messages=counters["kafka"],
                             elapsed_seconds=round(now-started, 2), sampling_hz=rates["kafka"],
                             rates=rates, actual_fps=fps, counters=counters, timings=timings,
                             diagnostics=detector.diagnostics(), camera_active=True, camera_index=args.camera,
                             preview_url=preview_server.url if preview_server else None, preview_error=preview_error,
                             camera_backend=camera.backend, camera_reported_fps=camera.reported_fps,
                             audio_error=audio.error, audio_device=audio.device_name, audio_selection=selection,
                             processing_fps=round(fps["inference"], 2),
                             frame_shape=list(latest[3].shape) if latest else None)
                if detector.device != "cpu":
                    state["cuda_memory"] = dict(allocated_mib=torch.cuda.memory_allocated()/1024**2,
                                                reserved_mib=torch.cuda.memory_reserved()/1024**2)
                else:
                    state.pop("cuda_memory", None)
                write_json(status_path, state)
            time.sleep(.01)
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        state.update(status="error", error=str(exc), updated=time.time())
        write_json(status_path, state)
        raise
    finally:
        write_preview(dict(status="stopped"))
        try:
            if camera is not None:
                camera.close()
        finally:
            if preview_server is not None:
                preview_server.close()
            if audio is not None:
                audio.close()
            if producer is not None:
                producer.flush(timeout=10)
                producer.close(timeout=10)
        if state.get("status") != "error":
            state.update(status="stopped", updated=time.time(), messages=counters["kafka"], camera_active=False)
            write_json(status_path, state)


if __name__ == "__main__":
    main()

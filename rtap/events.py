import json
import math
import time
import uuid

FIELDS = ("timestamp", "source_id", "n_person", "noise_level", "bias_flag",
          "voice_intensity", "speech_flag", "object_count", "top_object")
FEATURES = ["avg_person", "avg_noise", "avg_voice", "avg_speech", "avg_objects"]


def validate_event(event):
    if not isinstance(event, dict) or any(key not in event for key in FIELDS):
        raise ValueError("Missing event fields")
    if not isinstance(event["source_id"], str) or not event["source_id"].strip():
        raise ValueError("source_id must be a non-empty string")
    if not isinstance(event.get("event_id"), str) or not event["event_id"]:
        raise ValueError("event_id is required for deduplication")
    if event.get("source_kind") not in ("webcam", "test_fixture"):
        raise ValueError("Explicit source_kind required")
    value = event["timestamp"]
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError("Invalid epoch timestamp")
    for key in ("n_person", "object_count"):
        value = event[key]
        if type(value) is not int or value < 0:
            raise ValueError(f"{key} must be a non-negative integer")
    if event["n_person"] > event["object_count"]:
        raise ValueError("Person count cannot exceed total detections")
    for key in ("noise_level", "voice_intensity", "bias_flag", "speech_flag"):
        value = event[key]
        if value is None:
            continue
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1:
            raise ValueError(f"Invalid {key}")
        if key.endswith("flag") and value not in (0, 1):
            raise ValueError(f"{key} must be binary")
    if event["top_object"] is not None and not isinstance(event["top_object"], str):
        raise ValueError("top_object must be a string or null")
    return event


def encode_event(event):
    return json.dumps(validate_event(event), ensure_ascii=False, allow_nan=False).encode("utf-8")


def new_event(source_id, n_person, object_count, top_object=None, noise_level=None, **extra):
    event = dict(timestamp=time.time(), source_id=source_id, n_person=n_person,
                 object_count=object_count, top_object=top_object, noise_level=noise_level,
                 bias_flag=None, voice_intensity=None, speech_flag=None,
                 event_id=str(uuid.uuid4()), source_kind="webcam")
    event.update(extra)
    return validate_event(event)


def alert_state(row, persons=3, noise=0.3):
    """Only fully measured audio windows can satisfy the professor's rule."""
    count, level = row.get("avg_person"), row.get("avg_noise")
    if count is None or level is None or not math.isfinite(float(count)) or not math.isfinite(float(level)):
        return "UNAVAILABLE"
    if row.get("audio_samples", 0) < row.get("event_count", 1):
        return "UNAVAILABLE"
    return "ALERT" if count >= persons and level < noise else "NORMAL"

"""Local command inbox; the controller is the sole writer of analysis/audio state."""
import time
import uuid

from rtap.config import data_dir
from rtap.storage import read_json, write_json

COMMANDS = {"start", "pause", "resume", "shutdown", "audio", "performance"}


def request_command(action, **payload):
    if action not in COMMANDS:
        raise ValueError("Unknown lab command")
    command_id = uuid.uuid4().hex
    write_json(data_dir() / "commands" / f"{time.time_ns()}-{command_id}.json",
               dict(id=command_id, action=action, created=time.time(), **payload))
    return command_id


def controller_state():
    import psutil
    root = data_dir()
    saved = read_json(root / "process-controller.json", {})
    state = read_json(root / "controller.json", {})
    try:
        process = psutil.Process(saved["pid"])
        alive = abs(process.create_time()-saved["created"]) < .01 and process.exe().lower() == saved["exe"].lower()
    except (KeyError, psutil.Error):
        alive = False
    if not alive or state.get("updated", 0) < saved.get("created", 0) or time.time()-state.get("updated", 0) > 90:
        producer = read_json(root / "process-producer.json", {})
        try:
            process = psutil.Process(producer["pid"])
            if abs(process.create_time()-producer["created"]) < .01 and process.exe().lower() == producer["exe"].lower():
                return dict(state, status="ERROR", error="controller_orphaned", available=False)
        except (KeyError, psutil.Error):
            pass
        return dict(state, status="STOPPED", available=False)
    return dict(state, available=True)

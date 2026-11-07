"""Serialized local lifecycle commands. No network endpoint or shell input."""
import sys
from pathlib import Path
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.lab import DATA, ROOT, owned_process, launch, stop_component
from rtap.control import COMMANDS
from rtap.performance import validate_rates
from rtap.storage import read_json, write_json


def main():
    boot_time = time.time()
    state = dict(status="STOPPED", updated=time.time(), session_id=None, error=None)
    sessions = read_json(DATA / "sessions.json", [])
    for segment in sessions:
        if segment.get("end") is None:
            segment["end"] = read_json(DATA / "producer.json", {}).get("updated", time.time())
    write_json(DATA / "sessions.json", sessions)
    inbox = DATA / "commands"
    inbox.mkdir(exist_ok=True)

    def save(status=None, **values):
        state.update(updated=time.time(), **values)
        if status:
            state["status"] = status
        write_json(DATA / "controller.json", state)

    def segment_start():
        sessions.append(dict(session_id=state["session_id"], start=time.time(), end=None))
        write_json(DATA / "sessions.json", sessions)

    def segment_end():
        if sessions and sessions[-1]["end"] is None:
            sessions[-1]["end"] = time.time()
            write_json(DATA / "sessions.json", sessions)

    def wait_producer(status):
        deadline = time.monotonic()+45
        while time.monotonic() < deadline:
            observation = read_json(DATA / "producer.json", {})
            if owned_process("producer") and observation.get("status") == status and time.time()-observation.get("updated", 0) < 3:
                return
            if not owned_process("producer") or observation.get("status") == "error":
                raise RuntimeError(observation.get("error", "Producer stopped"))
            if (DATA / "stop-controller").exists():
                raise InterruptedError("Controller stopped")
            time.sleep(.1)
        raise TimeoutError("Producer did not confirm requested state")

    save()
    while not (DATA / "stop-controller").exists():
        if state["status"] == "ERROR" and state.get("error") == "producer_error" and owned_process("producer"):
            observation = read_json(DATA / "producer.json", {})
            if observation.get("status") == "running" and time.time()-observation.get("updated", 0) < 3:
                save("RUNNING", error=None, detail=None)
        if state["status"] in ("RUNNING", "PAUSED"):
            observation = read_json(DATA / "producer.json", {})
            if not owned_process("producer") or time.time()-observation.get("updated", 0) > 15:
                segment_end()
                save("ERROR", error="producer_unavailable")
            elif observation.get("status") in ("error", "publish_error", "camera_wait"):
                save("ERROR", error="producer_error", detail=observation.get("error", observation.get("status")))
        for path in sorted(inbox.glob("*.json")):
            command = read_json(path, {})
            path.unlink()
            action = command.get("action")
            if action not in COMMANDS or command.get("created", 0) < boot_time or not 0 <= time.time()-command.get("created", 0) < 120:
                continue
            try:
                if action == "performance":
                    rates = validate_rates(command.get("rates"))
                    write_json(DATA / "performance.json", dict(rates=rates, revision=command["id"]))
                elif action == "audio":
                    key = command.get("key")
                    if not isinstance(key, str) or len(key) > 1000:
                        raise ValueError("Invalid audio selection")
                    write_json(DATA / "audio_selection.json", dict(key=key, revision=command["id"]))
                elif action == "start" and state["status"] in ("STOPPED", "ERROR"):
                    if owned_process("producer"):
                        stop_component("producer")
                    save("STARTING", session_id=uuid.uuid4().hex, error=None, detail=None)
                    write_json(DATA / "analysis.json", dict(paused=False, session_id=state["session_id"]))
                    launch("producer", managed=True)
                    wait_producer("running")
                    segment_start()
                    save("RUNNING")
                elif action == "pause" and state["status"] == "RUNNING":
                    save("STOPPING", error=None)
                    write_json(DATA / "analysis.json", dict(paused=True, session_id=state["session_id"]))
                    wait_producer("paused")
                    segment_end()
                    save("PAUSED")
                elif action == "resume" and state["status"] == "PAUSED":
                    save("STARTING", error=None)
                    write_json(DATA / "analysis.json", dict(paused=False, session_id=state["session_id"]))
                    wait_producer("running")
                    segment_start()
                    save("RUNNING")
                elif action == "shutdown":
                    save("STOPPING", last_command=command["id"], error=None)
                    segment_end()
                    time.sleep(2)
                    for name in ("producer", "spark", "kafka", "dashboard"):
                        stop_component(name)
                    save("STOPPED")
                    return
                save(last_command=command["id"])
            except Exception as exc:
                segment_end()
                save("ERROR", error="command_failed", detail=str(exc), last_command=command.get("id"))
        save()
        time.sleep(.2)
    segment_end()
    save("STOPPED")


if __name__ == "__main__":
    main()

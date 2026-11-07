"""Windows lifecycle: own processes only, persistent Kafka identity, stop files."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
import base64
import urllib.request
import importlib.util
from contextlib import contextmanager
import msvcrt

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from rtap.config import configure_storage, data_dir, load_config, ROOT
configure_storage()
from rtap.storage import read_json, write_json
import psutil
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

TOOLING = Path(".runtime/tooling").resolve()
KAFKA = TOOLING / "kafka_2.13-3.9.1"
JAVA = Path(os.environ["JAVA_HOME"]) / "bin/java.exe"
CONFIG = ROOT / "config/kraft.properties"
DATA = data_dir()
LOGS = ROOT / "logs"
LOGS.mkdir(exist_ok=True)


def java_command(main, *args):
    return [str(JAVA), "-Xms128m", "-Xmx512m", "-Dkafka.logs.dir=.runtime/data/kafka-app-logs",
            f"-Dlog4j.configuration={ (KAFKA / 'config/log4j.properties').as_uri() }",
            "-cp", str(KAFKA / "libs/*"), main, *map(str,args)]


def initialize():
    storage = DATA / "kafka-live"
    meta = storage / "meta.properties"
    identity = DATA / "cluster.id.live.txt"
    if meta.exists():
        cluster = next(line.split("=",1)[1] for line in meta.read_text().splitlines() if line.startswith("cluster.id="))
        if identity.exists() and identity.read_text().strip() != cluster:
            raise RuntimeError("Cluster identity mismatch; refusing to format")
        identity.write_text(cluster,encoding="ascii")
        print("Existing cluster preserved:", cluster)
        return
    if storage.exists() and any(storage.iterdir()):
        raise RuntimeError("Kafka directory is not empty and lacks meta.properties; refusing to format")
    storage.mkdir(parents=True,exist_ok=True)
    if identity.exists():
        cluster = identity.read_text().strip()
    else:
        cluster = base64.urlsafe_b64encode(uuid.uuid4().bytes).decode("ascii").rstrip("=")
        identity.write_text(cluster,encoding="ascii")
    subprocess.run(java_command("kafka.tools.StorageTool","format","-t",cluster,"-c",CONFIG),check=True)
    print("Initialized cluster:",cluster)


def owned_process(name):
    saved = read_json(DATA / f"process-{name}.json")
    if not saved:
        return None
    try:
        process = psutil.Process(saved["pid"])
        if abs(process.create_time()-saved["created"]) < .01 and process.exe().lower() == saved["exe"].lower():
            return process
    except (psutil.NoSuchProcess,psutil.AccessDenied):
        pass
    return None


def admin():
    return KafkaAdminClient(bootstrap_servers=load_config()["bootstrap_servers"],
                            request_timeout_ms=5000, api_version_auto_timeout_ms=3000)


def topic():
    client = admin()
    try:
        name = load_config()["topic"]
        topics = client.list_topics()
        if name not in topics:
            client.create_topics([NewTopic(name,1,1,topic_configs={"retention.ms":"-1", "retention.bytes":"-1"})])
        for existing in topics:
            if existing == name or existing.startswith("rtap_test_"):
                subprocess.run(java_command("kafka.admin.ConfigCommand", "--bootstrap-server", load_config()["bootstrap_servers"],
                    "--entity-type", "topics", "--entity-name", existing, "--alter", "--add-config",
                    "retention.ms=-1,retention.bytes=-1"), check=True, stdout=subprocess.DEVNULL)
        print(client.describe_topics([name]))
    finally:
        client.close()


@contextmanager
def component_lock(name):
    with (DATA / f"lock-{name}").open("a+b") as handle:
        handle.seek(0)
        if not handle.read(1):
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        deadline = time.monotonic()+120
        while True:
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError(f"Another {name} command is still running")
                time.sleep(.2)
        try:
            yield
        finally:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)


def launch(name, managed=False):
    with component_lock(name):
        _launch(name, managed)


def _launch(name, managed=False):
    if owned_process(name):
        print(name,"already running")
        return
    stop = DATA / f"stop-{name}"
    if stop.exists():
        stop.unlink()
    if name == "kafka":
        if not (DATA / "kafka-live/meta.properties").exists():
            raise RuntimeError("Cluster not initialized. Run init_kafka_cluster.bat once.")
        classes = TOOLING / "kafka-launcher"
        classes.mkdir(exist_ok=True)
        subprocess.run([str(JAVA.with_name("javac.exe")), "-cp",str(KAFKA/"libs/*"),
                        "-d",str(classes),str(ROOT/"scripts/LabKafka.java")],check=True)
        command = java_command("LabKafka",stop,CONFIG)
        command[command.index("-cp")+1] += os.pathsep + str(classes)
    elif name == "spark":
        command = [sys.executable,"-u",str(ROOT/"spark_trio_MD.py")]
    elif name == "producer":
        write_json(DATA / "producer.json", dict(status="starting", updated=time.time()))
        command = [sys.executable,"-u",str(ROOT/"kafka_trio_MD.py")]
        if managed:
            command.append("--managed")
    elif name == "controller":
        command = [sys.executable,"-u",str(ROOT/"scripts/controller.py")]
    elif name == "dashboard":
        command = [sys.executable,"-m","streamlit","run",str(ROOT/"alert_trio_MD.py"),"--server.headless=true"]
    else:
        raise ValueError(name)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    with (LOGS/f"{name}-{stamp}.log").open("ab") as log:
        child = subprocess.Popen(command,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT,
                                 creationflags=subprocess.CREATE_NO_WINDOW)
    process = psutil.Process(child.pid)
    write_json(DATA/f"process-{name}.json", dict(pid=process.pid,created=process.create_time(),
               exe=process.exe(),command=command,log=str(LOGS/f"{name}-{stamp}.log")))
    print(name,"started",process.pid)


def stop_component(name):
    process = owned_process(name)
    if not process:
        print(name,"not running")
        return
    if name in ("producer","spark","kafka","controller"):
        (DATA/f"stop-{name}").touch()
        try:
            process.wait(timeout=45)
            print(name,"stopped gracefully")
            return
        except psutil.TimeoutExpired:
            print(name,"graceful stop timed out; terminating only owned process tree")
    children = process.children(recursive=True)
    for child in reversed(children):
        try:
            child.terminate()
        except psutil.NoSuchProcess:
            pass
    try:
        process.terminate()
    except psutil.NoSuchProcess:
        pass
    _, alive = psutil.wait_procs(children + [process], timeout=10)
    for child in alive:
        try:
            child.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(alive, timeout=5)


def health():
    states = {name:bool(owned_process(name)) for name in ("kafka","spark","producer","dashboard","controller")}
    try:
        client = admin()
        states["topic_exists"] = load_config()["topic"] in client.list_topics()
        client.close()
    except Exception as exc:
        states["kafka_error"] = str(exc)
    states["producer_observation"] = read_json(DATA/"producer.json",{})
    states["spark_observation"] = read_json(DATA/"aggregated_events/spark.json",{})
    write_json(ROOT/"reports/health.json",states)
    print(states)


def wait_component(name):
    for attempt in range(90):
        process = owned_process(name)
        if not process:
            raise RuntimeError(f"{name} exited. Inspect {LOGS}")
        if name == "dashboard":
            try:
                with urllib.request.urlopen("http://127.0.0.1:8501/_stcore/health", timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                pass
        else:
            path = DATA / ("aggregated_events/spark.json" if name == "spark" else f"{name}.json")
            state = read_json(path,{})
            if (state.get("status") == "running" or name == "controller" and state.get("status") in ("STOPPED", "RUNNING", "PAUSED")) and process.create_time() <= state.get("updated",0) and time.time()-state.get("updated",0)<10:
                return
            if state.get("status") == "error" and state.get("updated",0) >= process.create_time():
                raise RuntimeError(f"{name}: {state.get('error')}")
        time.sleep(1)
    raise TimeoutError(f"{name} did not become ready; inspect {LOGS}")


def start(open_browser=True):
    dashboard_was_running = bool(owned_process("dashboard"))
    for module in ("cv2", "ultralytics", "sounddevice", "webrtcvad", "pyspark", "sklearn", "shap", "streamlit"):
        if importlib.util.find_spec(module) is None:
            raise RuntimeError(f"Missing dependency: {module}. Run SETUP_LAB.bat.")
    if psutil.disk_usage(str(DATA.anchor)).free < 1024**3:
        raise RuntimeError("Less than 1 GiB free on data drive; archive lab data before starting")
    for path in (JAVA, KAFKA / "libs", Path(sys.executable), ROOT / "models/logistic_models.joblib",
                 Path(load_config()["model_weights"])):
        if not path.exists():
            raise FileNotFoundError(f"Missing environment component: {path}. Run SETUP_LAB.bat.")
    with component_lock("startup"):
        initialize()
        launch("kafka")
        for attempt in range(45):
            try:
                topic()
                break
            except Exception:
                if attempt == 44:
                    raise
                time.sleep(1)
        for name in ("spark", "controller", "dashboard"):
            if name == "controller" and not owned_process("controller") and owned_process("producer"):
                stop_component("producer")
            launch(name)
        for name in ("spark", "controller", "dashboard"):
            wait_component(name)
    print("Dashboard: http://127.0.0.1:8501 · Camera idle until Start · Logs:", LOGS)
    if open_browser and not dashboard_was_running:
        os.startfile("http://127.0.0.1:8501")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",choices=["init","topic","start","stop","health","kafka","spark","producer","dashboard","controller"])
    args = parser.parse_args()
    if args.action == "init":
        initialize()
    elif args.action == "topic":
        topic()
    elif args.action == "start":
        start()
    elif args.action == "stop":
        for name in ("controller","producer","spark","dashboard","kafka"):
            stop_component(name)
    elif args.action == "health":
        health()
    else:
        launch(args.action)


if __name__ == "__main__":
    main()

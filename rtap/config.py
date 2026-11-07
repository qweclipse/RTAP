import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def load_config():
    config = json.loads((ROOT / "config/lab.json").read_text(encoding="utf-8"))
    config["data_root"] = os.environ.get("RTAP_DATA", config["data_root"])
    return config


def configure_storage():
    """Redirect libraries before importing packages which create caches."""
    os.chdir(ROOT)
    cache = Path(os.environ.get("RTAP_CACHE", ".runtime/cache"))
    locations = {
        "TEMP": ".runtime/temp", "TMP": ".runtime/temp", "TMPDIR": ".runtime/temp",
        "USERPROFILE": str(cache / "profile"), "APPDATA": str(cache / "appdata"),
        "LOCALAPPDATA": str(cache / "localappdata"),
        "PIP_CACHE_DIR": str(cache / "pip"), "XDG_CACHE_HOME": str(cache / "xdg"),
        "XDG_CONFIG_HOME": str(cache / "config"), "TORCH_HOME": str(cache / "torch"),
        "HF_HOME": str(cache / "huggingface"), "HF_HUB_CACHE": str(cache / "huggingface/hub"),
        "YOLO_CONFIG_DIR": str(cache / "ultralytics"), "MPLCONFIGDIR": str(cache / "matplotlib"),
        "CUDA_CACHE_PATH": str(cache / "cuda"),
        "NUMBA_CACHE_DIR": str(cache / "numba"),
        "SPARK_LOCAL_DIRS": ".runtime/data/spark-temp",
    }
    for name, value in locations.items():
        os.environ[name] = str(Path(value).resolve())
        Path(value).mkdir(parents=True, exist_ok=True)
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("HADOOP_HOME", ".runtime/tooling/hadoop-3.4.1")


def data_dir():
    path = Path(load_config()["data_root"]).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path

import csv
import io
import json
import os
import time
from pathlib import Path


def atomic_text(path, text):
    atomic_bytes(path, text.encode("utf-8"))


def atomic_bytes(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_bytes(content)
    for attempt in range(10):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(0.05)


def write_json(path, value):
    atomic_text(path, json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False, default=str))


def read_json(path, default=None):
    for attempt in range(10):
        try:
            return json.loads(Path(path).read_text(encoding="utf-8"))
        except PermissionError:
            # Windows may briefly deny opening a file during atomic replacement.
            time.sleep(.01)
        except (FileNotFoundError, json.JSONDecodeError):
            return default
    return default


def write_csv(path, rows):
    if not rows:
        return
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    atomic_text(path, output.getvalue())


def read_aggregates(directory, limit=300):
    """Final windows only; deterministic batch names make retries idempotent."""
    import pandas as pd
    files = sorted(Path(directory).glob("batch-*.csv"))[-limit:]
    parts = [pd.read_csv(path) for path in files]
    if not parts:
        return pd.DataFrame()
    data = pd.concat(parts, ignore_index=True)
    return data.drop_duplicates(["window_start", "window_end", "source_id"], keep="last").sort_values(
        ["window_end", "source_id"]).tail(limit).reset_index(drop=True)

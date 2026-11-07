"""Session presentation only: no changes to Spark window mathematics/checkpoints."""
import pandas as pd


def tag_sessions(data, sessions):
    data = data.copy()
    starts = pd.to_datetime(data["window_start"], utc=True)
    ends = pd.to_datetime(data["window_end"], utc=True)
    data["session_id"] = "history"
    data["segment"] = "history"
    for i, segment in enumerate(sessions):
        mask = (starts >= pd.to_datetime(segment["start"], unit="s", utc=True))
        if segment.get("end") is not None:
            mask &= ends <= pd.to_datetime(segment["end"], unit="s", utc=True)
        data.loc[mask, "session_id"] = segment["session_id"]
        data.loc[mask, "segment"] = str(i)
    data = data.sort_values(["source_id", "window_end"])
    gap = pd.to_datetime(data["window_end"], utc=True).groupby(data["source_id"]).diff().dt.total_seconds().gt(15)
    data["line_group"] = data["source_id"] + ":" + data["segment"] + ":" + gap.cumsum().astype(str)
    return data.sort_values(["window_end", "source_id"])


def local_table(data):
    data = data.drop(columns=["line_group", "segment"], errors="ignore").copy()
    for column in ("window_start", "window_end"):
        if column in data:
            data[column] = pd.to_datetime(data[column], utc=True).dt.tz_convert("Europe/Chisinau").dt.strftime("%Y-%m-%d %H:%M:%S %z")
    return data

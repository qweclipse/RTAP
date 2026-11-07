"""Independent rates shared by CLI, controller and dashboard."""
import math

PROFILES = {
    "standard": dict(camera=30., inference=1., preview=1., kafka=1.),
    "performance": dict(camera=30., inference=8., preview=5., kafka=3.),
}


def next_tick(previous, now, rate):
    """Preserve the requested cadence, skipping missed slots without a backlog."""
    return previous+(math.floor((now-previous)*rate)+1)/rate if previous else now+1/rate


def validate_rates(values):
    if not isinstance(values, dict) or set(values) != set(PROFILES["standard"]):
        raise ValueError("Expected camera, inference, preview and kafka rates")
    limits = dict(camera=(1, 60), inference=(.1, 30), preview=(.1, 30), kafka=(.01, 5))
    result = {}
    for key, (low, high) in limits.items():
        value = values[key]
        if type(value) not in (int, float) or not math.isfinite(value) or not low <= value <= high:
            raise ValueError(f"Invalid {key} rate: {low}..{high}")
        result[key] = float(value)
    if result["kafka"] > result["inference"] or result["inference"] > result["camera"]:
        raise ValueError("Kafka rate must be <= inference rate <= camera rate")
    return result

"""Stream-processing building blocks: event-time tumbling windows and anomaly detection."""
import statistics
from collections import deque
from dataclasses import dataclass, asdict


@dataclass
class WindowResult:
    key: str
    start: float
    end: float
    count: int
    total: float
    minimum: float
    maximum: float

    @property
    def avg(self) -> float:
        return self.total / self.count if self.count else 0.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["avg"] = round(self.avg, 3)
        return d


class TumblingWindowAggregator:
    """Fixed, non-overlapping windows based on EVENT time.

    A window is emitted once the watermark (max event time seen - allowed lateness)
    passes its end. Events that arrive for an already-closed window are dropped and
    counted in `late_dropped` - the classic streaming trade-off between latency and
    completeness.
    """

    def __init__(self, size_s: float = 2.0, allowed_lateness_s: float = 1.0):
        self.size = size_s
        self.lateness = allowed_lateness_s
        self.max_ts = float("-inf")
        self.late_dropped = 0
        self._open: dict[tuple[str, float], WindowResult] = {}

    @property
    def watermark(self) -> float:
        return self.max_ts - self.lateness

    def add(self, key: str, value: float, ts: float) -> list[WindowResult]:
        start = ts - (ts % self.size)
        end = start + self.size
        if end <= self.watermark:
            self.late_dropped += 1
            return []
        w = self._open.get((key, start))
        if w is None:
            w = self._open[(key, start)] = WindowResult(key, start, end, 0, 0.0, value, value)
        w.count += 1
        w.total += value
        w.minimum = min(w.minimum, value)
        w.maximum = max(w.maximum, value)
        self.max_ts = max(self.max_ts, ts)
        return self._close_ready()

    def _close_ready(self) -> list[WindowResult]:
        wm = self.watermark
        ready = sorted((k for k, w in self._open.items() if w.end <= wm), key=lambda k: k[1])
        return [self._open.pop(k) for k in ready]

    def flush(self) -> list[WindowResult]:
        """Emit everything still open (used on shutdown)."""
        results = sorted(self._open.values(), key=lambda w: w.start)
        self._open.clear()
        return results


@dataclass
class Alert:
    ts: float
    key: str
    kind: str      # "threshold" or "zscore"
    value: float
    detail: str

    def to_dict(self) -> dict:
        return asdict(self)


class AnomalyDetector:
    """Per-key detector: hard limit + rolling z-score against recent normal behaviour."""

    def __init__(self, history: int = 30, min_samples: int = 10,
                 z_threshold: float = 4.0, static_max: float | None = None):
        self.history, self.min_samples = history, min_samples
        self.z_threshold, self.static_max = z_threshold, static_max
        self._values: dict[str, deque] = {}

    def check(self, key: str, value: float, ts: float) -> Alert | None:
        buf = self._values.setdefault(key, deque(maxlen=self.history))
        if self.static_max is not None and value > self.static_max:
            return Alert(ts, key, "threshold", value, f"{value:.1f} exceeds limit {self.static_max}")
        if len(buf) >= self.min_samples:
            mean = statistics.fmean(buf)
            std = statistics.pstdev(buf)
            if std > 0:
                z = (value - mean) / std
                if abs(z) >= self.z_threshold:
                    # anomalies are NOT added to the baseline, so they cannot hide later ones
                    return Alert(ts, key, "zscore", value, f"z={z:+.1f} vs recent mean {mean:.1f}")
        buf.append(value)
        return None

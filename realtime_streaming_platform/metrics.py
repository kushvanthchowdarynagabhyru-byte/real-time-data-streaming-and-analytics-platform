"""Live metrics shared between stream jobs and the dashboard (thread-safe)."""
import threading
import time
from collections import defaultdict, deque


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.started = time.time()
        self.counters = defaultdict(int)
        self._batches: dict[str, deque] = defaultdict(deque)   # topic -> (time, n)
        self.sensors: dict[str, dict] = {}
        self.windows: deque = deque(maxlen=30)
        self.alerts: deque = deque(maxlen=30)

    def incr(self, name: str, n: int = 1):
        with self.lock:
            self.counters[name] += n

    def record_batch(self, topic: str, n: int):
        with self.lock:
            self.counters[f"events:{topic}"] += n
            q = self._batches[topic]
            q.append((time.time(), n))
            while q and time.time() - q[0][0] > 30:
                q.popleft()

    def rate(self, topic: str, horizon: float = 5.0) -> float:
        now = time.time()
        span = max(1.0, min(horizon, now - self.started))   # avoid under-reporting right after start
        with self.lock:
            return round(sum(n for t, n in self._batches[topic] if now - t <= horizon) / span, 1)

    def update_sensor(self, key: str, value: float):
        with self.lock:
            s = self.sensors.setdefault(key, {"series": deque(maxlen=60), "latest": None})
            s["series"].append(round(value, 2))
            s["latest"] = round(value, 2)

    def add_window(self, stream: str, w):
        with self.lock:
            self.counters["windows"] += 1
            self.windows.appendleft({"stream": stream, **w.to_dict()})

    def add_alert(self, alert):
        with self.lock:
            self.counters["alerts"] += 1
            self.alerts.appendleft(alert.to_dict())

    def snapshot(self) -> dict:
        topics = [k.split(":", 1)[1] for k in list(self.counters) if k.startswith("events:")]
        rates = {t: self.rate(t) for t in topics}
        with self.lock:
            return {
                "uptime_s": round(time.time() - self.started, 1),
                "rates": rates,
                "totals": {
                    "events": {t: self.counters[f"events:{t}"] for t in topics},
                    "windows": self.counters["windows"],
                    "alerts": self.counters["alerts"],
                    "late_dropped": self.counters["late_dropped"],
                },
                "sensors": [{"id": k, "latest": v["latest"], "series": list(v["series"])}
                            for k, v in sorted(self.sensors.items())],
                "recent_alerts": list(self.alerts)[:10],
                "recent_windows": list(self.windows)[:10],
            }

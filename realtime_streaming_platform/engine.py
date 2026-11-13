"""Wires everything together: broker + producers + parallel stream jobs + sink + metrics."""
import threading
import time

from broker import Broker, Consumer
from metrics import Metrics
from processors import AnomalyDetector, TumblingWindowAggregator
from producers import OrderSimulator, ProducerThread, SensorSimulator
from store import Store

SENSOR_GROUP = "sensor-analytics"
ORDER_GROUP = "order-analytics"


class SensorHandler:
    """Per-record logic for sensor readings: live view, anomaly detection, windowed averages."""

    def __init__(self, store: Store, metrics: Metrics, window_s: float, lateness_s: float):
        self.store, self.metrics = store, metrics
        self.agg = TumblingWindowAggregator(window_s, lateness_s)
        self.detector = AnomalyDetector(history=30, min_samples=10, z_threshold=4.0, static_max=85.0)

    def __call__(self, rec):
        temp, key = float(rec.value["temperature"]), rec.key
        self.metrics.update_sensor(key, temp)
        alert = self.detector.check(key, temp, rec.timestamp)
        if alert:
            self.store.save_alert(alert)
            self.metrics.add_alert(alert)
        before = self.agg.late_dropped
        results = self.agg.add(key, temp, rec.timestamp)
        if self.agg.late_dropped > before:
            self.metrics.incr("late_dropped")
        self._emit(results)

    def _emit(self, results):
        for w in results:
            self.store.save_window("sensors", w)
            self.metrics.add_window("sensors", w)

    def flush(self):
        self._emit(self.agg.flush())


class OrderHandler:
    """Revenue per category per time window."""

    def __init__(self, store: Store, metrics: Metrics, window_s: float, lateness_s: float):
        self.store, self.metrics = store, metrics
        self.agg = TumblingWindowAggregator(window_s, lateness_s)

    def __call__(self, rec):
        before = self.agg.late_dropped
        results = self.agg.add(rec.key, float(rec.value["amount"]), rec.timestamp)
        if self.agg.late_dropped > before:
            self.metrics.incr("late_dropped")
        self._emit(results)

    def _emit(self, results):
        for w in results:
            self.store.save_window("orders", w)
            self.metrics.add_window("orders", w)

    def flush(self):
        self._emit(self.agg.flush())


class StreamJob(threading.Thread):
    """poll -> process -> commit loop for one consumer-group member."""

    def __init__(self, name: str, consumer: Consumer, handler, metrics: Metrics):
        super().__init__(daemon=True, name=name)
        self.consumer, self.handler, self.metrics = consumer, handler, metrics
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            records = self.consumer.poll(500)
            if records:
                for rec in records:
                    self.handler(rec)
                self.consumer.commit()               # commit AFTER processing
                self.metrics.record_batch(self.consumer.topic, len(records))
            else:
                time.sleep(0.05)
        # drain what is left, then emit still-open windows
        for rec in self.consumer.poll(1_000_000):
            self.handler(rec)
        self.consumer.commit()
        self.handler.flush()

    def stop(self):
        self.stop_event.set()


class StreamEngine:
    def __init__(self, db_path="stream_analytics.db", window_s=2.0, lateness_s=1.0,
                 sensor_rate=150, order_rate=40, sensor_workers=2, seed=None):
        self.broker = Broker()
        self.broker.create_topic(SensorSimulator.topic, partitions=4)
        self.broker.create_topic(OrderSimulator.topic, partitions=2)
        self.store = Store(db_path)
        self.metrics = Metrics()
        self.producers = [ProducerThread(self.broker, SensorSimulator(seed=seed), sensor_rate),
                          ProducerThread(self.broker, OrderSimulator(seed=seed), order_rate)]
        self.jobs = [
            StreamJob(f"sensor-job-{m}",
                      Consumer(self.broker, SensorSimulator.topic, SENSOR_GROUP, m, sensor_workers),
                      SensorHandler(self.store, self.metrics, window_s, lateness_s), self.metrics)
            for m in range(sensor_workers)
        ] + [StreamJob("order-job-0", Consumer(self.broker, OrderSimulator.topic, ORDER_GROUP),
                       OrderHandler(self.store, self.metrics, window_s, lateness_s), self.metrics)]

    def start(self):
        for j in self.jobs:
            j.start()
        for p in self.producers:
            p.start()

    def stop(self):
        for p in self.producers:
            p.stop()
        for p in self.producers:
            p.join(timeout=2)
        for j in self.jobs:
            j.stop()
        for j in self.jobs:
            j.join(timeout=5)

    def lag(self) -> dict:
        return {SensorSimulator.topic: self.broker.lag(SENSOR_GROUP, SensorSimulator.topic),
                OrderSimulator.topic: self.broker.lag(ORDER_GROUP, OrderSimulator.topic)}

    def snapshot(self) -> dict:
        snap = self.metrics.snapshot()
        snap["lag"] = self.lag()
        snap["revenue"] = self.store.revenue_by_category()
        snap["produced"] = {p.sim.topic: p.produced for p in self.producers}
        return snap

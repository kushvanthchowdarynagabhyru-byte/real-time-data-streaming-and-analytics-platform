"""Simulated event sources (stand-ins for IoT devices and an e-commerce site)."""
import random
import threading
import time

from broker import Broker


class SensorSimulator:
    """Factory-floor temperature sensors: mean-reverting noise, rare spikes, some late events."""
    topic = "sensor-readings"

    def __init__(self, n_sensors=6, seed=None, spike_prob=0.01, late_prob=0.03):
        self.rng = random.Random(seed)
        self.spike_prob, self.late_prob = spike_prob, late_prob
        self.base = {f"sensor-{i + 1}": self.rng.uniform(50, 70) for i in range(n_sensors)}
        self.temp = dict(self.base)

    def next_event(self, ts: float | None = None):
        ts = time.time() if ts is None else ts
        key = self.rng.choice(list(self.base))
        t = self.temp[key]
        t += 0.2 * (self.base[key] - t) + self.rng.gauss(0, 0.6)
        self.temp[key] = t
        reading = t
        if self.rng.random() < self.spike_prob:
            reading = t + self.rng.choice([-1, 1]) * self.rng.uniform(20, 35)
        if self.rng.random() < self.late_prob:   # simulate network delay -> out-of-order event
            ts -= self.rng.uniform(0.5, 4.0)
        value = {"sensor_id": key, "temperature": round(reading, 2),
                 "humidity": round(self.rng.uniform(30, 60), 1)}
        return key, value, ts


class OrderSimulator:
    """E-commerce orders; partitioned by product category."""
    topic = "orders"
    CATEGORIES = ["electronics", "fashion", "grocery", "home", "books"]

    def __init__(self, seed=None):
        self.rng = random.Random(seed)
        self.seq = 0

    def next_event(self, ts: float | None = None):
        ts = time.time() if ts is None else ts
        self.seq += 1
        cat = self.rng.choice(self.CATEGORIES)
        value = {"order_id": self.seq, "category": cat,
                 "amount": round(self.rng.lognormvariate(3.5, 0.7), 2),
                 "customer": f"cust-{self.rng.randint(1, 500)}"}
        return cat, value, ts


class ProducerThread(threading.Thread):
    """Publishes `rate_hz` events per second in 50 ms ticks."""

    def __init__(self, broker: Broker, simulator, rate_hz: float):
        super().__init__(daemon=True, name=f"producer-{simulator.topic}")
        self.broker, self.sim, self.rate = broker, simulator, rate_hz
        self.stop_event = threading.Event()
        self.produced = 0

    def run(self):
        tick, carry = 0.05, 0.0
        while not self.stop_event.is_set():
            carry += self.rate * tick
            n, carry = int(carry), carry - int(carry)
            for _ in range(n):
                key, value, ts = self.sim.next_event()
                self.broker.produce(self.sim.topic, key, value, ts)
                self.produced += 1
            time.sleep(tick)

    def stop(self):
        self.stop_event.set()

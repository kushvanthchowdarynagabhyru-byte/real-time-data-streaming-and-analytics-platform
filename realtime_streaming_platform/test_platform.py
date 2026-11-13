"""Run with: python -m unittest -v"""
import unittest

from broker import Broker, Consumer
from engine import SensorHandler
from metrics import Metrics
from processors import AnomalyDetector, TumblingWindowAggregator
from producers import OrderSimulator, SensorSimulator
from store import Store

T0 = 1000.0   # window-aligned base timestamp (divisible by 2)


class BrokerTests(unittest.TestCase):
    def test_same_key_same_partition_and_ordered_offsets(self):
        b = Broker()
        b.create_topic("t", 3)
        recs = [b.produce("t", "user-1", {"i": i}) for i in range(5)]
        self.assertEqual({r.partition for r in recs}, {recs[0].partition})
        self.assertEqual([r.offset for r in recs], [0, 1, 2, 3, 4])

    def test_consumer_resumes_from_committed_offset_without_duplicates(self):
        b = Broker()
        b.create_topic("t", 2)
        for i in range(10):
            b.produce("t", f"k{i}", {"i": i})
        c1 = Consumer(b, "t", "g")
        first = c1.poll(4)
        c1.commit()
        c2 = Consumer(b, "t", "g")            # a restarted consumer in the same group
        rest = c2.poll(100)
        seen = [r.value["i"] for r in first + rest]
        self.assertEqual(sorted(seen), list(range(10)))

    def test_groups_are_independent(self):
        b = Broker()
        b.create_topic("t", 2)
        for i in range(6):
            b.produce("t", f"k{i}", {"i": i})
        self.assertEqual(len(Consumer(b, "t", "a").poll(100)), 6)
        self.assertEqual(len(Consumer(b, "t", "b").poll(100)), 6)

    def test_members_split_partitions(self):
        b = Broker()
        b.create_topic("t", 4)
        m0, m1 = Consumer(b, "t", "g", 0, 2), Consumer(b, "t", "g", 1, 2)
        self.assertEqual(sorted(m0.partitions + m1.partitions), [0, 1, 2, 3])
        self.assertFalse(set(m0.partitions) & set(m1.partitions))

    def test_lag_and_retention(self):
        b = Broker(retention_per_partition=5)
        b.create_topic("t", 1)
        for i in range(12):
            b.produce("t", "k", {"i": i})
        c = Consumer(b, "t", "g")
        self.assertEqual(b.lag("g", "t"), 5)          # only retained records count
        self.assertEqual([r.value["i"] for r in c.poll(100)], [7, 8, 9, 10, 11])
        c.commit()
        self.assertEqual(b.lag("g", "t"), 0)


class WindowTests(unittest.TestCase):
    def test_window_closes_when_watermark_passes(self):
        agg = TumblingWindowAggregator(size_s=2, allowed_lateness_s=1)
        for ts, v in [(T0 + 0.1, 10), (T0 + 0.5, 20), (T0 + 1.9, 30)]:
            self.assertEqual(agg.add("k", v, ts), [])
        self.assertEqual(agg.add("k", 1, T0 + 2.5), [])      # watermark 1.5 -> not yet
        closed = agg.add("k", 1, T0 + 3.2)                    # watermark 2.2 -> closes [0,2)
        self.assertEqual(len(closed), 1)
        w = closed[0]
        self.assertEqual((w.count, w.total, w.minimum, w.maximum), (3, 60, 10, 30))
        self.assertAlmostEqual(w.avg, 20)

    def test_late_event_within_lateness_is_accepted_beyond_is_dropped(self):
        agg = TumblingWindowAggregator(size_s=2, allowed_lateness_s=1)
        agg.add("k", 1, T0 + 2.5)
        self.assertEqual(agg.add("k", 5, T0 + 1.9), [])       # late but window [0,2) still open
        self.assertEqual(agg.late_dropped, 0)
        agg.add("k", 1, T0 + 3.5)                             # watermark 2.5 closes [0,2)
        agg.add("k", 9, T0 + 1.0)                             # too late now
        self.assertEqual(agg.late_dropped, 1)

    def test_keys_are_windowed_independently_and_flush_emits_rest(self):
        agg = TumblingWindowAggregator(size_s=2, allowed_lateness_s=1)
        agg.add("a", 1, T0 + 0.1)
        agg.add("b", 2, T0 + 0.2)
        out = agg.flush()
        self.assertEqual(sorted(w.key for w in out), ["a", "b"])
        self.assertEqual(agg.flush(), [])


class AnomalyTests(unittest.TestCase):
    def test_zscore_alert_and_baseline_protected(self):
        det = AnomalyDetector(history=30, min_samples=10, z_threshold=4.0)
        for i in range(20):
            self.assertIsNone(det.check("s", 50 + (i % 3) * 0.1, i))
        alert = det.check("s", 80, 21)
        self.assertEqual(alert.kind, "zscore")
        self.assertIsNotNone(det.check("s", 80, 22))    # spike did not pollute the baseline

    def test_static_threshold(self):
        det = AnomalyDetector(static_max=85)
        self.assertEqual(det.check("s", 90, 1).kind, "threshold")
        self.assertIsNone(det.check("s", 60, 2))

    def test_no_alert_before_enough_samples(self):
        det = AnomalyDetector(min_samples=10)
        det.check("s", 50, 1)
        self.assertIsNone(det.check("s", 500, 2))


class PipelineTests(unittest.TestCase):
    def test_simulators_are_deterministic_with_seed(self):
        a, b = SensorSimulator(seed=1), SensorSimulator(seed=1)
        self.assertEqual([a.next_event(5.0) for _ in range(5)], [b.next_event(5.0) for _ in range(5)])
        o1, o2 = OrderSimulator(seed=3), OrderSimulator(seed=3)
        self.assertEqual(o1.next_event(1.0), o2.next_event(1.0))

    def test_end_to_end_without_threads(self):
        broker = Broker()
        broker.create_topic("sensor-readings", 2)
        store, metrics = Store(":memory:"), Metrics()
        handler = SensorHandler(store, metrics, window_s=2, lateness_s=1)
        consumer = Consumer(broker, "sensor-readings", "g")

        for i in range(30):                                   # calm readings, 2 windows
            broker.produce("sensor-readings", "sensor-1", {"temperature": 60.0 + (i % 2) * 0.1},
                           T0 + i * 0.1)
        broker.produce("sensor-readings", "sensor-1", {"temperature": 95.0}, T0 + 3.1)   # spike
        broker.produce("sensor-readings", "sensor-1", {"temperature": 60.0}, T0 + 6.0)   # advances watermark
        for rec in consumer.poll(1000):
            handler(rec)
        consumer.commit()
        handler.flush()

        stats = store.sensor_stats()[0]
        self.assertEqual(stats["sensor"], "sensor-1")
        self.assertEqual(stats["readings"], 32)
        self.assertEqual(store.counts()["alerts"], 1)
        self.assertEqual(metrics.snapshot()["totals"]["alerts"], 1)
        store.close()


if __name__ == "__main__":
    unittest.main()

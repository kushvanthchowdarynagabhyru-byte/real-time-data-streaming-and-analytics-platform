"""A miniature Kafka-style message broker (in-process, thread-safe).

Concepts implemented:
  * topics split into partitions (append-only logs with increasing offsets)
  * key-based partitioning (same key -> same partition -> ordered per key)
  * consumer groups with committed offsets (at-least-once delivery)
  * partition assignment across group members (parallel consumers)
  * bounded retention
"""
import threading
import time
import zlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    topic: str
    partition: int
    offset: int
    key: str
    value: dict
    timestamp: float  # event time (when the event happened, not when it was processed)


class _Partition:
    def __init__(self, retention: int):
        self.records: list[Record] = []
        self.base_offset = 0          # offset of records[0]
        self.retention = retention
        self.lock = threading.Lock()

    def append(self, topic, index, key, value, timestamp) -> Record:
        with self.lock:
            rec = Record(topic, index, self.base_offset + len(self.records), key, value, timestamp)
            self.records.append(rec)
            if len(self.records) > self.retention:
                drop = len(self.records) - self.retention
                del self.records[:drop]
                self.base_offset += drop
            return rec

    def read(self, offset: int, max_records: int) -> list[Record]:
        with self.lock:
            start = max(offset, self.base_offset) - self.base_offset
            return self.records[start:start + max_records]

    def end_offset(self) -> int:
        with self.lock:
            return self.base_offset + len(self.records)


class Broker:
    def __init__(self, retention_per_partition: int = 50_000):
        self.retention = retention_per_partition
        self._topics: dict[str, list[_Partition]] = {}
        self._committed: dict[tuple, int] = {}
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- admin
    def create_topic(self, name: str, partitions: int = 3) -> None:
        with self._lock:
            if name not in self._topics:
                self._topics[name] = [_Partition(self.retention) for _ in range(partitions)]

    def num_partitions(self, topic: str) -> int:
        return len(self._topics[topic])

    # ------------------------------------------------------------- produce
    @staticmethod
    def partition_for(key: str, n: int) -> int:
        return zlib.crc32(key.encode()) % n   # stable across runs (unlike hash())

    def produce(self, topic: str, key: str, value: dict, timestamp: float | None = None) -> Record:
        parts = self._topics[topic]
        idx = self.partition_for(key, len(parts))
        return parts[idx].append(topic, idx, key, value, timestamp if timestamp is not None else time.time())

    # ------------------------------------------------------------- consume
    def fetch(self, topic: str, partition: int, offset: int, max_records: int) -> list[Record]:
        return self._topics[topic][partition].read(offset, max_records)

    def commit(self, group: str, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            self._committed[(group, topic, partition)] = offset

    def committed(self, group: str, topic: str, partition: int) -> int:
        with self._lock:
            return self._committed.get((group, topic, partition), 0)

    def end_offset(self, topic: str, partition: int) -> int:
        return self._topics[topic][partition].end_offset()

    def lag(self, group: str, topic: str) -> int:
        """Records produced but not yet committed by the group."""
        total = 0
        for i, p in enumerate(self._topics[topic]):
            total += p.end_offset() - max(self.committed(group, topic, i), p.base_offset)
        return total


class Consumer:
    """Reads a topic as a member of a consumer group.

    With `members=N`, partitions are split round-robin so N consumers in the same
    group process different partitions in parallel.
    """

    def __init__(self, broker: Broker, topic: str, group: str, member: int = 0, members: int = 1):
        self.broker, self.topic, self.group = broker, topic, group
        n = broker.num_partitions(topic)
        self.partitions = [p for p in range(n) if p % members == member]
        self._position = {p: broker.committed(group, topic, p) for p in self.partitions}

    def poll(self, max_records: int = 500) -> list[Record]:
        out: list[Record] = []
        for p in self.partitions:
            recs = self.broker.fetch(self.topic, p, self._position[p], max_records - len(out))
            if recs:
                self._position[p] = recs[-1].offset + 1
                out.extend(recs)
            if len(out) >= max_records:
                break
        return out

    def commit(self) -> None:
        """Call after processing a batch -> at-least-once delivery."""
        for p, pos in self._position.items():
            self.broker.commit(self.group, self.topic, p, pos)

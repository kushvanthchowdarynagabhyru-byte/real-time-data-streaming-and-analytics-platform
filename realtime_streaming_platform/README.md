# Real-Time Data Streaming and Analytics Platform

A simple base platform that shows how a streaming system works end to end:
events are produced continuously, flow through a partitioned message broker,
are processed in parallel by stream jobs (windowed aggregation + anomaly
detection), stored, and shown on a live dashboard.
Pure Python 3.10+ standard library - nothing to install.

## Architecture
```
 Sensor simulator  -->  topic: sensor-readings (4 partitions) --> 2 parallel jobs --+
                                                                                    |--> SQLite sink
 Order simulator   -->  topic: orders          (2 partitions) --> 1 job ------------+--> live metrics
                                                                                    |--> web dashboard
   Broker = partitioned logs + consumer groups + committed offsets
   Job    = poll -> process -> commit   (windows, anomaly detection)
```

## Structure
| File | Purpose |
|---|---|
| `broker.py` | Mini Kafka: topics, partitions, key-based partitioning, consumer groups, offsets, lag, retention |
| `producers.py` | Simulated IoT temperature sensors and e-commerce orders (with spikes and late events) |
| `processors.py` | Event-time tumbling windows with watermark + allowed lateness; z-score & threshold anomaly detector |
| `engine.py` | Wires producers, parallel stream jobs, sink and metrics together |
| `store.py` | SQLite sink for window results and alerts |
| `metrics.py` | Live throughput / counters shared with the dashboard |
| `server.py` | Web dashboard (`/`) and JSON API (`/api/snapshot`) |
| `main.py` | CLI: `demo`, `serve` |
| `test_platform.py` | 13 unit tests |

## Run
```bash
python main.py demo --seconds 10    # headless run with a summary
python main.py serve                # live dashboard at http://127.0.0.1:8080
python -m unittest -v               # tests
```

## Streaming concepts demonstrated
- **Partitioning by key** - all readings of one sensor go to the same partition, so order per sensor is preserved and state can be kept locally
- **Consumer groups & offsets** - commit *after* processing gives at-least-once delivery; restarted consumers resume where they left off; several consumers split partitions for parallelism
- **Event time vs processing time** - windows use the timestamp the event happened, not when it arrived
- **Watermarks & late data** - a 2 s window closes when the watermark (latest event time minus 1 s) passes it; events arriving after that are dropped and counted (the *late events dropped* card)
- **Anomaly detection** - rolling z-score per sensor plus a hard temperature limit; spikes are kept out of the baseline so they cannot mask later spikes
- **Back-pressure visibility** - *consumer lag* shows whether processing keeps up with production

## Customise
- Change rates/windows: `StreamEngine(sensor_rate=300, window_s=5, lateness_s=2)` in `engine.py`
- New data source: write a simulator with `next_event()`, create a topic, add a handler + `StreamJob`
- Use real infrastructure: replace `broker.py` with Kafka (e.g. `kafka-python`/`confluent-kafka`) and the dashboard polling with WebSockets - the processors and handlers stay the same

## Limits of this base version
In-memory broker (data is lost on restart; only the SQLite sink persists), single process, no exactly-once guarantees, tumbling windows only (sliding/session windows are natural next steps).

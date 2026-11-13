"""CLI for the Real-Time Data Streaming and Analytics Platform.

  python main.py demo [--seconds 10]     run the pipeline headless and print a summary
  python main.py serve [--port 8080]     run the pipeline with a live web dashboard
"""
import argparse
import os
import time

from engine import StreamEngine
from server import serve

DB_PATH = os.getenv("STREAM_DB", "stream_analytics.db")


def table(rows, title):
    print(f"\n{title}")
    if not rows:
        print("  (none)")
        return
    cols = list(rows[0])
    widths = [max(len(c), *(len(str(r[c])) for r in rows)) for c in cols]
    print("  " + " | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  " + "-+-".join("-" * w for w in widths))
    for r in rows:
        print("  " + " | ".join(str(r[c]).ljust(w) for c, w in zip(cols, widths)))


def demo(seconds: int):
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    engine = StreamEngine(DB_PATH, seed=7)
    print(f"Streaming for {seconds}s  (2s tumbling windows, 1s allowed lateness) ...\n")
    engine.start()
    for _ in range(seconds):
        time.sleep(1)
        s = engine.snapshot()
        print(f"t={s['uptime_s']:>5.1f}s | sensors {s['rates'].get('sensor-readings', 0):>6.1f}/s | "
              f"orders {s['rates'].get('orders', 0):>5.1f}/s | windows {s['totals']['windows']:>3} | "
              f"alerts {s['totals']['alerts']:>2} | late-dropped {s['totals']['late_dropped']:>2} | "
              f"lag {sum(s['lag'].values())}")
    engine.stop()

    final = engine.snapshot()
    total = sum(final["totals"]["events"].values())
    print(f"\nProduced {sum(final['produced'].values()):,} events, processed {total:,}, "
          f"final lag {sum(engine.lag().values())}")
    table(engine.store.sensor_stats(), "SENSOR STATISTICS (from windowed aggregates)")
    table(engine.store.revenue_by_category(), "REVENUE BY CATEGORY")
    alerts = [{"sensor": a["key"], "type": a["kind"], "value": round(a["value"], 1), "detail": a["detail"]}
              for a in engine.store.recent_alerts(8)]
    table(alerts, "LATEST ANOMALY ALERTS")
    print(f"\nPersisted to {DB_PATH}: {engine.store.counts()}")
    engine.store.close()


def main():
    p = argparse.ArgumentParser(description="Real-Time Data Streaming and Analytics Platform")
    sub = p.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo")
    d.add_argument("--seconds", type=int, default=10)
    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=8080)
    args = p.parse_args()

    if args.cmd == "demo":
        demo(args.seconds)
    else:
        engine = StreamEngine(DB_PATH)
        engine.start()
        try:
            serve(engine, port=args.port)
        except KeyboardInterrupt:
            pass
        finally:
            engine.stop()
            print("Stopped.")


if __name__ == "__main__":
    main()

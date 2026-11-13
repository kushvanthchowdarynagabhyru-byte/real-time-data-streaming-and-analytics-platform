"""Persistent sink (SQLite) for window results and alerts."""
import sqlite3
import threading

SCHEMA = """
CREATE TABLE IF NOT EXISTS window_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT, stream TEXT, key TEXT,
    window_start REAL, window_end REAL, count INTEGER, total REAL, min REAL, max REAL
);
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, key TEXT, kind TEXT, value REAL, detail TEXT
);
CREATE INDEX IF NOT EXISTS idx_win ON window_results(stream, key, window_start);
"""


class Store:
    def __init__(self, path: str = "stream_analytics.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        with self.lock:
            self.conn.executescript(SCHEMA)

    def save_window(self, stream, w):
        with self.lock:
            self.conn.execute(
                "INSERT INTO window_results(stream,key,window_start,window_end,count,total,min,max) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (stream, w.key, w.start, w.end, w.count, w.total, w.minimum, w.maximum))
            self.conn.commit()

    def save_alert(self, a):
        with self.lock:
            self.conn.execute("INSERT INTO alerts(ts,key,kind,value,detail) VALUES (?,?,?,?,?)",
                              (a.ts, a.key, a.kind, a.value, a.detail))
            self.conn.commit()

    def _all(self, sql, params=()):
        with self.lock:
            return [dict(r) for r in self.conn.execute(sql, params).fetchall()]

    def revenue_by_category(self):
        return self._all("SELECT key AS category, ROUND(SUM(total),2) AS revenue, SUM(count) AS orders "
                         "FROM window_results WHERE stream='orders' GROUP BY key ORDER BY revenue DESC")

    def sensor_stats(self):
        return self._all("SELECT key AS sensor, SUM(count) AS readings, ROUND(SUM(total)/SUM(count),2) AS avg_temp, "
                         "ROUND(MIN(min),2) AS min_temp, ROUND(MAX(max),2) AS max_temp "
                         "FROM window_results WHERE stream='sensors' GROUP BY key ORDER BY key")

    def recent_alerts(self, limit=10):
        return self._all("SELECT ts,key,kind,value,detail FROM alerts ORDER BY id DESC LIMIT ?", (limit,))

    def counts(self):
        with self.lock:
            return {"windows": self.conn.execute("SELECT COUNT(*) FROM window_results").fetchone()[0],
                    "alerts": self.conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]}

    def close(self):
        with self.lock:
            self.conn.close()

import sqlite3

from prometheus_client.core import (
    CounterMetricFamily,
    GaugeMetricFamily,
    HistogramMetricFamily,
)

RUN_DURATION_BUCKETS = (1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0, 3600.0, float("inf"))


class SluiceCollector:
    def __init__(self, db_path: str):
        self._path = db_path

    def collect(self):
        conn = sqlite3.connect(self._path)
        try:
            results = list(self._run_total(conn))
            results.extend(self._run_duration(conn))
            results.extend(self._items_counters(conn))
            results.extend(self._llm_counters(conn))
            results.extend(self._failed_items_gauge(conn))
            results.extend(self._url_cache_rows(conn))
        finally:
            conn.close()
        yield from results

    def _run_total(self, conn):
        m = CounterMetricFamily(
            "sluice_run_total", "Total runs by status", labels=["pipeline_id", "status"]
        )
        for pid, status, n in conn.execute(
            "SELECT pipeline_id, status, COUNT(*) FROM run_log GROUP BY pipeline_id, status"
        ):
            m.add_metric([pid, status], n)
        yield m

    def _run_duration(self, conn):
        rows = conn.execute(
            "SELECT pipeline_id, "
            "(julianday(finished_at) - julianday(started_at)) * 86400 "
            "FROM run_log WHERE finished_at IS NOT NULL"
        ).fetchall()
        by_pid: dict[str, list[float]] = {}
        for pid, dur in rows:
            by_pid.setdefault(pid, []).append(dur)
        m = HistogramMetricFamily(
            "sluice_run_duration_seconds", "Pipeline run duration", labels=["pipeline_id"]
        )
        for pid, durs in by_pid.items():
            cum = [
                (
                    "+Inf" if b == float("inf") else str(b),
                    sum(1 for d in durs if d <= b),
                )
                for b in RUN_DURATION_BUCKETS
            ]
            m.add_metric([pid], buckets=cum, sum_value=sum(durs))
        yield m

    def _items_counters(self, conn):
        for col, name in (
            ("items_in", "sluice_items_in_total"),
            ("items_out", "sluice_items_out_total"),
        ):
            m = CounterMetricFamily(name, f"sum of {col}", labels=["pipeline_id"])
            for pid, total in conn.execute(
                f"SELECT pipeline_id, COALESCE(SUM({col}), 0) FROM run_log GROUP BY pipeline_id"
            ):
                m.add_metric([pid], total)
            yield m

    def _llm_counters(self, conn):
        m1 = CounterMetricFamily(
            "sluice_llm_calls_total", "sum of llm_calls", labels=["pipeline_id"]
        )
        m2 = CounterMetricFamily(
            "sluice_llm_cost_usd_total", "sum of est_cost_usd", labels=["pipeline_id"]
        )
        for pid, calls, cost in conn.execute(
            "SELECT pipeline_id, COALESCE(SUM(llm_calls), 0), "
            "       COALESCE(SUM(est_cost_usd), 0.0) FROM run_log "
            "GROUP BY pipeline_id"
        ):
            m1.add_metric([pid], calls)
            m2.add_metric([pid], cost)
        yield m1
        yield m2

    def _failed_items_gauge(self, conn):
        g = GaugeMetricFamily(
            "sluice_failed_items_open",
            "Open failed_items by status",
            labels=["pipeline_id", "status"],
        )
        for pid, status, n in conn.execute(
            "SELECT pipeline_id, status, COUNT(*) FROM failed_items "
            "WHERE status IN ('failed', 'dead_letter') "
            "GROUP BY pipeline_id, status"
        ):
            g.add_metric([pid, status], n)
        yield g

    def _url_cache_rows(self, conn):
        g = GaugeMetricFamily(
            "sluice_url_cache_rows",
            "Total rows in url_cache table",
            labels=[],
        )
        cur = conn.execute("SELECT COUNT(*) FROM url_cache")
        n = cur.fetchone()[0]
        g.add_metric([], n)
        yield g

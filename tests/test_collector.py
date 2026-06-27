"""Tests for Sentinel telemetry collector and anomaly detection."""

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pytest

from sentinel.collector import (
    ExportFormat,
    JSONExporter,
    MetricKind,
    MetricSeries,
    PipelineRun,
    RetentionPolicy,
    Sample,
    StageTiming,
    TelemetryCollector,
)
from sentinel.aggregator import (
    AggregationEngine,
    AnomalyDetector,
    AnomalySignal,
    RollingWindow,
    TrendLine,
)
from sentinel.reporter import ReportConfig, ReportGenerator


class TestMetricSeries:
    def test_record_and_range(self):
        s = MetricSeries(name="test_metric", kind=MetricKind.GAUGE, help_text="Test")
        s.record(1.0, host="a")
        s.record(2.0, host="b")
        s.record(3.0)
        assert len(s.samples) == 3
        all_vals = s.range()
        assert [x.value for x in all_vals] == [1.0, 2.0, 3.0]

    def test_range_filter(self):
        s = MetricSeries(name="test", kind=MetricKind.GAUGE, help_text="t")
        s.record(1.0)
        import time
        mid = time.time_ns()
        s.record(2.0)
        s.record(3.0)
        end = time.time_ns()
        s.record(4.0)
        filtered = s.range(start_ns=mid, end_ns=end)
        assert len(filtered) >= 2

    def test_stats(self):
        s = MetricSeries(name="test", kind=MetricKind.GAUGE, help_text="t")
        for v in [1, 2, 3, 4, 5]:
            s.record(float(v))
        stats = s.stats()
        assert stats["count"] == 5
        assert stats["mean"] == 3.0
        assert stats["min"] == 1.0
        assert stats["max"] == 5.0

    def test_quantile(self):
        s = MetricSeries(name="test", kind=MetricKind.GAUGE, help_text="t")
        for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
            s.record(float(v))
        assert s.quantile(0.50) == 5.0
        assert s.quantile(0.90) == 9.0


class TestStageTiming:
    def test_flatten(self):
        st = StageTiming(
            stage_name="build",
            started_at=datetime(2026, 6, 27, 10, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 6, 27, 10, 1, 30, tzinfo=timezone.utc),
            exit_code=0,
            retry_count=1,
        )
        rows = st.flatten()
        assert len(rows) == 1
        assert rows[0]["stage"] == "build"
        assert rows[0]["duration_s"] == 90.0
        assert rows[0]["exit_code"] == 0

    def test_substage_flatten(self):
        st = StageTiming(
            stage_name="ci",
            started_at=datetime(2026, 6, 27, 10, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 6, 27, 10, 5, 0, tzinfo=timezone.utc),
            exit_code=0,
        )
        sub = st.add_substage("test")
        sub.started_at = datetime(2026, 6, 27, 10, 1, 0, tzinfo=timezone.utc)
        sub.finished_at = datetime(2026, 6, 27, 10, 4, 0, tzinfo=timezone.utc)
        sub.exit_code = 0
        rows = st.flatten()
        assert len(rows) == 2

    def test_duration_none_when_no_finished(self):
        st = StageTiming(stage_name="pending")
        assert st.duration_seconds is None


class TestPipelineRun:
    def test_basic(self):
        run = PipelineRun(
            run_id="abc-123",
            pipeline_name="ci",
            repo_slug="owner/repo",
            branch="main",
            commit_sha="abc123def",
            trigger="push",
            started_at=datetime(2026, 6, 27, 10, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 6, 27, 10, 3, 20, tzinfo=timezone.utc),
            conclusion="success",
        )
        assert run.duration_seconds == 200.0

    def test_to_dict(self):
        run = PipelineRun(
            run_id="abc",
            pipeline_name="ci",
            repo_slug="o/r",
            branch="main",
            commit_sha="abc",
            trigger="push",
            started_at=datetime(2026, 6, 27, 10, 0, 0, tzinfo=timezone.utc),
            finished_at=datetime(2026, 6, 27, 10, 2, 0, tzinfo=timezone.utc),
            conclusion="success",
        )
        d = run.to_dict()
        assert d["run_id"] == "abc"
        assert d["duration_s"] == 120.0


class TestRetentionPolicy:
    def test_prune_keeps_recent(self):
        policy = RetentionPolicy(max_run_age_hours=1)
        now = datetime.now(timezone.utc)
        run = PipelineRun(
            run_id="r1", pipeline_name="p", repo_slug="a/b",
            branch="main", commit_sha="x", trigger="push",
            started_at=now,
        )
        assert len(policy.prune_runs([run])) == 1

    def test_prune_removes_old(self):
        policy = RetentionPolicy(max_run_age_hours=1)
        old = datetime(2020, 1, 1, tzinfo=timezone.utc)
        run = PipelineRun(
            run_id="r1", pipeline_name="p", repo_slug="a/b",
            branch="main", commit_sha="x", trigger="push",
            started_at=old,
        )
        assert len(policy.prune_runs([run])) == 0


class TestJSONExporter:
    def test_flush_writes_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            exporter = JSONExporter(Path(tmp), chunk_size=2)
            for i in range(3):
                run = PipelineRun(
                    run_id=f"r{i}", pipeline_name="ci", repo_slug="a/b",
                    branch="main", commit_sha="x", trigger="push",
                    started_at=datetime.now(timezone.utc),
                    finished_at=datetime.now(timezone.utc),
                )
                exporter.write_run(run)
            exporter.close()
            files = list(Path(tmp).glob("*.jsonl"))
            assert len(files) >= 1
            content = files[0].read_text()
            assert "r0" in content
            assert "r1" in content


class TestTelemetryCollector:
    def test_record_and_query(self):
        tc = TelemetryCollector()
        run = PipelineRun(
            run_id="r1", pipeline_name="ci", repo_slug="a/b",
            branch="main", commit_sha="x", trigger="push",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
            conclusion="success",
        )
        tc.record_run(run)
        assert len(tc.query_runs()) == 1
        assert tc.query_runs(conclusion="failure") == []

    def test_failure_rate(self):
        tc = TelemetryCollector()
        now = datetime.now(timezone.utc)
        for i in range(4):
            tc.record_run(PipelineRun(
                run_id=f"ok{i}", pipeline_name="ci", repo_slug="a/b",
                branch="main", commit_sha="x", trigger="push",
                started_at=now, finished_at=now, conclusion="success",
            ))
        tc.record_run(PipelineRun(
            run_id="fail1", pipeline_name="ci", repo_slug="a/b",
            branch="main", commit_sha="x", trigger="push",
            started_at=now, finished_at=now, conclusion="failure",
        ))
        rates = tc.failure_rate(window_hours=24)
        assert rates["ci"] == 0.2

    def test_register_series_idempotent(self):
        tc = TelemetryCollector()
        s1 = tc.register_series("test", MetricKind.GAUGE, "help")
        s2 = tc.register_series("test", MetricKind.GAUGE, "help")
        assert s1 is s2


class TestRollingWindow:
    def test_push_and_stats(self):
        w = RollingWindow(capacity=5)
        for v in [1, 2, 3, 4, 5]:
            w.push(float(v))
        assert w.full
        assert w.mean == 3.0
        assert len(w.values) == 5

    def test_capacity_enforcement(self):
        w = RollingWindow(capacity=3)
        for v in [1, 2, 3, 4, 5]:
            w.push(float(v))
        assert w.values == [3.0, 4.0, 5.0]


class TestAnomalyDetector:
    def test_zscore_normal(self):
        det = AnomalyDetector(zscore_threshold=2.0, window_capacity=10)
        baseline = [10.0 + i * 0.1 for i in range(20)]
        det.train("test", baseline)
        z, flagged = det.check_zscore("test", 10.5)
        assert not flagged
        z2, flagged2 = det.check_zscore("test", 50.0)
        assert flagged2

    def test_iqr_outlier(self):
        det = AnomalyDetector(iqr_multiplier=1.5, window_capacity=20)
        baseline = list(range(1, 21))
        det.train("test", [float(v) for v in baseline])
        _, flagged = det.check_iqr("test", 100.0)
        assert flagged

    def test_empty_runs_no_anomalies(self):
        det = AnomalyDetector()
        signals = det.analyze_runs([])
        assert signals == []


class TestAggregationEngine:
    def test_rollup(self):
        eng = AggregationEngine()
        now = datetime.now(timezone.utc)
        runs = []
        for i in range(10):
            runs.append(PipelineRun(
                run_id=f"r{i}", pipeline_name="ci", repo_slug="a/b",
                branch="main", commit_sha="x", trigger="push",
                started_at=now,
                finished_at=datetime.fromtimestamp(
                    now.timestamp() + 60 + i * 10, tz=timezone.utc
                ),
                conclusion="success" if i < 9 else "failure",
            ))
        rollup = eng.rollup(runs)
        assert rollup["count"] == 10
        assert rollup["failures"] == 1
        assert rollup["failure_rate"] == 0.1
        assert rollup["p50"] > 0

    def test_daily_report(self):
        eng = AggregationEngine()
        now = datetime.now(timezone.utc)
        runs = [PipelineRun(
            run_id="r1", pipeline_name="ci", repo_slug="a/b",
            branch="main", commit_sha="x", trigger="push",
            started_at=now, finished_at=now, conclusion="success",
        )]
        report = eng.daily_report(runs)
        assert "days" in report


class TestReportGenerator:
    def test_build_summary(self):
        gen = ReportGenerator()
        now = datetime.now(timezone.utc)
        runs = [
            PipelineRun(
                run_id=f"r{i}", pipeline_name="ci", repo_slug="a/b",
                branch="main", commit_sha="x", trigger="push",
                started_at=now,
                finished_at=datetime.fromtimestamp(
                    now.timestamp() + 120 + i * 30, tz=timezone.utc
                ),
                conclusion="success",
            )
            for i in range(5)
        ]
        summary = gen.build_summary(runs)
        assert summary["stats"]["total_runs"] == 5
        assert summary["stats"]["successful"] == 5

    def test_render_markdown(self):
        gen = ReportGenerator()
        summary = {
            "title": "Test Report",
            "generated_at": "2026-06-27T10:00:00Z",
            "stats": {
                "total_runs": 10, "successful": 8, "failed": 2, "cancelled": 0,
                "success_rate": 0.8,
                "duration": {
                    "min_seconds": 45, "p50_seconds": 120,
                    "p95_seconds": 300, "p99_seconds": 450, "max_seconds": 600,
                },
                "stage_breakdown": {},
            },
        }
        md = gen.render_markdown(summary)
        assert "# Test Report" in md
        assert "80.0%" in md

    def test_empty_runs(self):
        gen = ReportGenerator()
        summary = gen.build_summary([])
        assert summary["runs"] == 0

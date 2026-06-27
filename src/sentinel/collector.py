"""
Pipeline-aware telemetry collector with multi-backend export.

Collects build duration, stage timing, resource utilization, and failure
signals from CI pipeline executions. Supports windowed aggregation and
streaming export to JSON, Prometheus, and SQLite backends.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

logger = logging.getLogger("sentinel.collector")


class MetricKind(Enum):
    GAUGE = auto()
    COUNTER = auto()
    HISTOGRAM = auto()
    SUMMARY = auto()


class ExportFormat(Enum):
    JSON = "json"
    PROMETHEUS = "prometheus"
    SQLITE = "sqlite"


@dataclass(slots=True)
class Sample:
    """A single metric observation with nanosecond precision."""

    timestamp_ns: int
    value: float
    labels: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def now(cls, value: float, **labels: str) -> "Sample":
        return cls(timestamp_ns=time.time_ns(), value=value, labels=dict(labels))


@dataclass(slots=True)
class MetricSeries:
    """Named time-series with bounded retention window."""

    name: str
    kind: MetricKind
    help_text: str
    samples: List[Sample] = field(default_factory=list)
    _max_samples: int = 100_000
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, value: float, **labels: str) -> None:
        with self._lock:
            if len(self.samples) >= self._max_samples:
                self._compact()
            self.samples.append(Sample.now(value, **labels))

    def range(
        self,
        start_ns: Optional[int] = None,
        end_ns: Optional[int] = None,
    ) -> List[Sample]:
        """Return samples within [start_ns, end_ns]."""
        with self._lock:
            if start_ns is None and end_ns is None:
                return list(self.samples)
            result: List[Sample] = []
            for s in self.samples:
                if start_ns is not None and s.timestamp_ns < start_ns:
                    continue
                if end_ns is not None and s.timestamp_ns > end_ns:
                    break
                result.append(s)
            return result

    def quantile(self, q: float) -> Optional[float]:
        """Approximate quantile over retained samples."""
        values = [s.value for s in self.samples]
        if not values:
            return None
        values.sort()
        idx = int(len(values) * q)
        return values[min(idx, len(values) - 1)]

    def stats(self) -> Dict[str, float]:
        values = [s.value for s in self.samples]
        n = len(values)
        if n == 0:
            return {"count": 0}
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / n
        return {
            "count": n,
            "mean": mean,
            "stddev": variance ** 0.5,
            "min": min(values),
            "p50": self.quantile(0.50) or 0,
            "p95": self.quantile(0.95) or 0,
            "p99": self.quantile(0.99) or 0,
            "max": max(values),
        }

    def _compact(self) -> None:
        """Downsample by keeping every Nth sample + extrema."""
        keep = self.samples[::10]
        if len(self.samples) >= 2:
            extreme_samples = [min(self.samples, key=lambda s: s.value),
                               max(self.samples, key=lambda s: s.value)]
            keep.extend(extreme_samples)
        self.samples = keep


@dataclass
class StageTiming:
    """Execution timing for a single pipeline stage."""

    stage_name: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    retry_count: int = 0
    artifacts_bytes: int = 0
    _substages: Dict[str, "StageTiming"] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def succeeded(self) -> Optional[bool]:
        if self.exit_code is None:
            return None
        return self.exit_code == 0

    def add_substage(self, name: str) -> "StageTiming":
        sub = StageTiming(stage_name=name, started_at=datetime.now(timezone.utc))
        self._substages[name] = sub
        return sub

    def flatten(self, prefix: str = "") -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        full_name = f"{prefix}/{self.stage_name}" if prefix else self.stage_name
        if self.duration_seconds is not None:
            rows.append({
                "stage": full_name,
                "duration_s": self.duration_seconds,
                "exit_code": self.exit_code,
                "retries": self.retry_count,
                "artifacts_bytes": self.artifacts_bytes,
            })
        for sub in self._substages.values():
            rows.extend(sub.flatten(prefix=full_name))
        return rows


@dataclass
class PipelineRun:
    """Complete record of a single pipeline execution."""

    run_id: str
    pipeline_name: str
    repo_slug: str
    branch: str
    commit_sha: str
    trigger: str  # push, schedule, workflow_dispatch, etc.
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    conclusion: Optional[str] = None  # success, failure, cancelled, etc.
    stages: List[StageTiming] = field(default_factory=list)
    runner_os: str = ""
    runner_arch: str = ""

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def queue_delay_seconds(self) -> Optional[float]:
        if self.queued_at and self.started_at:
            return (self.started_at - self.queued_at).total_seconds()
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "pipeline_name": self.pipeline_name,
            "repo_slug": self.repo_slug,
            "branch": self.branch,
            "commit_sha": self.commit_sha,
            "trigger": self.trigger,
            "queued_at": self.queued_at.isoformat() if self.queued_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "conclusion": self.conclusion,
            "duration_s": self.duration_seconds,
            "queue_delay_s": self.queue_delay_seconds,
            "stages": [s.flatten() for s in self.stages],
            "runner_os": self.runner_os,
            "runner_arch": self.runner_arch,
        }


class RetentionPolicy:
    """Controls how long telemetry data is retained."""

    DEFAULT_WINDOW_STAGES = 168  # 7 days
    DEFAULT_WINDOW_RUNS = 720    # 30 days

    def __init__(
        self,
        max_stage_age_hours: int = DEFAULT_WINDOW_STAGES,
        max_run_age_hours: int = DEFAULT_WINDOW_RUNS,
    ):
        self.max_stage_age_hours = max_stage_age_hours
        self.max_run_age_hours = max_run_age_hours

    def prune_runs(self, runs: List[PipelineRun]) -> List[PipelineRun]:
        cutoff = datetime.now(timezone.utc).timestamp() - self.max_run_age_hours * 3600
        return [
            r for r in runs
            if r.started_at and r.started_at.timestamp() > cutoff
        ]


# ——— Exporter Backends ——————————————————————————————————————————————


class JSONExporter:
    """Streaming JSON-lines exporter with chunked batching."""

    def __init__(self, output_dir: Path, chunk_size: int = 500):
        self.output_dir = Path(output_dir)
        self.chunk_size = chunk_size
        self._buffer: List[Dict[str, Any]] = []
        self._seq = 0

    def write_run(self, run: PipelineRun) -> None:
        self._buffer.append(run.to_dict())
        if len(self._buffer) >= self.chunk_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return
        self.output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = self.output_dir / f"runs_{timestamp}_{self._seq:04d}.jsonl"
        with open(filename, "w", encoding="utf-8") as fh:
            for record in self._buffer:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        logger.debug("Flushed %d records to %s", len(self._buffer), filename)
        self._buffer.clear()
        self._seq += 1

    def close(self) -> None:
        self._flush()


class PrometheusExporter:
    """Exports MetricSeries to Prometheus text format."""

    @staticmethod
    def render(series_list: List[MetricSeries]) -> str:
        lines: List[str] = []
        for s in series_list:
            lines.append(f"# HELP {s.name} {s.help_text}")
            type_line = {
                MetricKind.GAUGE: "gauge",
                MetricKind.COUNTER: "counter",
                MetricKind.HISTOGRAM: "histogram",
                MetricKind.SUMMARY: "summary",
            }[s.kind]
            lines.append(f"# TYPE {s.name} {type_line}")
            for sample in s.samples:
                label_str = ",".join(
                    f'{k}="{v}"' for k, v in sample.labels.items()
                )
                if label_str:
                    lines.append(
                        f'{s.name}{{{label_str}}} {sample.value} {sample.timestamp_ns // 1_000_000}'
                    )
                else:
                    lines.append(
                        f'{s.name} {sample.value} {sample.timestamp_ns // 1_000_000}'
                    )
        return "\n".join(lines) + "\n"


class SQLiteExporter:
    """Persistent telemetry storage for offline analysis."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_id TEXT PRIMARY KEY,
        pipeline_name TEXT NOT NULL,
        repo_slug TEXT NOT NULL,
        branch TEXT,
        commit_sha TEXT,
        trigger TEXT,
        queued_at TEXT,
        started_at TEXT,
        finished_at TEXT,
        conclusion TEXT,
        duration_s REAL,
        queue_delay_s REAL,
        runner_os TEXT,
        runner_arch TEXT
    );
    CREATE TABLE IF NOT EXISTS stage_timings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL REFERENCES pipeline_runs(run_id),
        stage_name TEXT NOT NULL,
        duration_s REAL,
        exit_code INTEGER,
        retry_count INTEGER DEFAULT 0,
        artifacts_bytes INTEGER DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_stage_run ON stage_timings(run_id);
    CREATE INDEX IF NOT EXISTS idx_runs_repo ON pipeline_runs(repo_slug, started_at);
    """

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self) -> None:
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.executescript(self.SCHEMA)
        self._conn.commit()

    def insert_run(self, run: PipelineRun) -> None:
        if self._conn is None:
            raise RuntimeError("SQLiteExporter not connected")
        self._conn.execute(
            """INSERT OR REPLACE INTO pipeline_runs
               (run_id, pipeline_name, repo_slug, branch, commit_sha,
                trigger, queued_at, started_at, finished_at, conclusion,
                duration_s, queue_delay_s, runner_os, runner_arch)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run.run_id, run.pipeline_name, run.repo_slug, run.branch,
                run.commit_sha, run.trigger,
                run.queued_at.isoformat() if run.queued_at else None,
                run.started_at.isoformat() if run.started_at else None,
                run.finished_at.isoformat() if run.finished_at else None,
                run.conclusion, run.duration_seconds, run.queue_delay_seconds,
                run.runner_os, run.runner_arch,
            ),
        )
        for stage in run.stages:
            for row in stage.flatten():
                self._conn.execute(
                    """INSERT INTO stage_timings
                       (run_id, stage_name, duration_s, exit_code, retry_count, artifacts_bytes)
                       VALUES (?,?,?,?,?,?)""",
                    (
                        run.run_id, row["stage"], row["duration_s"],
                        row["exit_code"], row["retries"], row["artifacts_bytes"],
                    ),
                )
        self._conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


# ——— Collector ———————————————————————————————————————————————————————


class TelemetryCollector:
    """Central telemetry collection and export facade.

    Gathers pipeline run records and metric series, buffers them in-memory,
    and exports to configured backends on flush.
    """

    def __init__(
        self,
        retention: Optional[RetentionPolicy] = None,
        export_formats: Optional[Sequence[ExportFormat]] = None,
        output_dir: Optional[Path] = None,
    ):
        self.retention = retention or RetentionPolicy()
        self.export_formats = list(export_formats) if export_formats else [ExportFormat.JSON]
        self.output_dir = Path(output_dir) if output_dir else Path("telemetry_out")
        self.runs: List[PipelineRun] = []
        self.series: Dict[str, MetricSeries] = {}
        self._exporters: Dict[ExportFormat, Any] = {}
        self._lock = threading.Lock()

    def register_series(self, name: str, kind: MetricKind, help_text: str) -> MetricSeries:
        with self._lock:
            if name in self.series:
                return self.series[name]
            series = MetricSeries(name=name, kind=kind, help_text=help_text)
            self.series[name] = series
            return series

    def record_run(self, run: PipelineRun) -> None:
        with self._lock:
            self.runs.append(run)
        for stage in run.stages:
            dur = stage.duration_seconds
            if dur is not None:
                s = self.register_series(
                    "pipeline_stage_duration_seconds",
                    MetricKind.HISTOGRAM,
                    "Duration of individual pipeline stages",
                )
                s.record(dur, stage=stage.stage_name, pipeline=run.pipeline_name)

    def query_runs(
        self,
        repo_slug: Optional[str] = None,
        since_hours: Optional[int] = None,
        conclusion: Optional[str] = None,
    ) -> List[PipelineRun]:
        result = list(self.runs)
        if repo_slug:
            result = [r for r in result if r.repo_slug == repo_slug]
        if conclusion:
            result = [r for r in result if r.conclusion == conclusion]
        if since_hours is not None:
            cutoff = datetime.now(timezone.utc).timestamp() - since_hours * 3600
            result = [
                r for r in result
                if r.started_at and r.started_at.timestamp() > cutoff
            ]
        return result

    def failure_rate(self, window_hours: int = 24) -> Dict[str, float]:
        runs = self.query_runs(since_hours=window_hours)
        by_pipeline: Dict[str, List[PipelineRun]] = defaultdict(list)
        for r in runs:
            by_pipeline[r.pipeline_name].append(r)
        rates: Dict[str, float] = {}
        for name, pipe_runs in by_pipeline.items():
            total = len(pipe_runs)
            failed = sum(1 for r in pipe_runs if r.conclusion == "failure")
            rates[name] = failed / total if total > 0 else 0.0
        return rates

    def flush(self) -> Dict[str, Any]:
        """Export all buffered data to configured backends. Returns summary."""
        self.runs = self.retention.prune_runs(self.runs)
        summary: Dict[str, Any] = {
            "runs_exported": len(self.runs),
            "series_exported": len(self.series),
            "backends": {},
        }
        if ExportFormat.JSON in self.export_formats:
            exporter = JSONExporter(self.output_dir / "json")
            for run in self.runs:
                exporter.write_run(run)
            exporter.close()
            summary["backends"]["json"] = {"dir": str(self.output_dir / "json")}
        if ExportFormat.PROMETHEUS in self.export_formats:
            text = PrometheusExporter.render(list(self.series.values()))
            out_path = self.output_dir / "metrics.prom"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(text, encoding="utf-8")
            summary["backends"]["prometheus"] = {"file": str(out_path)}
        if ExportFormat.SQLITE in self.export_formats:
            db = SQLiteExporter(self.output_dir / "telemetry.db")
            db.connect()
            for run in self.runs:
                db.insert_run(run)
            db.close()
            summary["backends"]["sqlite"] = {"file": str(self.output_dir / "telemetry.db")}
        return summary

    def reset(self) -> None:
        """Clear all in-memory data. Irreversible."""
        with self._lock:
            self.runs.clear()
            self.series.clear()

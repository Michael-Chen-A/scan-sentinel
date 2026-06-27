"""
Pipeline watcher: scheduled observation of CI pipeline health.

Polls GitHub Actions runs, collects stage timings, feeds them into
the TelemetryCollector and AnomalyDetector, and triggers reports.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .aggregator import AggregationEngine, AnomalyDetector, AnomalySignal
from .collector import (
    ExportFormat,
    MetricKind,
    PipelineRun,
    RetentionPolicy,
    StageTiming,
    TelemetryCollector,
)

logger = logging.getLogger("sentinel.watcher")


@dataclass
class WatchTarget:
    """A repository+pipeline to monitor."""

    repo_slug: str
    workflow_id: str
    branch: str = "main"
    label: str = ""
    enabled: bool = True

    def __post_init__(self):
        if not self.label:
            self.label = f"{self.repo_slug}/{self.workflow_id}"


@dataclass
class WatchConfig:
    targets: List[WatchTarget] = field(default_factory=list)
    poll_interval_seconds: int = 300
    lookback_hours: int = 24
    anomaly_window_hours: int = 72
    alert_on_severity: str = "high"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WatchConfig":
        targets = [
            WatchTarget(
                repo_slug=t["repo"],
                workflow_id=t["workflow"],
                branch=t.get("branch", "main"),
                label=t.get("label", ""),
                enabled=t.get("enabled", True),
            )
            for t in data.get("targets", [])
        ]
        return cls(
            targets=targets,
            poll_interval_seconds=data.get("poll_interval", 300),
            lookback_hours=data.get("lookback_hours", 24),
            anomaly_window_hours=data.get("anomaly_window_hours", 72),
            alert_on_severity=data.get("alert_on_severity", "high"),
        )


class PipelineWatcher:
    """Orchestrates periodic CI telemetry collection and anomaly detection."""

    SEVERITY_RANK = {"low": 0, "medium": 1, "high": 2}

    def __init__(
        self,
        config: WatchConfig,
        collector: Optional[TelemetryCollector] = None,
        detector: Optional[AnomalyDetector] = None,
    ):
        self.config = config
        self.collector = collector or TelemetryCollector(
            retention=RetentionPolicy(),
            export_formats=[ExportFormat.JSON],
        )
        self.detector = detector or AnomalyDetector()
        self.aggregator = AggregationEngine()
        self._last_poll: Optional[datetime] = None
        self._run_count: int = 0

    def observe(self, runs: List[PipelineRun]) -> Dict[str, Any]:
        """Ingest a batch of pipeline runs and run analysis."""
        for run in runs:
            self.collector.record_run(run)

        anomalies = self.detector.analyze_runs(runs)

        rollup = self.aggregator.rollup(runs)
        self._run_count += len(runs)

        self._record_derived_metrics(runs, rollup)

        high_alerts = [a for a in anomalies
                       if self.SEVERITY_RANK.get(a.severity, 0)
                       >= self.SEVERITY_RANK.get(self.config.alert_on_severity, 2)]

        return {
            "runs_observed": len(runs),
            "rollup": rollup,
            "anomalies": len(anomalies),
            "alerts": [
                {
                    "metric": a.metric_name,
                    "severity": a.severity,
                    "description": a.description,
                    "zscore": a.zscore,
                }
                for a in high_alerts
            ],
        }

    def _record_derived_metrics(
        self, runs: List[PipelineRun], rollup: Dict[str, float]
    ) -> None:
        s = self.collector.register_series(
            "pipeline_success_rate",
            MetricKind.GAUGE,
            "Rolling success rate for observed pipelines",
        )
        if rollup.get("count", 0) > 0:
            success_rate = 1.0 - rollup.get("failure_rate", 0)
            s.record(success_rate)

        s = self.collector.register_series(
            "pipeline_duration_p95",
            MetricKind.GAUGE,
            "95th percentile pipeline duration",
        )
        s.record(rollup.get("p95", 0))

    def periodic_report(self) -> Dict[str, Any]:
        """Generate a comprehensive health report from accumulated data."""
        runs = self.collector.query_runs(since_hours=self.config.lookback_hours)
        anomalies = self.detector.analyze_runs(runs)
        daily = self.aggregator.daily_report(runs, label="overall")
        failure_rates = self.collector.failure_rate(window_hours=self.config.lookback_hours)
        flush_result = self.collector.flush()

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "period_hours": self.config.lookback_hours,
            "total_runs": len(runs),
            "failure_rates": failure_rates,
            "anomalies_detected": len(anomalies),
            "daily_breakdown": daily,
            "flush": flush_result,
        }

    def status_summary(self) -> Dict[str, Any]:
        runs = self.collector.query_runs(since_hours=1)
        failures = [r for r in runs if r.conclusion == "failure"]
        return {
            "watched_targets": len(self.config.targets),
            "runs_last_hour": len(runs),
            "failures_last_hour": len(failures),
            "total_observed": self._run_count,
            "last_poll": self._last_poll.isoformat() if self._last_poll else None,
        }

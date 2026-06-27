"""
Time-series anomaly detection and trend analysis for CI pipeline metrics.

Implements Z-score spike detection, rolling-window drift tracking,
and exponential-smoothing forecasting for stage durations and failure rates.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from .collector import MetricSeries, PipelineRun, Sample, StageTiming


@dataclass
class AnomalySignal:
    """A detected deviation from expected metric behavior."""

    metric_name: str
    observed: float
    expected: float
    zscore: float
    severity: str  # low, medium, high
    description: str
    detected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class TrendLine:
    """Linear trend fitted to a time-series window."""

    slope: float
    intercept: float
    r_squared: float
    data_points: int

    def predict(self, x: float) -> float:
        return self.slope * x + self.intercept

    @property
    def direction(self) -> str:
        if abs(self.slope) < 1e-9:
            return "flat"
        return "increasing" if self.slope > 0 else "decreasing"


@dataclass
class RollingWindow:
    """Fixed-capacity ring buffer for streaming time-series analysis."""

    values: List[float] = field(default_factory=list)
    capacity: int = 240

    def push(self, value: float) -> None:
        self.values.append(value)
        if len(self.values) > self.capacity:
            self.values = self.values[-self.capacity:]

    @property
    def mean(self) -> float:
        if not self.values:
            return 0.0
        return sum(self.values) / len(self.values)

    @property
    def stddev(self) -> float:
        n = len(self.values)
        if n < 2:
            return 0.0
        m = self.mean
        return math.sqrt(sum((v - m) ** 2 for v in self.values) / (n - 1))

    @property
    def full(self) -> bool:
        return len(self.values) >= self.capacity


class AnomalyDetector:
    """Detects anomalies in pipeline metrics using statistical methods.

    Supports three detection strategies:
      - ZSCORE: flags points beyond N standard deviations from rolling mean
      - IQR: flags points outside [Q1 - k*IQR, Q3 + k*IQR]
      - CUSUM: cumulative sum control chart for small persistent shifts
    """

    DEFAULT_ZSCORE_THRESHOLD = 3.0
    DEFAULT_IQR_MULTIPLIER = 1.5
    DEFAULT_CUSUM_THRESHOLD = 5.0
    DEFAULT_DRIFT_THRESHOLD = 0.2

    def __init__(
        self,
        zscore_threshold: float = DEFAULT_ZSCORE_THRESHOLD,
        iqr_multiplier: float = DEFAULT_IQR_MULTIPLIER,
        cusum_threshold: float = DEFAULT_CUSUM_THRESHOLD,
        drift_threshold: float = DEFAULT_DRIFT_THRESHOLD,
        window_capacity: int = 240,
    ):
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.cusum_threshold = cusum_threshold
        self.drift_threshold = drift_threshold
        self._windows: Dict[str, RollingWindow] = {}
        self._cusum_state: Dict[str, Tuple[float, float]] = {}  # (S_hi, S_lo)
        self._window_capacity = window_capacity

    def _get_window(self, key: str) -> RollingWindow:
        if key not in self._windows:
            self._windows[key] = RollingWindow(capacity=self._window_capacity)
        return self._windows[key]

    def train(self, key: str, values: Sequence[float]) -> None:
        w = self._get_window(key)
        for v in values:
            w.push(v)

    def check_zscore(self, key: str, value: float) -> Tuple[float, bool]:
        w = self._get_window(key)
        if not w.full or w.stddev < 1e-9:
            w.push(value)
            return 0.0, False
        z = (value - w.mean) / w.stddev
        w.push(value)
        return z, abs(z) > self.zscore_threshold

    def check_iqr(self, key: str, value: float) -> Tuple[float, bool]:
        w = self._get_window(key)
        w.push(value)
        if len(w.values) < 4:
            return 0.0, False
        sorted_vals = sorted(w.values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[3 * n // 4]
        iqr = q3 - q1
        if iqr < 1e-9:
            return 0.0, False
        lower = q1 - self.iqr_multiplier * iqr
        upper = q3 + self.iqr_multiplier * iqr
        outlier = value < lower or value > upper
        deviation = max(value - upper, lower - value, 0)
        return deviation / iqr if iqr > 0 else 0.0, outlier

    def check_cusum(self, key: str, value: float) -> Tuple[float, bool]:
        w = self._get_window(key)
        w.push(value)
        if not w.full:
            return 0.0, False
        target = w.mean
        s_hi, s_lo = self._cusum_state.get(key, (0.0, 0.0))
        s_hi = max(0.0, s_hi + (value - target) - 0.5 * w.stddev)
        s_lo = max(0.0, s_lo + (target - value) - 0.5 * w.stddev)
        self._cusum_state[key] = (s_hi, s_lo)
        cusum_val = max(s_hi, s_lo)
        return cusum_val, cusum_val > self.cusum_threshold * w.stddev

    def detect_drift(
        self, key: str, values: List[float], window_size: int = 50
    ) -> Optional[TrendLine]:
        """Fit a linear trend and report if slope exceeds drift threshold."""
        w = self._get_window(key)
        for v in values:
            w.push(v)
        if len(w.values) < window_size:
            return None
        recent = w.values[-window_size:]
        n = len(recent)
        x_mean = (n - 1) / 2.0
        y_mean = sum(recent) / n
        num = sum((i - x_mean) * (recent[i] - y_mean) for i in range(n))
        den = sum((i - x_mean) ** 2 for i in range(n))
        if den < 1e-12:
            return None
        slope = num / den
        intercept = y_mean - slope * x_mean
        y_pred = [slope * i + intercept for i in range(n)]
        ss_res = sum((recent[i] - y_pred[i]) ** 2 for i in range(n))
        ss_tot = sum((v - y_mean) ** 2 for v in recent)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 1e-12 else 0.0
        return TrendLine(
            slope=slope, intercept=intercept, r_squared=r_squared, data_points=n
        )

    def analyze_runs(
        self,
        runs: List[PipelineRun],
        pipeline_key: str = "",
    ) -> List[AnomalySignal]:
        """Run all detection methods over a batch of pipeline runs."""
        signals: List[AnomalySignal] = []

        durations = [
            r.duration_seconds
            for r in runs
            if r.duration_seconds is not None and r.conclusion == "success"
        ]
        for i, dur in enumerate(durations):
            z, flagged = self.check_zscore(f"{pipeline_key}_duration", dur)
            if flagged:
                signals.append(AnomalySignal(
                    metric_name="pipeline_duration_seconds",
                    observed=dur,
                    expected=self._get_window(f"{pipeline_key}_duration").mean,
                    zscore=z,
                    severity="high" if abs(z) > 4.0 else "medium",
                    description=f"Run duration z-score {z:.2f} exceeds threshold",
                ))

        queue_delays = [
            r.queue_delay_seconds
            for r in runs
            if r.queue_delay_seconds is not None
        ]
        for delay in queue_delays:
            _, flagged = self.check_iqr(f"{pipeline_key}_queue_delay", delay)
            if flagged:
                signals.append(AnomalySignal(
                    metric_name="queue_delay_seconds",
                    observed=delay,
                    expected=self._get_window(f"{pipeline_key}_queue_delay").mean,
                    zscore=0,
                    severity="low",
                    description=f"Unusual queue delay: {delay:.0f}s",
                ))

        fail_rate = 0.0
        if runs:
            fails = sum(1 for r in runs if r.conclusion == "failure")
            fail_rate = fails / len(runs)
        if fail_rate > 0:
            cusum_val, flagged = self.check_cusum(f"{pipeline_key}_fail_rate", fail_rate)
            if flagged:
                signals.append(AnomalySignal(
                    metric_name="failure_rate",
                    observed=fail_rate,
                    expected=self._get_window(f"{pipeline_key}_fail_rate").mean,
                    zscore=cusum_val,
                    severity="high",
                    description=f"Sustained failure rate shift: {fail_rate:.1%}",
                ))

        drift = self.detect_drift(
            f"{pipeline_key}_duration_trend", durations
        )
        if drift and abs(drift.slope) > self.drift_threshold:
            direction = drift.direction
            signals.append(AnomalySignal(
                metric_name="pipeline_duration_trend",
                observed=drift.slope,
                expected=0.0,
                zscore=abs(drift.slope) / self.drift_threshold,
                severity="medium",
                description=f"Duration {direction} trend: {drift.slope:.2f}s/run (r²={drift.r_squared:.3f})",
            ))

        for stage_name in self._collect_stage_names(runs):
            stage_durs: List[float] = []
            for r in runs:
                for stage in r.stages:
                    if stage.stage_name == stage_name:
                        dur = stage.duration_seconds
                        if dur is not None:
                            stage_durs.append(dur)
            for dur in stage_durs:
                z, flagged = self.check_zscore(
                    f"{pipeline_key}_stage_{stage_name}", dur
                )
                if flagged:
                    signals.append(AnomalySignal(
                        metric_name=f"stage_duration::{stage_name}",
                        observed=dur,
                        expected=self._get_window(
                            f"{pipeline_key}_stage_{stage_name}"
                        ).mean,
                        zscore=z,
                        severity="medium",
                        description=f"Stage '{stage_name}' duration anomaly: {dur:.1f}s",
                    ))

        return signals

    @staticmethod
    def _collect_stage_names(runs: List[PipelineRun]) -> List[str]:
        names: List[str] = []
        seen = set()
        for r in runs:
            for stage in r.stages:
                if stage.stage_name not in seen:
                    seen.add(stage.stage_name)
                    names.append(stage.stage_name)
        return names


class AggregationEngine:
    """Time-bucketed aggregation of pipeline metrics.

    Produces hourly and daily rollups suitable for dashboard rendering.
    """

    def __init__(self, bucket_size_seconds: int = 3600):
        self.bucket_size = bucket_size_seconds

    def bucket_durations(
        self, runs: List[PipelineRun]
    ) -> Dict[int, List[float]]:
        buckets: Dict[int, List[float]] = defaultdict(list)
        for r in runs:
            if r.started_at and r.duration_seconds is not None:
                bucket_key = int(r.started_at.timestamp()) // self.bucket_size
                buckets[bucket_key].append(r.duration_seconds)
        return dict(buckets)

    def rollup(self, runs: List[PipelineRun]) -> Dict[str, float]:
        durations = [
            r.duration_seconds
            for r in runs
            if r.duration_seconds is not None
        ]
        if not durations:
            return {
                "count": 0,
                "p50": 0, "p95": 0, "p99": 0,
                "mean": 0, "max": 0, "min": 0,
            }
        sorted_d = sorted(durations)
        n = len(sorted_d)

        def pct(q: float) -> float:
            idx = int(n * q)
            return sorted_d[min(idx, n - 1)]

        total = sum(durations)
        fails = sum(1 for r in runs if r.conclusion == "failure")
        return {
            "count": n,
            "failures": fails,
            "failure_rate": fails / len(runs) if runs else 0,
            "p50": pct(0.50),
            "p75": pct(0.75),
            "p95": pct(0.95),
            "p99": pct(0.99),
            "mean": total / n,
            "max": max(durations),
            "min": min(durations),
            "total_seconds": total,
        }

    def daily_report(
        self, runs: List[PipelineRun], label: str = ""
    ) -> Dict[str, Any]:
        by_day: Dict[str, List[PipelineRun]] = defaultdict(list)
        for r in runs:
            if r.started_at:
                day_key = r.started_at.strftime("%Y-%m-%d")
                by_day[day_key].append(r)
        result: Dict[str, Any] = {"label": label, "days": {}}
        for day, day_runs in sorted(by_day.items()):
            result["days"][day] = self.rollup(day_runs)
        return result

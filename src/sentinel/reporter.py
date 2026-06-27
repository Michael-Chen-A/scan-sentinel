"""
Report generation for pipeline telemetry observations.

Produces Markdown summaries, JSON blobs, and console dashboards
from TelemetryCollector and AnomalyDetector output.
"""

from __future__ import annotations

import json
import textwrap
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .aggregator import AggregationEngine, AnomalySignal, TrendLine
from .collector import PipelineRun


@dataclass
class ReportConfig:
    title: str = "CI Pipeline Health Report"
    include_anomalies: bool = True
    include_trends: bool = True
    include_daily_breakdown: bool = True
    max_anomalies: int = 20
    max_days: int = 14


class ReportGenerator:
    """Generates formatted reports from pipeline telemetry data."""

    def __init__(self, config: Optional[ReportConfig] = None):
        self.config = config or ReportConfig()
        self.aggregator = AggregationEngine()

    def build_summary(
        self,
        runs: List[PipelineRun],
        anomalies: Optional[List[AnomalySignal]] = None,
        daily: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not runs:
            return {"runs": 0, "message": "No pipeline runs observed."}
        failures = [r for r in runs if r.conclusion == "failure"]
        cancellations = [r for r in runs if r.conclusion == "cancelled"]
        durations = [
            r.duration_seconds
            for r in runs
            if r.duration_seconds is not None
        ]
        sorted_d = sorted(durations) if durations else []

        def pct(q: float) -> float:
            if not sorted_d:
                return 0.0
            return sorted_d[min(int(len(sorted_d) * q), len(sorted_d) - 1)]

        summary: Dict[str, Any] = {
            "title": self.config.title,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "stats": {
                "total_runs": len(runs),
                "successful": len(runs) - len(failures) - len(cancellations),
                "failed": len(failures),
                "cancelled": len(cancellations),
                "success_rate": (
                    (len(runs) - len(failures) - len(cancellations)) / len(runs)
                    if runs else 0
                ),
                "duration": {
                    "min_seconds": sorted_d[0] if sorted_d else 0,
                    "p50_seconds": pct(0.50),
                    "p95_seconds": pct(0.95),
                    "p99_seconds": pct(0.99),
                    "max_seconds": sorted_d[-1] if sorted_d else 0,
                },
                "stage_breakdown": self._stage_stats(runs),
            },
        }
        if self.config.include_anomalies and anomalies:
            high = [a for a in anomalies if a.severity == "high"]
            med = [a for a in anomalies if a.severity == "medium"]
            low = [a for a in anomalies if a.severity == "low"]
            summary["anomalies"] = {
                "total": len(anomalies),
                "high": len(high),
                "medium": len(med),
                "low": len(low),
                "details": [
                    {
                        "metric": a.metric_name,
                        "severity": a.severity,
                        "observed": a.observed,
                        "expected": a.expected,
                        "zscore": a.zscore,
                        "description": a.description,
                    }
                    for a in anomalies[: self.config.max_anomalies]
                ],
            }
        if self.config.include_daily_breakdown and daily:
            summary["daily"] = daily
        return summary

    def render_markdown(self, summary: Dict[str, Any]) -> str:
        s = summary.get("stats", {})
        lines = [
            f"# {summary.get('title', 'Pipeline Report')}",
            f"Generated: {summary.get('generated_at', '')}",
            "",
            "## Overview",
            f"| Metric | Value |",
            f"|---|---|",
            f"| Total Runs | {s.get('total_runs', 0)} |",
        ]
        success_rate = s.get("success_rate", 0)
        emoji = "🟢" if success_rate >= 0.95 else ("🟡" if success_rate >= 0.80 else "🔴")
        lines.append(f"| Success Rate | {emoji} {success_rate:.1%} |")
        lines.append(f"| Failed | {s.get('failed', 0)} |")
        lines.append(f"| Cancelled | {s.get('cancelled', 0)} |")
        lines.append("")
        dur = s.get("duration", {})
        lines.extend([
            "## Duration",
            f"| Percentile | Seconds |",
            f"|---|---|",
            f"| min | {dur.get('min_seconds', 0):.0f} |",
            f"| p50 | {dur.get('p50_seconds', 0):.0f} |",
            f"| p95 | {dur.get('p95_seconds', 0):.0f} |",
            f"| p99 | {dur.get('p99_seconds', 0):.0f} |",
            f"| max | {dur.get('max_seconds', 0):.0f} |",
            "",
        ])
        anomalies = summary.get("anomalies")
        if anomalies and anomalies.get("total", 0) > 0:
            lines.extend([
                "## Anomalies",
                f"- **{anomalies['high']}** high severity",
                f"- **{anomalies['medium']}** medium severity",
                f"- **{anomalies['low']}** low severity",
                "",
            ])
            for a in anomalies.get("details", [])[:10]:
                lines.append(
                    f"- `{a['metric']}` [{a['severity']}] z={a['zscore']:.1f}: {a['description']}"
                )
            lines.append("")
        daily = summary.get("daily", {}).get("days", {})
        if daily:
            lines.extend([
                "## Daily Breakdown",
                "| Day | Runs | Fail Rate | p50 | p95 |",
                "|---|---|---|---|---|",
            ])
            for day, rollup in list(daily.items())[-self.config.max_days:]:
                lines.append(
                    f"| {day} | {rollup.get('count', 0)} | "
                    f"{rollup.get('failure_rate', 0):.1%} | "
                    f"{rollup.get('p50', 0):.0f}s | "
                    f"{rollup.get('p95', 0):.0f}s |"
                )
            lines.append("")
        return "\n".join(lines)

    def render_console(self, summary: Dict[str, Any]) -> str:
        s = summary.get("stats", {})
        width = 60
        lines = [
            "=" * width,
            f"  {summary.get('title', 'Pipeline Report')}",
            "=" * width,
            f"  Runs: {s.get('total_runs', 0):>6}  "
            f"Success: {s.get('success_rate', 0):.1%}  "
            f"Failed: {s.get('failed', 0)}",
            f"  p50: {s.get('duration', {}).get('p50_seconds', 0):.0f}s  "
            f"p95: {s.get('duration', {}).get('p95_seconds', 0):.0f}s  "
            f"p99: {s.get('duration', {}).get('p99_seconds', 0):.0f}s",
            "-" * width,
        ]
        anomalies = summary.get("anomalies")
        if anomalies and anomalies["total"] > 0:
            lines.append(f"  ⚠ {anomalies['total']} anomalies "
                         f"({anomalies['high']}H/{anomalies['medium']}M/{anomalies['low']}L)")
        else:
            lines.append(f"  ✓ No anomalies detected")
        lines.append("=" * width)
        return "\n".join(lines)

    @staticmethod
    def _stage_stats(runs: List[PipelineRun]) -> Dict[str, Any]:
        stage_data: Dict[str, List[float]] = defaultdict(list)
        for r in runs:
            for stage in r.stages:
                dur = stage.duration_seconds
                if dur is not None:
                    stage_data[stage.stage_name].append(dur)
        result: Dict[str, Any] = {}
        for name, durs in stage_data.items():
            sd = sorted(durs)
            n = len(sd)
            result[name] = {
                "count": n,
                "p50": sd[n // 2] if n > 0 else 0,
                "p95": sd[min(int(n * 0.95), n - 1)] if n > 0 else 0,
                "mean": sum(durs) / n if n > 0 else 0,
            }
        return result

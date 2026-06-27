"""Sentinel — Lightweight CI telemetry and build health monitoring."""

__version__ = "0.5.3"
__author__ = "Serene Observer Maintainers"
__all__ = [
    "TelemetryCollector",
    "MetricSeries",
    "AnomalyDetector",
    "ReportGenerator",
    "PipelineWatcher",
]

from .collector import TelemetryCollector, MetricSeries
from .aggregator import AnomalyDetector
from .reporter import ReportGenerator
from .watcher import PipelineWatcher

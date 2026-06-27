"""
Command-line interface for the Sentinel telemetry agent.

Usage:
    sentinel watch   --config watch.yaml
    sentinel report  --input runs.jsonl
    sentinel status
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click

from . import __version__
from .collector import (
    ExportFormat,
    MetricKind,
    PipelineRun,
    StageTiming,
    TelemetryCollector,
)
from .aggregator import AggregationEngine, AnomalyDetector
from .reporter import ReportConfig, ReportGenerator
from .watcher import PipelineWatcher, WatchConfig, WatchTarget

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format=LOG_FORMAT, stream=sys.stderr)


def _load_runs_from_jsonl(path: str) -> list[PipelineRun]:
    runs: list[PipelineRun] = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            run = PipelineRun(
                run_id=data["run_id"],
                pipeline_name=data["pipeline_name"],
                repo_slug=data["repo_slug"],
                branch=data.get("branch", ""),
                commit_sha=data.get("commit_sha", ""),
                trigger=data.get("trigger", ""),
                queued_at=_parse_dt(data.get("queued_at")),
                started_at=_parse_dt(data.get("started_at")),
                finished_at=_parse_dt(data.get("finished_at")),
                conclusion=data.get("conclusion"),
            )
            runs.append(run)
    return runs


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


@click.group()
@click.version_option(version=__version__, prog_name="sentinel")
@click.option("-v", "--verbose", is_flag=True, help="Enable debug logging")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """Sentinel — CI pipeline telemetry and health monitoring."""
    setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@main.command()
@click.option("-c", "--config", "config_path", default="watch.yaml",
              type=click.Path(exists=True), help="Watch configuration file")
@click.option("-o", "--output-dir", default="telemetry_out",
              type=click.Path(), help="Output directory for exports")
@click.option("--once", is_flag=True, help="Run a single observation cycle and exit")
@click.option("--export-format", "-f", multiple=True,
              type=click.Choice(["json", "prometheus", "sqlite"]),
              default=["json"], help="Export backends to enable")
def watch(
    config_path: str,
    output_dir: str,
    once: bool,
    export_format: tuple[str, ...],
) -> None:
    """Run pipeline telemetry collection loop."""
    with open(config_path, encoding="utf-8") as fh:
        raw = json.load(fh) if config_path.endswith(".json") else _load_yaml(config_path)
    config = WatchConfig.from_dict(raw)
    fmts = [ExportFormat(f) for f in export_format]
    collector = TelemetryCollector(
        export_formats=fmts,
        output_dir=Path(output_dir),
    )
    watcher = PipelineWatcher(config=config, collector=collector)

    if once:
        click.echo("Single observation mode — not implemented without GitHub token.")
        click.echo(f"Would watch {len(config.targets)} target(s).")
        return

    click.echo(f"Starting watcher for {len(config.targets)} target(s)...")
    click.echo(f"Export formats: {[f.value for f in fmts]}")
    click.echo("Loop mode ready. Press Ctrl+C to stop.")


@main.command()
@click.option("-i", "--input", "input_path", type=click.Path(exists=True),
              help="JSONL file with pipeline runs")
@click.option("-f", "--format", "output_format", type=click.Choice(["markdown", "json", "console"]),
              default="console", help="Report output format")
@click.option("-o", "--output", "output_path", type=click.Path(),
              help="Write report to file")
@click.option("--no-anomalies", is_flag=True, help="Exclude anomaly detection")
def report(
    input_path: str,
    output_format: str,
    output_path: Optional[str],
    no_anomalies: bool,
) -> None:
    """Generate a health report from pipeline run data."""
    runs = _load_runs_from_jsonl(input_path)
    if not runs:
        click.echo("No runs found in input file.")
        return

    report_cfg = ReportConfig(include_anomalies=not no_anomalies)
    generator = ReportGenerator(config=report_cfg)
    detector = AnomalyDetector()
    aggregator = AggregationEngine()

    anomalies = detector.analyze_runs(runs) if not no_anomalies else []
    daily = aggregator.daily_report(runs)
    summary = generator.build_summary(runs, anomalies=anomalies, daily=daily)

    if output_format == "markdown":
        out = generator.render_markdown(summary)
    elif output_format == "json":
        out = json.dumps(summary, indent=2, ensure_ascii=False)
    else:
        out = generator.render_console(summary)

    if output_path:
        Path(output_path).write_text(out, encoding="utf-8")
        click.echo(f"Report written to {output_path}")
    else:
        click.echo(out)


@main.command()
def status() -> None:
    """Show current watcher status summary."""
    click.echo(f"Sentinel v{__version__}")
    click.echo(f"Time: {datetime.now(timezone.utc).isoformat()}")
    click.echo("Status: no active watcher (ephemeral-only runs)")
    click.echo("Use 'sentinel watch --config watch.yaml' to start monitoring.")


def _load_yaml(path: str) -> dict:
    try:
        import yaml as _yaml
        with open(path, encoding="utf-8") as fh:
            return _yaml.safe_load(fh)
    except ImportError:
        click.echo("Error: PyYAML required for YAML config. Install with: pip install pyyaml", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

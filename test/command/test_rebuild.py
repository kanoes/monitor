"""Utility script for rebuilding daily monitor reports locally.

This script fetches real Azure Log Analytics data for a user-provided date
range, generates the daily monitor Excel files, and saves them to the desired
output directory. Email delivery and blob uploads are intentionally disabled so
that the generated workbooks remain on the local filesystem for inspection.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import List

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))
os.chdir(project_root)

from core_analytics.config.settings import ConfigurationService
from core_analytics.model.repositories.azure_log_repository import AzureLogRepository
from core_analytics.services.analytics_service import AnalyticsService
from core_analytics.services.query_strategies.strategy_factory import QueryStrategyFactory
from core_analytics.view.factories.daily_monitor_factory import DailyMonitorFactory
from core_analytics.core.logging_config import LoggerSetup
from core_analytics.command.rebuild import (
    parse_date,
    generate_date_range,
    initialize_usage_report_from_template,
    process_single_day,
)

logger = LoggerSetup.setup_logger()


def _ensure_output_base_dir(config_service: ConfigurationService, output_dir: Path | None) -> Path:
    """Override the configuration output directory when requested."""
    app_settings = config_service.get_app_settings()
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        app_settings.output_base_dir = str(output_dir)
        config_service._app_settings = app_settings  # type: ignore[attr-defined]
        return output_dir

    resolved = Path(app_settings.output_base_dir)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def test_rebuild(rebuild_from: str, rebuild_to: str, output_dir: Path | None = None) -> List[str]:
    """Run the rebuild flow for a specific date range and keep files locally."""
    logger.info(f"Starting rebuild process from {rebuild_from} to {rebuild_to}")

    # Validate date inputs early.
    try:
        parse_date(rebuild_from)
        parse_date(rebuild_to)
    except ValueError as exc:
        logger.error(f"Invalid date format: {exc}")
        raise

    date_range = generate_date_range(rebuild_from, rebuild_to)
    logger.info(f"Processing {len(date_range)} day(s)")

    config_service = ConfigurationService(days_range=1)
    output_base_dir = _ensure_output_base_dir(config_service, output_dir)

    log_repository = AzureLogRepository(config_service)
    strategy_factory = QueryStrategyFactory()
    analytics_service = AnalyticsService(
        log_repository,
        config_service,
        strategy_factory,
    )
    daily_monitor_factory = DailyMonitorFactory()

    usage_report_path = output_base_dir / "市場GAI打鍵" / "市場GAI使用状況.xlsx"
    usage_report_path.parent.mkdir(parents=True, exist_ok=True)

    template_path = project_root / "config" / "report_template" / "市場GAI使用状況.xlsx"
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    initialize_usage_report_from_template(template_path, usage_report_path)

    generated_files: List[str] = []

    for target_date in date_range:
        try:
            logger.info("\n" + "=" * 60)
            logger.info(f"Processing date: {target_date.strftime('%Y-%m-%d')}")
            logger.info("=" * 60)

            files = process_single_day(
                target_date,
                config_service,
                log_repository,
                strategy_factory,
                analytics_service,
                daily_monitor_factory,
                storage_service=None,
                upload_blob=False,
            )

            generated_files.extend(files)
            logger.info(f"Generated {len(files)} history file(s): {files}")
        except Exception as exc:  # pragma: no cover - manual execution helper
            logger.error(f"Failed to process date {target_date.strftime('%Y%m%d')}: {exc}", exc_info=True)
            continue

    if usage_report_path.exists():
        generated_files.append(str(usage_report_path))

    logger.info("\n" + "=" * 60)
    logger.info("Rebuild completed!")
    logger.info(f"Generated {len(generated_files)} files")
    logger.info(f"Processed dates from {rebuild_from} to {rebuild_to}")
    logger.info(f"Files saved under: {output_base_dir}")
    logger.info("=" * 60 + "\n")

    print("✅ Test rebuild completed successfully!")
    print(f"📊 Generated {len(generated_files)} report files")
    print(f"📅 Processed dates from {rebuild_from} to {rebuild_to}")
    print(f"📁 Files saved under: {output_base_dir}")

    return generated_files


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate daily monitor Excel files locally for a date range",
    )
    parser.add_argument("--from", dest="rebuild_from", required=True, help="Start date in YYYYMMDD format")
    parser.add_argument("--to", dest="rebuild_to", required=True, help="End date in YYYYMMDD format")
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default=None,
        help="Optional directory to store generated files (defaults to configured output)",
    )

    args = parser.parse_args()
    resolved_output = Path(args.output_dir).expanduser().resolve() if args.output_dir else None

    try:
        test_rebuild(args.rebuild_from, args.rebuild_to, resolved_output)
    except Exception as exc:  # pragma: no cover - manual execution helper
        logger.error(f"Test rebuild failed: {exc}", exc_info=True)
        sys.exit(1)

import datetime
import os
import shutil
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import List, Optional

from core_analytics.config.settings import ConfigurationService
from core_analytics.model.repositories.azure_log_repository import AzureLogRepository
from core_analytics.services.analytics_service import AnalyticsService
from core_analytics.services.query_strategies.strategy_factory import QueryStrategyFactory
from core_analytics.view.factories.daily_monitor_factory import DailyMonitorFactory
from core_analytics.core.logging_config import LoggerSetup
from core_analytics.services.email_service import EmailService
from core_analytics.model.repositories.azure_blob_repository import AzureBlobRepository
from core_analytics.core.models import ProcessData

logger = LoggerSetup.setup_logger()


def parse_date(date_str: str) -> datetime.datetime:
    """Parse date string in YYYYMMDD format to datetime."""
    return datetime.datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=ZoneInfo("Asia/Tokyo"))


def generate_date_range(from_date: str, to_date: str) -> List[datetime.datetime]:
    """Generate list of dates between from_date and to_date (inclusive)."""
    start = parse_date(from_date)
    end = parse_date(to_date)
    
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += datetime.timedelta(days=1)
    
    return dates


def initialize_usage_report_from_template(template_path: Path, output_path: Path) -> None:
    """Initialize usage report from template."""
    shutil.copy2(template_path, output_path)
    logger.info(f"Initialized usage report from template: {output_path}")


def process_single_day(target_date: datetime.datetime, config_service: ConfigurationService,
                      log_repository: AzureLogRepository, strategy_factory: QueryStrategyFactory,
                      analytics_service: AnalyticsService, daily_monitor_factory: DailyMonitorFactory,
                      storage_service: Optional[AzureBlobRepository] = None, upload_blob: bool = True) -> List[str]:
    """Process data for a single day and generate reports."""
    jst = ZoneInfo("Asia/Tokyo")
    target_date_jst = target_date.astimezone(jst)
    
    start_time = target_date_jst.replace(hour=0, minute=0, second=0, microsecond=0)
    end_time = target_date_jst.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    start_time_utc = start_time.astimezone(datetime.UTC)
    end_time_utc = end_time.astimezone(datetime.UTC)
    
    logger.info(f"Processing date: {target_date_jst.strftime('%Y-%m-%d')} (from {start_time_utc} to {end_time_utc})")
    
    processed_data: ProcessData = analytics_service.fetch_and_process_data(
        start_time_utc,
        end_time_utc
    )
    
    app_settings = config_service.get_app_settings()
    output_dir = app_settings.output_base_dir
    
    generated_files = []
    
    usage_report_path = daily_monitor_factory._generate_cumulative_usage_report(
        Path(output_dir), processed_data, end_time_utc, None
    )
    logger.info(f"Updated usage report: {usage_report_path}")
    
    date_str = target_date_jst.strftime('%Y%m%d')
    ym_str = target_date_jst.strftime('%Y%m')
    
    base_dir = Path("output/市場GAI打鍵/打鍵詳細履歴") / ym_str
    base_dir.mkdir(parents=True, exist_ok=True)
    history_report_path = base_dir / f"市場GAI打鍵履歴_{date_str}.xlsx"
    
    template_path = Path("./config/report_template/市場GAI打鍵履歴_YYYYMMDD.xlsx")
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    shutil.copy2(template_path, history_report_path)
    logger.info(f"Created history report from template: {history_report_path}")
    
    daily_monitor_factory._fill_history_template_with_data(history_report_path, processed_data)
    generated_files.append(str(history_report_path))
    logger.info(f"Generated history report: {history_report_path}")
    
    if upload_blob and storage_service:
        storage_service.upload_file(str(history_report_path), str(history_report_path))
        logger.info(f"Uploaded {history_report_path} to blob")

    return generated_files


def rebuild():
    """Rebuild reports for date range specified in environment variables."""
    rebuild_from = os.environ.get("REBUILD_FROM")
    rebuild_to = os.environ.get("REBUILD_TO")
    
    if not rebuild_from or not rebuild_to:
        raise ValueError("REBUILD_FROM and REBUILD_TO environment variables are required")
    
    logger.info(f"Starting rebuild process from {rebuild_from} to {rebuild_to}")
    
    date_range = generate_date_range(rebuild_from, rebuild_to)
    logger.info(f"Processing {len(date_range)} days")
    
    upload_blob = os.environ.get("REBUILD_UPLOAD_BLOB", "true").lower() == "true"
    send_email = os.environ.get("REBUILD_SEND_EMAIL", "true").lower() == "true"

    config_service = ConfigurationService(days_range=1)
    log_repository = AzureLogRepository(config_service)
    strategy_factory = QueryStrategyFactory()
    analytics_service = AnalyticsService(
        log_repository,
        config_service,
        strategy_factory
    )
    daily_monitor_factory = DailyMonitorFactory()
    storage_service: Optional[AzureBlobRepository] = None
    if upload_blob:
        storage_service = AzureBlobRepository(config_service)

    if send_email:
        try:
            email_service = EmailService()
            email_enabled = True
        except Exception as e:
            logger.warning(f"Email service not available: {e}")
            email_service = None
            email_enabled = False
    else:
        email_service = None
        email_enabled = False
    
    all_generated_files = []
    
    usage_report_path = Path("output/市場GAI打鍵/市場GAI使用状況.xlsx")
    usage_report_path.parent.mkdir(parents=True, exist_ok=True)
    
    template_path = Path("./config/report_template/市場GAI使用状況.xlsx")
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    initialize_usage_report_from_template(template_path, usage_report_path)
    
    for target_date in date_range:
        try:
            files = process_single_day(
                target_date,
                config_service,
                log_repository,
                strategy_factory,
                analytics_service,
                daily_monitor_factory,
                storage_service,
                upload_blob=upload_blob
            )
            
            history_files = [f for f in files if "打鍵履歴" in f]
            all_generated_files.extend(history_files)
            
        except Exception as e:
            logger.error(f"Failed to process date {target_date.strftime('%Y%m%d')}: {e}")
            continue
    
    if usage_report_path.exists():
        all_generated_files.append(str(usage_report_path))
        if upload_blob and storage_service:
            storage_service.upload_file(str(usage_report_path), str(usage_report_path))
            logger.info(f"Final upload of usage report to blob")

    
    if email_enabled and all_generated_files:
        date_str = f"{rebuild_from} to {rebuild_to}"
        email_sent = email_service.send_daily_monitor_report(all_generated_files, date_str)
        if email_sent:
            logger.info("Rebuild report email sent successfully")
        else:
            logger.error("Failed to send rebuild report email")
    
    logger.info(f"Rebuild completed. Generated {len(all_generated_files)} files")
    print(f"✅ Rebuild completed successfully!")
    print(f"📊 Generated {len(all_generated_files)} report files")
    print(f"📅 Processed dates from {rebuild_from} to {rebuild_to}")


if __name__ == "__main__":
    rebuild()
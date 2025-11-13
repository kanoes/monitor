"""
Main entry point for Core Analytics application.
"""
import logging
import os
import sys

from core_analytics.control.AppActivityMonitor import AppActivityMonitor
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.DEBUG)
logging.getLogger("CoreAnalytics").setLevel(logging.DEBUG)
logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.ERROR)

if os.environ.get("REBUILD", "").lower() == "true":
    from core_analytics.command.rebuild import rebuild
    try:
        rebuild()
        sys.exit(0)
    except Exception as e:
        logging.error(f"Rebuild failed: {e}")
        sys.exit(1)

report_mode = os.environ.get("REPORT_MODE", "daily_monitor")
if report_mode == "daily_monitor":
    days_range = 1
else:
    days_range = 30

def run_app_activity_monitor():
    try:
        print("Scheduled job started")
        app_activity_monitor = AppActivityMonitor(days_range=days_range)
        app_activity_monitor.run()
    except Exception as e:
        print(f"Scheduled job failed: {e}")

# スケジューラーを起動
scheduler = BackgroundScheduler()

if os.environ.get("SCHEDULER_DEBUG_MODE", "0") == "1":
    for seconds in [0, 10, 20, 30, 40, 50]:
        scheduler.add_job(
            run_app_activity_monitor,
            CronTrigger(second=seconds)
        )
else:
    hour = int(os.environ.get("SCHEDULE_HOUR", "17"))
    minute = int(os.environ.get("SCHEDULE_MIN", "5"))

    scheduler.add_job(
        run_app_activity_monitor,
        CronTrigger(
            day_of_week="mon-fri",
            hour=hour,
            minute=minute,
            timezone="Asia/Tokyo"
        ),
        id="app_activity_monitor"
    )

scheduler.start()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # FastAPIシャットダウン時にスケジューラーを停止
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
def health_check():
    return "OK"

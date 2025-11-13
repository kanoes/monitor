import os
import datetime
from pathlib import Path
from dotenv import load_dotenv

os.chdir(Path(__file__).resolve().parents[2])
load_dotenv()

from core_analytics.control.AppActivityMonitor import AppActivityMonitor

os.environ["TEMPLATE_TYPE"] = "prod"


def set_test_environment(send_email: bool, upload_blob: bool) -> None:
    # Force daily monitor mode
    os.environ["REPORT_MODE"] = "daily_monitor"


    # Override output base to test/output by adjusting working directory
    # We set CWD to project root if not already
    project_root = Path(__file__).resolve().parents[2]
    os.chdir(project_root)

    # Ensure test/output exists and is used as base by pointing output_base_dir via env if supported
    # Our code reads output_base_dir from settings.AppSettings which defaults to "output".
    # We can prefix CWD to test so that output goes under test/output by changing CWD into test.
    out_base = project_root / "test" / "output"
    out_base.mkdir(parents=True, exist_ok=True)
    os.environ["OUTPUT_BASE_DIR"] = str(out_base)

    # Control email sending by environment variables so EmailService init can be toggled
    if not send_email:
        os.environ["TO_EMAILS"] = ""  # makes send skip as no recipients

    # Control blob upload by flag checked in this runner
    if not upload_blob:
        os.environ["DISABLE_BLOB_UPLOAD"] = "1"


def run_daily_monitor(send_email: bool = False, upload_blob: bool = False) -> None:
    from pathlib import Path
    import os

    os.chdir(Path(__file__).resolve().parents[2])
    set_test_environment(send_email, upload_blob)

    # Reduce days range if needed (e.g. 30 days)
    days_range = 1

    app = AppActivityMonitor(days_range=days_range)

    # Monkey-patch upload if disabled
    if os.environ.get("DISABLE_BLOB_UPLOAD") == "1":
        original_upload = app.storage_service.upload_file

        def _noop_upload(local_file_path: str, remote_path: str) -> str:
            return remote_path

        app.storage_service.upload_file = _noop_upload  # type: ignore

    # Monkey-patch email if disabled
    if not send_email and getattr(app, "email_service", None):
        def _noop_send(files, date_str):
            return True
        app.email_service.send_daily_monitor_report = _noop_send  # type: ignore

    app.run()


if __name__ == "__main__":
    SEND_EMAIL = True
    UPLOAD_BLOB = False
    run_daily_monitor(send_email=SEND_EMAIL, upload_blob=UPLOAD_BLOB)



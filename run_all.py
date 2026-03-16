#!/usr/bin/env python3
"""
Combined Data Fetch Pipeline — run_all.py

Orchestrates all data fetching tasks:
  1. Always: fetch_news.py (RSS -> translate -> categorize -> summarize -> Google Sheets)
  2. Once per day: fetch_sensortower.py (app rankings) and fetch_producthunt_top.py (top products)

Daily tasks are tracked via a marker file (.daily_marker) that stores today's date.
If the marker matches today's date, daily tasks are skipped.

Output goes to native Google Sheets in Drive folder:
  1hzvd_SkU3z2oP-op9LtYn3Q50Op7qY_P/{year}/Latest and Cumulative

Usage:
  cd /home/ubuntu/manus-data-fetch
  SENSORTOWER_API_KEY="..." PH_API_KEY="..." PH_API_SECRET="..." python3 run_all.py
"""

import os
import sys
import time
import logging
import traceback
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_all")

DAILY_MARKER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".daily_marker")


def get_today_str() -> str:
    """Get today's date string in SGT (UTC+8) for marker comparison."""
    sgt = timezone(timedelta(hours=8))
    return datetime.now(sgt).strftime("%Y-%m-%d")


def should_run_daily() -> bool:
    """Check if daily tasks should run (first run of the day)."""
    today = get_today_str()
    if os.path.exists(DAILY_MARKER_FILE):
        with open(DAILY_MARKER_FILE, "r") as f:
            marker_date = f.read().strip()
        if marker_date == today:
            log.info(f"Daily marker shows tasks already ran today ({today}). Skipping daily tasks.")
            return False
    log.info(f"Daily tasks have not run today ({today}). Will run daily tasks.")
    return True


def mark_daily_done():
    """Write today's date to the daily marker file."""
    today = get_today_str()
    with open(DAILY_MARKER_FILE, "w") as f:
        f.write(today)
    log.info(f"Daily marker updated: {today}")


def run_news():
    """Run the news fetcher (always runs)."""
    log.info("=" * 60)
    log.info("TASK 1: Fetching RSS News")
    log.info("=" * 60)
    try:
        from fetch_news import main as news_main
        news_main()
        log.info("News fetch completed successfully.")
        return True
    except Exception as e:
        log.error(f"News fetch FAILED: {e}")
        log.error(traceback.format_exc())
        return False


def run_sensortower():
    """Run the Sensor Tower fetcher (daily only)."""
    log.info("=" * 60)
    log.info("TASK 2: Fetching Sensor Tower App Rankings")
    log.info("=" * 60)
    try:
        from fetch_sensortower import main as st_main
        st_main()
        log.info("Sensor Tower fetch completed successfully.")
        return True
    except Exception as e:
        log.error(f"Sensor Tower fetch FAILED: {e}")
        log.error(traceback.format_exc())
        return False


def run_producthunt():
    """Run the Product Hunt fetcher (daily only)."""
    log.info("=" * 60)
    log.info("TASK 3: Fetching Product Hunt Top Products")
    log.info("=" * 60)
    try:
        from fetch_producthunt_top import main as ph_main
        ph_main()
        log.info("Product Hunt fetch completed successfully.")
        return True
    except Exception as e:
        log.error(f"Product Hunt fetch FAILED: {e}")
        log.error(traceback.format_exc())
        return False


def main():
    overall_start = time.monotonic()

    log.info("=" * 60)
    log.info("COMBINED DATA FETCH PIPELINE")
    log.info(f"Started at: {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    results = {}

    # ─── Task 1: News (always runs) ────────────────────────────────────
    results["news"] = run_news()

    # ─── Tasks 2 & 3: Daily tasks (once per day) ──────────────────────
    run_daily = should_run_daily()

    if run_daily:
        results["sensortower"] = run_sensortower()
        results["producthunt"] = run_producthunt()

        # Mark daily tasks as done (even if some failed, to avoid re-running broken tasks)
        mark_daily_done()
    else:
        results["sensortower"] = "skipped"
        results["producthunt"] = "skipped"

    # ─── Summary ──────────────────────────────────────────────────────
    elapsed = time.monotonic() - overall_start
    log.info("")
    log.info("=" * 60)
    log.info("PIPELINE SUMMARY")
    log.info("=" * 60)
    for task, status in results.items():
        if status is True:
            status_str = "SUCCESS"
        elif status is False:
            status_str = "FAILED"
        else:
            status_str = "SKIPPED (already ran today)"
        log.info(f"  {task:20s}: {status_str}")
    log.info(f"  Total time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log.info("=" * 60)

    # Exit with error code if any task failed
    if any(v is False for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()

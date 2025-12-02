#!/usr/bin/env python3
"""
Daily Pipeline Worker - Runs all workers sequentially.

This is the main entry point for Render deployment that runs all workers
in sequence with a single cron schedule:
1. Pipeline Worker - Scrapes Google Places and scans domains for tech
2. Outreach Worker - Sends personalized outreach emails
3. Calendly Sync Worker - Syncs Calendly bookings for conversion tracking

Environment Variables Required:
    See individual workers for their specific requirements:
    - pipeline_worker.py
    - outreach_worker.py
    - calendly_worker.py
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone


# ---------- LOGGING SETUP ----------

def setup_logging():
    """Configure logging for Render deployment with detailed output."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    # Create formatter with timestamp, level, and message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Clear existing handlers
    root_logger.handlers.clear()

    # Add stdout handler (Render captures stdout)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)

    return logging.getLogger("daily_worker")


# Initialize logger
logger = setup_logging()


def run_pipeline_worker() -> bool:
    """Run the pipeline worker to scan domains for technology."""
    logger.info("=" * 60)
    logger.info("STEP 1: PIPELINE WORKER")
    logger.info("=" * 60)
    
    try:
        from pipeline_worker import main as pipeline_main
        pipeline_main()
        logger.info("Pipeline worker completed successfully")
        return True
    except Exception as e:
        logger.error(f"Pipeline worker failed: {e}")
        logger.exception("Full traceback:")
        return False


def run_outreach_worker() -> bool:
    """Run the outreach worker to send emails."""
    logger.info("=" * 60)
    logger.info("STEP 2: OUTREACH WORKER")
    logger.info("=" * 60)
    
    try:
        from outreach_worker import run_outreach
        run_outreach()
        logger.info("Outreach worker completed successfully")
        return True
    except Exception as e:
        logger.error(f"Outreach worker failed: {e}")
        logger.exception("Full traceback:")
        return False


def run_calendly_worker() -> bool:
    """Run the Calendly sync worker to track bookings."""
    logger.info("=" * 60)
    logger.info("STEP 3: CALENDLY SYNC WORKER")
    logger.info("=" * 60)
    
    # Check if Calendly is configured
    if not os.getenv("CALENDLY_API_TOKEN"):
        logger.info("CALENDLY_API_TOKEN not set, skipping Calendly sync")
        return True
    
    try:
        from calendly_worker import run_sync
        run_sync()
        logger.info("Calendly sync worker completed successfully")
        return True
    except ImportError as e:
        logger.warning(f"Calendly module not available, skipping sync: {e}")
        return True
    except Exception as e:
        logger.error(f"Calendly sync worker failed: {e}")
        logger.exception("Full traceback:")
        return False


def main():
    """Main entry point - runs all workers sequentially."""
    start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("DAILY WORKER STARTING")
    logger.info("=" * 60)
    logger.info(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)
    logger.info("This worker runs all tasks sequentially:")
    logger.info("  1. Pipeline Worker - Scan domains for technology")
    logger.info("  2. Outreach Worker - Send personalized emails")
    logger.info("  3. Calendly Sync - Track meeting bookings")
    logger.info("=" * 60)
    
    results = {
        "pipeline": False,
        "outreach": False,
        "calendly": False,
    }
    
    # Step 1: Pipeline Worker
    results["pipeline"] = run_pipeline_worker()
    
    # Step 2: Outreach Worker (runs even if pipeline failed)
    results["outreach"] = run_outreach_worker()
    
    # Step 3: Calendly Sync Worker (runs even if previous steps failed)
    results["calendly"] = run_calendly_worker()
    
    # Summary
    elapsed = time.time() - start_time
    
    logger.info("=" * 60)
    logger.info("DAILY WORKER COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    logger.info("Results:")
    logger.info(f"  Pipeline Worker: {'✓ Success' if results['pipeline'] else '✗ Failed'}")
    logger.info(f"  Outreach Worker: {'✓ Success' if results['outreach'] else '✗ Failed'}")
    logger.info(f"  Calendly Sync:   {'✓ Success' if results['calendly'] else '✗ Failed'}")
    logger.info("=" * 60)
    
    # Exit with error if any worker failed
    if not all(results.values()):
        logger.warning("One or more workers failed")
        sys.exit(1)
    
    logger.info("All workers completed successfully!")


if __name__ == "__main__":
    main()

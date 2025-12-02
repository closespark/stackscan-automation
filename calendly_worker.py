#!/usr/bin/env python3
"""
Calendly Worker for Render Deployment.

This worker syncs Calendly bookings with Supabase leads to track conversions:
1. Fetches recent scheduled events from Calendly API
2. Matches invitee emails to leads in Supabase
3. Updates matched leads with booking status
4. Saves booking records for analytics (persona, variant, tech tracking)

Environment Variables Required:
    CALENDLY_API_TOKEN: Your Calendly Personal Access Token
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_SERVICE_KEY: Your Supabase service role key

Optional Environment Variables:
    CALENDLY_SYNC_TABLE: Table for leads (default: tech_scans)
    CALENDLY_BOOKINGS_TABLE: Table for booking records (default: calendly_bookings)
    CALENDLY_LOOKBACK_DAYS: Days to look back for events (default: 7)
    LOG_LEVEL: Logging level (default: INFO)
"""

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from calendly_sync import sync_calendly_bookings, get_booking_analytics


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

    return logging.getLogger("calendly_worker")


# Initialize logger
logger = setup_logging()


# ---------- ENV VARIABLES ----------

CALENDLY_API_TOKEN = os.getenv("CALENDLY_API_TOKEN")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

CALENDLY_SYNC_TABLE = os.getenv("CALENDLY_SYNC_TABLE", "tech_scans")
CALENDLY_BOOKINGS_TABLE = os.getenv("CALENDLY_BOOKINGS_TABLE", "calendly_bookings")
CALENDLY_LOOKBACK_DAYS = int(os.getenv("CALENDLY_LOOKBACK_DAYS", "7"))


def log_config():
    """Log current configuration (without sensitive values)."""
    logger.info("=" * 60)
    logger.info("CALENDLY WORKER CONFIGURATION")
    logger.info("=" * 60)
    logger.info(f"  CALENDLY_API_TOKEN: {'[SET]' if CALENDLY_API_TOKEN else '[NOT SET]'}")
    logger.info(f"  SUPABASE_URL: {'[SET]' if SUPABASE_URL else '[NOT SET]'}")
    logger.info(f"  SUPABASE_SERVICE_KEY: {'[SET]' if SUPABASE_SERVICE_KEY else '[NOT SET]'}")
    logger.info(f"  CALENDLY_SYNC_TABLE: {CALENDLY_SYNC_TABLE}")
    logger.info(f"  CALENDLY_BOOKINGS_TABLE: {CALENDLY_BOOKINGS_TABLE}")
    logger.info(f"  CALENDLY_LOOKBACK_DAYS: {CALENDLY_LOOKBACK_DAYS}")
    logger.info("=" * 60)


def validate_config() -> bool:
    """Validate required configuration is present."""
    missing = []
    if not CALENDLY_API_TOKEN:
        missing.append("CALENDLY_API_TOKEN")
    if not SUPABASE_URL:
        missing.append("SUPABASE_URL")
    if not SUPABASE_SERVICE_KEY:
        missing.append("SUPABASE_SERVICE_KEY")

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return False

    return True


def run_sync() -> dict[str, Any]:
    """
    Run the Calendly sync process.

    Returns:
        Statistics dictionary with sync results
    """
    worker_start_time = time.time()

    logger.info("=" * 60)
    logger.info("CALENDLY WORKER STARTING")
    logger.info("=" * 60)
    logger.info(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # Log configuration
    log_config()

    # Validate configuration
    if not validate_config():
        logger.error("Configuration validation failed. Exiting.")
        return {"error": "Missing configuration"}

    try:
        # Run the sync
        stats = sync_calendly_bookings(
            calendly_token=CALENDLY_API_TOKEN,
            supabase_url=SUPABASE_URL,
            supabase_key=SUPABASE_SERVICE_KEY,
            leads_table=CALENDLY_SYNC_TABLE,
            bookings_table=CALENDLY_BOOKINGS_TABLE,
            lookback_days=CALENDLY_LOOKBACK_DAYS,
        )

        # Get analytics
        logger.info("Fetching booking analytics...")
        analytics = get_booking_analytics(
            supabase_url=SUPABASE_URL,
            supabase_key=SUPABASE_SERVICE_KEY,
            bookings_table=CALENDLY_BOOKINGS_TABLE,
        )

        # Log analytics summary
        logger.info("=" * 60)
        logger.info("BOOKING ANALYTICS")
        logger.info("=" * 60)
        logger.info(f"  Total bookings: {analytics.get('total_bookings', 0)}")
        logger.info(f"  Matched to leads: {analytics.get('matched_bookings', 0)}")

        if analytics.get("by_persona"):
            logger.info("  By Persona:")
            for persona, count in sorted(analytics["by_persona"].items(), key=lambda x: -x[1]):
                logger.info(f"    {persona}: {count}")

        if analytics.get("by_variant"):
            logger.info("  By Variant (top 10):")
            variants = sorted(analytics["by_variant"].items(), key=lambda x: -x[1])[:10]
            for variant, count in variants:
                logger.info(f"    {variant}: {count}")

        if analytics.get("by_tech"):
            logger.info("  By Technology (top 10):")
            techs = sorted(analytics["by_tech"].items(), key=lambda x: -x[1])[:10]
            for tech, count in techs:
                logger.info(f"    {tech}: {count}")

        # Print summary
        worker_elapsed = time.time() - worker_start_time

        logger.info("=" * 60)
        logger.info("CALENDLY WORKER COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Total time: {worker_elapsed:.1f} seconds")
        logger.info(f"  Events processed: {stats.get('events_processed', 0)}")
        logger.info(f"  Bookings found: {stats.get('bookings_found', 0)}")
        logger.info(f"  Leads matched: {stats.get('leads_matched', 0)}")
        logger.info(f"  Leads updated: {stats.get('leads_updated', 0)}")
        logger.info("=" * 60)
        logger.info("Calendly worker finished successfully!")

        return stats

    except Exception as e:
        worker_elapsed = time.time() - worker_start_time
        logger.error("=" * 60)
        logger.error("CALENDLY WORKER FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error(f"Time before failure: {worker_elapsed:.1f} seconds")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_sync()

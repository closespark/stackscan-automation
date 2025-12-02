#!/usr/bin/env python3
"""
Daily Pipeline Worker for Render Deployment.

This is the Render worker entrypoint that:
1. Picks today's category (deterministic rotation based on date)
2. Calls Compass/Apify to scrape Google Places (max 1000 places)
3. Extracts and normalizes domains
4. Dedupes against Supabase domains_seen table
5. Runs technology scanning with Wappalyzer-style detection
6. Stores results in Supabase tech_scans table

The Apify Google Maps scraper is run asynchronously with polling to avoid
HTTP connection timeouts on long-running scrape jobs. The run is started
immediately and then polled for completion status.

Environment Variables Required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_SERVICE_KEY: Your Supabase service role key
    APIFY_TOKEN: Your Apify API token

Optional Environment Variables:
    SUPABASE_TABLE: Table for scan results (default: tech_scans)
    SUPABASE_DOMAIN_TABLE: Table for domain tracking (default: domains_seen)
    APIFY_ACTOR: Apify actor ID (default: compass/crawler-google-places)
    APIFY_MAX_PLACES: Max places to crawl per search (default: 1000)
    APIFY_POLL_INTERVAL: Seconds between Apify run status polls (default: 30)
    APIFY_RUN_TIMEOUT: Maximum seconds to wait for Apify run (default: 3600)
    CATEGORIES_FILE: Path to categories JSON (default: config/categories-250.json)
    SCANNER_DISABLE_EMAIL_GENERATION: Set to 'true' to skip email generation
    CATEGORY_OVERRIDE: Override the daily category selection
    LOG_LEVEL: Logging level (default: INFO)
"""

import json
import logging
import os
import sys
import time
from datetime import date

from apify_client import ApifyClient
from supabase import create_client

from hubspot_scanner import scan_technologies


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
    
    return logging.getLogger("pipeline")


# Initialize logger
logger = setup_logging()


# ---------- ENV & CLIENTS ----------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "tech_scans")
SUPABASE_DOMAIN_TABLE = os.getenv("SUPABASE_DOMAIN_TABLE", "domains_seen")

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "compass/crawler-google-places")
APIFY_MAX_PLACES = int(os.getenv("APIFY_MAX_PLACES", "1000"))
APIFY_POLL_INTERVAL = int(os.getenv("APIFY_POLL_INTERVAL", "30"))  # seconds
APIFY_RUN_TIMEOUT = int(os.getenv("APIFY_RUN_TIMEOUT", "3600"))  # seconds (1 hour default)

CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "config/categories-250.json")
SCANNER_DISABLE_EMAIL_GENERATION = os.getenv("SCANNER_DISABLE_EMAIL_GENERATION", "false").lower() == "true"


def log_config():
    """Log current configuration (without sensitive values)."""
    logger.info("=" * 60)
    logger.info("PIPELINE CONFIGURATION")
    logger.info("=" * 60)
    logger.info(f"  SUPABASE_URL: {'[SET]' if SUPABASE_URL else '[NOT SET]'}")
    logger.info(f"  SUPABASE_SERVICE_KEY: {'[SET]' if SUPABASE_SERVICE_KEY else '[NOT SET]'}")
    logger.info(f"  SUPABASE_TABLE: {SUPABASE_TABLE}")
    logger.info(f"  SUPABASE_DOMAIN_TABLE: {SUPABASE_DOMAIN_TABLE}")
    logger.info(f"  APIFY_TOKEN: {'[SET]' if APIFY_TOKEN else '[NOT SET]'}")
    logger.info(f"  APIFY_ACTOR: {APIFY_ACTOR}")
    logger.info(f"  APIFY_MAX_PLACES: {APIFY_MAX_PLACES}")
    logger.info(f"  APIFY_POLL_INTERVAL: {APIFY_POLL_INTERVAL} seconds")
    logger.info(f"  APIFY_RUN_TIMEOUT: {APIFY_RUN_TIMEOUT} seconds")
    logger.info(f"  CATEGORIES_FILE: {CATEGORIES_FILE}")
    logger.info(f"  SCANNER_DISABLE_EMAIL_GENERATION: {SCANNER_DISABLE_EMAIL_GENERATION}")
    logger.info("=" * 60)


def get_supabase_client():
    """Create and return a Supabase client."""
    logger.info("Initializing Supabase client...")
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.error("Missing required environment variables: SUPABASE_URL and/or SUPABASE_SERVICE_KEY")
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required"
        )
    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    logger.info("Supabase client initialized successfully")
    return client


def get_apify_client():
    """Create and return an Apify client."""
    logger.info("Initializing Apify client...")
    if not APIFY_TOKEN:
        logger.error("Missing required environment variable: APIFY_TOKEN")
        raise ValueError("APIFY_TOKEN environment variable is required")
    client = ApifyClient(APIFY_TOKEN)
    logger.info("Apify client initialized successfully")
    return client


# ---------- CATEGORY SELECTION ----------


def load_categories() -> list[str]:
    """Load categories from the JSON config file."""
    logger.info(f"Loading categories from: {CATEGORIES_FILE}")
    try:
        with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
            categories = json.load(f)
            if not categories:
                logger.error("Categories file is empty")
                raise ValueError("Categories file is empty")
            logger.info(f"Loaded {len(categories)} categories successfully")
            logger.debug(f"First 5 categories: {categories[:5]}")
            return categories
    except FileNotFoundError:
        logger.error(f"Categories file not found: {CATEGORIES_FILE}")
        raise FileNotFoundError(
            f"Categories file not found: {CATEGORIES_FILE}. "
            "Please ensure config/categories-250.json exists."
        )
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in categories file: {e}")
        raise ValueError(f"Invalid JSON in categories file: {e}")


def pick_today_category(categories: list[str]) -> str:
    """
    Deterministic rotation: one category per day based on date.
    No external state required.

    Args:
        categories: List of category strings

    Returns:
        Today's category string
    """
    override = os.getenv("CATEGORY_OVERRIDE")
    if override:
        logger.info(f"Using CATEGORY_OVERRIDE environment variable: {override}")
        return override

    idx = date.today().toordinal() % len(categories)
    selected_category = categories[idx]
    logger.info(f"Selected category index {idx} of {len(categories)}: '{selected_category}'")
    return selected_category


# ---------- APIFY / GOOGLE PLACES SCRAPE ----------

# Terminal statuses for Apify runs
APIFY_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"}


def get_domains_from_category(apify_client: ApifyClient, category: str) -> list[str]:
    """
    Scrape Google Places for a category and extract domains.

    Uses async run with polling to avoid HTTP connection timeouts on long-running
    scrape jobs. The run is started asynchronously and then polled for completion.

    Args:
        apify_client: The Apify client instance
        category: The business category to search for

    Returns:
        List of normalized domain strings

    Raises:
        RuntimeError: If the Apify run fails, times out, or is aborted
    """
    logger.info("=" * 60)
    logger.info("STEP: APIFY GOOGLE PLACES SCRAPE")
    logger.info("=" * 60)
    logger.info(f"Starting Google Places scrape for category: '{category}'")

    payload = {
        "countryCode": "us",
        "includeWebResults": True,
        "language": "en",
        "maxImages": 0,
        "maxQuestions": 0,
        "scrapeContacts": False,
        "scrapeDirectories": False,
        "scrapeImageAuthors": False,
        "scrapePlaceDetailPage": False,
        "scrapeReviewsPersonalData": False,
        "scrapeTableReservationProvider": False,
        "searchStringsArray": [category],
        "skipClosedPlaces": True,
        "website": "withWebsite",
        "searchMatching": "all",
        "placeMinimumStars": "",
        "maximumLeadsEnrichmentRecords": 0,
        "maxReviews": 0,
        "reviewsSort": "newest",
        "reviewsFilterString": "",
        "reviewsOrigin": "all",
        "allPlacesNoSearchAction": "",
        "maxCrawledPlacesPerSearch": APIFY_MAX_PLACES,
    }

    logger.info(f"Apify actor: {APIFY_ACTOR}")
    logger.info(f"Max places per search: {APIFY_MAX_PLACES}")
    logger.info(f"Run timeout: {APIFY_RUN_TIMEOUT} seconds")
    logger.info(f"Poll interval: {APIFY_POLL_INTERVAL} seconds")

    # Start the actor run asynchronously (returns immediately)
    logger.info("Starting Apify actor run asynchronously...")
    start_time = time.time()
    run = apify_client.actor(APIFY_ACTOR).start(run_input=payload)
    run_id = run.get("id")
    dataset_id = run.get("defaultDatasetId")

    logger.info(f"Apify run started successfully")
    logger.info(f"  Run ID: {run_id}")
    logger.info(f"  Dataset ID: {dataset_id}")
    logger.info(f"  Status: {run.get('status')}")
    logger.info("Waiting for run to complete... (polling for status)")

    # Poll for run completion
    poll_count = 0
    while True:
        elapsed = time.time() - start_time

        # Check for timeout
        if elapsed > APIFY_RUN_TIMEOUT:
            logger.error(f"Apify run timed out after {APIFY_RUN_TIMEOUT} seconds")
            # Try to abort the run
            try:
                apify_client.run(run_id).abort()
                logger.info("Run aborted successfully")
            except Exception as e:
                logger.warning(f"Failed to abort run: {e}")
            raise RuntimeError(
                f"Apify run timed out after {APIFY_RUN_TIMEOUT} seconds"
            )

        # Wait for the next poll interval (capped at 60s to ensure timely status updates)
        wait_time = min(APIFY_POLL_INTERVAL, 60)
        run_info = apify_client.run(run_id).wait_for_finish(wait_secs=wait_time)
        poll_count += 1

        if run_info is None:
            logger.warning(f"Failed to get run info, retrying...")
            time.sleep(5)
            continue

        status = run_info.get("status", "UNKNOWN")
        status_message = run_info.get("statusMessage", "")

        # Log progress
        logger.info(
            f"  [{poll_count}] Status: {status} | Elapsed: {elapsed:.0f}s | {status_message}"
        )

        # Check if run is complete
        if status in APIFY_TERMINAL_STATUSES:
            if status == "SUCCEEDED":
                logger.info(f"Apify run completed successfully in {elapsed:.1f} seconds")
                break
            elif status == "FAILED":
                error_msg = run_info.get("statusMessage", "Unknown error")
                logger.error(f"Apify run failed: {error_msg}")
                raise RuntimeError(f"Apify run failed: {error_msg}")
            elif status == "TIMED-OUT":
                logger.error("Apify run timed out on the server side")
                raise RuntimeError("Apify run timed out on the server side")
            elif status == "ABORTED":
                logger.error("Apify run was aborted")
                raise RuntimeError("Apify run was aborted")

    elapsed = time.time() - start_time
    logger.info(f"Total run time: {elapsed:.1f} seconds")
    logger.info(f"Total poll requests: {poll_count}")

    # Fetch dataset items
    logger.info("Fetching dataset items...")
    dataset_items = list(
        apify_client.dataset(dataset_id).iterate_items()
    )

    logger.info(f"Retrieved {len(dataset_items)} places from Google Places")

    # Extract domains
    logger.info("Extracting and normalizing domains...")
    domains = []
    domains_without_website = 0
    
    for item in dataset_items:
        url = item.get("website")
        if not url:
            domains_without_website += 1
            continue
        # Normalize domain
        url = url.replace("https://", "").replace("http://", "")
        url = url.split("/")[0].strip().lower()
        # Remove www. prefix
        if url.startswith("www."):
            url = url[4:]
        if url:
            domains.append(url)

    logger.info(f"Places without website: {domains_without_website}")
    logger.info(f"Places with website: {len(domains)}")

    # Remove duplicates while preserving order
    seen = set()
    unique_domains = []
    for d in domains:
        if d not in seen:
            seen.add(d)
            unique_domains.append(d)

    duplicates_removed = len(domains) - len(unique_domains)
    logger.info(f"Duplicate domains removed: {duplicates_removed}")
    logger.info(f"Unique domains extracted: {len(unique_domains)}")
    
    if unique_domains:
        logger.debug(f"Sample domains (first 10): {unique_domains[:10]}")
    
    return unique_domains


# ---------- SUPABASE DEDUPING ----------


def filter_new_domains(supabase, domains: list[str], category: str) -> list[str]:
    """
    Filter out domains that have already been processed.

    Args:
        supabase: Supabase client instance
        domains: List of domain strings to check
        category: The current category being processed

    Returns:
        List of new (not previously seen) domain strings
    """
    logger.info("=" * 60)
    logger.info("STEP: DOMAIN DEDUPLICATION")
    logger.info("=" * 60)
    
    if not domains:
        logger.warning("No domains provided for deduplication")
        return []

    logger.info(f"Checking {len(domains)} domains against Supabase table: {SUPABASE_DOMAIN_TABLE}")
    
    # Single roundtrip: check which exist
    logger.info("Querying Supabase for existing domains...")
    start_time = time.time()
    res = (
        supabase.table(SUPABASE_DOMAIN_TABLE)
        .select("domain")
        .in_("domain", domains)
        .execute()
    )
    elapsed = time.time() - start_time
    logger.info(f"Supabase query completed in {elapsed:.2f} seconds")

    seen = {row["domain"] for row in (res.data or [])}
    new_domains = [d for d in domains if d not in seen]

    logger.info(f"Domains already in database: {len(seen)}")
    logger.info(f"New domains to process: {len(new_domains)}")
    
    if new_domains:
        logger.debug(f"Sample new domains (first 10): {new_domains[:10]}")

    # Upsert new domains into domains_seen
    if new_domains:
        logger.info(f"Inserting {len(new_domains)} new domains into {SUPABASE_DOMAIN_TABLE}...")
        rows = [{"domain": d, "category": category} for d in new_domains]
        start_time = time.time()
        supabase.table(SUPABASE_DOMAIN_TABLE).upsert(
            rows, on_conflict="domain"
        ).execute()
        elapsed = time.time() - start_time
        logger.info(f"Domain insertion completed in {elapsed:.2f} seconds")

    return new_domains


# ---------- TECHNOLOGY SCAN + SAVE ----------


def save_scan_result(supabase, result: dict, category: str) -> None:
    """
    Save a technology scan result to Supabase.

    Args:
        supabase: Supabase client instance
        result: Scan result dictionary from scan_technologies
        category: The business category
    """
    row = {
        "domain": result["domain"],
        "category": category,
        "technologies": result.get("technologies", []),
        "scored_technologies": result.get("scored_technologies", []),
        "top_technology": result.get("top_technology"),
        "emails": result.get("emails", []),
        "error": result.get("error"),
    }
    supabase.table(SUPABASE_TABLE).insert(row).execute()
    logger.debug(f"Saved scan result for {result['domain']} to {SUPABASE_TABLE}")


def run_technology_scans(supabase, domains: list[str], category: str) -> list[dict]:
    """
    Run technology scans on all domains and save results.

    Args:
        supabase: Supabase client instance
        domains: List of domain strings to scan
        category: The business category

    Returns:
        List of scan result dictionaries
    """
    logger.info("=" * 60)
    logger.info("STEP: TECHNOLOGY SCANNING")
    logger.info("=" * 60)
    logger.info(f"Starting technology scans for {len(domains)} domains")
    logger.info(f"Email generation: {'DISABLED' if SCANNER_DISABLE_EMAIL_GENERATION else 'ENABLED'}")
    
    results = []
    tech_detected_count = 0
    email_generated_count = 0
    error_count = 0
    scan_start_time = time.time()
    
    for idx, domain in enumerate(domains, start=1):
        domain_start_time = time.time()
        logger.info(f"[{idx}/{len(domains)}] Scanning: {domain}")
        
        try:
            # Run the technology scanner
            result = scan_technologies(
                domain,
                generate_email=not SCANNER_DISABLE_EMAIL_GENERATION,
            )

            # Convert TechScanResult to dict
            result_dict = result.to_dict()
            results.append(result_dict)
            save_scan_result(supabase, result_dict, category)
            
            domain_elapsed = time.time() - domain_start_time

            if result.technologies:
                tech_detected_count += 1
                tech_count = len(result.technologies)
                has_email = result.generated_email is not None
                if has_email:
                    email_generated_count += 1
                top_tech_name = result.top_technology.get("name", "N/A") if result.top_technology else "N/A"
                logger.info(f"  ✓ {tech_count} technologies detected (top: {top_tech_name}, email: {'Yes' if has_email else 'No'}) [{domain_elapsed:.1f}s]")
                if result.technologies[:5]:
                    logger.info(f"    Technologies: {', '.join(result.technologies[:5])}")
            else:
                if result.error:
                    logger.info(f"  ✗ Scan error: {result.error} [{domain_elapsed:.1f}s]")
                else:
                    logger.info(f"  ✗ No technologies detected [{domain_elapsed:.1f}s]")

        except Exception as e:
            error_count += 1
            domain_elapsed = time.time() - domain_start_time
            logger.error(f"  ✗ SCAN FAILED for {domain}: {e} [{domain_elapsed:.1f}s]")
            err_result = {
                "domain": domain,
                "technologies": [],
                "scored_technologies": [],
                "top_technology": None,
                "emails": [],
                "error": str(e),
            }
            save_scan_result(supabase, err_result, category)
            results.append(err_result)
        
        # Log progress every 10 domains
        if idx % 10 == 0:
            elapsed = time.time() - scan_start_time
            rate = idx / elapsed if elapsed > 0 else 0
            remaining = len(domains) - idx
            eta = remaining / rate if rate > 0 else 0
            logger.info(f"  >> Progress: {idx}/{len(domains)} domains scanned | Rate: {rate:.1f}/s | ETA: {eta:.0f}s")

    total_elapsed = time.time() - scan_start_time
    logger.info("-" * 60)
    logger.info(f"Technology scanning completed in {total_elapsed:.1f} seconds")
    logger.info(f"  Total domains scanned: {len(domains)}")
    logger.info(f"  Domains with technologies: {tech_detected_count}")
    logger.info(f"  Emails generated: {email_generated_count}")
    logger.info(f"  Scan errors: {error_count}")
    
    return results


# ---------- MAIN ENTRYPOINT ----------


def main():
    """Main pipeline entrypoint."""
    pipeline_start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("PIPELINE WORKER STARTING")
    logger.info("=" * 60)
    logger.info(f"Date: {date.today().isoformat()}")
    logger.info(f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    
    # Log configuration
    log_config()

    try:
        # Initialize clients
        logger.info("=" * 60)
        logger.info("STEP: INITIALIZING CLIENTS")
        logger.info("=" * 60)
        supabase = get_supabase_client()
        apify_client = get_apify_client()

        # Load and select category
        logger.info("=" * 60)
        logger.info("STEP: CATEGORY SELECTION")
        logger.info("=" * 60)
        categories = load_categories()
        category = pick_today_category(categories)
        category_idx = categories.index(category) if category in categories else -1
        logger.info(f"Today's category: '{category}' (index {category_idx} of {len(categories)})")

        # Scrape Google Places
        domains = get_domains_from_category(apify_client, category)

        if not domains:
            logger.warning("No domains found from Google Places scrape. Exiting pipeline.")
            return

        # Deduplicate against Supabase
        new_domains = filter_new_domains(supabase, domains, category)

        if not new_domains:
            logger.info("All domains have been previously processed. No new domains to scan.")
            logger.info("Pipeline completed successfully (no work needed)")
            return

        # Run technology scans
        results = run_technology_scans(supabase, new_domains, category)

        # Print summary
        pipeline_elapsed = time.time() - pipeline_start_time
        tech_count = sum(1 for r in results if r.get("technologies"))
        email_count = sum(1 for r in results if r.get("generated_email"))
        error_count = sum(1 for r in results if r.get("error"))

        logger.info("=" * 60)
        logger.info("PIPELINE RUN COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Category: {category}")
        logger.info(f"  Total time: {pipeline_elapsed:.1f} seconds")
        logger.info(f"  Domains from Google Places: {len(domains)}")
        logger.info(f"  New domains scanned: {len(new_domains)}")
        logger.info(f"  Domains with technologies: {tech_count} ({tech_count/len(new_domains)*100:.1f}% detection rate)")
        logger.info(f"  Emails generated: {email_count}")
        logger.info(f"  Scan errors: {error_count}")
        logger.info("=" * 60)
        logger.info("Pipeline worker finished successfully!")
        
    except Exception as e:
        pipeline_elapsed = time.time() - pipeline_start_time
        logger.error("=" * 60)
        logger.error("PIPELINE FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error(f"Time before failure: {pipeline_elapsed:.1f} seconds")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    main()

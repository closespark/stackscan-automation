#!/usr/bin/env python3
"""
Daily Pipeline Worker for Render Deployment.

This is the Render worker entrypoint that:
1. Picks today's category (deterministic rotation based on date)
2. Calls Compass/Apify to scrape Google Places (max 1000 places)
3. Extracts and normalizes domains
4. Dedupes against Supabase domains_seen table
5. Runs HubSpot scanning + email extraction
6. Stores results in Supabase hubspot_scans table

Environment Variables Required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_SERVICE_KEY: Your Supabase service role key
    APIFY_TOKEN: Your Apify API token

Optional Environment Variables:
    SUPABASE_TABLE: Table for scan results (default: hubspot_scans)
    SUPABASE_DOMAIN_TABLE: Table for domain tracking (default: domains_seen)
    APIFY_ACTOR: Apify actor ID (default: compass/crawler-google-places)
    CATEGORIES_FILE: Path to categories JSON (default: config/categories-250.json)
    SCANNER_MAX_EMAIL_PAGES: Max pages to crawl for emails (default: 10)
    SCANNER_DISABLE_EMAILS: Set to 'true' to skip email extraction
    CATEGORY_OVERRIDE: Override the daily category selection
"""

import json
import os
from datetime import date

from apify_client import ApifyClient
from supabase import create_client

from hubspot_scanner import scan_domain


# ---------- ENV & CLIENTS ----------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "hubspot_scans")
SUPABASE_DOMAIN_TABLE = os.getenv("SUPABASE_DOMAIN_TABLE", "domains_seen")

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR = os.getenv("APIFY_ACTOR", "compass/crawler-google-places")

CATEGORIES_FILE = os.getenv("CATEGORIES_FILE", "config/categories-250.json")
SCANNER_MAX_EMAIL_PAGES = int(os.getenv("SCANNER_MAX_EMAIL_PAGES", "10"))
SCANNER_DISABLE_EMAILS = os.getenv("SCANNER_DISABLE_EMAILS", "false").lower() == "true"


def get_supabase_client():
    """Create and return a Supabase client."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required"
        )
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)


def get_apify_client():
    """Create and return an Apify client."""
    if not APIFY_TOKEN:
        raise ValueError("APIFY_TOKEN environment variable is required")
    return ApifyClient(APIFY_TOKEN)


# ---------- CATEGORY SELECTION ----------


def load_categories() -> list[str]:
    """Load categories from the JSON config file."""
    with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


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
        print(f"[PIPELINE] Using category override: {override}")
        return override

    idx = date.today().toordinal() % len(categories)
    return categories[idx]


# ---------- APIFY / GOOGLE PLACES SCRAPE ----------


def get_domains_from_category(apify_client: ApifyClient, category: str) -> list[str]:
    """
    Scrape Google Places for a category and extract domains.

    Args:
        apify_client: The Apify client instance
        category: The business category to search for

    Returns:
        List of normalized domain strings
    """
    print(f"[PIPELINE] Running category scrape: {category}")

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
        "maxCrawledPlacesPerSearch": 1000,
    }

    run = apify_client.actor(APIFY_ACTOR).call(run_input=payload)
    dataset_items = list(
        apify_client.dataset(run["defaultDatasetId"]).iterate_items()
    )

    print(f"[PIPELINE] Category '{category}' returned {len(dataset_items)} places")

    domains = []
    for item in dataset_items:
        url = item.get("website")
        if not url:
            continue
        # Normalize domain
        url = url.replace("https://", "").replace("http://", "")
        url = url.split("/")[0].strip().lower()
        # Remove www. prefix
        if url.startswith("www."):
            url = url[4:]
        if url:
            domains.append(url)

    # Remove duplicates while preserving order
    seen = set()
    unique_domains = []
    for d in domains:
        if d not in seen:
            seen.add(d)
            unique_domains.append(d)

    print(f"[PIPELINE] Extracted {len(unique_domains)} unique domains with websites")
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
    if not domains:
        return []

    # Single roundtrip: check which exist
    res = (
        supabase.table(SUPABASE_DOMAIN_TABLE)
        .select("domain")
        .in_("domain", domains)
        .execute()
    )

    seen = {row["domain"] for row in (res.data or [])}
    new_domains = [d for d in domains if d not in seen]

    print(f"[PIPELINE] {len(new_domains)} new domains (out of {len(domains)})")

    # Upsert new domains into domains_seen
    if new_domains:
        rows = [{"domain": d, "category": category} for d in new_domains]
        supabase.table(SUPABASE_DOMAIN_TABLE).upsert(
            rows, on_conflict="domain"
        ).execute()

    return new_domains


# ---------- HUBSPOT SCAN + SAVE ----------


def save_scan_result(supabase, result: dict, category: str) -> None:
    """
    Save a scan result to Supabase.

    Args:
        supabase: Supabase client instance
        result: Scan result dictionary
        category: The business category
    """
    row = {
        "domain": result["domain"],
        "hubspot_detected": result["hubspot_detected"],
        "confidence_score": result.get("confidence_score", 0),
        "portal_ids": result.get("portal_ids", []),
        "hubspot_signals": result.get("hubspot_signals", []),
        "emails": result.get("emails", []),
        "category": category,
        "error": result.get("error"),
    }
    supabase.table(SUPABASE_TABLE).insert(row).execute()


def run_hubspot_scans(supabase, domains: list[str], category: str) -> list[dict]:
    """
    Run HubSpot scans on all domains and save results.

    Args:
        supabase: Supabase client instance
        domains: List of domain strings to scan
        category: The business category

    Returns:
        List of scan result dictionaries
    """
    results = []
    for idx, domain in enumerate(domains, start=1):
        print(f"[SCAN] ({idx}/{len(domains)}) {domain}")
        try:
            # Run the HubSpot scanner
            result = scan_domain(
                domain,
                crawl_emails=not SCANNER_DISABLE_EMAILS,
                max_pages=SCANNER_MAX_EMAIL_PAGES,
            )

            # Convert DetectionResult to dict
            result_dict = result.to_dict()
            results.append(result_dict)
            save_scan_result(supabase, result_dict, category)

            if result.hubspot_detected:
                print(f"  ✓ HubSpot detected (confidence: {result.confidence_score}%)")
                if result.emails:
                    print(f"  ✓ Emails: {', '.join(result.emails)}")

        except Exception as e:
            print(f"[ERROR] scan failed for {domain}: {e}")
            err_result = {
                "domain": domain,
                "hubspot_detected": False,
                "confidence_score": 0,
                "portal_ids": [],
                "hubspot_signals": [],
                "emails": [],
                "error": str(e),
            }
            save_scan_result(supabase, err_result, category)
            results.append(err_result)

    return results


# ---------- MAIN ENTRYPOINT ----------


def main():
    """Main pipeline entrypoint."""
    print("[PIPELINE] Starting daily run")
    print(f"[PIPELINE] Date: {date.today().isoformat()}")

    # Initialize clients
    supabase = get_supabase_client()
    apify_client = get_apify_client()

    # Load and select category
    categories = load_categories()
    category = pick_today_category(categories)
    category_idx = categories.index(category) if category in categories else -1
    print(f"[PIPELINE] Today's category: {category} (index {category_idx})")

    # Scrape Google Places
    domains = get_domains_from_category(apify_client, category)

    if not domains:
        print("[PIPELINE] No domains found. Exiting.")
        return

    # Deduplicate against Supabase
    new_domains = filter_new_domains(supabase, domains, category)

    if not new_domains:
        print("[PIPELINE] No new domains to scan. Exiting.")
        return

    # Run HubSpot scans
    results = run_hubspot_scans(supabase, new_domains, category)

    # Print summary
    hubspot_count = sum(1 for r in results if r.get("hubspot_detected"))
    email_count = sum(len(r.get("emails", [])) for r in results)

    print("\n" + "=" * 50)
    print("[PIPELINE] Daily run complete")
    print(f"  Category: {category}")
    print(f"  Domains scraped: {len(domains)}")
    print(f"  New domains scanned: {len(new_domains)}")
    print(f"  HubSpot detected: {hubspot_count}")
    print(f"  Emails extracted: {email_count}")
    print("=" * 50)


if __name__ == "__main__":
    main()

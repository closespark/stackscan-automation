#!/usr/bin/env python3
"""
Category-based Google Places scraper using Apify.

This script scrapes Google Places data for business categories using Apify's
Google Places crawler. It processes one category at a time and collects up to
1000 results per category (configurable via --max-places).

Usage:
    # Set your Apify token as an environment variable
    export APIFY_TOKEN="your_token_here"

    # Run the scraper
    python category_scraper.py

    # Or run with a specific category index (0-based)
    python category_scraper.py --category-index 5

    # Run a specific category by name
    python category_scraper.py --category "accountant"

    # Limit results per category
    python category_scraper.py --max-places 500
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

from apify_client import ApifyClient


# Configuration constants
DEFAULT_MAX_PLACES = 1000
DEFAULT_ACTOR_ID = "compass/crawler-google-places"
DEFAULT_POLL_INTERVAL = 30  # seconds
DEFAULT_RUN_TIMEOUT = 3600  # 1 hour

# Terminal statuses for Apify runs
APIFY_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"}


# Business categories to scrape
CATEGORIES = [
    "accountant",
    "tax preparer",
    "bookkeeper",
    "financial planner",
    "insurance agency",
    "mortgage broker",
    "real estate agency",
    "property management company",
    "attorney",
    "family lawyer",
    "personal injury lawyer",
    "employment lawyer",
    "immigration lawyer",
    "estate planning attorney",
    "bankruptcy lawyer",
    "business lawyer",
    "marketing agency",
    "advertising agency",
    "digital marketing agency",
    "seo agency",
    "web design agency",
    "graphic design studio",
    "video production company",
    "photography studio",
    "it services",
    "managed service provider",
    "computer repair",
    "cybersecurity service",
    "data recovery service",
    "phone repair shop",
    "printer repair service",
    "hr consultant",
    "recruiting agency",
    "staffing agency",
    "business consultant",
    "management consultant",
    "coaching service",
    "career coach",
    "life coach",
    "event planner",
    "wedding planner",
    "party planner",
    "printing services",
    "sign shop",
    "copy center",
    "coworking space",
    "virtual office provider",
    "shipping store",
    "mailbox rental",
    "notary public",
    "electrician",
    "plumber",
    "hvac contractor",
    "heating contractor",
    "cooling contractor",
    "roofer",
    "siding contractor",
    "window installer",
    "door installer",
    "insulation contractor",
    "general contractor",
    "home builder",
    "remodeling contractor",
    "kitchen remodeler",
    "bathroom remodeler",
    "basement remodeler",
    "cabinet maker",
    "fence contractor",
    "deck builder",
    "patio contractor",
    "concrete contractor",
    "masonry contractor",
    "drywall contractor",
    "tile contractor",
    "flooring installer",
    "carpet installer",
    "garage door repair",
    "locksmith",
    "security system installer",
    "pest control",
    "exterminator",
    "water damage restoration",
    "mold remediation",
    "fire damage restoration",
    "window cleaning",
    "pressure washing",
    "gutter cleaning",
    "roofing restoration",
    "pool cleaning service",
    "pool contractor",
    "landscaper",
    "lawn care service",
    "tree service",
    "arborist",
    "irrigation contractor",
    "junk removal",
    "dumpster rental",
    "cleaning service",
    "maid service",
    "commercial cleaning",
    "auto repair shop",
    "transmission shop",
    "brake shop",
    "tire shop",
    "oil change service",
    "car wash",
    "auto detailing",
    "window tinting",
    "auto glass repair",
    "auto body shop",
    "radiator repair",
    "muffler shop",
    "smog check station",
    "motorcycle repair",
    "atv repair",
    "boat repair",
    "rv repair",
    "towing service",
    "roadside assistance",
    "car stereo store",
    "dentist",
    "orthodontist",
    "pediatric dentist",
    "cosmetic dentist",
    "periodontist",
    "endodontist",
    "chiropractor",
    "physical therapist",
    "occupational therapist",
    "speech therapist",
    "podiatrist",
    "dermatologist",
    "pediatrician",
    "family doctor",
    "primary care clinic",
    "urgent care",
    "eye doctor",
    "optometrist",
    "ophthalmologist",
    "hearing aid store",
    "nutritionist",
    "acupuncturist",
    "massage therapist",
    "med spa",
    "day spa",
    "yoga studio",
    "pilates studio",
    "personal trainer",
    "counseling center",
    "mental health clinic",
    "veterinarian",
    "animal hospital",
    "dog groomer",
    "pet grooming",
    "dog trainer",
    "pet boarding",
    "doggy daycare",
    "pet sitter",
    "pet store",
    "aquarium store",
    "furniture store",
    "mattress store",
    "flooring store",
    "tile store",
    "kitchen and bath store",
    "appliance store",
    "lighting store",
    "hardware store",
    "paint store",
    "home decor store",
    "jewelry store",
    "pawn shop",
    "antique store",
    "thrift store",
    "consignment shop",
    "bridal shop",
    "gift shop",
    "hobby store",
    "toy store",
    "bookstore",
    "music store",
    "bicycle shop",
    "skate shop",
    "surf shop",
    "sporting goods store",
    "gun shop",
    "archery store",
    "vape shop",
    "cbd store",
    "cannabis dispensary",
    "hair salon",
    "barber shop",
    "nail salon",
    "eyelash studio",
    "waxing studio",
    "tanning salon",
    "tattoo shop",
    "piercing studio",
    "makeup artist",
    "beauty supply store",
    "restaurant",
    "italian restaurant",
    "mexican restaurant",
    "chinese restaurant",
    "indian restaurant",
    "thai restaurant",
    "japanese restaurant",
    "sushi restaurant",
    "bbq restaurant",
    "pizza restaurant",
    "cafe",
    "coffee shop",
    "bakery",
    "dessert shop",
    "ice cream shop",
    "juice bar",
    "smoothie shop",
    "sandwich shop",
    "food truck",
    "catering company",
    "preschool",
    "daycare",
    "montessori school",
    "tutoring center",
    "learning center",
    "test prep center",
    "music school",
    "dance school",
    "martial arts school",
    "art school",
    "assisted living",
    "nursing home",
    "home health care",
    "senior transportation",
    "disability services",
    "moving company",
    "freight company",
    "courier service",
    "warehouse",
    "packaging supplier",
    "travel agency",
    "tour operator",
    "auto tag agency",
    "bail bonds",
    "funeral home",
    "cemetery services",
    "roofing inspector",
    "home inspector",
    "real estate appraiser",
    "environmental consultant",
]


def get_apify_token() -> str:
    """
    Get the Apify token from environment variable.

    Returns:
        The Apify API token

    Raises:
        SystemExit: If token is not set
    """
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print(
            "Error: APIFY_TOKEN environment variable is not set.",
            file=sys.stderr,
        )
        print(
            "Please set it with: export APIFY_TOKEN='your_token_here'",
            file=sys.stderr,
        )
        sys.exit(1)
    return token


def run_category(
    client: ApifyClient,
    category: str,
    max_places: int = DEFAULT_MAX_PLACES,
    poll_interval: int = DEFAULT_POLL_INTERVAL,
    run_timeout: int = DEFAULT_RUN_TIMEOUT,
) -> list[dict]:
    """
    Run the Apify crawler for a single category.

    Uses async run with polling to avoid HTTP connection timeouts on long-running
    scrape jobs. The run is started asynchronously and then polled for completion.

    Args:
        client: The Apify client instance
        category: The business category to search for
        max_places: Maximum number of places to crawl per category
        poll_interval: Seconds between status polls (default: 30)
        run_timeout: Maximum seconds to wait for run completion (default: 3600)

    Returns:
        List of places data from the crawler

    Raises:
        RuntimeError: If the Apify run fails, times out, or is aborted
    """
    print(f"Running category: {category}")

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
        "maxCrawledPlacesPerSearch": max_places,
    }

    # Start the actor run asynchronously (returns immediately)
    print("  Starting Apify actor run asynchronously...")
    start_time = time.time()
    run = client.actor(DEFAULT_ACTOR_ID).start(run_input=payload)
    run_id = run.get("id")
    dataset_id = run.get("defaultDatasetId")

    print(f"  Run ID: {run_id}")
    print(f"  Dataset ID: {dataset_id}")
    print("  Waiting for run to complete... (polling for status)")

    # Poll for run completion
    poll_count = 0
    while True:
        elapsed = time.time() - start_time

        # Check for timeout
        if elapsed > run_timeout:
            print(f"  ERROR: Run timed out after {run_timeout} seconds")
            # Try to abort the run
            try:
                client.run(run_id).abort()
                print("  Run aborted successfully")
            except Exception as e:
                print(f"  Warning: Failed to abort run: {e}")
            raise RuntimeError(f"Apify run timed out after {run_timeout} seconds")

        # Wait for the next poll interval (capped at 60s to ensure timely status updates)
        wait_time = min(poll_interval, 60)
        run_info = client.run(run_id).wait_for_finish(wait_secs=wait_time)
        poll_count += 1

        if run_info is None:
            print("  Warning: Failed to get run info, retrying...")
            time.sleep(5)
            continue

        status = run_info.get("status", "UNKNOWN")
        status_message = run_info.get("statusMessage", "")

        # Log progress
        print(f"  [{poll_count}] Status: {status} | Elapsed: {elapsed:.0f}s | {status_message}")

        # Check if run is complete
        if status in APIFY_TERMINAL_STATUSES:
            if status == "SUCCEEDED":
                print(f"  Run completed successfully in {elapsed:.1f} seconds")
                break
            elif status == "FAILED":
                error_msg = run_info.get("statusMessage", "Unknown error")
                print(f"  ERROR: Run failed: {error_msg}")
                raise RuntimeError(f"Apify run failed: {error_msg}")
            elif status == "TIMED-OUT":
                print("  ERROR: Run timed out on the server side")
                raise RuntimeError("Apify run timed out on the server side")
            elif status == "ABORTED":
                print("  ERROR: Run was aborted")
                raise RuntimeError("Apify run was aborted")

    # Fetch dataset items
    print("  Fetching dataset items...")
    dataset_items = list(client.dataset(dataset_id).iterate_items())
    count = len(dataset_items)

    print(f"Category '{category}' returned {count} places.")

    return dataset_items


def run_all_categories(
    client: ApifyClient,
    max_places: int = DEFAULT_MAX_PLACES,
) -> dict[str, list[dict]]:
    """
    Run the crawler for all categories.

    Args:
        client: The Apify client instance
        max_places: Maximum number of places to crawl per category

    Returns:
        Dictionary mapping category names to their results
    """
    all_results = {}

    for category in CATEGORIES:
        items = run_category(client, category, max_places)

        if len(items) >= max_places:
            print(f"Reached {max_places} results for {category}. Switching to next category.")
        else:
            print(f"{category} returned {len(items)}, but moving on anyway.")

        all_results[category] = items

    return all_results


def run_single_category(
    client: ApifyClient,
    category: str,
    max_places: int = DEFAULT_MAX_PLACES,
) -> dict[str, list[dict]]:
    """
    Run the crawler for a single category.

    Args:
        client: The Apify client instance
        category: The category to run
        max_places: Maximum number of places to crawl

    Returns:
        Dictionary with the category and its results
    """
    items = run_category(client, category, max_places)

    if len(items) >= max_places:
        print(f"Reached {max_places} results for {category}.")
    else:
        print(f"{category} returned {len(items)} results.")

    return {category: items}


def get_category_for_today() -> tuple[int, str]:
    """
    Get the category index and name for today based on day of year.

    Returns:
        Tuple of (index, category_name)
    """
    day_of_year = datetime.now().timetuple().tm_yday
    index = day_of_year % len(CATEGORIES)
    return index, CATEGORIES[index]


def save_results(results: dict[str, list[dict]], output_file: str) -> None:
    """
    Save results to a JSON file.

    Args:
        results: The results dictionary to save
        output_file: Path to the output file
    """
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to: {output_file}")


def main() -> int:
    """Main entry point for the category scraper."""
    parser = argparse.ArgumentParser(
        description="Scrape Google Places for business categories using Apify",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run today's category (based on day of year)
    python category_scraper.py

    # Run a specific category by index
    python category_scraper.py --category-index 5

    # Run a specific category by name
    python category_scraper.py --category "accountant"

    # Run all categories
    python category_scraper.py --all

    # List all categories
    python category_scraper.py --list

    # Save results to a specific file
    python category_scraper.py --output my_results.json

    # Limit results per category
    python category_scraper.py --max-places 500
        """,
    )

    parser.add_argument(
        "--category-index",
        type=int,
        help="Index of the category to run (0-based)",
    )

    parser.add_argument(
        "--category",
        type=str,
        help="Name of the category to run",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all categories sequentially",
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available categories and exit",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="places_data.json",
        help="Output file for JSON results (default: places_data.json)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be run without actually running",
    )

    parser.add_argument(
        "--max-places",
        type=int,
        default=DEFAULT_MAX_PLACES,
        help=f"Maximum places to crawl per category (default: {DEFAULT_MAX_PLACES})",
    )

    args = parser.parse_args()

    # Handle --list flag
    if args.list:
        print(f"Available categories ({len(CATEGORIES)} total):")
        for i, category in enumerate(CATEGORIES):
            print(f"  {i}: {category}")
        return 0

    # Determine which category/categories to run
    if args.all:
        categories_to_run = "all"
    elif args.category_index is not None:
        if args.category_index < 0 or args.category_index >= len(CATEGORIES):
            print(
                f"Error: Invalid category index {args.category_index}. "
                f"Valid range: 0-{len(CATEGORIES) - 1}",
                file=sys.stderr,
            )
            return 1
        categories_to_run = CATEGORIES[args.category_index]
    elif args.category:
        # Find the category (case-insensitive)
        category_lower = args.category.lower()
        matching = [c for c in CATEGORIES if c.lower() == category_lower]
        if not matching:
            print(f"Error: Category '{args.category}' not found.", file=sys.stderr)
            print("Use --list to see available categories.", file=sys.stderr)
            return 1
        categories_to_run = matching[0]
    else:
        # Default: run today's category
        index, category = get_category_for_today()
        print(f"Today's category (index {index}): {category}")
        categories_to_run = category

    # Handle dry-run
    if args.dry_run:
        if categories_to_run == "all":
            print("Dry run: Would run all categories:")
            for i, category in enumerate(CATEGORIES):
                print(f"  {i}: {category}")
        else:
            print(f"Dry run: Would run category: {categories_to_run}")
        print(f"Output would be saved to: {args.output}")
        return 0

    # Get Apify token and create client
    token = get_apify_token()
    client = ApifyClient(token)

    # Run the scraper
    if categories_to_run == "all":
        results = run_all_categories(client, args.max_places)
    else:
        results = run_single_category(client, categories_to_run, args.max_places)

    # Save results
    save_results(results, args.output)

    print("Scraping completed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

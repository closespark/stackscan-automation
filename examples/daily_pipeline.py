#!/usr/bin/env python3
"""
Daily Lead Generation Pipeline.

This script implements an end-to-end automated daily pipeline that:
1. Scrapes Google Places for a single business category using Apify
2. Normalizes and deduplicates domains against historical data
3. Scans each new domain for HubSpot presence
4. Extracts non-generic contact emails from HubSpot-powered sites
5. Stores structured lead records in JSON format
6. Optionally sends outbound emails through Zapmail pre-warmed inboxes

Usage:
    # Set required environment variables
    export APIFY_TOKEN="your_apify_token"

    # Run the daily pipeline
    python daily_pipeline.py

    # Run with a specific category
    python daily_pipeline.py --category "accountant"

    # Run with email sending enabled
    export ZAPMAIL_CONFIG="path/to/zapmail_config.json"
    python daily_pipeline.py --send-emails

    # Dry run to see what would be processed
    python daily_pipeline.py --dry-run
"""

import argparse
import json
import os
import random
import smtplib
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from apify_client import ApifyClient

# Import the HubSpot scanner
sys.path.insert(0, str(Path(__file__).parent.parent))
from stackscanner import scan_domain


# Configuration constants
DEFAULT_MAX_PLACES = 1000
DEFAULT_ACTOR_ID = "compass/crawler-google-places"
DEFAULT_LEADS_DIR = "leads"
DEFAULT_HISTORY_FILE = "processed_domains.json"
DEFAULT_EMAILS_PER_INBOX = 40  # 35-50 emails per inbox per day
DEFAULT_INBOX_COUNT = 10

# Business categories to scrape (250 total)
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


@dataclass
class LeadRecord:
    """Structured lead record for the pipeline."""

    domain: str
    business_name: str
    category: str
    hubspot_detected: bool
    confidence_score: float
    hubspot_signals: list[dict[str, Any]] = field(default_factory=list)
    portal_ids: list[str] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)
    phone: str | None = None
    address: str | None = None
    website_url: str | None = None
    scan_timestamp: str = ""
    scan_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)


@dataclass
class ZapmailInbox:
    """Zapmail pre-warmed inbox configuration."""

    email: str
    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    daily_limit: int = DEFAULT_EMAILS_PER_INBOX
    sent_today: int = 0
    last_send_time: float = 0


def get_apify_token() -> str:
    """Get the Apify token from environment variable."""
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print(
            "Error: APIFY_TOKEN environment variable is not set.",
            file=sys.stderr,
        )
        sys.exit(1)
    return token


def normalize_domain(url: str) -> str:
    """
    Normalize a URL to a clean domain.

    Args:
        url: The URL to normalize

    Returns:
        Clean domain string (e.g., "example.com")
    """
    if not url:
        return ""

    # Add scheme if missing
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    # Remove www. prefix
    if domain.startswith("www."):
        domain = domain[4:]

    return domain


def load_processed_domains(history_file: str) -> set[str]:
    """
    Load the set of previously processed domains.

    Args:
        history_file: Path to the history file

    Returns:
        Set of processed domain strings
    """
    if not os.path.exists(history_file):
        return set()

    try:
        with open(history_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data.get("domains", []))
    except (json.JSONDecodeError, IOError):
        return set()


def save_processed_domains(domains: set[str], history_file: str) -> None:
    """
    Save the set of processed domains.

    Args:
        domains: Set of domain strings
        history_file: Path to the history file
    """
    data = {
        "last_updated": datetime.now().isoformat(),
        "count": len(domains),
        "domains": sorted(domains),
    }
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def scrape_category(
    client: ApifyClient,
    category: str,
    max_places: int = DEFAULT_MAX_PLACES,
) -> list[dict]:
    """
    Scrape Google Places for a single category.

    Args:
        client: The Apify client instance
        category: The business category to search for
        max_places: Maximum number of places to crawl

    Returns:
        List of places data from the crawler
    """
    print(f"Scraping Google Places for category: {category}")

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

    run = client.actor(DEFAULT_ACTOR_ID).call(run_input=payload)
    dataset_items = list(client.dataset(run["defaultDatasetId"]).iterate_items())

    print(f"Retrieved {len(dataset_items)} places for category '{category}'")
    return dataset_items


def deduplicate_places(
    places: list[dict],
    processed_domains: set[str],
) -> tuple[list[dict], list[str]]:
    """
    Deduplicate places against historical data.

    Args:
        places: List of places from the scraper
        processed_domains: Set of previously processed domains

    Returns:
        Tuple of (new_places, new_domains)
    """
    new_places = []
    new_domains = []
    no_website_count = 0
    duplicate_count = 0

    for place in places:
        website = place.get("website", "")
        if not website:
            no_website_count += 1
            continue

        domain = normalize_domain(website)
        if not domain:
            no_website_count += 1
            continue

        if domain in processed_domains:
            duplicate_count += 1
        else:
            new_places.append(place)
            new_domains.append(domain)
            processed_domains.add(domain)

    print(
        f"Found {len(new_places)} new domains "
        f"(skipped {no_website_count} without website, {duplicate_count} duplicates)"
    )
    return new_places, new_domains


def scan_domains_for_hubspot(
    places: list[dict],
    category: str,
    timeout: int = 10,
    max_pages: int = 10,
) -> list[LeadRecord]:
    """
    Scan domains for HubSpot presence and extract emails.

    Args:
        places: List of places with websites
        category: The business category
        timeout: Request timeout in seconds
        max_pages: Maximum pages to crawl for emails

    Returns:
        List of LeadRecord objects
    """
    leads = []
    total = len(places)

    for i, place in enumerate(places, 1):
        website = place.get("website", "")
        domain = normalize_domain(website)
        business_name = place.get("title", place.get("name", "Unknown"))

        print(f"  [{i}/{total}] Scanning {domain}...")

        try:
            result = scan_domain(
                domain,
                timeout=timeout,
                crawl_emails=True,
                max_pages=max_pages,
            )

            lead = LeadRecord(
                domain=domain,
                business_name=business_name,
                category=category,
                hubspot_detected=result.hubspot_detected,
                confidence_score=result.confidence_score,
                hubspot_signals=result.signals,
                portal_ids=result.portal_ids,
                emails=result.emails,
                phone=place.get("phone"),
                address=place.get("address"),
                website_url=website,
                scan_timestamp=datetime.now().isoformat(),
                scan_error=result.error,
            )
            leads.append(lead)

            if result.hubspot_detected:
                print(f"    ✓ HubSpot detected (confidence: {result.confidence_score}%)")
                if result.emails:
                    print(f"    ✓ Emails found: {', '.join(result.emails)}")

        except Exception as e:
            lead = LeadRecord(
                domain=domain,
                business_name=business_name,
                category=category,
                hubspot_detected=False,
                confidence_score=0.0,
                phone=place.get("phone"),
                address=place.get("address"),
                website_url=website,
                scan_timestamp=datetime.now().isoformat(),
                scan_error=str(e),
            )
            leads.append(lead)
            print(f"    ✗ Error: {e}")

    return leads


def save_leads(leads: list[LeadRecord], output_dir: str, category: str) -> str:
    """
    Save leads to a JSON file.

    Args:
        leads: List of LeadRecord objects
        output_dir: Directory to save leads
        category: The business category

    Returns:
        Path to the saved file
    """
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_category = category.replace(" ", "_").replace("/", "_")
    filename = f"leads_{safe_category}_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)

    data = {
        "category": category,
        "generated_at": datetime.now().isoformat(),
        "total_leads": len(leads),
        "hubspot_detected_count": sum(1 for l in leads if l.hubspot_detected),
        "emails_found_count": sum(len(l.emails) for l in leads),
        "leads": [lead.to_dict() for lead in leads],
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Leads saved to: {filepath}")
    return filepath


def load_zapmail_config(config_path: str) -> list[ZapmailInbox]:
    """
    Load Zapmail inbox configuration.

    Args:
        config_path: Path to the Zapmail config JSON file

    Returns:
        List of ZapmailInbox objects

    Expected config format:
    {
        "inboxes": [
            {
                "email": "sender1@warmeddomain.com",
                "smtp_host": "smtp.gmail.com",
                "smtp_port": 587,
                "smtp_user": "sender1@warmeddomain.com",
                "smtp_password": "app_password_here",
                "daily_limit": 40
            },
            ...
        ]
    }
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    inboxes = []
    for inbox_config in config.get("inboxes", []):
        inbox = ZapmailInbox(
            email=inbox_config["email"],
            smtp_host=inbox_config.get("smtp_host", "smtp.gmail.com"),
            smtp_port=inbox_config.get("smtp_port", 587),
            smtp_user=inbox_config["smtp_user"],
            smtp_password=inbox_config["smtp_password"],
            daily_limit=inbox_config.get("daily_limit", DEFAULT_EMAILS_PER_INBOX),
        )
        inboxes.append(inbox)

    return inboxes


def get_available_inbox(inboxes: list[ZapmailInbox]) -> ZapmailInbox | None:
    """
    Get an available inbox that hasn't hit its daily limit.

    Args:
        inboxes: List of ZapmailInbox objects

    Returns:
        An available inbox or None if all are exhausted
    """
    available = [inbox for inbox in inboxes if inbox.sent_today < inbox.daily_limit]
    if not available:
        return None
    # Return a random inbox from available ones for better distribution
    return random.choice(available)


def send_email(
    inbox: ZapmailInbox,
    to_email: str,
    subject: str,
    body_html: str,
    body_text: str | None = None,
) -> bool:
    """
    Send an email through a Zapmail inbox.

    Args:
        inbox: The ZapmailInbox to send from
        to_email: Recipient email address
        subject: Email subject
        body_html: HTML body content
        body_text: Optional plain text body

    Returns:
        True if email was sent successfully
    """
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = inbox.email
    msg["To"] = to_email

    # Add plain text version
    if body_text:
        msg.attach(MIMEText(body_text, "plain"))

    # Add HTML version
    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP(inbox.smtp_host, inbox.smtp_port) as server:
            server.starttls()
            server.login(inbox.smtp_user, inbox.smtp_password)
            server.send_message(msg)

        inbox.sent_today += 1
        inbox.last_send_time = time.time()
        return True

    except smtplib.SMTPException as e:
        print(f"Failed to send email from {inbox.email}: {e}", file=sys.stderr)
        return False


def send_outreach_emails(
    leads: list[LeadRecord],
    inboxes: list[ZapmailInbox],
    email_template: dict[str, str] | None = None,
    delay_between_emails: float = 60.0,
) -> dict[str, int]:
    """
    Send outreach emails to leads with HubSpot and emails.

    Args:
        leads: List of LeadRecord objects
        inboxes: List of ZapmailInbox objects
        email_template: Optional custom email template
        delay_between_emails: Seconds to wait between emails

    Returns:
        Statistics dictionary with sent/failed counts
    """
    if not email_template:
        email_template = {
            "subject": "Quick question about your website",
            "body_html": """
            <p>Hi,</p>
            <p>I noticed you're using HubSpot on your website and wanted to reach out.</p>
            <p>We help businesses like yours get more out of their HubSpot investment.</p>
            <p>Would you be open to a quick 15-minute call this week?</p>
            <p>Best regards</p>
            """,
            "body_text": """
Hi,

I noticed you're using HubSpot on your website and wanted to reach out.

We help businesses like yours get more out of their HubSpot investment.

Would you be open to a quick 15-minute call this week?

Best regards
            """,
        }

    stats = {"sent": 0, "failed": 0, "skipped": 0}

    # Filter leads with HubSpot and emails
    qualified_leads = [
        lead for lead in leads if lead.hubspot_detected and lead.emails
    ]

    print(f"Sending emails to {len(qualified_leads)} qualified leads...")

    for lead in qualified_leads:
        inbox = get_available_inbox(inboxes)
        if not inbox:
            print("All inboxes have reached their daily limit.")
            stats["skipped"] += len(lead.emails)
            continue

        for email in lead.emails:
            # Personalize subject if possible
            subject = email_template["subject"]
            if lead.business_name and lead.business_name != "Unknown":
                subject = f"Quick question for {lead.business_name}"

            success = send_email(
                inbox=inbox,
                to_email=email,
                subject=subject,
                body_html=email_template["body_html"],
                body_text=email_template.get("body_text"),
            )

            if success:
                stats["sent"] += 1
                print(f"  ✓ Sent to {email} from {inbox.email}")
            else:
                stats["failed"] += 1
                print(f"  ✗ Failed to send to {email}")

            # Rate limiting
            time.sleep(delay_between_emails)

    return stats


def get_category_for_today() -> tuple[int, str]:
    """
    Get the category index and name for today based on day of year.

    Returns:
        Tuple of (index, category_name)
    """
    day_of_year = datetime.now().timetuple().tm_yday
    index = day_of_year % len(CATEGORIES)
    return index, CATEGORIES[index]


def main() -> int:
    """Main entry point for the daily pipeline."""
    parser = argparse.ArgumentParser(
        description="Daily lead generation pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run the daily pipeline with today's category
    python daily_pipeline.py

    # Run with a specific category
    python daily_pipeline.py --category "accountant"

    # Run with email sending enabled
    python daily_pipeline.py --send-emails --zapmail-config config.json

    # Dry run to see what would be processed
    python daily_pipeline.py --dry-run

    # Skip HubSpot scanning (only scrape)
    python daily_pipeline.py --scrape-only
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
        "--max-places",
        type=int,
        default=DEFAULT_MAX_PLACES,
        help=f"Maximum places to scrape per category (default: {DEFAULT_MAX_PLACES})",
    )

    parser.add_argument(
        "--leads-dir",
        type=str,
        default=DEFAULT_LEADS_DIR,
        help=f"Directory to save leads (default: {DEFAULT_LEADS_DIR})",
    )

    parser.add_argument(
        "--history-file",
        type=str,
        default=DEFAULT_HISTORY_FILE,
        help=f"File to track processed domains (default: {DEFAULT_HISTORY_FILE})",
    )

    parser.add_argument(
        "--send-emails",
        action="store_true",
        help="Enable email sending through Zapmail",
    )

    parser.add_argument(
        "--zapmail-config",
        type=str,
        help="Path to Zapmail inbox configuration file",
    )

    parser.add_argument(
        "--scrape-only",
        action="store_true",
        help="Only scrape places, skip HubSpot scanning",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be processed without actually running",
    )

    parser.add_argument(
        "--list-categories",
        action="store_true",
        help="List all available categories and exit",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Request timeout in seconds (default: 10)",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Max pages to crawl for emails per domain (default: 10)",
    )

    args = parser.parse_args()

    # Handle --list-categories flag
    if args.list_categories:
        print(f"Available categories ({len(CATEGORIES)} total):")
        for i, category in enumerate(CATEGORIES):
            print(f"  {i}: {category}")
        return 0

    # Determine which category to run
    if args.category_index is not None:
        if args.category_index < 0 or args.category_index >= len(CATEGORIES):
            print(
                f"Error: Invalid category index {args.category_index}. "
                f"Valid range: 0-{len(CATEGORIES) - 1}",
                file=sys.stderr,
            )
            return 1
        category_index = args.category_index
        category = CATEGORIES[category_index]
    elif args.category:
        category_lower = args.category.lower()
        matching = [c for c in CATEGORIES if c.lower() == category_lower]
        if not matching:
            print(f"Error: Category '{args.category}' not found.", file=sys.stderr)
            print("Use --list-categories to see available categories.", file=sys.stderr)
            return 1
        category = matching[0]
        category_index = CATEGORIES.index(category)
    else:
        category_index, category = get_category_for_today()

    print("=" * 60)
    print("DAILY LEAD GENERATION PIPELINE")
    print("=" * 60)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Category: {category} (index {category_index})")
    print(f"Max places: {args.max_places}")
    print("=" * 60)

    # Handle dry-run
    if args.dry_run:
        print("\n[DRY RUN MODE]")
        print(f"Would scrape: {category}")
        print(f"Would save leads to: {args.leads_dir}/")
        print(f"Would update history file: {args.history_file}")
        if args.send_emails:
            print("Would send emails through Zapmail")
        return 0

    # Load processed domains history
    print("\nStep 1: Loading domain history...")
    processed_domains = load_processed_domains(args.history_file)
    print(f"  Found {len(processed_domains)} previously processed domains")

    # Get Apify token and scrape places
    print("\nStep 2: Scraping Google Places...")
    token = get_apify_token()
    client = ApifyClient(token)
    places = scrape_category(client, category, args.max_places)

    if not places:
        print("No places found for this category.")
        return 0

    # Deduplicate against historical data
    print("\nStep 3: Deduplicating domains...")
    new_places, new_domains = deduplicate_places(places, processed_domains)

    if not new_places:
        print("No new domains to process.")
        save_processed_domains(processed_domains, args.history_file)
        return 0

    # Scan domains for HubSpot
    leads = []
    if not args.scrape_only:
        print("\nStep 4: Scanning domains for HubSpot...")
        leads = scan_domains_for_hubspot(
            new_places,
            category,
            timeout=args.timeout,
            max_pages=args.max_pages,
        )
    else:
        print("\nStep 4: Skipping HubSpot scan (--scrape-only)")
        for place in new_places:
            website = place.get("website", "")
            domain = normalize_domain(website)
            lead = LeadRecord(
                domain=domain,
                business_name=place.get("title", place.get("name", "Unknown")),
                category=category,
                hubspot_detected=False,
                confidence_score=0.0,
                phone=place.get("phone"),
                address=place.get("address"),
                website_url=website,
                scan_timestamp=datetime.now().isoformat(),
            )
            leads.append(lead)

    # Save leads
    print("\nStep 5: Saving leads...")
    save_leads(leads, args.leads_dir, category)

    # Update domain history
    save_processed_domains(processed_domains, args.history_file)
    print(f"Updated history file with {len(new_domains)} new domains")

    # Send emails if enabled
    if args.send_emails:
        print("\nStep 6: Sending outreach emails...")
        if not args.zapmail_config:
            zapmail_config = os.environ.get("ZAPMAIL_CONFIG")
            if not zapmail_config:
                print(
                    "Error: Zapmail config not specified. "
                    "Use --zapmail-config or set ZAPMAIL_CONFIG env var.",
                    file=sys.stderr,
                )
                return 1
            args.zapmail_config = zapmail_config

        try:
            inboxes = load_zapmail_config(args.zapmail_config)
            print(f"Loaded {len(inboxes)} Zapmail inboxes")

            stats = send_outreach_emails(leads, inboxes)
            print(f"\nEmail stats: {stats['sent']} sent, {stats['failed']} failed, {stats['skipped']} skipped")

        except FileNotFoundError:
            print(f"Error: Zapmail config file not found: {args.zapmail_config}", file=sys.stderr)
            return 1
        except json.JSONDecodeError as e:
            print(f"Error: Invalid Zapmail config file: {e}", file=sys.stderr)
            return 1

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Category processed: {category}")
    print(f"Total places scraped: {len(places)}")
    print(f"New domains processed: {len(new_places)}")
    print(f"HubSpot detected: {sum(1 for l in leads if l.hubspot_detected)}")
    print(f"Emails extracted: {sum(len(l.emails) for l in leads)}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())

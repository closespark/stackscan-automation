"""Email extraction and filtering functionality."""

import json
import logging
import os
import re
from typing import Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)

# Path to disposable/honeypot email domains blocklist
BLOCKLIST_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "config",
    "disposable_email_blocklist.json"
)

# Cache for loaded blocklist
_disposable_domains_cache: frozenset | None = None


def load_disposable_domains() -> frozenset:
    """
    Load the disposable/honeypot email domains blocklist.
    
    Returns:
        Frozenset of blocked domain names
    """
    global _disposable_domains_cache
    
    if _disposable_domains_cache is not None:
        return _disposable_domains_cache
    
    try:
        with open(BLOCKLIST_PATH, "r", encoding="utf-8") as f:
            domains = json.load(f)
            _disposable_domains_cache = frozenset(domains)
            logger.debug(f"Loaded {len(_disposable_domains_cache)} disposable email domains")
            return _disposable_domains_cache
    except FileNotFoundError:
        logger.warning(f"Disposable email blocklist not found: {BLOCKLIST_PATH}")
        _disposable_domains_cache = frozenset()
        return _disposable_domains_cache
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in blocklist file: {e}")
        _disposable_domains_cache = frozenset()
        return _disposable_domains_cache


def is_disposable_email(email: str) -> bool:
    """
    Check if an email is from a disposable/honeypot domain.
    
    Args:
        email: Email address to check
        
    Returns:
        True if the email is from a disposable domain, False otherwise
    """
    if "@" not in email:
        return False
    parts = email.lower().split("@")
    if len(parts) != 2 or not parts[1]:
        return False
    email_domain = parts[1]
    disposable_domains = load_disposable_domains()
    return email_domain in disposable_domains


# Generic email prefixes to exclude
GENERIC_EMAIL_PREFIXES = frozenset([
    "info",
    "support",
    "admin",
    "hello",
    "sales",
    "contact",
    "help",
    "noreply",
    "no-reply",
    "webmaster",
    "postmaster",
    "mail",
    "email",
    "enquiries",
    "enquiry",
    "office",
    "team",
    "general",
])

# Invalid/fake email domains to exclude
INVALID_DOMAINS = frozenset([
    "example.com",
    "example.org",
    "test.com",
    "domain.com",
])

# Email regex pattern
EMAIL_PATTERN = re.compile(
    r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
)

# Common pages that might contain contact information
CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/about",
    "/about-us",
    "/team",
    "/our-team",
    "/leadership",
    "/people",
    "/staff",
]


def is_generic_email(email: str) -> bool:
    """
    Check if an email is generic (should be excluded).

    Args:
        email: Email address to check

    Returns:
        True if the email is generic, False otherwise
    """
    local_part = email.split("@")[0].lower()
    return local_part in GENERIC_EMAIL_PREFIXES


def is_valid_email(email: str, domain: str) -> bool:
    """
    Check if an email is valid and relevant.

    Args:
        email: Email address to check
        domain: The domain being scanned

    Returns:
        True if the email is valid and relevant
    """
    email_lower = email.lower()

    # Skip generic emails
    if is_generic_email(email_lower):
        return False

    # Skip disposable/honeypot email domains
    if is_disposable_email(email_lower):
        logger.debug(f"Skipping disposable email: {email_lower}")
        return False

    # Skip obviously fake or example emails
    email_parts = email_lower.split("@")
    email_domain = email_parts[1] if len(email_parts) == 2 else ""
    if email_domain in INVALID_DOMAINS:
        return False

    # Skip image and file extensions mistakenly captured
    if email_domain.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
        return False

    return True


def extract_emails_from_html(html_content: str, domain: str) -> Set[str]:
    """
    Extract non-generic emails from HTML content.

    Args:
        html_content: The HTML content to parse
        domain: The domain being scanned

    Returns:
        Set of non-generic email addresses
    """
    emails = set()

    # Find all email patterns
    found_emails = EMAIL_PATTERN.findall(html_content)

    # Also look for mailto: links
    soup = BeautifulSoup(html_content, "lxml")
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "").split("?")[0].strip()
            if email:
                found_emails.append(email)

    # Process all found emails with detailed logging
    for email in found_emails:
        email_lower = email.lower()

        # Check for generic email
        if is_generic_email(email_lower):
            logger.debug(f"Email extractor: filtered generic email: {email_lower}")
            continue

        # Check for disposable/honeypot email
        if is_disposable_email(email_lower):
            logger.debug(f"Email extractor: filtered disposable/honeypot: {email_lower}")
            continue

        # Check for invalid domains
        email_parts = email_lower.split("@")
        if len(email_parts) == 2:
            email_domain = email_parts[1]
            if email_domain in INVALID_DOMAINS:
                logger.debug(f"Email extractor: filtered invalid domain: {email_lower}")
                continue
            if email_domain.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
                logger.debug(f"Email extractor: filtered file extension: {email_lower}")
                continue

        # Email passed all filters
        if email_lower not in emails:
            logger.info(f"Email extractor: accepted email: {email_lower}")
            emails.add(email_lower)

    return emails


def get_internal_links(html_content: str, base_url: str, domain: str) -> Set[str]:
    """
    Extract internal links from HTML content.

    Args:
        html_content: The HTML content to parse
        base_url: The base URL for resolving relative links
        domain: The domain being scanned

    Returns:
        Set of internal URLs
    """
    soup = BeautifulSoup(html_content, "lxml")
    links = set()
    parsed_base = urlparse(base_url)

    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()

        # Skip empty, javascript, and anchor links
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        # Resolve relative URLs
        full_url = urljoin(base_url, href)
        parsed_url = urlparse(full_url)

        # Only keep internal links
        if parsed_url.netloc == parsed_base.netloc or parsed_url.netloc == domain:
            # Normalize the URL
            normalized = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
            links.add(normalized)

    return links


def crawl_for_emails(
    base_url: str,
    domain: str,
    initial_html: str,
    timeout: int = 10,
    user_agent: str = None,
    max_pages: int = 10,
) -> Set[str]:
    """
    Crawl a site to find non-generic email addresses.

    Args:
        base_url: The base URL to start crawling from
        domain: The domain being scanned
        initial_html: The HTML content of the homepage
        timeout: Request timeout in seconds
        user_agent: User agent string
        max_pages: Maximum number of pages to crawl

    Returns:
        Set of non-generic email addresses found
    """
    from .scanner import DEFAULT_USER_AGENT

    if user_agent is None:
        user_agent = DEFAULT_USER_AGENT

    headers = {"User-Agent": user_agent}
    all_emails = set()
    visited_urls = set()
    urls_to_visit = set()

    # Start with emails from the homepage
    all_emails.update(extract_emails_from_html(initial_html, domain))
    visited_urls.add(base_url)

    # Get initial links
    internal_links = get_internal_links(initial_html, base_url, domain)

    # Prioritize contact-related pages
    parsed_base = urlparse(base_url)
    for path in CONTACT_PATHS:
        contact_url = f"{parsed_base.scheme}://{parsed_base.netloc}{path}"
        urls_to_visit.add(contact_url)

    # Add other internal links
    urls_to_visit.update(internal_links)

    # Crawl additional pages
    pages_crawled = 1
    while urls_to_visit and pages_crawled < max_pages:
        url = urls_to_visit.pop()

        if url in visited_urls:
            continue

        visited_urls.add(url)

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=timeout,
                allow_redirects=True,
            )

            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                if "text/html" in content_type:
                    all_emails.update(extract_emails_from_html(response.text, domain))
                    pages_crawled += 1

        except requests.RequestException:
            # Skip pages that fail to load
            continue

    logger.info(f"Email extractor: FINAL accepted emails for {domain}: {sorted(all_emails)}")
    return all_emails

"""Domain scanning functionality for HubSpot detection."""

import re
from collections.abc import Callable
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

from .detector import DetectionResult, HubSpotDetector


DEFAULT_TIMEOUT = 10
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def normalize_domain(domain: str) -> str:
    """
    Normalize a domain to a proper URL.

    Args:
        domain: Raw domain input (e.g., "example.com" or "https://example.com")

    Returns:
        Normalized URL with scheme
    """
    domain = domain.strip()

    # If already has scheme, parse and normalize
    if domain.startswith(("http://", "https://")):
        parsed = urlparse(domain)
        return f"{parsed.scheme}://{parsed.netloc}"

    # Add https by default
    return f"https://{domain}"


def fetch_page(
    url: str,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
) -> tuple[str | None, dict[str, str], str | None]:
    """
    Fetch a web page and return its content.

    Args:
        url: The URL to fetch
        timeout: Request timeout in seconds
        user_agent: User agent string to use

    Returns:
        Tuple of (html_content, headers, error_message)
    """
    headers = {"User-Agent": user_agent}

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=timeout,
            allow_redirects=True,
        )
        response.raise_for_status()
        return response.text, dict(response.headers), None

    except requests.exceptions.SSLError:
        # Try HTTP if HTTPS fails
        if url.startswith("https://"):
            http_url = url.replace("https://", "http://", 1)
            try:
                response = requests.get(
                    http_url,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=True,
                )
                response.raise_for_status()
                return response.text, dict(response.headers), None
            except requests.RequestException as e:
                return None, {}, f"SSL and HTTP failed: {str(e)}"
        return None, {}, "SSL certificate error"

    except requests.exceptions.ConnectionError:
        return None, {}, "Connection failed"

    except requests.exceptions.Timeout:
        return None, {}, "Request timed out"

    except requests.exceptions.RequestException as e:
        return None, {}, str(e)


def extract_script_sources(html_content: str) -> list[str]:
    """
    Extract all script source URLs from HTML.

    Args:
        html_content: The HTML content to parse

    Returns:
        List of script source URLs
    """
    soup = BeautifulSoup(html_content, "lxml")
    sources = []

    for script in soup.find_all("script", src=True):
        src = script.get("src", "")
        if src:
            sources.append(src)

    return sources


def extract_inline_scripts(html_content: str) -> list[str]:
    """
    Extract inline script content from HTML.

    Args:
        html_content: The HTML content to parse

    Returns:
        List of inline script contents
    """
    soup = BeautifulSoup(html_content, "lxml")
    scripts = []

    for script in soup.find_all("script"):
        if not script.get("src") and script.string:
            scripts.append(script.string)

    return scripts


def extract_link_sources(html_content: str) -> list[str]:
    """
    Extract link href URLs from HTML.

    Args:
        html_content: The HTML content to parse

    Returns:
        List of link href URLs
    """
    soup = BeautifulSoup(html_content, "lxml")
    sources = []

    for link in soup.find_all("link", href=True):
        href = link.get("href", "")
        if href:
            sources.append(href)

    return sources


def scan_domain(
    domain: str,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    crawl_emails: bool = True,
    max_pages: int = 10,
) -> DetectionResult:
    """
    Scan a single domain for HubSpot presence.

    Args:
        domain: The domain to scan
        timeout: Request timeout in seconds
        user_agent: User agent string to use
        crawl_emails: Whether to crawl for emails when HubSpot is detected
        max_pages: Maximum number of pages to crawl for emails

    Returns:
        DetectionResult with scan results
    """
    from .email_extractor import crawl_for_emails

    detector = HubSpotDetector()
    url = normalize_domain(domain)

    # Extract clean domain for result
    parsed = urlparse(url)
    clean_domain = parsed.netloc or domain

    # Fetch the page
    html_content, response_headers, error = fetch_page(url, timeout, user_agent)

    if error:
        return DetectionResult(
            domain=clean_domain,
            hubspot_detected=False,
            confidence_score=0.0,
            error=error,
        )

    # Detect HubSpot in HTML content
    result = detector.detect(clean_domain, html_content)

    # Add header-based signals
    header_signals = detector.detect_from_response_headers(response_headers)
    if header_signals:
        result.signals.extend(header_signals)
        # Recalculate confidence with header signals
        total_weight = sum(s["weight"] for s in result.signals)
        result.confidence_score = min(100, round(total_weight, 1))
        result.hubspot_detected = result.confidence_score >= 20

    # Extract and scan script sources for additional context
    script_sources = extract_script_sources(html_content)
    link_sources = extract_link_sources(html_content)

    # Check for HubSpot patterns in external resources
    all_sources = script_sources + link_sources
    source_patterns = [
        ("hubspot.net", 15, "External HubSpot CDN resource"),
        ("hubspot.com", 15, "External HubSpot resource"),
        ("hs-scripts.com", 25, "HubSpot script loader"),
        ("hsforms.net", 20, "HubSpot forms"),
        ("hscta.net", 15, "HubSpot CTA"),
    ]

    for source in all_sources:
        for pattern, weight, description in source_patterns:
            if pattern in source.lower():
                # Check if this signal was already detected
                signal_exists = any(
                    s["name"] == f"external-{pattern}" for s in result.signals
                )
                if not signal_exists:
                    result.signals.append(
                        {
                            "name": f"external-{pattern}",
                            "description": description,
                            "weight": weight,
                        }
                    )

    # Scan inline scripts for HubSpot patterns
    inline_scripts = extract_inline_scripts(html_content)
    inline_content = " ".join(inline_scripts)

    # Look for HubSpot initialization patterns in inline scripts
    inline_patterns = [
        (r"_hsq\s*=\s*", 20, "HubSpot tracking queue"),
        (r"hbspt\.", 20, "HubSpot JavaScript object"),
        (r"HubSpotConversations", 15, "HubSpot conversations"),
        (r"hs-cta-trigger", 15, "HubSpot CTA trigger"),
    ]

    for pattern, weight, description in inline_patterns:
        if re.search(pattern, inline_content, re.IGNORECASE):
            signal_name = f"inline-{description.lower().replace(' ', '-')}"
            signal_exists = any(s["name"] == signal_name for s in result.signals)
            if not signal_exists:
                result.signals.append(
                    {
                        "name": signal_name,
                        "description": description,
                        "weight": weight,
                    }
                )

    # Recalculate final confidence score
    total_weight = sum(s["weight"] for s in result.signals)
    result.confidence_score = min(100, round(total_weight, 1))
    result.hubspot_detected = result.confidence_score >= 20

    # If HubSpot is detected and email crawling is enabled, crawl for emails
    if result.hubspot_detected and crawl_emails:
        emails = crawl_for_emails(
            base_url=url,
            domain=clean_domain,
            initial_html=html_content,
            timeout=timeout,
            user_agent=user_agent,
            max_pages=max_pages,
        )
        result.emails = sorted(emails)

    return result


def scan_domains(
    domains: list[str],
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    progress_callback: Callable[[int, int, str], None] | None = None,
    crawl_emails: bool = True,
    max_pages: int = 10,
) -> list[dict[str, Any]]:
    """
    Scan multiple domains for HubSpot presence.

    Args:
        domains: List of domains to scan
        timeout: Request timeout in seconds
        user_agent: User agent string to use
        progress_callback: Optional callback function(current, total, domain)
        crawl_emails: Whether to crawl for emails when HubSpot is detected
        max_pages: Maximum number of pages to crawl for emails

    Returns:
        List of detection results as dictionaries
    """
    results = []
    total = len(domains)

    for i, domain in enumerate(domains, 1):
        if progress_callback:
            progress_callback(i, total, domain)

        result = scan_domain(
            domain,
            timeout,
            user_agent,
            crawl_emails=crawl_emails,
            max_pages=max_pages,
        )
        results.append(result.to_dict())

    return results

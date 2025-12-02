"""
Unified technology scanner with email generation.

Combines:
- Wappalyzer-style technology detection
- Technology scoring by value
- Personalized outreach email generation
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any

import requests

from .tech_detector import TechDetector, TechDetectionResult
from .tech_scorer import score_technologies, get_highest_value_tech, to_dict
from .email_generator import (
    generate_outreach_email, 
    GeneratedEmail,
    generate_outreach_email_with_persona,
    DEFAULT_PERSONA,
)
from .email_extractor import crawl_for_emails


logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT = 10
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


@dataclass
class TechScanResult:
    """Complete result of technology scan with scoring and email.
    
    Attributes:
        domain: The scanned domain name.
        technologies: List of detected technology names.
        scored_technologies: List of technologies with scores and categories.
        top_technology: The highest-scored technology detected.
        emails: List of extracted email addresses from the page (non-generic).
        generated_email: AI-generated outreach email content (if requested).
        error: Error message if scan failed, None otherwise.
    """

    domain: str
    technologies: list[str] = field(default_factory=list)
    scored_technologies: list[dict[str, Any]] = field(default_factory=list)
    top_technology: dict[str, Any] | None = None
    emails: list[str] = field(default_factory=list)
    generated_email: dict[str, Any] | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "domain": self.domain,
            "technologies": self.technologies,
            "scored_technologies": self.scored_technologies,
            "top_technology": self.top_technology,
            "emails": self.emails,
            "generated_email": self.generated_email,
            "error": self.error,
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)


def fetch_page(
    url: str,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
) -> tuple[str | None, dict[str, str], str | None]:
    """
    Fetch a web page and return its content and headers.

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


def normalize_url(domain: str) -> str:
    """Normalize a domain to a proper URL."""
    from urllib.parse import urlparse

    domain = domain.strip()

    # If already has scheme, parse and normalize
    if domain.startswith(("http://", "https://")):
        parsed = urlparse(domain)
        return f"{parsed.scheme}://{parsed.netloc}"

    # Add https by default
    return f"https://{domain}"


def scan_technologies(
    domain: str,
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    generate_email: bool = True,
    consultant_profile: dict[str, str] | None = None,
) -> TechScanResult:
    """
    Scan a domain for technologies, score them, and optionally generate email.

    Args:
        domain: The domain to scan
        timeout: Request timeout in seconds
        user_agent: User agent string to use
        generate_email: Whether to generate outreach email
        consultant_profile: Optional profile override for email generation

    Returns:
        TechScanResult with all detection and generation results
    """
    url = normalize_url(domain)

    # Fetch the page
    html_content, headers, error = fetch_page(url, timeout, user_agent)

    if error:
        return TechScanResult(
            domain=domain,
            error=error,
        )

    # Detect technologies
    detector = TechDetector()
    detection_result = detector.detect(domain, html_content, headers)

    if not detection_result.technologies:
        return TechScanResult(
            domain=domain,
            technologies=[],
            scored_technologies=[],
        )

    # Extract emails (crawls multiple pages and filters out generic/disposable)
    logger.info(f"Email extractor: crawling {domain} for emails...")
    extracted_emails = sorted(crawl_for_emails(
        base_url=url,
        domain=domain,
        initial_html=html_content,
        timeout=timeout,
        user_agent=user_agent,
    ))
    logger.info(f"Email extractor: {len(extracted_emails)} emails found for {domain}")

    # Score technologies
    scored = score_technologies(detection_result.technologies)
    scored_dicts = [to_dict(s) for s in scored]

    # Get top technology
    top_tech = scored[0] if scored else None
    top_tech_dict = to_dict(top_tech) if top_tech else None

    # Generate email if requested using persona-based generation
    email_dict = None
    if generate_email and detection_result.technologies:
        # Use default persona (Scott) for pipeline-generated emails
        # The actual persona will be selected when sending via outreach_worker
        default_email = DEFAULT_PERSONA.get("email", "scott@closespark.co")
        email_data = generate_outreach_email_with_persona(
            domain=domain,
            technologies=detection_result.technologies,
            from_email=default_email,
        )
        if email_data:
            email_dict = email_data

    return TechScanResult(
        domain=domain,
        technologies=detection_result.technologies,
        scored_technologies=scored_dicts,
        top_technology=top_tech_dict,
        emails=extracted_emails,
        generated_email=email_dict,
    )


def scan_technologies_batch(
    domains: list[str],
    timeout: int = DEFAULT_TIMEOUT,
    user_agent: str = DEFAULT_USER_AGENT,
    generate_email: bool = True,
    consultant_profile: dict[str, str] | None = None,
    progress_callback=None,
) -> list[dict[str, Any]]:
    """
    Scan multiple domains for technologies.

    Args:
        domains: List of domains to scan
        timeout: Request timeout in seconds
        user_agent: User agent string to use
        generate_email: Whether to generate outreach emails
        consultant_profile: Optional profile override for email generation
        progress_callback: Optional callback(current, total, domain)

    Returns:
        List of scan results as dictionaries
    """
    results = []
    total = len(domains)

    for i, domain in enumerate(domains, 1):
        if progress_callback:
            progress_callback(i, total, domain)

        result = scan_technologies(
            domain,
            timeout=timeout,
            user_agent=user_agent,
            generate_email=generate_email,
            consultant_profile=consultant_profile,
        )
        results.append(result.to_dict())

    return results

"""HubSpot detection logic for analyzing web page content."""

import re
from typing import Any
from dataclasses import dataclass, field


@dataclass
class DetectionResult:
    """Result of HubSpot detection on a domain."""

    domain: str
    hubspot_detected: bool
    confidence_score: float
    signals: list[dict[str, Any]] = field(default_factory=list)
    portal_ids: list[str] = field(default_factory=list)
    emails: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result = {
            "domain": self.domain,
            "hubspot_detected": self.hubspot_detected,
            "confidence_score": self.confidence_score,
            "hubspot_signals": self.signals,
            "portal_ids": self.portal_ids,
            "emails": self.emails,
            "error": self.error,
        }
        return result


class HubSpotDetector:
    """Detector for HubSpot signatures in web content."""

    # HubSpot script and tracking patterns
    SCRIPT_PATTERNS = [
        {
            "name": "hs-script-loader",
            "pattern": r"js\.hs-scripts\.com/(\d+)\.js",
            "weight": 30,
            "description": "HubSpot tracking script loader",
        },
        {
            "name": "hs-analytics",
            "pattern": r"js\.hs-analytics\.net",
            "weight": 25,
            "description": "HubSpot analytics script",
        },
        {
            "name": "hubspot-tracking",
            "pattern": r"track\.hubspot\.com",
            "weight": 25,
            "description": "HubSpot tracking endpoint",
        },
        {
            "name": "hs-banner",
            "pattern": r"js\.hs-banner\.com",
            "weight": 20,
            "description": "HubSpot cookie banner",
        },
        {
            "name": "hs-feedback",
            "pattern": r"js\.usemessages\.com/conversations-embed\.js",
            "weight": 20,
            "description": "HubSpot conversations/chat widget",
        },
        {
            "name": "hubspot-forms",
            "pattern": r"js\.hsforms\.net",
            "weight": 25,
            "description": "HubSpot forms library",
        },
        {
            "name": "hubspot-forms-v2",
            "pattern": r"js\.hscollectedforms\.net",
            "weight": 20,
            "description": "HubSpot collected forms",
        },
        {
            "name": "hubspot-cta",
            "pattern": r"js\.hscta\.net",
            "weight": 20,
            "description": "HubSpot CTA (Call-to-Action)",
        },
    ]

    # HubSpot COS (Content Optimization System) patterns
    COS_PATTERNS = [
        {
            "name": "cos-assets",
            "pattern": r"cdn2?\.hubspot\.net",
            "weight": 20,
            "description": "HubSpot CDN assets",
        },
        {
            "name": "hubfs-assets",
            "pattern": r"/hubfs/",
            "weight": 15,
            "description": "HubSpot File System assets",
        },
        {
            "name": "hs-cos-wrapper",
            "pattern": r"hs-cos-wrapper",
            "weight": 25,
            "description": "HubSpot COS wrapper class",
        },
        {
            "name": "hs-menu",
            "pattern": r"hs-menu-wrapper",
            "weight": 20,
            "description": "HubSpot menu wrapper",
        },
        {
            "name": "hs-blog",
            "pattern": r"hs-blog-post",
            "weight": 15,
            "description": "HubSpot blog post class",
        },
    ]

    # HubSpot meta and header patterns
    META_PATTERNS = [
        {
            "name": "generator-hubspot",
            "pattern": r'<meta[^>]*name=["\']generator["\'][^>]*content=["\'][^"\']*hubspot[^"\']*["\']',
            "weight": 30,
            "description": "HubSpot generator meta tag",
        },
        {
            "name": "hs-portal-id",
            "pattern": r"data-hsjs-portal\s*=\s*[\"']?(\d+)",
            "weight": 25,
            "description": "HubSpot portal ID in data attribute",
        },
        {
            "name": "hbspt-portal",
            "pattern": r'hbspt\.forms\.create\([^)]*portalId["\s:]+["\']?(\d+)',
            "weight": 25,
            "description": "HubSpot form with portal ID",
        },
        {
            "name": "hs-cta-wrapper",
            "pattern": r"hs-cta-wrapper",
            "weight": 20,
            "description": "HubSpot CTA wrapper element",
        },
        {
            "name": "async-hubspot-comment",
            "pattern": r"<!--\s*Start of Async HubSpot",
            "weight": 30,
            "description": "HubSpot async script HTML comment",
        },
        {
            "name": "hs-cookie-banner",
            "pattern": r'id\s*=\s*["\']?hs-eu-cookie-confirmation["\']?',
            "weight": 20,
            "description": "HubSpot cookie policy banner element",
        },
    ]

    # HubSpot API endpoint patterns
    API_PATTERNS = [
        {
            "name": "api-hubspot",
            "pattern": r"api\.hubspot\.com",
            "weight": 25,
            "description": "HubSpot API endpoint",
        },
        {
            "name": "forms-api",
            "pattern": r"forms\.hubspot\.com",
            "weight": 25,
            "description": "HubSpot forms API",
        },
        {
            "name": "hubspot-embed",
            "pattern": r"app\.hubspot\.com/embed",
            "weight": 20,
            "description": "HubSpot embedded content",
        },
    ]

    # Portal ID extraction patterns
    PORTAL_ID_PATTERNS = [
        r"js\.hs-scripts\.com/(\d+)\.js",
        r"data-hsjs-portal\s*=\s*[\"']?(\d+)",
        r'portalId["\s:]+["\']?(\d+)',
        r"js\.hs-analytics\.net/analytics/\d+/(\d+)\.js",
        r"/hubfs/(\d+)/",
        r"hsFormContainerPortal\s*=\s*(\d+)",
    ]

    def __init__(self):
        """Initialize the HubSpot detector."""
        self.all_patterns = (
            self.SCRIPT_PATTERNS
            + self.COS_PATTERNS
            + self.META_PATTERNS
            + self.API_PATTERNS
        )

    def detect(self, domain: str, html_content: str) -> DetectionResult:
        """
        Detect HubSpot presence in HTML content.

        Args:
            domain: The domain being scanned
            html_content: The HTML content of the page

        Returns:
            DetectionResult with detection details
        """
        signals = []
        total_weight = 0
        portal_ids = set()

        # Check all patterns
        for pattern_info in self.all_patterns:
            match = re.search(pattern_info["pattern"], html_content, re.IGNORECASE)
            if match:
                signal = {
                    "name": pattern_info["name"],
                    "description": pattern_info["description"],
                    "weight": pattern_info["weight"],
                }

                # Try to extract portal ID if pattern has a capture group
                if match.lastindex and match.lastindex >= 1:
                    potential_portal_id = match.group(1)
                    if potential_portal_id.isdigit():
                        signal["portal_id"] = potential_portal_id
                        portal_ids.add(potential_portal_id)

                signals.append(signal)
                total_weight += pattern_info["weight"]

        # Extract additional portal IDs
        for pattern in self.PORTAL_ID_PATTERNS:
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            portal_ids.update(matches)

        # Calculate confidence score (0-100)
        # 100+ weight = 100% confidence, scale linearly below that
        confidence_score = min(100, total_weight)

        # Determine if HubSpot is detected (threshold of 20)
        hubspot_detected = confidence_score >= 20

        return DetectionResult(
            domain=domain,
            hubspot_detected=hubspot_detected,
            confidence_score=round(confidence_score, 1),
            signals=signals,
            portal_ids=sorted(portal_ids),
        )

    def detect_from_response_headers(
        self, headers: dict[str, str]
    ) -> list[dict[str, Any]]:
        """
        Detect HubSpot signals from HTTP response headers.

        Args:
            headers: HTTP response headers

        Returns:
            List of detected signals from headers
        """
        signals = []

        # Check for HubSpot-specific headers
        hubspot_headers = {
            "x-hs-cache-config": {
                "weight": 20,
                "description": "HubSpot cache configuration header",
            },
            "x-hs-content-id": {
                "weight": 25,
                "description": "HubSpot content ID header",
            },
            "x-hs-hub-id": {
                "weight": 30,
                "description": "HubSpot hub/portal ID header",
            },
            "x-powered-by": {
                "weight": 30,
                "description": "HubSpot powered-by header",
                "value_pattern": r"hubspot",
            },
        }

        for header, info in hubspot_headers.items():
            # Find the header case-insensitively
            header_value = None
            for k, v in headers.items():
                if k.lower() == header.lower():
                    header_value = v
                    break

            if header_value is not None:
                # Check if there's a value pattern to match
                if "value_pattern" in info:
                    if re.search(info["value_pattern"], header_value, re.IGNORECASE):
                        signals.append(
                            {
                                "name": f"header-{header}",
                                "description": info["description"],
                                "weight": info["weight"],
                            }
                        )
                else:
                    # Header presence is enough
                    signals.append(
                        {
                            "name": f"header-{header}",
                            "description": info["description"],
                            "weight": info["weight"],
                        }
                    )

        return signals

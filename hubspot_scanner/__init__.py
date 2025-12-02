"""HubSpot Presence Scanner - Detect HubSpot usage on websites."""

from .detector import HubSpotDetector, DetectionResult
from .scanner import scan_domains, scan_domain
from .email_extractor import (
    extract_emails_from_html,
    crawl_for_emails,
    is_disposable_email,
    load_disposable_domains,
)

# Wappalyzer-style multi-technology detection
from .tech_detector import TechDetector, TechDetectionResult
from .tech_scorer import (
    score_technologies,
    get_highest_value_tech,
    ScoredTechnology,
    TECH_SCORES,
    RECENT_PROJECTS,
)
from .email_generator import (
    generate_outreach_email,
    generate_subject_lines,
    generate_email_body,
    GeneratedEmail,
    CONSULTANT_PROFILE,
    # New A/B email generation
    GeneratedEmailAB,
    TECHNOLOGY_CATEGORIES,
    generate_email_ab,
    generate_all_category_emails,
    generate_outreach_email_ab,
    generate_version_a_email,
    generate_version_b_email,
    generate_subject_lines_ab,
    # NEW: Persona and variant support
    CLOSESPARK_PROFILE,
    PERSONA_MAP,
    EMAIL_VARIANTS,
    SUBJECT_VARIANTS,
    PersonaEmail,
    get_persona_for_email,
    get_variant_for_tech,
    generate_persona_outreach_email,
    generate_outreach_email_with_persona,
    # NEW: Variant suppression
    get_unused_persona_for_domain,
    select_variant_with_suppression,
)
from .tech_scanner import (
    scan_technologies,
    scan_technologies_batch,
    TechScanResult,
)

__version__ = "1.0.0"
__all__ = [
    # HubSpot-specific detection
    "HubSpotDetector",
    "DetectionResult",
    "scan_domains",
    "scan_domain",
    "extract_emails_from_html",
    "crawl_for_emails",
    "is_disposable_email",
    "load_disposable_domains",
    # Multi-technology detection (Wappalyzer-style)
    "TechDetector",
    "TechDetectionResult",
    "score_technologies",
    "get_highest_value_tech",
    "ScoredTechnology",
    "TECH_SCORES",
    "RECENT_PROJECTS",
    # Email generation
    "generate_outreach_email",
    "generate_subject_lines",
    "generate_email_body",
    "GeneratedEmail",
    "CONSULTANT_PROFILE",
    # A/B email generation
    "GeneratedEmailAB",
    "TECHNOLOGY_CATEGORIES",
    "generate_email_ab",
    "generate_all_category_emails",
    "generate_outreach_email_ab",
    "generate_version_a_email",
    "generate_version_b_email",
    "generate_subject_lines_ab",
    # NEW: Persona and variant support
    "CLOSESPARK_PROFILE",
    "PERSONA_MAP",
    "EMAIL_VARIANTS",
    "SUBJECT_VARIANTS",
    "PersonaEmail",
    "get_persona_for_email",
    "get_variant_for_tech",
    "generate_persona_outreach_email",
    "generate_outreach_email_with_persona",
    # NEW: Variant suppression
    "get_unused_persona_for_domain",
    "select_variant_with_suppression",
    # Unified scanner
    "scan_technologies",
    "scan_technologies_batch",
    "TechScanResult",
]

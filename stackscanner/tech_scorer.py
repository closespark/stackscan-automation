"""
Technology scoring and ranking system.

Scores detected technologies by value/specialization and ranks them
for use in outreach prioritization.
"""

from dataclasses import dataclass
from typing import Any


# Technologies to skip when selecting the highest-value tech for outreach.
# These are detected but deprioritized in favor of other technologies
# because Magento leads typically have lower conversion rates for outreach.
TECH_BLACKLIST: set[str] = {"Magento"}


# Technology value scores (higher = more valuable/specialized)
TECH_SCORES = {
    # Enterprise / High-value (Score: 5)
    "Salesforce": 5,
    "Marketo": 5,
    "HubSpot": 5,
    "Segment": 5,
    "Magento": 5,
    "Pardot": 5,
    "Optimizely": 5,
    # Ecommerce + Payments + Advanced (Score: 4)
    "Shopify": 4,
    "BigCommerce": 4,
    "Stripe": 4,
    "PayPal": 4,
    "Braintree": 4,
    "Klaviyo": 4,
    "Mixpanel": 4,
    "Amplitude": 4,
    "VWO": 4,
    "Square": 4,
    # Mainstream CMS + Marketing (Score: 3)
    "WordPress": 3,
    "WooCommerce": 3,
    "Mailchimp": 3,
    "SendGrid": 3,
    "ActiveCampaign": 3,
    "Intercom": 3,
    "Drift": 3,
    "Zendesk Chat": 3,
    "Freshchat": 3,
    "Zoho": 3,
    "Pipedrive": 3,
    "Webflow": 3,
    # Infrastructure (Score: 2)
    "AWS": 2,
    "Vercel": 2,
    "Netlify": 2,
    "Cloudflare": 2,
    "nginx": 2,
    "Apache": 2,
    # Basic Analytics (Score: 1)
    "Google Analytics": 1,
    "GA4": 1,
    "Google Optimize": 1,
    "Heap": 1,
    "Hotjar": 1,
}


# Recent project examples for each technology
RECENT_PROJECTS = {
    # Ecommerce
    "Shopify": "rebuilt a Shopify checkout flow and fixed server-side tracking for Stripe + Klaviyo events.",
    "WooCommerce": "cleaned up plugin conflicts and repaired broken purchase tracking.",
    "Magento": "consolidated customer data into unified workflows and improved site speed.",
    "BigCommerce": "optimized product feeds and automated behavior-triggered email flows.",
    # Payments
    "Stripe": "cleaned up webhook failures and rebuilt subscription renewal logic.",
    "PayPal": "fixed PayPal order confirmation discrepancies hitting CRM + analytics.",
    "Square": "set up Square→CRM syncing and automated follow-ups.",
    "Braintree": "debugged Braintree failures and unified checkout data.",
    # Email Marketing
    "Klaviyo": "added behavior-based flows and repaired missing ecommerce event tracking.",
    "Mailchimp": "updated automation triggers and cleaned up subscriber data.",
    "SendGrid": "fixed deliverability issues tied to DNS/SPF/DMARC misconfigurations.",
    # Marketing Automation
    "Marketo": "rebuilt lead scoring and lifecycle automation tied to CRM signals.",
    "Pardot": "repaired Salesforce sync and rebuilt MQL handoff logic.",
    "ActiveCampaign": "built multi-step automations connecting forms, CRM, and tags.",
    "HubSpot": "fixed broken workflows and rebuilt lead routing tied to form submissions.",
    # CRM
    "Salesforce": "cleaned up workflow loops and rebuilt opportunity automation.",
    "Zoho": "implemented workflow rules and API syncing for web leads.",
    "Pipedrive": "automated follow-up logic and lead enrichment.",
    # Chat
    "Intercom": "restructured chat routing and built automated follow-ups.",
    "Drift": "built qualification playbooks and CRM routing.",
    "Zendesk Chat": "set up routing rules and automated ticket creation.",
    "Freshchat": "configured chat flows and CRM integration.",
    # Analytics
    "Google Analytics": "fixed event tracking and implemented server-side tagging.",
    "GA4": "migrated tracking from UA to GA4 and set up custom events.",
    "Mixpanel": "built funnels + retention dashboards tied to automation triggers.",
    "Amplitude": "instrumented product events and drop-off alerts.",
    "Heap": "aligned autocapture events with CRM data.",
    "Hotjar": "set up heatmaps and connected insights to UX improvements.",
    # A/B Testing
    "Optimizely": "built experiments and connected results to analytics.",
    "VWO": "set up A/B tests and personalization campaigns.",
    "Google Optimize": "configured experiments and goal tracking.",
    # CMS
    "WordPress": "removed plugin bloat and fixed broken form tracking.",
    "Webflow": "fixed Webflow form → CRM automations and improved performance.",
    # CDP
    "Segment": "cleaned up event taxonomy and unified customer data across tools.",
    # Infrastructure
    "AWS": "created Lambda automations and fixed caching issues.",
    "Vercel": "set up optimized builds and environment-based deployments.",
    "Netlify": "connected form events to CRM + automated builds.",
    "Cloudflare": "optimized caching rules and set up page rules.",
    # Default
    "_default": "just wrapped up a project fixing broken automation and tracking across a multi-tool stack.",
}


@dataclass
class ScoredTechnology:
    """A technology with its score and details."""

    name: str
    score: int
    category: str
    recent_project: str


def score_technologies(technologies: list[str]) -> list[ScoredTechnology]:
    """
    Score and rank detected technologies.

    Args:
        technologies: List of detected technology names

    Returns:
        List of ScoredTechnology objects, sorted by score (highest first)
    """
    scored = []

    for tech in technologies:
        score = TECH_SCORES.get(tech, 1)
        project = RECENT_PROJECTS.get(tech, RECENT_PROJECTS["_default"])

        # Determine category based on tech name
        category = _get_category(tech)

        scored.append(
            ScoredTechnology(
                name=tech,
                score=score,
                category=category,
                recent_project=project,
            )
        )

    # Sort by score (highest first)
    scored.sort(key=lambda x: x.score, reverse=True)
    return scored


def get_highest_value_tech(technologies: list[str]) -> ScoredTechnology | None:
    """
    Get the highest-value technology from a list, skipping blacklisted ones.

    If the highest-scoring technology is blacklisted, returns the next
    highest-scoring non-blacklisted technology instead.

    Args:
        technologies: List of detected technology names

    Returns:
        The highest-scoring non-blacklisted technology, or None if list is empty
        or all technologies are blacklisted
    """
    scored = score_technologies(technologies)
    for tech in scored:
        if tech.name not in TECH_BLACKLIST:
            return tech
    return None


def _get_category(tech_name: str) -> str:
    """Get the category for a technology."""
    categories = {
        # Ecommerce
        "Shopify": "Ecommerce",
        "WooCommerce": "Ecommerce",
        "Magento": "Ecommerce",
        "BigCommerce": "Ecommerce",
        # Payments
        "Stripe": "Payment Processor",
        "PayPal": "Payment Processor",
        "Square": "Payment Processor",
        "Braintree": "Payment Processor",
        # Email
        "Klaviyo": "Email Marketing",
        "Mailchimp": "Email Marketing",
        "SendGrid": "Email Marketing",
        # Marketing Automation
        "Marketo": "Marketing Automation",
        "Pardot": "Marketing Automation",
        "ActiveCampaign": "Marketing Automation",
        "HubSpot": "Marketing Automation",
        # CRM
        "Salesforce": "CRM",
        "Zoho": "CRM",
        "Pipedrive": "CRM",
        # Chat
        "Intercom": "Live Chat",
        "Drift": "Live Chat",
        "Zendesk Chat": "Live Chat",
        "Freshchat": "Live Chat",
        # Analytics
        "Google Analytics": "Analytics",
        "GA4": "Analytics",
        "Mixpanel": "Analytics",
        "Amplitude": "Analytics",
        "Heap": "Analytics",
        "Hotjar": "Analytics",
        # A/B Testing
        "Optimizely": "A/B Testing",
        "VWO": "A/B Testing",
        "Google Optimize": "A/B Testing",
        # CMS
        "WordPress": "CMS",
        "Webflow": "CMS",
        # CDP
        "Segment": "Customer Data Platform",
        # Infrastructure
        "AWS": "Infrastructure",
        "Vercel": "Infrastructure",
        "Netlify": "Infrastructure",
        "Cloudflare": "Infrastructure",
        "nginx": "Web Server",
        "Apache": "Web Server",
    }
    return categories.get(tech_name, "Technology")


def to_dict(scored_tech: ScoredTechnology) -> dict[str, Any]:
    """Convert ScoredTechnology to dictionary."""
    return {
        "name": scored_tech.name,
        "score": scored_tech.score,
        "category": scored_tech.category,
        "recent_project": scored_tech.recent_project,
    }

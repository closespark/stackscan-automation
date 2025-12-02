"""
Email generation for technology-based outreach.

Generates personalized cold outreach emails based on detected technologies,
following best practices for consultant-style outreach.

Supports two email versions:
- Version A (Rate Version): Includes $85/hr rate, 120-150 words
- Version B (No-Rate Version): Softer, value-first approach, 110-140 words
"""

import random
from dataclasses import dataclass
from typing import Any

from .tech_scorer import ScoredTechnology, get_highest_value_tech


# Consultant profile configuration
CONSULTANT_PROFILE = {
    "name": "Chris",
    "location": "Richmond, VA",
    "hourly_rate": "$85/hr",
    "github": "https://github.com/inevitablesale",
    "calendly": "https://calendly.com/inevitable-sale/hubspot-systems-consultation",
    "positioning": "freelance technical specialist",
}

# 12 Technology Categories with representative technologies
TECHNOLOGY_CATEGORIES = {
    "CRM": {
        "main_techs": ["Salesforce", "Zoho", "Pipedrive"],
        "subject_templates": [
            "Quick {{MainTech}} question",
            "{{MainTech}} workflow help available",
            "Short-term {{MainTech}} specialist",
        ],
        "recent_projects": {
            "Salesforce": "fixed a Salesforce lead-routing and automation flow that was dropping records",
            "Zoho": "repaired a Zoho CRM integration where deals weren't syncing to email sequences",
            "Pipedrive": "cleaned up a Pipedrive integration that wasn't syncing deals properly",
            "_default": "fixed a CRM lead-routing and automation flow that was dropping records",
        },
    },
    "Marketing Automation": {
        "main_techs": ["HubSpot", "Marketo", "Pardot", "ActiveCampaign"],
        "subject_templates": [
            "{{MainTech}} workflow idea",
            "Quick {{MainTech}} question",
            "Short-term {{MainTech}} help?",
        ],
        "recent_projects": {
            "HubSpot": "rebuilt a HubSpot workflow where forms weren't syncing into lists correctly",
            "Marketo": "fixed a Marketo nurture flow that stopped sending triggers",
            "Pardot": "repaired Pardot-Salesforce sync and rebuilt MQL handoff logic",
            "ActiveCampaign": "cleaned up ActiveCampaign automations connecting forms, CRM, and tags",
            "_default": "rebuilt a marketing automation workflow where forms weren't syncing correctly",
        },
    },
    "Email Marketing": {
        "main_techs": ["Klaviyo", "Mailchimp", "SendGrid"],
        "subject_templates": [
            "{{MainTech}} flow idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} automation help",
        ],
        "recent_projects": {
            "Klaviyo": "repaired a Klaviyo event-triggered flow that stopped firing after a checkout update",
            "Mailchimp": "fixed Mailchimp automation triggers and cleaned up subscriber data",
            "SendGrid": "resolved SendGrid deliverability issues tied to DNS/SPF/DMARC misconfigurations",
            "_default": "repaired an email flow that stopped firing after a checkout update",
        },
    },
    "Live Chat": {
        "main_techs": ["Intercom", "Drift", "Zendesk Chat", "Freshchat"],
        "subject_templates": [
            "{{MainTech}} routing idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} integration help",
        ],
        "recent_projects": {
            "Intercom": "restructured Intercom chat routing and built automated follow-ups",
            "Drift": "integrated Drift with a CRM and fixed custom event triggers",
            "Zendesk Chat": "set up Zendesk Chat routing rules and automated ticket creation",
            "Freshchat": "configured Freshchat flows and CRM integration",
            "_default": "integrated live chat with a CRM and fixed custom event triggers",
        },
    },
    "Ecommerce": {
        "main_techs": ["Shopify", "WooCommerce", "Magento", "BigCommerce"],
        "subject_templates": [
            "{{MainTech}} checkout idea",
            "Quick {{MainTech}} question",
            "Short-term {{MainTech}} help?",
        ],
        "recent_projects": {
            "Shopify": "cleaned up a Shopify checkout + webhook integration that was losing order data",
            "WooCommerce": "fixed WooCommerce → CRM syncing failures",
            "Magento": "consolidated Magento customer data into unified workflows",
            "BigCommerce": "optimized BigCommerce product feeds and automated behavior-triggered flows",
            "_default": "cleaned up a checkout + webhook integration that was losing order data",
        },
    },
    "Payments": {
        "main_techs": ["Stripe", "PayPal", "Braintree", "Square"],
        "subject_templates": [
            "{{MainTech}} integration idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} tracking help",
        ],
        "recent_projects": {
            "Stripe": "resolved a Stripe payment tracking issue tied to abandoned checkout events",
            "PayPal": "fixed PayPal order confirmation discrepancies hitting CRM + analytics",
            "Braintree": "debugged Braintree failures and unified checkout data",
            "Square": "set up Square→CRM syncing and automated follow-ups",
            "_default": "resolved a payment tracking issue tied to abandoned checkout events",
        },
    },
    "Analytics": {
        "main_techs": ["Google Analytics", "Mixpanel", "Amplitude", "Heap", "Hotjar"],
        "subject_templates": [
            "{{MainTech}} tracking idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} setup help",
        ],
        "recent_projects": {
            "Google Analytics": "rebuilt a broken GA4 + Mixpanel tracking setup that was missing key conversions",
            "Mixpanel": "built Mixpanel funnels + retention dashboards tied to automation triggers",
            "Amplitude": "instrumented Amplitude product events and drop-off alerts",
            "Heap": "aligned Heap autocapture events with CRM data",
            "Hotjar": "set up Hotjar heatmaps and connected insights to UX improvements",
            "_default": "rebuilt a tracking setup that was missing key conversions",
        },
    },
    "A/B Testing": {
        "main_techs": ["Optimizely", "VWO", "Google Optimize"],
        "subject_templates": [
            "{{MainTech}} experiment idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} optimization help",
        ],
        "recent_projects": {
            "Optimizely": "cleaned up an Optimizely experiment where tracking wasn't reporting correctly",
            "VWO": "set up VWO A/B tests and personalization campaigns",
            "Google Optimize": "configured Google Optimize experiments and goal tracking",
            "_default": "cleaned up an A/B test where tracking wasn't reporting correctly",
        },
    },
    "CDP": {
        "main_techs": ["Segment"],
        "subject_templates": [
            "{{MainTech}} integration idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} data flow help",
        ],
        "recent_projects": {
            "Segment": "fixed Segment sources where events weren't hitting downstream destinations",
            "_default": "fixed CDP sources where events weren't hitting downstream destinations",
        },
    },
    "CMS": {
        "main_techs": ["WordPress", "Webflow"],
        "subject_templates": [
            "{{MainTech}} performance idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} integration help",
        ],
        "recent_projects": {
            "WordPress": "patched a WordPress plugin integration breaking form submissions",
            "Webflow": "fixed Webflow form → CRM automations and improved performance",
            "_default": "patched a CMS integration breaking form submissions",
        },
    },
    "Hosting/CDN": {
        "main_techs": ["AWS", "Vercel", "Netlify", "Cloudflare"],
        "subject_templates": [
            "{{MainTech}} config idea",
            "Quick {{MainTech}} question",
            "{{MainTech}} deployment help",
        ],
        "recent_projects": {
            "AWS": "created AWS Lambda automations and fixed caching issues",
            "Vercel": "cleaned up a Vercel deployment with misconfigured env vars affecting API calls",
            "Netlify": "connected Netlify form events to CRM + automated builds",
            "Cloudflare": "optimized Cloudflare caching rules and set up page rules",
            "_default": "cleaned up a deployment with misconfigured env vars affecting API calls",
        },
    },
    "Web Servers": {
        "main_techs": ["nginx", "Apache"],
        "subject_templates": [
            "{{MainTech}} config idea",
            "Quick {{MainTech}} question",
            "Server optimization help",
        ],
        "recent_projects": {
            "nginx": "optimized nginx configuration for better performance and caching",
            "Apache": "fixed Apache configuration issues affecting site performance",
            "_default": "optimized server configuration for better performance",
        },
    },
}


@dataclass
class GeneratedEmail:
    """Generated outreach email with metadata."""

    domain: str
    selected_technology: str
    recent_project: str
    subject_lines: list[str]
    email_body: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "domain": self.domain,
            "selected_technology": self.selected_technology,
            "recent_project": self.recent_project,
            "subject_lines": self.subject_lines,
            "email_body": self.email_body,
        }


def generate_subject_lines(tech_name: str, category: str) -> list[str]:
    """
    Generate 3 subject lines for the outreach email.

    Args:
        tech_name: The selected technology name
        category: The technology category

    Returns:
        List of 3 subject lines
    """
    # Subject line templates by category
    templates = {
        "Ecommerce": [
            f"Quick {tech_name} question",
            f"{tech_name} help from Richmond",
            f"Your {tech_name} setup",
            f"Short-term {tech_name} help?",
            f"Noticed your {tech_name} store",
        ],
        "Payment Processor": [
            f"{tech_name} integration help",
            f"Quick {tech_name} question",
            f"Payment flow thoughts",
            f"Richmond-based {tech_name} help",
        ],
        "Email Marketing": [
            f"{tech_name} automation ideas",
            f"Quick {tech_name} thought",
            f"Email flow improvements",
            f"Noticed your {tech_name} setup",
        ],
        "Marketing Automation": [
            f"{tech_name} workflow ideas",
            f"Quick automation thought",
            f"Noticed your {tech_name} setup",
            f"Short-term {tech_name} help",
        ],
        "CRM": [
            f"{tech_name} workflow question",
            f"Quick CRM thought",
            f"Noticed your {tech_name}",
            f"Richmond {tech_name} consultant",
        ],
        "Live Chat": [
            f"{tech_name} routing idea",
            f"Quick chat flow thought",
            f"Noticed your {tech_name}",
        ],
        "Analytics": [
            f"Tracking question",
            f"{tech_name} setup thoughts",
            f"Analytics improvement idea",
            f"Quick data question",
        ],
        "A/B Testing": [
            f"{tech_name} experiment idea",
            f"Quick testing thought",
            f"Optimization question",
        ],
        "CMS": [
            f"{tech_name} performance idea",
            f"Quick site thought",
            f"Noticed your {tech_name} site",
        ],
        "Infrastructure": [
            f"Quick infrastructure thought",
            f"Performance question",
            f"Technical help available",
        ],
        "Customer Data Platform": [
            f"{tech_name} integration idea",
            f"Data flow question",
            f"CDP optimization thought",
        ],
    }

    # Get templates for this category, or use generic ones
    category_templates = templates.get(
        category,
        [
            f"Quick {tech_name} question",
            f"Short-term help available",
            f"Technical consultant in Richmond",
        ],
    )

    # Select 3 random subject lines
    if len(category_templates) >= 3:
        return random.sample(category_templates, 3)
    else:
        # Pad with generic templates if needed
        generic = [
            f"Quick {tech_name} question",
            f"Short-term technical help",
            f"Richmond-based consultant",
        ]
        combined = category_templates + generic
        return random.sample(combined, min(3, len(combined)))


def generate_email_body(
    domain: str,
    tech: ScoredTechnology,
    profile: dict[str, str] | None = None,
) -> str:
    """
    Generate the email body for outreach.

    Args:
        domain: The target domain
        tech: The selected technology with scoring info
        profile: Optional consultant profile override

    Returns:
        The generated email body (under 180 words)
    """
    p = profile or CONSULTANT_PROFILE

    # Build the email
    email = f"""Hey there,

I was looking at {domain} and noticed you're using {tech.name}. I'm {p['name']}, a {p['positioning']} based in {p['location']}.

I recently {tech.recent_project} It's the kind of short-term work I specialize in—no agency overhead, just direct technical help.

I handle {tech.category.lower()}, automation, CRM, and analytics tasks at {p['hourly_rate']}. Not looking to replace anyone on your team or become a long-term fixture—just available if you ever need a hand with something specific.

If that's useful, happy to chat: {p['calendly']}

You can also see my work here: {p['github']}

Either way, hope things are going well with {tech.name}.

– {p['name']}"""

    return email


def generate_outreach_email(
    domain: str,
    technologies: list[str],
    profile: dict[str, str] | None = None,
) -> GeneratedEmail | None:
    """
    Generate a complete outreach email for a domain.

    Args:
        domain: The target domain
        technologies: List of detected technology names
        profile: Optional consultant profile override

    Returns:
        GeneratedEmail object, or None if no technologies detected
    """
    if not technologies:
        return None

    # Get the highest-value technology
    top_tech = get_highest_value_tech(technologies)
    if not top_tech:
        return None

    # Generate subject lines
    subject_lines = generate_subject_lines(top_tech.name, top_tech.category)

    # Generate email body
    email_body = generate_email_body(domain, top_tech, profile)

    return GeneratedEmail(
        domain=domain,
        selected_technology=top_tech.name,
        recent_project=top_tech.recent_project,
        subject_lines=subject_lines,
        email_body=email_body,
    )


@dataclass
class GeneratedEmailAB:
    """Generated A/B version outreach emails with metadata."""

    category: str
    main_tech: str
    subject_lines: list[str]
    version_a: str  # Rate version ($85/hr)
    version_b: str  # No-rate version (softer, value-first)
    other_tech_1: str
    other_tech_2: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "category": self.category,
            "main_tech": self.main_tech,
            "subject_lines": self.subject_lines,
            "version_a": self.version_a,
            "version_b": self.version_b,
            "other_tech_1": self.other_tech_1,
            "other_tech_2": self.other_tech_2,
        }


def _get_category_for_tech(tech_name: str) -> str | None:
    """Get the category for a given technology."""
    for category, config in TECHNOLOGY_CATEGORIES.items():
        if tech_name in config["main_techs"]:
            return category
    return None


def _get_recent_project(category: str, tech_name: str) -> str:
    """Get a recent project reference for a technology."""
    if category not in TECHNOLOGY_CATEGORIES:
        return "helped a client fix a broken integration and automation flow"
    
    projects = TECHNOLOGY_CATEGORIES[category]["recent_projects"]
    return projects.get(tech_name, projects.get("_default", "helped a client fix a broken integration"))


def _get_other_techs(main_tech: str, detected_techs: list[str] | None = None) -> tuple[str, str]:
    """
    Get two other technologies to mention in the email.
    
    If detected_techs is provided, use technologies actually found on the domain.
    Otherwise, use common complementary technologies.
    """
    # Common complementary technologies by main tech category
    complementary_techs = {
        # CRM
        "Salesforce": ["HubSpot", "Segment", "Zapier"],
        "Zoho": ["Mailchimp", "Stripe", "WordPress"],
        "Pipedrive": ["ActiveCampaign", "Slack", "Stripe"],
        # Marketing Automation
        "HubSpot": ["Salesforce", "Segment", "Stripe"],
        "Marketo": ["Salesforce", "Segment", "Google Analytics"],
        "Pardot": ["Salesforce", "Google Analytics", "Segment"],
        "ActiveCampaign": ["Shopify", "Stripe", "WordPress"],
        # Email Marketing
        "Klaviyo": ["Shopify", "Stripe", "Segment"],
        "Mailchimp": ["WordPress", "Stripe", "Zapier"],
        "SendGrid": ["Stripe", "AWS", "Segment"],
        # Live Chat
        "Intercom": ["Segment", "Salesforce", "Slack"],
        "Drift": ["Salesforce", "HubSpot", "Segment"],
        "Zendesk Chat": ["Salesforce", "Segment", "Slack"],
        "Freshchat": ["Freshdesk", "Stripe", "WordPress"],
        # Ecommerce
        "Shopify": ["Klaviyo", "Stripe", "Google Analytics"],
        "WooCommerce": ["Mailchimp", "Stripe", "Google Analytics"],
        "Magento": ["Salesforce", "Segment", "Stripe"],
        "BigCommerce": ["Klaviyo", "Stripe", "Google Analytics"],
        # Payments
        "Stripe": ["Shopify", "Segment", "Google Analytics"],
        "PayPal": ["WooCommerce", "Mailchimp", "Google Analytics"],
        "Braintree": ["Salesforce", "Segment", "Google Analytics"],
        "Square": ["Mailchimp", "QuickBooks", "Google Analytics"],
        # Analytics
        "Google Analytics": ["Segment", "HubSpot", "Hotjar"],
        "Mixpanel": ["Segment", "Intercom", "Amplitude"],
        "Amplitude": ["Segment", "Mixpanel", "Intercom"],
        "Heap": ["Segment", "Intercom", "Google Analytics"],
        "Hotjar": ["Google Analytics", "Segment", "Intercom"],
        # A/B Testing
        "Optimizely": ["Google Analytics", "Segment", "Amplitude"],
        "VWO": ["Google Analytics", "Hotjar", "Segment"],
        "Google Optimize": ["Google Analytics", "Hotjar", "Tag Manager"],
        # CDP
        "Segment": ["Salesforce", "Amplitude", "Intercom"],
        # CMS
        "WordPress": ["Mailchimp", "Google Analytics", "WooCommerce"],
        "Webflow": ["Mailchimp", "Google Analytics", "Zapier"],
        # Hosting/CDN
        "AWS": ["Segment", "Datadog", "CloudWatch"],
        "Vercel": ["Next.js", "Segment", "Google Analytics"],
        "Netlify": ["Gatsby", "Segment", "Google Analytics"],
        "Cloudflare": ["AWS", "Google Analytics", "Segment"],
        # Web Servers
        "nginx": ["AWS", "Cloudflare", "Docker"],
        "Apache": ["AWS", "Cloudflare", "PHP"],
    }
    
    # If we have detected technologies, prefer those
    if detected_techs:
        other_techs = [t for t in detected_techs if t != main_tech]
        if len(other_techs) >= 2:
            return (other_techs[0], other_techs[1])
        elif len(other_techs) == 1:
            # Get one complementary tech
            complements = complementary_techs.get(main_tech, ["Google Analytics", "Segment"])
            return (other_techs[0], complements[0])
    
    # Fall back to complementary techs
    complements = complementary_techs.get(main_tech, ["Google Analytics", "Segment"])
    return (complements[0], complements[1] if len(complements) > 1 else "Zapier")


def generate_subject_lines_ab(main_tech: str, category: str) -> list[str]:
    """
    Generate 3 subject lines containing the main technology.
    
    Args:
        main_tech: The main technology name
        category: The technology category
        
    Returns:
        List of 3 subject lines with {{MainTech}} replaced
    """
    if category in TECHNOLOGY_CATEGORIES:
        templates = TECHNOLOGY_CATEGORIES[category]["subject_templates"]
    else:
        templates = [
            "Quick {{MainTech}} question",
            "{{MainTech}} help available",
            "Short-term {{MainTech}} specialist",
        ]
    
    # Replace {{MainTech}} with actual tech name
    return [t.replace("{{MainTech}}", main_tech) for t in templates]


def generate_version_a_email(
    main_tech: str,
    category: str,
    recent_project: str,
    other_tech_1: str,
    other_tech_2: str,
    profile: dict[str, str] | None = None,
) -> str:
    """
    Generate Version A (Rate Version) email.
    
    Target: 120-150 words
    Includes: $85/hr rate, short-term specialist framing, recent project,
              other technologies, Calendly + GitHub links
    """
    p = profile or CONSULTANT_PROFILE
    
    email = f"""Hey there,

I'm {p['name']} — freelance {main_tech} specialist based in {p['location']}.

I recently helped a client who {recent_project}. It's the kind of short-term, high-impact work I focus on — quick fixes and clean implementations, no long-term commitment required.

I take on small but important tasks that often get stuck in backlogs: checkout fixes, automation cleanup, event tracking repairs, webhook repairs, form→CRM routing fixes, segmentation and flow issues, hosting and CDN configs, and API integrations. My rate is {p['hourly_rate']} for direct technical work with no agency overhead.

I also work with {other_tech_1} and {other_tech_2} if you ever need help in those areas.

You can grab time here if you want to talk through specifics:
{p['calendly']}

– {p['name']}
{p['github']}"""
    
    return email


def generate_version_b_email(
    main_tech: str,
    category: str,
    recent_project: str,
    other_tech_1: str,
    other_tech_2: str,
    profile: dict[str, str] | None = None,
) -> str:
    """
    Generate Version B (No-Rate Version) email.
    
    Target: 110-140 words
    Softer, value-first approach. No price mentioned.
    Includes: short-term specialist framing, recent project,
              other technologies, Calendly + GitHub links
    """
    p = profile or CONSULTANT_PROFILE
    
    email = f"""Hey there,

I'm {p['name']} — freelance {main_tech} specialist based in {p['location']}.

I recently helped a client who {recent_project}. I specialize in short-term technical work — the kind of important fixes and improvements that often get stuck in backlogs but can make a real difference.

If you ever need a hand with {main_tech}, automation cleanup, event tracking, webhook repairs, or form→CRM routing, I'm happy to help with quick projects. No long-term commitment required — just direct technical work when you need it.

I also support {other_tech_1} and {other_tech_2} if those are part of your stack.

You can grab time here if you want to talk through specifics:
{p['calendly']}

– {p['name']}
{p['github']}"""
    
    return email


def generate_email_ab(
    main_tech: str,
    detected_techs: list[str] | None = None,
    profile: dict[str, str] | None = None,
) -> GeneratedEmailAB | None:
    """
    Generate both Version A and Version B emails for a technology.
    
    Args:
        main_tech: The main technology to focus the email on
        detected_techs: Optional list of other detected technologies
        profile: Optional consultant profile override
        
    Returns:
        GeneratedEmailAB with both versions, or None if tech not recognized
    """
    category = _get_category_for_tech(main_tech)
    if not category:
        return None
    
    recent_project = _get_recent_project(category, main_tech)
    other_tech_1, other_tech_2 = _get_other_techs(main_tech, detected_techs)
    subject_lines = generate_subject_lines_ab(main_tech, category)
    
    version_a = generate_version_a_email(
        main_tech, category, recent_project, other_tech_1, other_tech_2, profile
    )
    version_b = generate_version_b_email(
        main_tech, category, recent_project, other_tech_1, other_tech_2, profile
    )
    
    return GeneratedEmailAB(
        category=category,
        main_tech=main_tech,
        subject_lines=subject_lines,
        version_a=version_a,
        version_b=version_b,
        other_tech_1=other_tech_1,
        other_tech_2=other_tech_2,
    )


def generate_all_category_emails(
    profile: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """
    Generate emails for all 12 technology categories.
    
    This produces the complete output format with all categories,
    each containing:
    - Category name
    - MainTech variable
    - 3 subject lines
    - Version A email (rate version)
    - Version B email (no-rate version)
    
    Args:
        profile: Optional consultant profile override
        
    Returns:
        List of dictionaries, one per category
    """
    results = []
    
    for category, config in TECHNOLOGY_CATEGORIES.items():
        # Use first tech as the representative main tech for this category
        main_tech = config["main_techs"][0]
        
        recent_project = _get_recent_project(category, main_tech)
        other_tech_1, other_tech_2 = _get_other_techs(main_tech, None)
        subject_lines = generate_subject_lines_ab(main_tech, category)
        
        version_a = generate_version_a_email(
            main_tech, category, recent_project, other_tech_1, other_tech_2, profile
        )
        version_b = generate_version_b_email(
            main_tech, category, recent_project, other_tech_1, other_tech_2, profile
        )
        
        results.append({
            "category": category,
            "main_tech": main_tech,
            "subject_lines": subject_lines,
            "version_a": version_a,
            "version_b": version_b,
            "other_tech_1": other_tech_1,
            "other_tech_2": other_tech_2,
        })
    
    return results


def generate_outreach_email_ab(
    domain: str,
    technologies: list[str],
    profile: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """
    Generate A/B version outreach emails for a domain.
    
    This is the main entry point for generating personalized emails
    based on detected technologies on a prospect's website.
    
    Args:
        domain: The target domain
        technologies: List of detected technology names
        profile: Optional consultant profile override
        
    Returns:
        Dictionary with both email versions and metadata,
        or None if no suitable technology detected
    """
    if not technologies:
        return None
    
    # Get the highest-value technology
    top_tech = get_highest_value_tech(technologies)
    if not top_tech:
        return None
    
    # Generate A/B emails
    email_ab = generate_email_ab(top_tech.name, technologies, profile)
    if not email_ab:
        # Fall back to legacy email if tech not in new categories
        legacy = generate_outreach_email(domain, technologies, profile)
        if legacy:
            return {
                "domain": domain,
                "main_tech": legacy.selected_technology,
                "category": top_tech.category,
                "subject_lines": legacy.subject_lines,
                "version_a": legacy.email_body,
                "version_b": legacy.email_body,  # Same as A for legacy
                "other_tech_1": technologies[1] if len(technologies) > 1 else "N/A",
                "other_tech_2": technologies[2] if len(technologies) > 2 else "N/A",
            }
        return None
    
    return {
        "domain": domain,
        "main_tech": email_ab.main_tech,
        "category": email_ab.category,
        "subject_lines": email_ab.subject_lines,
        "version_a": email_ab.version_a,
        "version_b": email_ab.version_b,
        "other_tech_1": email_ab.other_tech_1,
        "other_tech_2": email_ab.other_tech_2,
    }

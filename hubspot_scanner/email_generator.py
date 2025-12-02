"""
Email generation for technology-based outreach.

Generates personalized cold outreach emails based on detected technologies,
following best practices for consultant-style outreach.

Supports:
- Multiple variants per MainTech (Shopify, Salesforce, WordPress, etc.)
- Persona-based email generation (Scott, Tracy, Willa)
- Variant tracking for A/B analysis
"""

import random
from dataclasses import dataclass
from typing import Any

from .tech_scorer import ScoredTechnology, get_highest_value_tech


# CloseSpark company configuration
CLOSESPARK_PROFILE = {
    "company": "CloseSpark",
    "location": "Richmond, VA",
    "hourly_rate": "$85/hr",
    "github": "https://github.com/closespark/",
    "calendly": "https://calendly.com/closespark/technical-systems-consultation",
}

# Persona map based on SMTP_ACCOUNTS_JSON email addresses
PERSONA_MAP = {
    "scott@closespark.co": {
        "name": "Scott",
        "role": "Systems Engineer",
        "tone": "concise, technical, straight to the point",
    },
    "tracy@closespark.co": {
        "name": "Tracy",
        "role": "Technical Project Lead",
        "tone": "structured, slightly more formal",
    },
    "willa@closespark.co": {
        "name": "Willa",
        "role": "Automation Specialist",
        "tone": "friendly but still professional",
    },
}

# Default persona for backwards compatibility
DEFAULT_PERSONA = {
    "name": "Scott",
    "email": "scott@closespark.co",
    "role": "Systems Engineer",
    "tone": "concise, technical, straight to the point",
}

# Legacy consultant profile for backwards compatibility
# Updated to use CloseSpark profile (no longer uses "Chris")
CONSULTANT_PROFILE = {
    "name": DEFAULT_PERSONA["name"],
    "location": CLOSESPARK_PROFILE["location"],
    "hourly_rate": CLOSESPARK_PROFILE["hourly_rate"],
    "github": CLOSESPARK_PROFILE["github"],
    "calendly": CLOSESPARK_PROFILE["calendly"],
    "positioning": "freelance technical specialist",
}


# Email variants per MainTech - each tech has 2-3 variants
EMAIL_VARIANTS = {
    "Shopify": [
        {
            "id": "shopify_v1",
            "subject_template": "Shopify integration issue on {{domain}}?",
            "bullets": [
                "Checkout or webhook issues affecting orders",
                "Payment + analytics events not lining up (GA4, Klaviyo, etc.)",
                "Small automation gaps that slow down the team",
            ],
        },
        {
            "id": "shopify_v2",
            "subject_template": "Quick Shopify improvement idea for {{domain}}",
            "bullets": [
                "Order tracking and fulfillment sync problems",
                "Broken email triggers (abandoned cart, post-purchase)",
                "Third-party app conflicts causing errors",
            ],
        },
        {
            "id": "shopify_v3",
            "subject_template": "Saw something in your Shopify setup",
            "bullets": [
                "Webhook reliability and event handling",
                "Checkout customization issues",
                "Inventory sync with external systems",
            ],
        },
    ],
    "Salesforce": [
        {
            "id": "salesforce_v1",
            "subject_template": "Salesforce routing issue on {{domain}}?",
            "bullets": [
                "Lead routing rules not firing correctly",
                "Automation flows dropping records",
                "Reporting gaps affecting pipeline visibility",
            ],
        },
        {
            "id": "salesforce_v2",
            "subject_template": "Quick Salesforce fix for {{domain}}",
            "bullets": [
                "Workflow automation cleanup",
                "Data sync between Salesforce and other tools",
                "Custom object and field configuration",
            ],
        },
        {
            "id": "salesforce_v3",
            "subject_template": "Noticed your Salesforce setup",
            "bullets": [
                "Integration issues with marketing tools",
                "Duplicate record cleanup and prevention",
                "Process builder optimization",
            ],
        },
    ],
    "WordPress": [
        {
            "id": "wordpress_v1",
            "subject_template": "WordPress performance idea for {{domain}}",
            "bullets": [
                "Site speed and caching optimization",
                "Plugin conflicts causing errors",
                "Form integration issues (submissions not reaching CRM)",
            ],
        },
        {
            "id": "wordpress_v2",
            "subject_template": "Quick WordPress fix for {{domain}}",
            "bullets": [
                "Broken contact forms and lead capture",
                "Analytics tracking not firing properly",
                "Theme and plugin update conflicts",
            ],
        },
        {
            "id": "wordpress_v3",
            "subject_template": "Saw something on your WordPress site",
            "bullets": [
                "Database optimization and cleanup",
                "Security and update management",
                "Custom functionality issues",
            ],
        },
    ],
    "HubSpot": [
        {
            "id": "hubspot_v1",
            "subject_template": "HubSpot workflow issue on {{domain}}?",
            "bullets": [
                "Form submissions not triggering workflows",
                "Lead scoring and lifecycle stage issues",
                "CRM sync problems with external tools",
            ],
        },
        {
            "id": "hubspot_v2",
            "subject_template": "Quick HubSpot improvement for {{domain}}",
            "bullets": [
                "Email automation and sequence cleanup",
                "Deal pipeline and reporting gaps",
                "Contact property mapping issues",
            ],
        },
        {
            "id": "hubspot_v3",
            "subject_template": "Noticed your HubSpot setup",
            "bullets": [
                "Workflow logic and branching issues",
                "Integration with Shopify, Stripe, or Salesforce",
                "List segmentation and targeting",
            ],
        },
    ],
    "Klaviyo": [
        {
            "id": "klaviyo_v1",
            "subject_template": "Klaviyo flow issue on {{domain}}?",
            "bullets": [
                "Abandoned cart emails not triggering",
                "Event tracking gaps from Shopify/WooCommerce",
                "Segment sync problems",
            ],
        },
        {
            "id": "klaviyo_v2",
            "subject_template": "Quick Klaviyo fix for {{domain}}",
            "bullets": [
                "Post-purchase flow optimization",
                "Revenue attribution not matching",
                "Integration with other marketing tools",
            ],
        },
        {
            "id": "klaviyo_v3",
            "subject_template": "Noticed your Klaviyo setup",
            "bullets": [
                "Browse abandonment flow issues",
                "SMS and email coordination",
                "Customer profile enrichment",
            ],
        },
    ],
    "Google Analytics": [
        {
            "id": "ga_v1",
            "subject_template": "Analytics tracking issue on {{domain}}?",
            "bullets": [
                "Conversion tracking not firing properly",
                "Ecommerce data missing or incorrect",
                "Cross-domain tracking issues",
            ],
        },
        {
            "id": "ga_v2",
            "subject_template": "Quick GA4 fix for {{domain}}",
            "bullets": [
                "Event tracking configuration",
                "Attribution model setup",
                "Integration with Google Ads",
            ],
        },
        {
            "id": "ga_v3",
            "subject_template": "Saw something in your analytics setup",
            "bullets": [
                "Custom dimension and metric setup",
                "Funnel visualization issues",
                "Data layer configuration",
            ],
        },
    ],
    "GA4": [
        {
            "id": "ga4_v1",
            "subject_template": "GA4 tracking issue on {{domain}}?",
            "bullets": [
                "Event tracking not configured correctly",
                "Ecommerce purchase data missing",
                "Conversion goals not counting",
            ],
        },
        {
            "id": "ga4_v2",
            "subject_template": "Quick GA4 improvement for {{domain}}",
            "bullets": [
                "Custom events and parameters setup",
                "Attribution and conversion paths",
                "Integration with BigQuery",
            ],
        },
        {
            "id": "ga4_v3",
            "subject_template": "Noticed your GA4 setup",
            "bullets": [
                "Debug mode and data validation",
                "Audience building and remarketing",
                "Cross-platform tracking",
            ],
        },
    ],
    "Magento": [
        {
            "id": "magento_v1",
            "subject_template": "Magento issue on {{domain}}?",
            "bullets": [
                "Checkout and payment flow problems",
                "Inventory sync with external systems",
                "Extension conflicts causing errors",
            ],
        },
        {
            "id": "magento_v2",
            "subject_template": "Quick Magento fix for {{domain}}",
            "bullets": [
                "Performance and caching optimization",
                "Order processing automation",
                "Customer data integration with CRM",
            ],
        },
        {
            "id": "magento_v3",
            "subject_template": "Noticed your Magento store",
            "bullets": [
                "API integration issues",
                "Custom module troubleshooting",
                "Indexer and cache management",
            ],
        },
    ],
    "Stripe": [
        {
            "id": "stripe_v1",
            "subject_template": "Stripe integration issue on {{domain}}?",
            "bullets": [
                "Webhook failures affecting order tracking",
                "Subscription and recurring payment issues",
                "Checkout flow not working properly",
            ],
        },
        {
            "id": "stripe_v2",
            "subject_template": "Quick Stripe fix for {{domain}}",
            "bullets": [
                "Payment event tracking gaps",
                "Refund and dispute handling",
                "Integration with accounting tools",
            ],
        },
        {
            "id": "stripe_v3",
            "subject_template": "Saw something in your Stripe setup",
            "bullets": [
                "Customer portal configuration",
                "Invoice automation",
                "Revenue reconciliation issues",
            ],
        },
    ],
    "WooCommerce": [
        {
            "id": "woocommerce_v1",
            "subject_template": "WooCommerce issue on {{domain}}?",
            "bullets": [
                "Checkout errors or abandoned cart issues",
                "Payment gateway integration problems",
                "Order tracking and fulfillment sync",
            ],
        },
        {
            "id": "woocommerce_v2",
            "subject_template": "Quick WooCommerce fix for {{domain}}",
            "bullets": [
                "Plugin conflicts causing errors",
                "Email notification failures",
                "Inventory sync with external systems",
            ],
        },
        {
            "id": "woocommerce_v3",
            "subject_template": "Noticed your WooCommerce store",
            "bullets": [
                "Performance optimization",
                "CRM and email marketing integration",
                "Shipping and tax calculation issues",
            ],
        },
    ],
    "Mailchimp": [
        {
            "id": "mailchimp_v1",
            "subject_template": "Mailchimp issue on {{domain}}?",
            "bullets": [
                "Automation triggers not firing",
                "List sync problems with other tools",
                "Subscriber data not updating",
            ],
        },
        {
            "id": "mailchimp_v2",
            "subject_template": "Quick Mailchimp fix for {{domain}}",
            "bullets": [
                "Email deliverability issues",
                "Segmentation and targeting problems",
                "Integration with ecommerce platform",
            ],
        },
        {
            "id": "mailchimp_v3",
            "subject_template": "Noticed your Mailchimp setup",
            "bullets": [
                "Campaign automation optimization",
                "Merge tag and personalization issues",
                "Reporting and analytics gaps",
            ],
        },
    ],
    "Segment": [
        {
            "id": "segment_v1",
            "subject_template": "Segment issue on {{domain}}?",
            "bullets": [
                "Events not reaching downstream destinations",
                "Source configuration problems",
                "Data quality and validation issues",
            ],
        },
        {
            "id": "segment_v2",
            "subject_template": "Quick Segment fix for {{domain}}",
            "bullets": [
                "Identity resolution issues",
                "Warehouse sync problems",
                "Integration with analytics tools",
            ],
        },
        {
            "id": "segment_v3",
            "subject_template": "Noticed your Segment setup",
            "bullets": [
                "Event taxonomy cleanup",
                "Destination configuration",
                "Tracking plan implementation",
            ],
        },
    ],
    "Intercom": [
        {
            "id": "intercom_v1",
            "subject_template": "Intercom issue on {{domain}}?",
            "bullets": [
                "Chat routing not working properly",
                "Automation and bot flow issues",
                "CRM sync problems",
            ],
        },
        {
            "id": "intercom_v2",
            "subject_template": "Quick Intercom fix for {{domain}}",
            "bullets": [
                "Custom bot configuration",
                "User data enrichment",
                "Integration with support tools",
            ],
        },
        {
            "id": "intercom_v3",
            "subject_template": "Noticed your Intercom setup",
            "bullets": [
                "Qualification playbook optimization",
                "Event tracking for targeting",
                "Help center integration",
            ],
        },
    ],
    "Mixpanel": [
        {
            "id": "mixpanel_v1",
            "subject_template": "Mixpanel issue on {{domain}}?",
            "bullets": [
                "Event tracking gaps",
                "Funnel analysis not accurate",
                "User identity issues",
            ],
        },
        {
            "id": "mixpanel_v2",
            "subject_template": "Quick Mixpanel fix for {{domain}}",
            "bullets": [
                "Cohort analysis setup",
                "Integration with other tools",
                "Custom property configuration",
            ],
        },
        {
            "id": "mixpanel_v3",
            "subject_template": "Noticed your Mixpanel setup",
            "bullets": [
                "Retention analysis configuration",
                "A/B test tracking",
                "Data export and warehousing",
            ],
        },
    ],
}

# Subject variants per persona and MainTech
SUBJECT_VARIANTS = {
    "scott@closespark.co": {
        "Shopify": [
            "Shopify integration issue on {{domain}}?",
            "Quick Shopify improvement idea for {{domain}}",
            "Saw something in your Shopify setup",
        ],
        "Salesforce": [
            "Salesforce routing issue on {{domain}}?",
            "Quick Salesforce fix for {{domain}}",
            "Noticed your Salesforce setup",
        ],
        "WordPress": [
            "WordPress performance idea for {{domain}}",
            "Quick WordPress fix for {{domain}}",
            "Saw something on your WordPress site",
        ],
        "HubSpot": [
            "HubSpot workflow issue on {{domain}}?",
            "Quick HubSpot fix for {{domain}}",
            "Noticed your HubSpot setup",
        ],
        "Klaviyo": [
            "Klaviyo flow issue on {{domain}}?",
            "Quick Klaviyo fix for {{domain}}",
            "Noticed your Klaviyo setup",
        ],
        "Google Analytics": [
            "Analytics tracking issue on {{domain}}?",
            "Quick GA4 fix for {{domain}}",
            "Saw something in your analytics setup",
        ],
        "GA4": [
            "GA4 tracking issue on {{domain}}?",
            "Quick GA4 improvement for {{domain}}",
            "Noticed your GA4 setup",
        ],
        "Magento": [
            "Magento issue on {{domain}}?",
            "Quick Magento fix for {{domain}}",
            "Noticed your Magento store",
        ],
        "Stripe": [
            "Stripe integration issue on {{domain}}?",
            "Quick Stripe fix for {{domain}}",
            "Saw something in your Stripe setup",
        ],
        "WooCommerce": [
            "WooCommerce issue on {{domain}}?",
            "Quick WooCommerce fix for {{domain}}",
            "Noticed your WooCommerce store",
        ],
    },
    "tracy@closespark.co": {
        "Shopify": [
            "Technical review: Shopify on {{domain}}",
            "Shopify integration assessment for {{domain}}",
            "Following up on your Shopify setup",
        ],
        "Salesforce": [
            "Technical review: Salesforce on {{domain}}",
            "Salesforce workflow assessment for {{domain}}",
            "Following up on your Salesforce setup",
        ],
        "WordPress": [
            "Technical review: WordPress on {{domain}}",
            "WordPress performance assessment for {{domain}}",
            "Following up on your WordPress site",
        ],
        "HubSpot": [
            "Technical review: HubSpot on {{domain}}",
            "HubSpot workflow assessment for {{domain}}",
            "Following up on your HubSpot setup",
        ],
        "Klaviyo": [
            "Technical review: Klaviyo on {{domain}}",
            "Klaviyo flow assessment for {{domain}}",
            "Following up on your Klaviyo setup",
        ],
        "Google Analytics": [
            "Technical review: Analytics on {{domain}}",
            "Analytics assessment for {{domain}}",
            "Following up on your analytics setup",
        ],
        "GA4": [
            "Technical review: GA4 on {{domain}}",
            "GA4 assessment for {{domain}}",
            "Following up on your GA4 setup",
        ],
        "Magento": [
            "Technical review: Magento on {{domain}}",
            "Magento assessment for {{domain}}",
            "Following up on your Magento store",
        ],
        "Stripe": [
            "Technical review: Stripe on {{domain}}",
            "Stripe integration assessment for {{domain}}",
            "Following up on your Stripe setup",
        ],
        "WooCommerce": [
            "Technical review: WooCommerce on {{domain}}",
            "WooCommerce assessment for {{domain}}",
            "Following up on your WooCommerce store",
        ],
    },
    "willa@closespark.co": {
        "Shopify": [
            "Hi from CloseSpark — Shopify help for {{domain}}",
            "Quick idea for your Shopify store",
            "Reaching out about your Shopify setup",
        ],
        "Salesforce": [
            "Hi from CloseSpark — Salesforce help for {{domain}}",
            "Quick idea for your Salesforce setup",
            "Reaching out about your Salesforce",
        ],
        "WordPress": [
            "Hi from CloseSpark — WordPress help for {{domain}}",
            "Quick idea for your WordPress site",
            "Reaching out about your WordPress setup",
        ],
        "HubSpot": [
            "Hi from CloseSpark — HubSpot help for {{domain}}",
            "Quick idea for your HubSpot setup",
            "Reaching out about your HubSpot",
        ],
        "Klaviyo": [
            "Hi from CloseSpark — Klaviyo help for {{domain}}",
            "Quick idea for your Klaviyo flows",
            "Reaching out about your Klaviyo setup",
        ],
        "Google Analytics": [
            "Hi from CloseSpark — Analytics help for {{domain}}",
            "Quick idea for your analytics setup",
            "Reaching out about your tracking",
        ],
        "GA4": [
            "Hi from CloseSpark — GA4 help for {{domain}}",
            "Quick idea for your GA4 setup",
            "Reaching out about your analytics",
        ],
        "Magento": [
            "Hi from CloseSpark — Magento help for {{domain}}",
            "Quick idea for your Magento store",
            "Reaching out about your Magento setup",
        ],
        "Stripe": [
            "Hi from CloseSpark — Stripe help for {{domain}}",
            "Quick idea for your Stripe integration",
            "Reaching out about your Stripe setup",
        ],
        "WooCommerce": [
            "Hi from CloseSpark — WooCommerce help for {{domain}}",
            "Quick idea for your WooCommerce store",
            "Reaching out about your WooCommerce setup",
        ],
    },
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
            "{{MainTech}} optimization help",
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

I take on small but important tasks that often get stuck in backlogs: checkout fixes, automation cleanup, event tracking repairs, webhook repairs, and form→CRM routing. I also handle segmentation, hosting/CDN configs, and API integrations. My rate is {p['hourly_rate']} for direct technical work with no agency overhead.

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
        # Fall back to legacy email generation if the detected technology
        # is not in the new 12-category system. In this case, both versions
        # use the same email body since the legacy system doesn't support A/B.
        # This is intentional for backwards compatibility with edge cases.
        legacy = generate_outreach_email(domain, technologies, profile)
        if legacy:
            return {
                "domain": domain,
                "main_tech": legacy.selected_technology,
                "category": top_tech.category,
                "subject_lines": legacy.subject_lines,
                "version_a": legacy.email_body,
                "version_b": legacy.email_body,
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


# ============================================================================
# NEW: Persona-based email generation with variants
# ============================================================================


def get_persona_for_email(from_email: str) -> dict[str, str]:
    """
    Get persona details for a given SMTP email address.
    
    Args:
        from_email: The SMTP sender email address
        
    Returns:
        Persona dictionary with name, role, and tone
    """
    persona = PERSONA_MAP.get(from_email, {
        "name": DEFAULT_PERSONA["name"],
        "role": DEFAULT_PERSONA["role"],
        "tone": DEFAULT_PERSONA["tone"],
    })
    return {
        "name": persona["name"],
        "role": persona["role"],
        "tone": persona["tone"],
        "email": from_email,
    }


def get_variant_for_tech(main_tech: str, exclude_variant_ids: list[str] | None = None) -> dict[str, Any]:
    """
    Get a random variant for a given MainTech, optionally excluding certain variants.
    
    Args:
        main_tech: The main technology name
        exclude_variant_ids: List of variant IDs to exclude (for suppression)
        
    Returns:
        Variant dictionary with id, subject_template, and bullets
    """
    variants = EMAIL_VARIANTS.get(main_tech, [])
    if not variants:
        # Create a default variant if tech not in our list
        return {
            "id": f"{main_tech.lower().replace(' ', '_')}_default",
            "subject_template": f"Quick {{{{domain}}}} question about {main_tech}",
            "bullets": [
                f"Integration and configuration issues with {main_tech}",
                "Automation and workflow problems",
                "Data sync and tracking gaps",
            ],
        }
    
    # Start with all variants, then filter if exclusions specified
    available_variants = variants
    if exclude_variant_ids:
        filtered = [v for v in variants if v["id"] not in exclude_variant_ids]
        # Only use filtered list if it's not empty (otherwise reset to all)
        if filtered:
            available_variants = filtered
    
    return random.choice(available_variants)


def get_unused_persona_for_domain(
    domain: str,
    available_personas: list[str],
    used_personas: list[str] | None = None,
) -> str | None:
    """
    Get a persona email that hasn't been used for this domain.
    
    Args:
        domain: The target domain
        available_personas: List of available persona emails
        used_personas: List of persona emails already used for this domain
        
    Returns:
        A persona email that hasn't been used, or None if all have been used
    """
    if not used_personas:
        return available_personas[0] if available_personas else None
    
    unused = [p for p in available_personas if p not in used_personas]
    if unused:
        return unused[0]
    
    # All personas have been used - return None to signal rotation complete
    return None


def select_variant_with_suppression(
    main_tech: str,
    from_email: str,
    domain_history: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Select a variant while avoiding previously used combinations for this domain.
    
    This implements variant suppression logic:
    - Don't send the same variant twice to the same domain
    - Don't send the same persona twice to the same domain
    - Prefer new combinations first
    
    Args:
        main_tech: The main technology name
        from_email: The sender email (persona)
        domain_history: Optional dict with 'used_variant_ids' and 'used_personas' lists
        
    Returns:
        Selected variant dictionary
    """
    exclude_variant_ids = []
    
    if domain_history:
        # Get previously used variant IDs for this domain
        exclude_variant_ids = domain_history.get("used_variant_ids", [])
    
    return get_variant_for_tech(main_tech, exclude_variant_ids=exclude_variant_ids)


def get_subject_for_persona_tech(
    from_email: str, 
    main_tech: str, 
    domain: str
) -> str:
    """
    Get a subject line for a persona and tech combination.
    
    Args:
        from_email: The SMTP sender email address
        main_tech: The main technology name
        domain: The target domain
        
    Returns:
        Formatted subject line
    """
    persona_subjects = SUBJECT_VARIANTS.get(from_email, {})
    tech_subjects = persona_subjects.get(main_tech, [])
    
    if tech_subjects:
        subject_template = random.choice(tech_subjects)
    else:
        # Fall back to variant-based subject
        variant = get_variant_for_tech(main_tech)
        subject_template = variant["subject_template"]
    
    return subject_template.replace("{{domain}}", domain)


def generate_persona_email_body(
    domain: str,
    main_tech: str,
    supporting_techs: list[str],
    persona: dict[str, str],
    variant: dict[str, Any],
) -> str:
    """
    Generate email body following the new format with persona and variant.
    
    Layout pattern (≤ ~150 words):
    - One-line greeting/introduction from persona + mention CloseSpark, Richmond, VA
    - One-sentence context referencing domain and MainTech
    - Short bulleted list (3 bullets) calling out what we typically fix
    - Explicit pricing line: $85/hr, short-term, no long-term commitment
    - Clear CTA to book a call via Calendly
    - Persona signature + GitHub link
    
    Args:
        domain: The target domain
        main_tech: The main technology name
        supporting_techs: List of supporting technologies
        persona: Persona dictionary with name, role, tone
        variant: Variant dictionary with bullets
        
    Returns:
        Formatted email body (plain text)
    """
    name = persona["name"]
    role = persona["role"]
    bullets = variant.get("bullets", [
        "Integration and sync issues",
        "Automation and workflow problems",
        "Tracking and analytics gaps",
    ])
    
    # Format supporting techs mention if available
    supporting_mention = ""
    if supporting_techs:
        supporting_techs_filtered = [t for t in supporting_techs if t != main_tech][:2]
        if supporting_techs_filtered:
            supporting_mention = " + " + ", ".join(supporting_techs_filtered)
    
    # Build bullet list
    bullet_list = "\n".join([f"• {bullet}" for bullet in bullets[:3]])
    
    # Build the email based on persona tone
    if persona.get("tone", "").startswith("friendly"):
        # Willa's friendly tone
        greeting = f"Hi — I'm {name} from CloseSpark in {CLOSESPARK_PROFILE['location']}."
    elif persona.get("tone", "").startswith("structured"):
        # Tracy's more formal tone
        greeting = f"Hello — I'm {name} with CloseSpark, based in {CLOSESPARK_PROFILE['location']}."
    else:
        # Scott's concise/technical tone (default)
        greeting = f"Hi — I'm {name} from CloseSpark in {CLOSESPARK_PROFILE['location']}."
    
    email = f"""{greeting}

I saw that {domain} is running {main_tech}{supporting_mention}, and I specialize in short-term technical fixes for stacks like yours.

{bullet_list}

Hourly: {CLOSESPARK_PROFILE['hourly_rate']}, strictly short-term — no long-term commitment.

If it would help to have a specialist jump in, you can grab time here:
{CLOSESPARK_PROFILE['calendly']}

– {name}
{role}, CloseSpark
{CLOSESPARK_PROFILE['github']}"""
    
    return email


@dataclass
class PersonaEmail:
    """Generated persona-based email with full metadata."""
    
    subject: str
    body: str
    main_tech: str
    supporting_techs: list[str]
    persona: str
    persona_email: str
    persona_role: str
    variant_id: str
    domain: str
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "subject": self.subject,
            "body": self.body,
            "main_tech": self.main_tech,
            "supporting_techs": self.supporting_techs,
            "persona": self.persona,
            "persona_email": self.persona_email,
            "persona_role": self.persona_role,
            "variant_id": self.variant_id,
            "domain": self.domain,
        }


def generate_persona_outreach_email(
    domain: str,
    main_tech: str,
    supporting_techs: list[str],
    from_email: str,
    domain_history: dict[str, Any] | None = None,
) -> PersonaEmail:
    """
    Generate a complete persona-based outreach email.
    
    This is the new primary email generation function that:
    - Takes main_tech, supporting_techs, domain, and persona (from_email) as inputs
    - Chooses one variant for that main_tech (random, with optional suppression)
    - Returns subject, body, and metadata including variant_id
    
    Args:
        domain: The target domain
        main_tech: The main/top technology detected
        supporting_techs: List of other detected technologies
        from_email: The SMTP sender email address (determines persona)
        domain_history: Optional dict with 'used_variant_ids' for suppression
        
    Returns:
        PersonaEmail with subject, body, and full metadata
    """
    # Get persona for this email address
    persona = get_persona_for_email(from_email)
    
    # Get a variant for this tech (with optional suppression)
    if domain_history:
        variant = select_variant_with_suppression(main_tech, from_email, domain_history)
    else:
        variant = get_variant_for_tech(main_tech)
    
    # Generate subject
    subject = get_subject_for_persona_tech(from_email, main_tech, domain)
    
    # Generate body
    body = generate_persona_email_body(
        domain=domain,
        main_tech=main_tech,
        supporting_techs=supporting_techs,
        persona=persona,
        variant=variant,
    )
    
    return PersonaEmail(
        subject=subject,
        body=body,
        main_tech=main_tech,
        supporting_techs=supporting_techs,
        persona=persona["name"],
        persona_email=from_email,
        persona_role=persona["role"],
        variant_id=variant["id"],
        domain=domain,
    )


def generate_outreach_email_with_persona(
    domain: str,
    technologies: list[str],
    from_email: str,
) -> dict[str, Any] | None:
    """
    Generate outreach email with persona and variant tracking.
    
    This extends the existing email generation to support:
    - Persona selection based on from_email
    - Variant selection for MainTech
    - Full metadata for tracking
    
    Args:
        domain: The target domain
        technologies: List of detected technology names
        from_email: The SMTP sender email address
        
    Returns:
        Dictionary with email content and metadata, or None if no tech detected
    """
    if not technologies:
        return None
    
    # Get the highest-value technology
    top_tech = get_highest_value_tech(technologies)
    if not top_tech:
        return None
    
    # Get supporting technologies (excluding the main one)
    supporting_techs = [t for t in technologies if t != top_tech.name]
    
    # Generate persona-based email
    email = generate_persona_outreach_email(
        domain=domain,
        main_tech=top_tech.name,
        supporting_techs=supporting_techs,
        from_email=from_email,
    )
    
    return email.to_dict()

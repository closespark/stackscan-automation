"""
Technology detection using Wappalyzer-style patterns.

Detects various technologies on websites including:
- Marketing automation (HubSpot, Marketo, Pardot, etc.)
- CRM systems (Salesforce, Zoho, Pipedrive)
- Ecommerce platforms (Shopify, WooCommerce, Magento)
- Analytics tools (Google Analytics, Mixpanel, Amplitude)
- Payment processors (Stripe, PayPal, Braintree)
- And more...
"""

import re
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TechDetectionResult:
    """Result of technology detection on a domain."""

    domain: str
    technologies: list[str] = field(default_factory=list)
    tech_details: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "domain": self.domain,
            "technologies": self.technologies,
            "tech_details": self.tech_details,
            "error": self.error,
        }


# Technology detection patterns organized by category
# Each pattern includes: name, category, patterns (script, html, headers), score
TECHNOLOGY_PATTERNS = {
    # ========== ENTERPRISE / HIGH-VALUE (Score: 5) ==========
    "Salesforce": {
        "category": "CRM",
        "score": 5,
        "patterns": {
            "scripts": [r"force\.com", r"salesforce\.com", r"lightning\.force\.com"],
            "html": [r"salesforce", r"_sf[a-z]+_"],
            "js_vars": [r"SfdcApp", r"sforce"],
        },
    },
    "Marketo": {
        "category": "Marketing Automation",
        "score": 5,
        "patterns": {
            "scripts": [r"munchkin\.marketo\.net", r"marketo\.com"],
            "html": [r"mktoForm", r"marketo"],
            "js_vars": [r"Munchkin", r"MktoForms2"],
        },
    },
    "HubSpot": {
        "category": "Marketing Automation",
        "score": 5,
        "patterns": {
            "scripts": [
                r"js\.hs-scripts\.com",
                r"js\.hs-analytics\.net",
                r"js\.hsforms\.net",
                r"js\.hscta\.net",
            ],
            "html": [
                r"hs-cos-wrapper",
                r"hubspot",
                r"<!--\s*Start of Async HubSpot",
                r'id=["\']?hs-eu-cookie-confirmation',
            ],
            "headers": {"x-powered-by": r"hubspot", "x-hs-hub-id": r".+"},
            "js_vars": [r"_hsq", r"hbspt", r"HubSpotConversations"],
        },
    },
    "Segment": {
        "category": "Customer Data Platform",
        "score": 5,
        "patterns": {
            "scripts": [r"cdn\.segment\.com", r"api\.segment\.io"],
            "js_vars": [r"analytics\.identify", r"analytics\.track"],
        },
    },
    "Magento": {
        "category": "Ecommerce",
        "score": 5,
        "patterns": {
            "scripts": [r"mage/", r"magento"],
            "html": [r"Magento", r"mage-translation", r"/static/version"],
            "headers": {"x-magento-": r".+"},
            "js_vars": [r"Mage\."],
        },
    },
    "Pardot": {
        "category": "Marketing Automation",
        "score": 5,
        "patterns": {
            "scripts": [r"pi\.pardot\.com", r"pardot\.com"],
            "html": [r"pardot"],
            "js_vars": [r"piAId", r"piCId"],
        },
    },
    "Optimizely": {
        "category": "A/B Testing",
        "score": 5,
        "patterns": {
            "scripts": [r"cdn\.optimizely\.com", r"optimizely\.com"],
            "js_vars": [r"optimizely"],
        },
    },
    # ========== ECOMMERCE + PAYMENTS + ADVANCED (Score: 4) ==========
    "Shopify": {
        "category": "Ecommerce",
        "score": 4,
        "patterns": {
            "scripts": [r"cdn\.shopify\.com", r"shopify\.com"],
            "html": [r"shopify", r"Shopify\.theme"],
            "headers": {"x-shopify-": r".+"},
            "js_vars": [r"Shopify\.", r"ShopifyAnalytics"],
        },
    },
    "BigCommerce": {
        "category": "Ecommerce",
        "score": 4,
        "patterns": {
            "scripts": [r"bigcommerce\.com", r"cdn\.bcapp"],
            "html": [r"bigcommerce"],
            "headers": {"x-bc-": r".+"},
        },
    },
    "Stripe": {
        "category": "Payment Processor",
        "score": 4,
        "patterns": {
            "scripts": [r"js\.stripe\.com", r"stripe\.com"],
            "js_vars": [r"Stripe\("],
        },
    },
    "PayPal": {
        "category": "Payment Processor",
        "score": 4,
        "patterns": {
            "scripts": [r"paypal\.com", r"paypalobjects\.com"],
            "html": [r"paypal"],
            "js_vars": [r"paypal\."],
        },
    },
    "Braintree": {
        "category": "Payment Processor",
        "score": 4,
        "patterns": {
            "scripts": [r"braintree", r"braintreegateway\.com"],
            "js_vars": [r"braintree\."],
        },
    },
    "Klaviyo": {
        "category": "Email Marketing",
        "score": 4,
        "patterns": {
            "scripts": [r"klaviyo\.com", r"static\.klaviyo\.com"],
            "js_vars": [r"_learnq", r"klaviyo"],
        },
    },
    "Mixpanel": {
        "category": "Analytics",
        "score": 4,
        "patterns": {
            "scripts": [r"cdn\.mxpnl\.com", r"mixpanel\.com"],
            "js_vars": [r"mixpanel\."],
        },
    },
    "Amplitude": {
        "category": "Analytics",
        "score": 4,
        "patterns": {
            "scripts": [r"cdn\.amplitude\.com", r"amplitude\.com"],
            "js_vars": [r"amplitude\."],
        },
    },
    "VWO": {
        "category": "A/B Testing",
        "score": 4,
        "patterns": {
            "scripts": [r"dev\.visualwebsiteoptimizer\.com", r"vwo\.com"],
            "js_vars": [r"_vwo_", r"VWO"],
        },
    },
    "Square": {
        "category": "Payment Processor",
        "score": 4,
        "patterns": {
            "scripts": [r"squareup\.com", r"square\.com"],
            "js_vars": [r"Square\."],
        },
    },
    # ========== MAINSTREAM CMS + MARKETING (Score: 3) ==========
    "WordPress": {
        "category": "CMS",
        "score": 3,
        "patterns": {
            "scripts": [r"wp-content", r"wp-includes"],
            "html": [r"wordpress", r"wp-content", r"wp-json"],
            "headers": {"x-powered-by": r"wordpress", "link": r"wp-json"},
        },
    },
    "WooCommerce": {
        "category": "Ecommerce",
        "score": 3,
        "patterns": {
            "scripts": [r"woocommerce"],
            "html": [r"woocommerce", r"wc-"],
            "js_vars": [r"wc_add_to_cart", r"woocommerce"],
        },
    },
    "Mailchimp": {
        "category": "Email Marketing",
        "score": 3,
        "patterns": {
            "scripts": [r"chimpstatic\.com", r"mailchimp\.com", r"list-manage\.com"],
            "html": [r"mailchimp", r"mc-embedded"],
            "js_vars": [r"MailchimpSubscribe"],
        },
    },
    "SendGrid": {
        "category": "Email Marketing",
        "score": 3,
        "patterns": {
            "scripts": [r"sendgrid\.com", r"sendgrid\.net"],
            "html": [r"sendgrid"],
        },
    },
    "ActiveCampaign": {
        "category": "Marketing Automation",
        "score": 3,
        "patterns": {
            "scripts": [r"activecampaign\.com", r"trackcmp\.net"],
            "js_vars": [r"ActiveCampaign", r"_ac"],
        },
    },
    "Intercom": {
        "category": "Live Chat",
        "score": 3,
        "patterns": {
            "scripts": [r"widget\.intercom\.io", r"intercom\.com"],
            "js_vars": [r"Intercom\(", r"intercomSettings"],
        },
    },
    "Drift": {
        "category": "Live Chat",
        "score": 3,
        "patterns": {
            "scripts": [r"js\.driftt\.com", r"drift\.com"],
            "js_vars": [r"drift\.", r"driftt"],
        },
    },
    "Zendesk Chat": {
        "category": "Live Chat",
        "score": 3,
        "patterns": {
            "scripts": [r"zopim\.com", r"zendesk\.com"],
            "js_vars": [r"\$zopim", r"zESettings"],
        },
    },
    "Freshchat": {
        "category": "Live Chat",
        "score": 3,
        "patterns": {
            "scripts": [r"wchat\.freshchat\.com", r"freshchat\.com"],
            "js_vars": [r"fcWidget"],
        },
    },
    "Zoho": {
        "category": "CRM",
        "score": 3,
        "patterns": {
            "scripts": [r"zoho\.com", r"salesiq\.zoho"],
            "html": [r"zoho"],
            "js_vars": [r"\$zoho"],
        },
    },
    "Pipedrive": {
        "category": "CRM",
        "score": 3,
        "patterns": {
            "scripts": [r"pipedrive\.com", r"leadbooster-chat\.pipedrive"],
            "js_vars": [r"pipedrive"],
        },
    },
    "Webflow": {
        "category": "CMS",
        "score": 3,
        "patterns": {
            "scripts": [r"webflow\.com"],
            "html": [r"webflow", r'data-wf-'],
            "headers": {"x-webflow-": r".+"},
        },
    },
    # ========== INFRASTRUCTURE (Score: 2) ==========
    "AWS": {
        "category": "Infrastructure",
        "score": 2,
        "patterns": {
            "scripts": [r"amazonaws\.com", r"cloudfront\.net"],
            "headers": {"x-amz-": r".+", "server": r"AmazonS3"},
        },
    },
    "Vercel": {
        "category": "Infrastructure",
        "score": 2,
        "patterns": {
            "headers": {"x-vercel-": r".+", "server": r"Vercel"},
        },
    },
    "Netlify": {
        "category": "Infrastructure",
        "score": 2,
        "patterns": {
            "headers": {"x-nf-": r".+", "server": r"Netlify"},
        },
    },
    "Cloudflare": {
        "category": "Infrastructure",
        "score": 2,
        "patterns": {
            "headers": {"cf-ray": r".+", "server": r"cloudflare"},
        },
    },
    "nginx": {
        "category": "Web Server",
        "score": 2,
        "patterns": {
            "headers": {"server": r"nginx"},
        },
    },
    "Apache": {
        "category": "Web Server",
        "score": 2,
        "patterns": {
            "headers": {"server": r"Apache"},
        },
    },
    # ========== BASIC ANALYTICS (Score: 1) ==========
    "Google Analytics": {
        "category": "Analytics",
        "score": 1,
        "patterns": {
            "scripts": [
                r"google-analytics\.com",
                r"googletagmanager\.com",
                r"gtag/js",
            ],
            "js_vars": [r"ga\(", r"gtag\(", r"_gaq"],
        },
    },
    "GA4": {
        "category": "Analytics",
        "score": 1,
        "patterns": {
            "scripts": [r"gtag/js\?id=G-"],
            "js_vars": [r"gtag\("],
        },
    },
    "Google Optimize": {
        "category": "A/B Testing",
        "score": 1,
        "patterns": {
            "scripts": [r"optimize\.google\.com", r"googlesyndication"],
            "js_vars": [r"dataLayer"],
        },
    },
    "Heap": {
        "category": "Analytics",
        "score": 1,
        "patterns": {
            "scripts": [r"heapanalytics\.com", r"heap-"],
            "js_vars": [r"heap\."],
        },
    },
    "Hotjar": {
        "category": "Analytics",
        "score": 1,
        "patterns": {
            "scripts": [r"hotjar\.com", r"static\.hotjar\.com"],
            "js_vars": [r"hj\(", r"_hjSettings"],
        },
    },
}


class TechDetector:
    """Detector for various technologies on websites."""

    def __init__(self):
        """Initialize the technology detector."""
        self.patterns = TECHNOLOGY_PATTERNS

    def detect(
        self,
        domain: str,
        html_content: str,
        headers: dict[str, str] | None = None,
    ) -> TechDetectionResult:
        """
        Detect technologies in HTML content and headers.

        Args:
            domain: The domain being scanned
            html_content: The HTML content of the page
            headers: Optional HTTP response headers

        Returns:
            TechDetectionResult with detected technologies
        """
        detected_techs = []
        tech_details = []
        headers = headers or {}

        # Pre-process headers to lowercase keys for efficient matching
        headers_lower = {k.lower(): v for k, v in headers.items()}

        for tech_name, tech_info in self.patterns.items():
            matched = False
            matched_patterns = []

            patterns = tech_info.get("patterns", {})

            # Check script patterns
            for pattern in patterns.get("scripts", []):
                if re.search(pattern, html_content, re.IGNORECASE):
                    matched = True
                    matched_patterns.append(f"script: {pattern}")

            # Check HTML patterns
            for pattern in patterns.get("html", []):
                if re.search(pattern, html_content, re.IGNORECASE):
                    matched = True
                    matched_patterns.append(f"html: {pattern}")

            # Check JS variable patterns
            for pattern in patterns.get("js_vars", []):
                if re.search(pattern, html_content, re.IGNORECASE):
                    matched = True
                    matched_patterns.append(f"js: {pattern}")

            # Check header patterns (using pre-processed lowercase headers)
            header_patterns = patterns.get("headers", {})
            for header_name, header_pattern in header_patterns.items():
                header_name_lower = header_name.lower()
                # Check for exact match or prefix match
                for h_name, h_value in headers_lower.items():
                    if h_name.startswith(header_name_lower):
                        if re.search(header_pattern, h_value, re.IGNORECASE):
                            matched = True
                            matched_patterns.append(f"header: {header_name}")
                            break

            if matched:
                detected_techs.append(tech_name)
                tech_details.append(
                    {
                        "name": tech_name,
                        "category": tech_info["category"],
                        "score": tech_info["score"],
                        "matched_patterns": matched_patterns,
                    }
                )

        return TechDetectionResult(
            domain=domain,
            technologies=detected_techs,
            tech_details=tech_details,
        )

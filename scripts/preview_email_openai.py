#!/usr/bin/env python3
"""
Email Preview CLI Tool with OpenAI Rewrite Comparison.

Generates and displays a comparison of raw (pre-rewrite) and OpenAI-rewritten
(post-rewrite) outreach emails. Useful for QA and testing the OpenAI
deliverability optimization.

Usage:
    python scripts/preview_email_openai.py --tech Shopify --from scott@closespark.co --domain some-shop.com
    python scripts/preview_email_openai.py --tech Salesforce --from tracy@closespark.co --domain example.com --supporting Stripe Klaviyo

Examples:
    # Basic preview with Shopify and Scott persona
    python scripts/preview_email_openai.py --tech Shopify --from scott@closespark.co --domain some-shop.com

    # Preview with supporting techs
    python scripts/preview_email_openai.py --tech Klaviyo --from willa@closespark.co --domain example.com --supporting Shopify Stripe
"""

import argparse
import json
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from stackscanner.email_generator import (
    generate_persona_outreach_email,
    get_persona_for_email,
    get_variant_for_tech,
    COMPANY_PROFILE,
)
from stackscanner.openai_email_rewriter import rewrite_email_with_openai


def build_raw_email(domain, tech, supporting_techs, from_email):
    """
    Rebuild the *raw* email (pre-OpenAI rewrite) using the same logic
    inside generate_persona_outreach_email, but stopping before rewrite.
    """
    persona_cfg = get_persona_for_email(from_email)
    persona_name = persona_cfg["name"]
    persona_role = persona_cfg["role"]

    variant = get_variant_for_tech(tech)
    variant_id = variant["id"]

    # Get company profile values
    company_name = COMPANY_PROFILE["company"]
    company_location = COMPANY_PROFILE["location"]
    company_hourly_rate = COMPANY_PROFILE["hourly_rate"]
    company_calendly = COMPANY_PROFILE["calendly"]
    company_github = COMPANY_PROFILE["github"]

    # subject --------------------------------------------------
    subject_tmpl = variant.get("subject_template", "{{tech}} issue on {{domain}}?")
    subject = (
        subject_tmpl.replace("{{tech}}", tech)
        .replace("{{domain}}", domain)
        .replace("{{persona}}", persona_name)
    )

    # body -----------------------------------------------------
    tech_str = tech
    if supporting_techs:
        tech_str = ", ".join([tech] + supporting_techs)

    lines = []
    lines.append(f"Hi — I'm {persona_name} from {company_name} in {company_location}.")
    lines.append(f"I noticed {domain} is running {tech_str}.")

    bullets = variant.get("bullets", [])
    if bullets:
        lines.append("")
        for b in bullets:
            lines.append(f"• {b}")

    lines.append("")
    lines.append(
        f"I handle short-term technical fixes at {company_hourly_rate}, no long-term commitments."
    )
    lines.append(f"If you'd like help, you can book time here: {company_calendly}")

    lines.append("")
    lines.append(f"– {persona_name}")
    lines.append(f"{persona_role}, {company_name}")
    if company_github:
        lines.append(company_github)

    body = "\n".join(lines)

    return subject, body, persona_cfg, variant_id


def main():
    parser = argparse.ArgumentParser(
        description="Preview pre-rewrite and post-rewrite outreach emails."
    )
    parser.add_argument("--tech", required=True, help="Main technology (Shopify, Salesforce, etc.)")
    parser.add_argument("--from", dest="from_email", required=True, help="Persona inbox email")
    parser.add_argument("--domain", required=True, help="Target domain")
    parser.add_argument("--supporting", nargs="*", default=None, help="Optional supporting techs")
    args = parser.parse_args()

    domain = args.domain
    main_tech = args.tech
    from_email = args.from_email
    supporting = args.supporting

    # Get company profile values
    company_name = COMPANY_PROFILE["company"]
    company_location = COMPANY_PROFILE["location"]
    company_hourly_rate = COMPANY_PROFILE["hourly_rate"]

    print("\n===================================================")
    print("PREVIEW: Email Generation (Pre + Post OpenAI Rewrite)")
    print("===================================================\n")

    # -------------------------------
    # 1. Build RAW / PRE-REWRITE
    # -------------------------------
    raw_subject, raw_body, persona_cfg, variant_id = build_raw_email(
        domain, main_tech, supporting, from_email
    )

    persona_name = persona_cfg["name"]
    persona_role = persona_cfg["role"]

    context = {
        "domain": domain,
        "persona": persona_name,
        "persona_email": from_email,
        "persona_role": persona_role,
        "company_name": company_name,
        "company_location": company_location,
        "company_rate": company_hourly_rate,
        "main_tech": main_tech,
        "variant_id": variant_id,
    }

    # -------------------------------
    # 2. OpenAI REWRITE
    # -------------------------------
    new_subject, new_body, meta = rewrite_email_with_openai(
        subject=raw_subject,
        body=raw_body,
        context=context,
    )

    # -------------------------------
    # DISPLAY OUTPUT
    # -------------------------------

    print("-------------")
    print("RAW SUBJECT")
    print("-------------")
    print(raw_subject)

    print("\n-------------")
    print("RAW BODY")
    print("-------------")
    print(raw_body)

    print("\n\n===============================================")
    print("OPENAI-REWRITTEN (DELIVERABILITY OPTIMIZED)")
    print("===============================================\n")

    print("-------------")
    print("REWRITTEN SUBJECT")
    print("-------------")
    print(new_subject)

    print("\n-------------")
    print("REWRITTEN BODY")
    print("-------------")
    print(new_body)

    print("\n\n---------------------------------------------")
    print("METADATA")
    print("---------------------------------------------")
    print(json.dumps(meta, indent=2))

    print("\nDone.\n")


if __name__ == "__main__":
    main()

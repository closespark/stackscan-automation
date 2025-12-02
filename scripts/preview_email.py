#!/usr/bin/env python3
"""
Email Preview CLI Tool.

Generates and displays a preview of outreach emails without sending them.
Useful for QA and testing different persona/tech/variant combinations.

Usage:
    python scripts/preview_email.py --tech Shopify --from scott@closespark.co
    python scripts/preview_email.py --tech Salesforce --from tracy@closespark.co --domain example.com
    python scripts/preview_email.py --tech WordPress --from willa@closespark.co --supporting Mailchimp GA4

Examples:
    # Basic preview with Shopify and Scott persona
    python scripts/preview_email.py --tech Shopify --from scott@closespark.co

    # Preview with custom domain and supporting techs
    python scripts/preview_email.py --tech HubSpot --from tracy@closespark.co --domain acme-corp.com --supporting Salesforce Stripe

    # Generate multiple variants for comparison
    python scripts/preview_email.py --tech Klaviyo --from willa@closespark.co --count 3
"""

import argparse
import sys
from pathlib import Path
from typing import Any

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from hubspot_scanner.email_generator import (
    generate_persona_outreach_email,
    get_persona_for_email,
    get_variant_for_tech,
    PERSONA_MAP,
    EMAIL_VARIANTS,
    CLOSESPARK_PROFILE,
)


def print_separator(char: str = "=", length: int = 70) -> None:
    """Print a separator line."""
    print(char * length)


def print_email_preview(
    tech: str,
    from_email: str,
    domain: str = "example-domain.com",
    supporting_techs: list[str] | None = None,
) -> dict[str, Any]:
    """
    Generate and display an email preview.
    
    Args:
        tech: Main technology name
        from_email: Sender email (determines persona)
        domain: Target domain
        supporting_techs: List of supporting technologies
        
    Returns:
        Dictionary with email details
    """
    if supporting_techs is None:
        supporting_techs = []
    
    # Generate the email
    email = generate_persona_outreach_email(
        domain=domain,
        main_tech=tech,
        supporting_techs=supporting_techs,
        from_email=from_email,
    )
    
    # Get persona details
    persona = get_persona_for_email(from_email)
    
    # Display the preview
    print_separator("=")
    print("EMAIL PREVIEW")
    print_separator("=")
    print()
    
    # Metadata section
    print("METADATA")
    print_separator("-", 40)
    print(f"  Domain:           {email.domain}")
    print(f"  Main Tech:        {email.main_tech}")
    print(f"  Supporting Techs: {', '.join(email.supporting_techs) if email.supporting_techs else 'None'}")
    print(f"  Persona:          {email.persona}")
    print(f"  Persona Email:    {email.persona_email}")
    print(f"  Persona Role:     {email.persona_role}")
    print(f"  Persona Tone:     {persona.get('tone', 'N/A')}")
    print(f"  Variant ID:       {email.variant_id}")
    print()
    
    # Subject
    print("SUBJECT")
    print_separator("-", 40)
    print(f"  {email.subject}")
    print()
    
    # Body
    print("BODY")
    print_separator("-", 40)
    for line in email.body.split("\n"):
        print(f"  {line}")
    print()
    
    # Stats
    word_count = len(email.body.split())
    char_count = len(email.body)
    line_count = len(email.body.split("\n"))
    
    print("STATS")
    print_separator("-", 40)
    print(f"  Word count:  {word_count}")
    print(f"  Char count:  {char_count}")
    print(f"  Line count:  {line_count}")
    print()
    
    print_separator("=")
    
    return email.to_dict()


def list_available_options() -> None:
    """Display all available personas and technologies."""
    print_separator("=")
    print("AVAILABLE OPTIONS")
    print_separator("=")
    print()
    
    print("PERSONAS")
    print_separator("-", 40)
    for email, persona in PERSONA_MAP.items():
        print(f"  {email}")
        print(f"    Name: {persona['name']}")
        print(f"    Role: {persona['role']}")
        print(f"    Tone: {persona['tone']}")
        print()
    
    print("TECHNOLOGIES (with variants)")
    print_separator("-", 40)
    for tech, variants in EMAIL_VARIANTS.items():
        variant_ids = [v["id"] for v in variants]
        print(f"  {tech}: {', '.join(variant_ids)}")
    print()
    
    print_separator("=")


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Preview outreach emails without sending them.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python scripts/preview_email.py --tech Shopify --from scott@closespark.co
    python scripts/preview_email.py --tech Salesforce --from tracy@closespark.co --domain example.com
    python scripts/preview_email.py --tech WordPress --from willa@closespark.co --supporting Mailchimp GA4
    python scripts/preview_email.py --list
        """,
    )
    
    parser.add_argument(
        "--tech",
        type=str,
        help="Main technology (e.g., Shopify, Salesforce, WordPress)",
    )
    
    parser.add_argument(
        "--from",
        dest="from_email",
        type=str,
        help="Sender email address (determines persona)",
    )
    
    parser.add_argument(
        "--domain",
        type=str,
        default="example-domain.com",
        help="Target domain (default: example-domain.com)",
    )
    
    parser.add_argument(
        "--supporting",
        nargs="*",
        default=[],
        help="Supporting technologies (e.g., --supporting Stripe Klaviyo)",
    )
    
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Number of variants to generate (for comparison)",
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available personas and technologies",
    )
    
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of formatted text",
    )
    
    args = parser.parse_args()
    
    # Handle --list
    if args.list:
        list_available_options()
        return 0
    
    # Validate required args
    if not args.tech:
        print("Error: --tech is required. Use --list to see available options.")
        return 1
    
    if not args.from_email:
        print("Error: --from is required. Use --list to see available personas.")
        return 1
    
    # Validate persona
    if args.from_email not in PERSONA_MAP:
        print(f"Warning: '{args.from_email}' is not a known persona. Using default.")
    
    # Validate tech
    if args.tech not in EMAIL_VARIANTS:
        print(f"Warning: '{args.tech}' is not in EMAIL_VARIANTS. A default variant will be used.")
    
    # Generate preview(s)
    if args.json:
        import json
        results = []
        for _ in range(args.count):
            email = generate_persona_outreach_email(
                domain=args.domain,
                main_tech=args.tech,
                supporting_techs=args.supporting,
                from_email=args.from_email,
            )
            results.append(email.to_dict())
        print(json.dumps(results if args.count > 1 else results[0], indent=2))
    else:
        for i in range(args.count):
            if args.count > 1:
                print(f"\n{'#' * 70}")
                print(f"# VARIANT {i + 1} of {args.count}")
                print(f"{'#' * 70}\n")
            
            print_email_preview(
                tech=args.tech,
                from_email=args.from_email,
                domain=args.domain,
                supporting_techs=args.supporting,
            )
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

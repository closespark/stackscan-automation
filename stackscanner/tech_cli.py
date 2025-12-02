#!/usr/bin/env python3
"""Command-line interface for Technology Scanner with Email Generation."""

import argparse
import json
import sys
from typing import TextIO

from .tech_scanner import scan_technologies, scan_technologies_batch


def parse_domains_file(file_path: str) -> list[str]:
    """
    Parse domains from a file (one domain per line).

    Args:
        file_path: Path to the domains file

    Returns:
        List of domains
    """
    domains = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            # Skip empty lines and comments
            if line and not line.startswith("#"):
                domains.append(line)
    return domains


def output_results(
    results: list[dict],
    output_file: TextIO | None = None,
    pretty: bool = True,
) -> None:
    """
    Output results as JSON.

    Args:
        results: List of scan results
        output_file: Optional file to write to (defaults to stdout)
        pretty: Whether to pretty-print the JSON
    """
    output = output_file or sys.stdout
    indent = 2 if pretty else None
    json.dump(results, output, indent=indent)
    output.write("\n")


def print_progress(current: int, total: int, domain: str) -> None:
    """Print progress to stderr."""
    sys.stderr.write(f"\rScanning {current}/{total}: {domain}...")
    sys.stderr.flush()
    if current == total:
        sys.stderr.write("\n")


def print_summary(results: list[dict]) -> None:
    """Print a summary of scan results to stderr."""
    total = len(results)
    with_techs = sum(1 for r in results if r.get("technologies"))
    errors = sum(1 for r in results if r.get("error"))
    all_techs = set()
    for r in results:
        all_techs.update(r.get("technologies", []))

    sys.stderr.write(f"\n{'='*60}\n")
    sys.stderr.write(f"Technology Scan Summary\n")
    sys.stderr.write(f"{'='*60}\n")
    sys.stderr.write(f"Total domains scanned: {total}\n")
    sys.stderr.write(f"Domains with technologies: {with_techs}\n")
    sys.stderr.write(f"Unique technologies found: {len(all_techs)}\n")
    sys.stderr.write(f"Errors: {errors}\n")

    if with_techs > 0:
        sys.stderr.write(f"\nTop Technologies Detected:\n")
        tech_counts = {}
        for r in results:
            for tech in r.get("technologies", []):
                tech_counts[tech] = tech_counts.get(tech, 0) + 1

        # Sort by count
        sorted_techs = sorted(tech_counts.items(), key=lambda x: x[1], reverse=True)
        for tech, count in sorted_techs[:10]:
            sys.stderr.write(f"  - {tech}: {count} domains\n")

        sys.stderr.write(f"\nEmails Generated:\n")
        for r in results:
            if r.get("generated_email"):
                email_data = r["generated_email"]
                sys.stderr.write(f"  - {r['domain']}: {email_data.get('selected_technology')}\n")
                sys.stderr.write(f"    Subject: {email_data.get('subject_lines', [''])[0]}\n")


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Detect technologies on websites and generate outreach emails",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan a single domain
  tech-scanner example.com

  # Scan multiple domains
  tech-scanner example.com another-site.com shopify.com

  # Scan domains from a file
  tech-scanner -f domains.txt

  # Save results to a file
  tech-scanner -f domains.txt -o results.json

  # Quiet mode (no progress output)
  tech-scanner -f domains.txt -q

  # Skip email generation
  tech-scanner example.com --no-email

Output Format:
  {
    "domain": "example.com",
    "technologies": ["Shopify", "Stripe", "Klaviyo"],
    "scored_technologies": [...],
    "top_technology": {"name": "Shopify", "score": 4, ...},
    "generated_email": {
      "selected_technology": "Shopify",
      "subject_lines": [...],
      "email_body": "..."
    }
  }
        """,
    )

    parser.add_argument(
        "domains",
        nargs="*",
        help="Domain(s) to scan",
    )

    parser.add_argument(
        "-f",
        "--file",
        dest="domains_file",
        help="File containing domains to scan (one per line)",
    )

    parser.add_argument(
        "-o",
        "--output",
        dest="output_file",
        help="Output file for JSON results (default: stdout)",
    )

    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=10,
        help="Request timeout in seconds (default: 10)",
    )

    parser.add_argument(
        "--user-agent",
        dest="user_agent",
        default=None,
        help="Custom user agent string",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Quiet mode - suppress progress output",
    )

    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Suppress summary output",
    )

    parser.add_argument(
        "--compact",
        action="store_true",
        help="Output compact JSON (no pretty-printing)",
    )

    parser.add_argument(
        "--no-email",
        action="store_true",
        help="Skip email generation",
    )

    parser.add_argument(
        "--name",
        default=None,
        help="Consultant name for email (default: Chris)",
    )

    parser.add_argument(
        "--location",
        default=None,
        help="Consultant location for email (default: Richmond, VA)",
    )

    parser.add_argument(
        "--rate",
        default=None,
        help="Hourly rate for email (default: $85/hr)",
    )

    parser.add_argument(
        "--github",
        default=None,
        help="GitHub URL for email",
    )

    parser.add_argument(
        "--calendly",
        default=None,
        help="Calendly URL for email",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )

    args = parser.parse_args()

    # Collect domains
    domains = list(args.domains) if args.domains else []

    if args.domains_file:
        try:
            file_domains = parse_domains_file(args.domains_file)
            domains.extend(file_domains)
        except FileNotFoundError:
            sys.stderr.write(f"Error: File not found: {args.domains_file}\n")
            return 1
        except IOError as e:
            sys.stderr.write(f"Error reading file: {e}\n")
            return 1

    if not domains:
        parser.print_help()
        sys.stderr.write("\nError: No domains specified\n")
        return 1

    # Remove duplicates while preserving order
    seen = set()
    unique_domains = []
    for d in domains:
        if d.lower() not in seen:
            seen.add(d.lower())
            unique_domains.append(d)
    domains = unique_domains

    # Build consultant profile if any custom values provided
    consultant_profile = None
    if any([args.name, args.location, args.rate, args.github, args.calendly]):
        from .email_generator import CONSULTANT_PROFILE
        consultant_profile = dict(CONSULTANT_PROFILE)
        if args.name:
            consultant_profile["name"] = args.name
        if args.location:
            consultant_profile["location"] = args.location
        if args.rate:
            consultant_profile["hourly_rate"] = args.rate
        if args.github:
            consultant_profile["github"] = args.github
        if args.calendly:
            consultant_profile["calendly"] = args.calendly

    # Set up scanning options
    scan_kwargs = {
        "timeout": args.timeout,
        "generate_email": not args.no_email,
        "consultant_profile": consultant_profile,
    }
    if args.user_agent:
        scan_kwargs["user_agent"] = args.user_agent

    # Progress callback
    progress_callback = None if args.quiet else print_progress

    # Scan domains
    results = scan_technologies_batch(
        domains,
        progress_callback=progress_callback,
        **scan_kwargs,
    )

    # Output results
    output_file = None
    if args.output_file:
        try:
            output_file = open(args.output_file, "w", encoding="utf-8")
        except IOError as e:
            sys.stderr.write(f"Error opening output file: {e}\n")
            return 1

    try:
        output_results(results, output_file, pretty=not args.compact)
    finally:
        if output_file:
            output_file.close()

    # Print summary
    if not args.no_summary and not args.quiet:
        print_summary(results)

    return 0


if __name__ == "__main__":
    sys.exit(main())

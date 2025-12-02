#!/usr/bin/env python3
"""Command-line interface for HubSpot Presence Scanner."""

import argparse
import json
import sys
from typing import TextIO

from .scanner import scan_domains, scan_domain


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
        results: List of detection results
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
    detected = sum(1 for r in results if r["hubspot_detected"])
    errors = sum(1 for r in results if r["error"])
    total_emails = sum(len(r.get("emails", [])) for r in results)

    sys.stderr.write(f"\n{'='*50}\n")
    sys.stderr.write(f"Scan Summary\n")
    sys.stderr.write(f"{'='*50}\n")
    sys.stderr.write(f"Total domains scanned: {total}\n")
    sys.stderr.write(f"HubSpot detected: {detected}\n")
    sys.stderr.write(f"Non-generic emails found: {total_emails}\n")
    sys.stderr.write(f"Errors: {errors}\n")

    if detected > 0:
        sys.stderr.write(f"\nDomains with HubSpot:\n")
        for r in results:
            if r["hubspot_detected"]:
                portal_info = ""
                if r["portal_ids"]:
                    portal_info = f" (Portal IDs: {', '.join(r['portal_ids'])})"
                sys.stderr.write(
                    f"  - {r['domain']} (confidence: {r['confidence_score']}%){portal_info}\n"
                )
                if r.get("emails"):
                    sys.stderr.write(f"    Emails: {', '.join(r['emails'])}\n")


def main() -> int:
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Detect HubSpot presence on websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan a single domain
  hubspot-scanner example.com

  # Scan multiple domains
  hubspot-scanner example.com another-site.com hubspot.com

  # Scan domains from a file
  hubspot-scanner -f domains.txt

  # Save results to a file
  hubspot-scanner -f domains.txt -o results.json

  # Quiet mode (no progress output)
  hubspot-scanner -f domains.txt -q

  # Skip email extraction (faster)
  hubspot-scanner example.com --no-emails

  # Crawl more pages for emails
  hubspot-scanner example.com --max-pages 20
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
        "--no-emails",
        action="store_true",
        help="Skip email extraction (faster scanning)",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum pages to crawl for emails per domain (default: 10)",
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

    # Set up scanning options
    scan_kwargs = {
        "timeout": args.timeout,
        "crawl_emails": not args.no_emails,
        "max_pages": args.max_pages,
    }
    if args.user_agent:
        scan_kwargs["user_agent"] = args.user_agent

    # Progress callback
    progress_callback = None if args.quiet else print_progress

    # Scan domains
    results = scan_domains(domains, progress_callback=progress_callback, **scan_kwargs)

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

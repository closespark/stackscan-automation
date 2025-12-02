#!/usr/bin/env python3
"""
Example: Batch scanning multiple domains.

This example demonstrates how to scan multiple domains and process results.
"""

from stackscanner import scan_domains
import json


def progress(current, total, domain):
    """Display progress."""
    print(f"[{current}/{total}] Scanning: {domain}")


def main():
    # List of domains to scan
    domains = [
        "hubspot.com",
        "example.com",
        "github.com",
    ]

    print(f"Scanning {len(domains)} domains...\n")

    # Scan all domains with progress callback
    # Emails are automatically extracted when HubSpot is detected
    results = scan_domains(
        domains,
        timeout=15,
        progress_callback=progress,
        crawl_emails=True,  # Default: True
        max_pages=10,       # Default: 10
    )

    # Separate results by detection status
    hubspot_sites = [r for r in results if r["hubspot_detected"]]
    non_hubspot_sites = [r for r in results if not r["hubspot_detected"] and not r["error"]]
    error_sites = [r for r in results if r["error"]]

    # Print summary
    print(f"\n{'='*50}")
    print("SCAN RESULTS")
    print(f"{'='*50}")

    print(f"\nHubSpot Detected ({len(hubspot_sites)}):")
    for site in hubspot_sites:
        print(f"  ✓ {site['domain']} (confidence: {site['confidence_score']}%)")
        if site["portal_ids"]:
            print(f"    Portal IDs: {', '.join(site['portal_ids'])}")
        if site.get("emails"):
            print(f"    Emails: {', '.join(site['emails'])}")

    print(f"\nNo HubSpot ({len(non_hubspot_sites)}):")
    for site in non_hubspot_sites:
        print(f"  ✗ {site['domain']}")

    if error_sites:
        print(f"\nErrors ({len(error_sites)}):")
        for site in error_sites:
            print(f"  ! {site['domain']}: {site['error']}")

    # Save full results to JSON
    output_file = "/tmp/scan_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nFull results saved to: {output_file}")


if __name__ == "__main__":
    main()

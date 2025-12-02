#!/usr/bin/env python3
"""
Example: Basic usage of the HubSpot Presence Scanner.

This example demonstrates how to scan a single domain programmatically.
"""

from stackscanner import scan_domain
import json


def main():
    # Scan a single domain
    print("Scanning hubspot.com...")
    result = scan_domain("hubspot.com")

    # Print the result as formatted JSON
    print("\nResult:")
    print(json.dumps(result.to_dict(), indent=2))

    # Access individual fields
    print(f"\nHubSpot Detected: {result.hubspot_detected}")
    print(f"Confidence Score: {result.confidence_score}%")

    if result.signals:
        print(f"\nDetected {len(result.signals)} signal(s):")
        for signal in result.signals:
            print(f"  - {signal['name']}: {signal['description']}")

    if result.portal_ids:
        print(f"\nPortal IDs found: {', '.join(result.portal_ids)}")

    if result.emails:
        print(f"\nNon-generic emails found: {', '.join(result.emails)}")
    else:
        print("\nNo non-generic emails found")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Example: Technology Scanner with Email Generation

This example shows how to:
1. Scan a website for technologies (Wappalyzer-style detection)
2. Score technologies by value
3. Generate personalized outreach emails
"""

import json

from stackscanner import (
    scan_technologies,
    scan_technologies_batch,
    TechDetector,
    score_technologies,
    get_highest_value_tech,
    generate_outreach_email,
    TECH_SCORES,
    CONSULTANT_PROFILE,
)


def example_single_domain():
    """Scan a single domain and generate email."""
    print("=" * 60)
    print("Example 1: Single Domain Scan")
    print("=" * 60)

    # Scan the domain
    result = scan_technologies("shopify.com")

    print(f"\nDomain: {result.domain}")
    print(f"Technologies detected: {result.technologies}")

    if result.top_technology:
        print(f"\nTop technology:")
        print(f"  Name: {result.top_technology['name']}")
        print(f"  Score: {result.top_technology['score']}")
        print(f"  Category: {result.top_technology['category']}")

    if result.generated_email:
        email = result.generated_email
        print(f"\nGenerated Email:")
        print(f"  Selected tech: {email['selected_technology']}")
        print(f"  Subject lines:")
        for subj in email['subject_lines']:
            print(f"    - {subj}")
        print(f"\n  Email body:\n{email['email_body']}")


def example_batch_scan():
    """Scan multiple domains."""
    print("\n" + "=" * 60)
    print("Example 2: Batch Domain Scan")
    print("=" * 60)

    domains = [
        "shopify.com",
        "hubspot.com",
        "stripe.com",
    ]

    def progress(current, total, domain):
        print(f"  Scanning {current}/{total}: {domain}")

    results = scan_technologies_batch(
        domains,
        progress_callback=progress,
    )

    print("\nResults Summary:")
    for r in results:
        if r.get("technologies"):
            print(f"  {r['domain']}: {', '.join(r['technologies'][:5])}")
            if r.get("top_technology"):
                print(f"    → Top: {r['top_technology']['name']} (score: {r['top_technology']['score']})")


def example_custom_profile():
    """Generate email with custom consultant profile."""
    print("\n" + "=" * 60)
    print("Example 3: Custom Consultant Profile")
    print("=" * 60)

    # Custom profile
    my_profile = {
        "name": "Alex",
        "location": "Austin, TX",
        "hourly_rate": "$100/hr",
        "github": "https://github.com/myprofile",
        "calendly": "https://calendly.com/myprofile/call",
        "positioning": "freelance automation expert",
    }

    # Generate email
    technologies = ["Klaviyo", "Shopify", "Stripe"]
    email = generate_outreach_email("example-store.com", technologies, my_profile)

    if email:
        print(f"\nGenerated for: {email.selected_technology}")
        print(f"\nEmail body:\n{email.email_body}")


def example_technology_scoring():
    """Demonstrate technology scoring."""
    print("\n" + "=" * 60)
    print("Example 4: Technology Scoring")
    print("=" * 60)

    technologies = [
        "Google Analytics",  # Score: 1
        "WordPress",         # Score: 3
        "Stripe",            # Score: 4
        "Salesforce",        # Score: 5
        "Mailchimp",         # Score: 3
    ]

    print(f"\nTechnologies: {technologies}")
    print("\nScored (highest first):")

    scored = score_technologies(technologies)
    for tech in scored:
        print(f"  {tech.name}: score={tech.score}, category={tech.category}")

    top = get_highest_value_tech(technologies)
    print(f"\nHighest value: {top.name} (score: {top.score})")


def example_programmatic_detection():
    """Demonstrate programmatic technology detection."""
    print("\n" + "=" * 60)
    print("Example 5: Programmatic Detection")
    print("=" * 60)

    # Example HTML content
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://cdn.shopify.com/s/trekkie.js"></script>
        <script src="https://js.stripe.com/v3/"></script>
        <script src="https://static.klaviyo.com/onsite/js/klaviyo.js"></script>
        <script>
            (function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
            (i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
            m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
            })(window,document,'script','https://www.google-analytics.com/analytics.js','ga');
        </script>
    </head>
    <body></body>
    </html>
    """

    # Detect technologies
    detector = TechDetector()
    result = detector.detect("example.com", html_content)

    print(f"\nDetected technologies: {result.technologies}")
    print("\nDetails:")
    for tech in result.tech_details:
        print(f"  {tech['name']}:")
        print(f"    Category: {tech['category']}")
        print(f"    Score: {tech['score']}")
        print(f"    Matched: {tech['matched_patterns'][:2]}")


def example_json_output():
    """Show JSON output format."""
    print("\n" + "=" * 60)
    print("Example 6: JSON Output Format")
    print("=" * 60)

    technologies = ["Shopify", "Klaviyo", "Stripe", "Google Analytics"]
    email = generate_outreach_email("example-store.com", technologies)

    if email:
        output = {
            "selected_technology": email.selected_technology,
            "recent_project": email.recent_project,
            "subject_lines": email.subject_lines,
            "email_body": email.email_body,
        }
        print("\nJSON Output:")
        print(json.dumps(output, indent=2))


if __name__ == "__main__":
    example_single_domain()
    example_batch_scan()
    example_custom_profile()
    example_technology_scoring()
    example_programmatic_detection()
    example_json_output()

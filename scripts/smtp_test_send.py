#!/usr/bin/env python3
"""
SMTP Test Send Script.

Loops through all SMTP inboxes, generates real outreach emails using the existing
email generation pipeline, and sends them to a test recipient for validation.

This script imports the real email generator to produce authentic outreach emails
(subject + body) from the template system, allowing you to see exactly how emails
look in Gmail per sender and per detected tech.

Environment Variables Required:
    SMTP_ACCOUNTS_JSON: JSON object with "inboxes" array containing SMTP configs

Usage:
    python scripts/smtp_test_send.py
"""

import json
import os
import random
import smtplib
import ssl
from email.mime.text import MIMEText

# Import the real email generator from the hubspot_scanner package
from hubspot_scanner import generate_email_ab


TEST_RECIPIENT = os.getenv("SMTP_TEST_RECIPIENT", "christabb@gmail.com")

# Test domains with different tech stacks so each mailbox sends a unique tech email
TEST_DOMAINS = [
    {
        "domain": "sample-shopify-store.com",
        "main_tech": "Shopify",
        "tech_list": ["Shopify", "Klaviyo", "Stripe", "Google Analytics"],
    },
    {
        "domain": "b2b-salesforce-firm.com",
        "main_tech": "Salesforce",
        "tech_list": ["Salesforce", "HubSpot", "Intercom", "SendGrid"],
    },
    {
        "domain": "wordpress-local-service.com",
        "main_tech": "WordPress",
        "tech_list": ["WordPress", "WooCommerce", "Mailchimp", "Hotjar"],
    },
]


def load_accounts():
    """
    Load SMTP accounts from environment variable.

    Returns:
        List of inbox configurations

    Raises:
        ValueError: If SMTP_ACCOUNTS_JSON is not set or invalid
    """
    raw = os.getenv("SMTP_ACCOUNTS_JSON")
    if not raw:
        raise ValueError("SMTP_ACCOUNTS_JSON not set")

    try:
        data = json.loads(raw)
        return data["inboxes"]
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed parsing SMTP_ACCOUNTS_JSON: {e}")
    except KeyError:
        raise ValueError("SMTP_ACCOUNTS_JSON must contain 'inboxes' key")


def generate_test_email(test_data):
    """
    Generate a real outreach email using the existing email generation pipeline.

    Args:
        test_data: Dict with domain, main_tech, and tech_list

    Returns:
        Tuple of (subject, body)
    """
    main_tech = test_data["main_tech"]
    tech_list = test_data["tech_list"]

    # Generate email using the real generator
    email_result = generate_email_ab(
        main_tech=main_tech,
        detected_techs=tech_list,
    )

    if email_result is None:
        # Fallback if tech is not recognized
        subject = f"Quick {main_tech} question"
        body = f"""Hey there,

I noticed you're using {main_tech} and wanted to reach out.

We help businesses like yours get more out of their tech stack.

Would you be open to a quick 15-minute call this week?

Best regards
"""
        return subject, body

    # Pick a random subject line from the generated options
    subject = random.choice(email_result.subject_lines)

    # Use version_a (rate version) for testing - you can switch to version_b if preferred
    body = email_result.version_a

    return subject, body


def send_email(inbox, subject, body):
    """
    Send an email via SMTP.

    Args:
        inbox: Dict with email, smtp_host, smtp_port, smtp_user, smtp_password
        subject: Email subject
        body: Email body text
    """
    email = inbox["email"]
    host = inbox["smtp_host"]
    port = inbox["smtp_port"]
    user = inbox["smtp_user"]
    password = inbox["smtp_password"]

    print(f"\n===============================")
    print(f"Sending REAL outreach template from {email}")
    print("===============================")

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = email
    msg["To"] = TEST_RECIPIENT

    try:
        print(f"Connecting to SMTP: {host}:{port} ...")
        server = smtplib.SMTP(host, port, timeout=20)
        context = ssl.create_default_context()
        server.starttls(context=context)

        print("Logging in...")
        server.login(user, password)

        print("Sending email...")
        server.sendmail(email, [TEST_RECIPIENT], msg.as_string())

        server.quit()
        print("✅ SUCCESS — sent!")
    except smtplib.SMTPException as e:
        print(f"❌ ERROR — SMTP error: {e}")
    except Exception as e:
        print(f"❌ ERROR — {e}")


def main():
    """Main entry point for the SMTP test send script."""
    print("=" * 50)
    print("SMTP TEST SEND SCRIPT")
    print("=" * 50)
    print(f"Test recipient: {TEST_RECIPIENT}")
    print()

    print("Loading SMTP accounts...")
    try:
        accounts = load_accounts()
    except ValueError as e:
        print(f"❌ ERROR: {e}")
        return

    if len(accounts) != len(TEST_DOMAINS):
        print(f"ℹ️  INFO: {len(accounts)} inboxes, {len(TEST_DOMAINS)} test domains.")
        print("   Each inbox will use one of the test tech stacks (cycling through).")

    print(f"Loaded {len(accounts)} SMTP inboxes\n")

    for i, inbox in enumerate(accounts):
        test_data = TEST_DOMAINS[i % len(TEST_DOMAINS)]

        print(f"\n--- Inbox {i + 1}/{len(accounts)} ---")
        print(f"From: {inbox['email']}")
        print(f"Tech: {test_data['main_tech']}")
        print(f"Domain: {test_data['domain']}")

        # Generate real outreach email using the pipeline
        subject, body = generate_test_email(test_data)

        print(f"Subject: {subject}")
        print("-" * 40)

        send_email(inbox, subject, body)

    print("\n" + "=" * 50)
    print("SMTP TEST SEND COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    main()

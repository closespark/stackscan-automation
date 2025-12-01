#!/usr/bin/env python3
"""
Outreach Worker for Automated Email Sending.

This script sends personalized outreach emails to HubSpot-detected leads:
1. Pulls leads from Supabase (hubspot_detected=true, has emails, not emailed)
2. Rotates through Zapmail pre-warmed SMTP inboxes
3. Sends personalized emails with rate limiting
4. Marks leads as emailed in Supabase

Environment Variables Required:
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_SERVICE_KEY: Your Supabase service role key
    SMTP_ACCOUNTS_JSON: JSON array of SMTP inbox configurations

Optional Environment Variables:
    OUTREACH_TABLE: Table with leads (default: hubspot_scans)
    OUTREACH_DAILY_LIMIT: Max emails per day (default: 500)
    OUTREACH_PER_INBOX_LIMIT: Max emails per inbox (default: 50)
    OUTREACH_EMAIL_TEMPLATE: Path to email template (default: templates/outreach_email.txt)
    OUTREACH_SUBJECT: Email subject line
    SMTP_SEND_DELAY_SECONDS: Delay between emails (default: 4)
"""

import json
import os
import smtplib
import ssl
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from supabase import create_client


# ========================
# ENV VARIABLES
# ========================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

OUTREACH_TABLE = os.getenv("OUTREACH_TABLE", "hubspot_scans")
DAILY_LIMIT = int(os.getenv("OUTREACH_DAILY_LIMIT", "500"))
PER_INBOX_LIMIT = int(os.getenv("OUTREACH_PER_INBOX_LIMIT", "50"))
EMAIL_TEMPLATE_PATH = os.getenv(
    "OUTREACH_EMAIL_TEMPLATE", "templates/outreach_email.txt"
)
EMAIL_SUBJECT = os.getenv("OUTREACH_SUBJECT", "Quick question about your website")

SEND_DELAY = int(os.getenv("SMTP_SEND_DELAY_SECONDS", "4"))


def get_supabase_client():
    """Create and return a Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required"
        )
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_smtp_fleet() -> list[dict]:
    """
    Load SMTP accounts from environment variable.

    Note: For production deployments, use Render's secret environment variables
    or a dedicated secret management service to store SMTP credentials securely.
    Never log or expose the SMTP_ACCOUNTS_JSON value.
    """
    smtp_json = os.getenv("SMTP_ACCOUNTS_JSON", "[]")
    try:
        fleet = json.loads(smtp_json)
        if not fleet:
            print("[OUTREACH] Warning: No SMTP accounts configured")
        else:
            # Log inbox count without exposing credentials
            print(f"[OUTREACH] Loaded {len(fleet)} SMTP accounts")
        return fleet
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid SMTP_ACCOUNTS_JSON: {e}")
        return []


# ========================
# Load template
# ========================


def load_template() -> str:
    """Load the email template from file."""
    try:
        with open(EMAIL_TEMPLATE_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        print(f"[ERROR] Template file not found: {EMAIL_TEMPLATE_PATH}")
        # Return a default template
        return """Hi,

I noticed you're using HubSpot on {{domain}} and wanted to reach out.

We help businesses like yours get more out of their HubSpot investment.

Would you be open to a quick 15-minute call this week?

Best regards
"""


# ========================
# Fetch leads
# ========================


def fetch_leads(supabase) -> list[dict]:
    """
    Pull leads who:
    - hubspot_detected = true
    - emails not empty
    - not emailed yet

    Args:
        supabase: Supabase client instance

    Returns:
        List of lead records with valid emails
    """
    query = (
        supabase.table(OUTREACH_TABLE)
        .select("*")
        .eq("hubspot_detected", True)
        .is_("emailed", "null")
        .limit(DAILY_LIMIT)
        .execute()
    )

    # Filter to only leads with non-empty email arrays
    leads = [
        lead for lead in (query.data or [])
        if lead.get("emails")
    ]
    print(f"[OUTREACH] Loaded {len(leads)} available leads with emails")
    return leads


# ========================
# SMTP Rotation Engine
# ========================


def send_email_smtp(
    smtp_conf: dict,
    to_email: str,
    subject: str,
    body: str,
) -> bool:
    """
    Send an email through SMTP.

    Args:
        smtp_conf: SMTP configuration dict with host, port, user, pass
        to_email: Recipient email address
        subject: Email subject
        body: Email body text

    Returns:
        True if email was sent successfully
    """
    msg = MIMEMultipart()
    msg["From"] = smtp_conf["user"]
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    context = ssl.create_default_context()

    try:
        with smtplib.SMTP(smtp_conf["host"], smtp_conf.get("port", 587)) as server:
            server.starttls(context=context)
            server.login(smtp_conf["user"], smtp_conf["pass"])
            server.sendmail(msg["From"], [msg["To"]], msg.as_string())
        return True
    except smtplib.SMTPException as e:
        print(f"[ERROR] SMTP error: {e}")
        return False


def mark_lead_emailed(supabase, lead_id: str) -> None:
    """
    Mark a lead as emailed in Supabase.

    Args:
        supabase: Supabase client instance
        lead_id: The lead's ID
    """
    supabase.table(OUTREACH_TABLE).update(
        {
            "emailed": True,
            "emailed_at": datetime.now(timezone.utc).isoformat(),
        }
    ).eq("id", lead_id).execute()


# ========================
# Main Outreach Logic
# ========================


def run_outreach() -> dict[str, int]:
    """
    Run the outreach process.

    Returns:
        Statistics dictionary with sent/failed counts
    """
    print("[OUTREACH] Starting outreach run")
    print(f"[OUTREACH] Date: {datetime.now(timezone.utc).isoformat()}")
    print(f"[OUTREACH] Daily limit: {DAILY_LIMIT}")
    print(f"[OUTREACH] Per-inbox limit: {PER_INBOX_LIMIT}")

    # Initialize
    supabase = get_supabase_client()
    smtp_fleet = get_smtp_fleet()

    if not smtp_fleet:
        print("[OUTREACH] No SMTP accounts configured. Exiting.")
        return {"sent": 0, "failed": 0, "skipped": 0}

    print(f"[OUTREACH] SMTP fleet size: {len(smtp_fleet)} inboxes")

    template = load_template()
    leads = fetch_leads(supabase)

    if not leads:
        print("[OUTREACH] No leads to send today.")
        return {"sent": 0, "failed": 0, "skipped": 0}

    stats = {"sent": 0, "failed": 0, "skipped": 0}
    leads_iterator = iter(leads)
    current_lead = None

    for smtp_conf in smtp_fleet:
        sent_this_inbox = 0
        inbox_email = smtp_conf.get("user", "unknown")
        print(f"\n[OUTREACH] Switching to inbox: {inbox_email}")

        while sent_this_inbox < PER_INBOX_LIMIT and stats["sent"] < DAILY_LIMIT:
            # Get next lead if we don't have one
            if current_lead is None:
                try:
                    current_lead = next(leads_iterator)
                except StopIteration:
                    print("[OUTREACH] No more leads to process.")
                    break

            lead = current_lead
            current_lead = None  # Mark as consumed

            # Get email list
            email_list = lead.get("emails") or []
            if not email_list:
                stats["skipped"] += 1
                continue

            # Pick first valid email
            recipient = email_list[0]
            domain = lead.get("domain", "your website")
            lead_id = lead.get("id")

            # Personalize the email
            personalized_body = template.replace("{{domain}}", domain)

            try:
                success = send_email_smtp(
                    smtp_conf,
                    to_email=recipient,
                    subject=EMAIL_SUBJECT,
                    body=personalized_body,
                )

                if success:
                    print(f"  ✓ Sent to {recipient} via {inbox_email}")
                    mark_lead_emailed(supabase, lead_id)
                    stats["sent"] += 1
                    sent_this_inbox += 1
                else:
                    print(f"  ✗ Failed to send to {recipient}")
                    stats["failed"] += 1

            except Exception as e:
                print(f"  ✗ Error sending to {recipient}: {e}")
                stats["failed"] += 1

            # Throttle to prevent rate limiting
            time.sleep(SEND_DELAY)

        # Check if we've hit the daily limit
        if stats["sent"] >= DAILY_LIMIT:
            print("[OUTREACH] Hit global daily limit.")
            break

    # Print summary
    print("\n" + "=" * 50)
    print("[OUTREACH] Outreach run complete")
    print(f"  Sent: {stats['sent']}")
    print(f"  Failed: {stats['failed']}")
    print(f"  Skipped: {stats['skipped']}")
    print("=" * 50)

    return stats


if __name__ == "__main__":
    run_outreach()

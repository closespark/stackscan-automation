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
    SMTP_SEND_DELAY_SECONDS: Delay between emails (default: 4)
    LOG_LEVEL: Logging level (default: INFO)
"""

import json
import logging
import os
import smtplib
import ssl
import sys
import time
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from supabase import create_client

from hubspot_scanner.email_generator import (
    generate_outreach_email_with_persona,
    get_persona_for_email,
    CLOSESPARK_PROFILE,
)


# ---------- LOGGING SETUP ----------

def setup_logging():
    """Configure logging for Render deployment with detailed output."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    
    # Create formatter with timestamp, level, and message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level, logging.INFO))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Add stdout handler (Render captures stdout)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    root_logger.addHandler(stdout_handler)
    
    return logging.getLogger("outreach")


# Initialize logger
logger = setup_logging()


# ========================
# ENV VARIABLES
# ========================

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

OUTREACH_TABLE = os.getenv("OUTREACH_TABLE", "hubspot_scans")
DAILY_LIMIT = int(os.getenv("OUTREACH_DAILY_LIMIT", "500"))
PER_INBOX_LIMIT = int(os.getenv("OUTREACH_PER_INBOX_LIMIT", "50"))

SEND_DELAY = int(os.getenv("SMTP_SEND_DELAY_SECONDS", "4"))


def log_config():
    """Log current configuration (without sensitive values)."""
    logger.info("=" * 60)
    logger.info("OUTREACH CONFIGURATION")
    logger.info("=" * 60)
    logger.info(f"  SUPABASE_URL: {'[SET]' if SUPABASE_URL else '[NOT SET]'}")
    logger.info(f"  SUPABASE_SERVICE_KEY: {'[SET]' if SUPABASE_KEY else '[NOT SET]'}")
    logger.info(f"  OUTREACH_TABLE: {OUTREACH_TABLE}")
    logger.info(f"  DAILY_LIMIT: {DAILY_LIMIT}")
    logger.info(f"  PER_INBOX_LIMIT: {PER_INBOX_LIMIT}")
    logger.info(f"  SEND_DELAY: {SEND_DELAY} seconds")
    logger.info(f"  SMTP_ACCOUNTS_JSON: {'[SET]' if os.getenv('SMTP_ACCOUNTS_JSON') else '[NOT SET]'}")
    logger.info("=" * 60)


def get_supabase_client():
    """Create and return a Supabase client."""
    logger.info("Initializing Supabase client...")
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("Missing required environment variables: SUPABASE_URL and/or SUPABASE_SERVICE_KEY")
        raise ValueError(
            "SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables are required"
        )
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logger.info("Supabase client initialized successfully")
    return client


def get_smtp_fleet() -> list[dict]:
    """
    Load SMTP accounts from environment variable.

    Supports two formats:
    1. New format with 'inboxes' key:
       {"inboxes": [{"email": "...", "smtp_host": "...", "smtp_port": 587, 
                     "smtp_user": "...", "smtp_password": "..."}]}
    2. Legacy format (array directly):
       [{"user": "...", "host": "...", "port": 587, "pass": "..."}]

    Note: For production deployments, use Render's secret environment variables
    or a dedicated secret management service to store SMTP credentials securely.
    Never log or expose the SMTP_ACCOUNTS_JSON value.
    """
    logger.info("Loading SMTP accounts from environment...")
    smtp_json = os.getenv("SMTP_ACCOUNTS_JSON", "[]")
    try:
        data = json.loads(smtp_json)
        
        # Check if it's the new format with 'inboxes' key
        if isinstance(data, dict) and "inboxes" in data:
            raw_fleet = data["inboxes"]
        else:
            # Legacy format - array directly
            raw_fleet = data
        
        if not raw_fleet:
            logger.warning("No SMTP accounts configured in SMTP_ACCOUNTS_JSON")
            return []
        
        # Normalize to internal format
        fleet = []
        for account in raw_fleet:
            # Handle new format keys
            if "smtp_user" in account:
                normalized = {
                    "email": account.get("email", account.get("smtp_user")),
                    "user": account.get("smtp_user"),
                    "host": account.get("smtp_host", "smtp.gmail.com"),
                    "port": account.get("smtp_port", 587),
                    "pass": account.get("smtp_password"),
                }
            else:
                # Legacy format
                normalized = {
                    "email": account.get("user"),
                    "user": account.get("user"),
                    "host": account.get("host", "smtp.gmail.com"),
                    "port": account.get("port", 587),
                    "pass": account.get("pass"),
                }
            fleet.append(normalized)
        
        # Log inbox count without exposing credentials
        logger.info(f"Loaded {len(fleet)} SMTP accounts successfully")
        for i, account in enumerate(fleet):
            # Mask email for privacy
            email = account.get("email", "unknown")
            masked_email = email[:3] + "***" + email[email.find("@"):] if "@" in email else email[:3] + "***"
            logger.debug(f"  Account {i+1}: {masked_email} @ {account.get('host', 'unknown')}:{account.get('port', 587)}")
        return fleet
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in SMTP_ACCOUNTS_JSON: {e}")
        return []


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
    logger.info("=" * 60)
    logger.info("STEP: FETCHING LEADS FROM SUPABASE")
    logger.info("=" * 60)
    logger.info(f"Querying table: {OUTREACH_TABLE}")
    logger.info("Filters: hubspot_detected=true, emailed=null")
    logger.info(f"Limit: {DAILY_LIMIT}")
    
    start_time = time.time()
    query = (
        supabase.table(OUTREACH_TABLE)
        .select("*")
        .eq("hubspot_detected", True)
        .is_("emailed", "null")
        .limit(DAILY_LIMIT)
        .execute()
    )
    elapsed = time.time() - start_time
    
    logger.info(f"Supabase query completed in {elapsed:.2f} seconds")
    logger.info(f"Total leads returned: {len(query.data or [])}")

    # Filter to only leads with non-empty email arrays
    leads = [
        lead for lead in (query.data or [])
        if lead.get("emails")
    ]
    
    leads_without_emails = len(query.data or []) - len(leads)
    logger.info(f"Leads with emails: {len(leads)}")
    logger.info(f"Leads without emails (skipped): {leads_without_emails}")
    
    if leads:
        # Log sample leads (first 5)
        logger.debug(f"Sample leads (first 5):")
        for i, lead in enumerate(leads[:5]):
            logger.debug(f"  {i+1}. {lead.get('domain')} - emails: {lead.get('emails', [])}")
    
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
        logger.debug(f"Connecting to SMTP server {smtp_conf['host']}:{smtp_conf.get('port', 587)}...")
        with smtplib.SMTP(smtp_conf["host"], smtp_conf.get("port", 587)) as server:
            server.starttls(context=context)
            logger.debug("STARTTLS established, logging in...")
            server.login(smtp_conf["user"], smtp_conf["pass"])
            logger.debug(f"Sending email to {to_email}...")
            server.sendmail(msg["From"], [msg["To"]], msg.as_string())
        logger.debug("Email sent successfully")
        return True
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error while sending to {to_email}: {e}")
        return False


def mark_lead_emailed(supabase, lead_id: str) -> None:
    """
    Mark a lead as emailed in Supabase.

    Args:
        supabase: Supabase client instance
        lead_id: The lead's ID
    """
    logger.debug(f"Marking lead {lead_id} as emailed in Supabase...")
    supabase.table(OUTREACH_TABLE).update(
        {
            "emailed": True,
            "emailed_at": datetime.now(timezone.utc).isoformat(),
        }
    ).eq("id", lead_id).execute()
    logger.debug(f"Lead {lead_id} marked as emailed")


# ========================
# Main Outreach Logic
# ========================


def run_outreach() -> dict[str, int]:
    """
    Run the outreach process.

    Returns:
        Statistics dictionary with sent/failed counts
    """
    outreach_start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("OUTREACH WORKER STARTING")
    logger.info("=" * 60)
    logger.info(f"Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
    logger.info(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    
    # Log configuration
    log_config()

    try:
        # Initialize
        logger.info("=" * 60)
        logger.info("STEP: INITIALIZING CLIENTS")
        logger.info("=" * 60)
        supabase = get_supabase_client()
        smtp_fleet = get_smtp_fleet()

        if not smtp_fleet:
            logger.warning("No SMTP accounts configured. Exiting outreach worker.")
            return {"sent": 0, "failed": 0, "skipped": 0}

        logger.info(f"SMTP fleet ready: {len(smtp_fleet)} inboxes available")
        logger.info(f"Maximum capacity: {len(smtp_fleet) * PER_INBOX_LIMIT} emails (fleet × per-inbox limit)")

        leads = fetch_leads(supabase)

        if not leads:
            logger.info("No leads available to send emails to.")
            logger.info("Outreach worker completed (no work needed)")
            return {"sent": 0, "failed": 0, "skipped": 0}

        logger.info("=" * 60)
        logger.info("STEP: SENDING EMAILS")
        logger.info("=" * 60)
        logger.info(f"Leads to process: {len(leads)}")
        logger.info(f"Daily limit: {DAILY_LIMIT}")
        logger.info(f"Per-inbox limit: {PER_INBOX_LIMIT}")
        logger.info(f"Delay between emails: {SEND_DELAY} seconds")

        stats = {"sent": 0, "failed": 0, "skipped": 0}
        leads_iterator = iter(leads)
        current_lead = None
        inbox_number = 0

        for smtp_conf in smtp_fleet:
            sent_this_inbox = 0
            inbox_number += 1
            from_email = smtp_conf.get("email", smtp_conf.get("user", "unknown"))
            masked_inbox = from_email[:3] + "***" + from_email[from_email.find("@"):] if "@" in from_email else from_email[:3] + "***"
            
            # Get persona for this inbox
            persona = get_persona_for_email(from_email)
            logger.info("-" * 60)
            logger.info(f"INBOX {inbox_number}/{len(smtp_fleet)}: {masked_inbox}")
            logger.info(f"  Persona: {persona['name']} ({persona['role']})")
            logger.info(f"  Host: {smtp_conf.get('host', 'unknown')}:{smtp_conf.get('port', 587)}")
            logger.info(f"  Capacity: {PER_INBOX_LIMIT} emails")

            while sent_this_inbox < PER_INBOX_LIMIT and stats["sent"] < DAILY_LIMIT:
                # Get next lead if we don't have one
                if current_lead is None:
                    try:
                        current_lead = next(leads_iterator)
                    except StopIteration:
                        logger.info("  No more leads to process from queue")
                        break

                lead = current_lead
                current_lead = None  # Mark as consumed

                # Get email list
                email_list = lead.get("emails") or []
                if not email_list:
                    stats["skipped"] += 1
                    logger.debug(f"  Skipping lead {lead.get('domain')} - no emails")
                    continue

                # Pick first valid email
                recipient = email_list[0]
                domain = lead.get("domain", "your website")
                lead_id = lead.get("id")
                
                # Get technologies from the lead (for persona-based email generation)
                # Try different possible field names for technologies
                technologies = (
                    lead.get("technologies") or 
                    lead.get("scored_technologies") or 
                    []
                )
                
                # Extract tech names if they're dicts
                if technologies and isinstance(technologies[0], dict):
                    technologies = [t.get("name", str(t)) for t in technologies]
                
                # Get top_technology as main tech if available
                top_tech = lead.get("top_technology")
                main_tech = None
                if top_tech:
                    if isinstance(top_tech, dict):
                        main_tech = top_tech.get("name")
                    elif isinstance(top_tech, str):
                        main_tech = top_tech
                
                # If no main_tech but we have technologies, use first one
                if not main_tech and technologies:
                    main_tech = technologies[0]
                
                # Generate persona-based email
                email_data = None
                if main_tech:
                    email_data = generate_outreach_email_with_persona(
                        domain=domain,
                        technologies=technologies if technologies else [main_tech],
                        from_email=from_email,
                    )
                
                if email_data:
                    subject = email_data["subject"]
                    body = email_data["body"]
                    variant_id = email_data.get("variant_id", "unknown")
                else:
                    # Fallback to simple email if no tech data
                    subject = f"Quick question about {domain}"
                    body = f"""Hi — I'm {persona['name']} from CloseSpark in {CLOSESPARK_PROFILE['location']}.

I came across {domain} and wanted to reach out. I specialize in short-term technical fixes for web stacks — integration issues, automation gaps, and tracking problems.

• Integration and sync issues between tools
• Automation and workflow problems
• Tracking and analytics gaps

Hourly: {CLOSESPARK_PROFILE['hourly_rate']}, strictly short-term — no long-term commitment.

If it would help to have a specialist jump in, you can grab time here:
{CLOSESPARK_PROFILE['calendly']}

– {persona['name']}
{persona['role']}, CloseSpark
{CLOSESPARK_PROFILE['github']}"""
                    variant_id = "fallback"

                try:
                    email_start_time = time.time()
                    success = send_email_smtp(
                        smtp_conf,
                        to_email=recipient,
                        subject=subject,
                        body=body,
                    )
                    email_elapsed = time.time() - email_start_time

                    if success:
                        logger.info(f"  ✓ [{stats['sent']+1}] Sent to {recipient} (domain: {domain}, variant: {variant_id}) [{email_elapsed:.1f}s]")
                        mark_lead_emailed(supabase, lead_id)
                        stats["sent"] += 1
                        sent_this_inbox += 1
                    else:
                        logger.warning(f"  ✗ Failed to send to {recipient} (domain: {domain})")
                        stats["failed"] += 1

                except Exception as e:
                    logger.error(f"  ✗ Error sending to {recipient}: {e}")
                    stats["failed"] += 1

                # Throttle to prevent rate limiting
                if sent_this_inbox < PER_INBOX_LIMIT and stats["sent"] < DAILY_LIMIT:
                    logger.debug(f"  Waiting {SEND_DELAY} seconds before next email...")
                    time.sleep(SEND_DELAY)

            logger.info(f"  Inbox {inbox_number} complete: {sent_this_inbox} emails sent")

            # Check if we've hit the daily limit
            if stats["sent"] >= DAILY_LIMIT:
                logger.info("Hit global daily limit. Stopping email sending.")
                break

        # Print summary
        outreach_elapsed = time.time() - outreach_start_time
        
        logger.info("=" * 60)
        logger.info("OUTREACH RUN COMPLETE")
        logger.info("=" * 60)
        logger.info(f"  Total time: {outreach_elapsed:.1f} seconds")
        logger.info(f"  Emails sent: {stats['sent']}")
        logger.info(f"  Emails failed: {stats['failed']}")
        logger.info(f"  Leads skipped: {stats['skipped']}")
        if stats['sent'] > 0:
            avg_time = outreach_elapsed / stats['sent']
            logger.info(f"  Average time per email: {avg_time:.1f} seconds")
        logger.info("=" * 60)
        logger.info("Outreach worker finished successfully!")

        return stats
        
    except Exception as e:
        outreach_elapsed = time.time() - outreach_start_time
        logger.error("=" * 60)
        logger.error("OUTREACH FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {e}")
        logger.error(f"Time before failure: {outreach_elapsed:.1f} seconds")
        logger.exception("Full traceback:")
        raise


if __name__ == "__main__":
    run_outreach()

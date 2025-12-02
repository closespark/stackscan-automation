#!/usr/bin/env python3
"""
Calendly Sync Module for Lead Conversion Tracking.

This module integrates with the Calendly API to:
1. Fetch scheduled events (meetings booked)
2. Extract invitee email addresses
3. Match emails against leads in Supabase
4. Update lead records with booking status and metadata

Environment Variables Required:
    CALENDLY_API_TOKEN: Your Calendly Personal Access Token
    SUPABASE_URL: Your Supabase project URL
    SUPABASE_SERVICE_KEY: Your Supabase service role key

Optional Environment Variables:
    CALENDLY_SYNC_TABLE: Table for sync results (default: tech_scans)
    CALENDLY_BOOKINGS_TABLE: Table for booking records (default: calendly_bookings)
    CALENDLY_LOOKBACK_DAYS: Days to look back for events (default: 7)
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from supabase import create_client

# Calendly API Base URL
CALENDLY_API_BASE = "https://api.calendly.com"

# Logger
logger = logging.getLogger("calendly_sync")


class CalendlyClient:
    """Client for interacting with the Calendly API v2."""

    def __init__(self, api_token: str):
        """
        Initialize the Calendly client.

        Args:
            api_token: Calendly Personal Access Token
        """
        self.api_token = api_token
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }
        self._user_uri: str | None = None
        self._organization_uri: str | None = None

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Make a request to the Calendly API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (e.g., /users/me)
            params: Query parameters

        Returns:
            JSON response as dictionary

        Raises:
            requests.HTTPError: If the request fails
        """
        url = f"{CALENDLY_API_BASE}{endpoint}"
        response = requests.request(
            method=method,
            url=url,
            headers=self.headers,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_current_user(self) -> dict[str, Any]:
        """
        Get the current user's information.

        Returns:
            User resource dictionary containing uri, organization, etc.
        """
        response = self._request("GET", "/users/me")
        return response.get("resource", {})

    @property
    def user_uri(self) -> str:
        """Get the current user's URI (cached after first call)."""
        if self._user_uri is None:
            user = self.get_current_user()
            self._user_uri = user.get("uri", "")
        return self._user_uri

    @property
    def organization_uri(self) -> str:
        """Get the current user's organization URI (cached after first call)."""
        if self._organization_uri is None:
            user = self.get_current_user()
            self._organization_uri = user.get("current_organization", "")
        return self._organization_uri

    def list_scheduled_events(
        self,
        min_start_time: datetime | None = None,
        max_start_time: datetime | None = None,
        status: str = "active",
        count: int = 100,
    ) -> list[dict[str, Any]]:
        """
        List scheduled events for the current user.

        Args:
            min_start_time: Minimum event start time (UTC)
            max_start_time: Maximum event start time (UTC)
            status: Event status filter ('active' or 'canceled')
            count: Number of results per page (max 100)

        Returns:
            List of scheduled event resources
        """
        params = {
            "user": self.user_uri,
            "status": status,
            "count": min(count, 100),
        }

        if min_start_time:
            params["min_start_time"] = min_start_time.isoformat()
        if max_start_time:
            params["max_start_time"] = max_start_time.isoformat()

        all_events = []
        page_token = None

        while True:
            if page_token:
                params["page_token"] = page_token

            response = self._request("GET", "/scheduled_events", params=params)
            events = response.get("collection", [])
            all_events.extend(events)

            # Check for next page
            pagination = response.get("pagination", {})
            page_token = pagination.get("next_page_token")

            if not page_token:
                break

            logger.debug(f"Fetched {len(all_events)} events so far, getting next page...")

        return all_events

    def get_event_invitees(self, event_uri: str) -> list[dict[str, Any]]:
        """
        Get all invitees for a scheduled event.

        Args:
            event_uri: The full URI of the scheduled event

        Returns:
            List of invitee resources with email, name, status, etc.
        """
        # Extract event UUID from URI
        # URI format: https://api.calendly.com/scheduled_events/{uuid}
        event_uuid = event_uri.split("/")[-1]
        endpoint = f"/scheduled_events/{event_uuid}/invitees"

        all_invitees = []
        page_token = None

        while True:
            params = {"count": 100}
            if page_token:
                params["page_token"] = page_token

            response = self._request("GET", endpoint, params=params)
            invitees = response.get("collection", [])
            all_invitees.extend(invitees)

            # Check for next page
            pagination = response.get("pagination", {})
            page_token = pagination.get("next_page_token")

            if not page_token:
                break

        return all_invitees


def extract_booking_info(
    event: dict[str, Any],
    invitees: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Extract booking information from an event and its invitees.

    Args:
        event: Scheduled event resource from Calendly
        invitees: List of invitee resources

    Returns:
        List of booking info dictionaries with email, event details, etc.
    """
    bookings = []

    for invitee in invitees:
        email = invitee.get("email", "").lower().strip()
        if not email:
            continue

        # Extract event UUID from URI with validation
        event_uri = event.get("uri", "")
        event_uuid = ""
        if event_uri and "/" in event_uri:
            event_uuid = event_uri.split("/")[-1]

        booking = {
            "invitee_email": email,
            "invitee_name": invitee.get("name", ""),
            "event_uri": event_uri,
            "event_uuid": event_uuid,
            "event_name": event.get("name", ""),
            "event_type_uri": event.get("event_type", ""),
            "event_start_time": event.get("start_time"),
            "event_end_time": event.get("end_time"),
            "event_status": event.get("status", ""),
            "invitee_status": invitee.get("status", ""),
            "invitee_uri": invitee.get("uri", ""),
            "created_at": invitee.get("created_at"),
            "questions_and_answers": invitee.get("questions_and_answers", []),
        }
        bookings.append(booking)

    return bookings


def match_booking_to_lead(
    supabase,
    invitee_email: str,
    table: str = "tech_scans",
) -> dict[str, Any] | None:
    """
    Find a lead in Supabase that matches the invitee email.

    Args:
        supabase: Supabase client instance
        invitee_email: The invitee's email address
        table: Table to search for leads

    Returns:
        Matching lead record or None if not found
    """
    # Search for leads where the emails array contains this email
    # Use JSONB containment operator for efficient database-level filtering
    try:
        # Try using the contains filter for JSONB arrays
        response = supabase.table(table).select("*").contains(
            "emails", [invitee_email]
        ).execute()

        if response.data:
            return response.data[0]
    except Exception:
        # Fall back to fetching leads that have been emailed and checking in memory
        # This handles cases where the JSONB query might not work as expected
        response = supabase.table(table).select("*").eq("emailed", True).execute()

        for lead in response.data or []:
            emails = lead.get("emails", [])
            if not emails:
                continue

            # Check if invitee_email matches any email in the lead
            for email in emails:
                if email.lower().strip() == invitee_email:
                    return lead

    return None


def update_lead_with_booking(
    supabase,
    lead_id: str,
    booking: dict[str, Any],
    table: str = "tech_scans",
) -> None:
    """
    Update a lead record with booking information.

    Args:
        supabase: Supabase client instance
        lead_id: The lead's ID
        booking: Booking information dictionary
        table: Table to update
    """
    update_data = {
        "booked": True,
        "booked_at": booking.get("event_start_time"),
        "calendly_event_uri": booking.get("event_uri"),
        "calendly_invitee_email": booking.get("invitee_email"),
        "calendly_event_name": booking.get("event_name"),
    }

    supabase.table(table).update(update_data).eq("id", lead_id).execute()
    logger.info(f"Updated lead {lead_id} with booking info")


def save_booking_record(
    supabase,
    booking: dict[str, Any],
    lead: dict[str, Any] | None,
    table: str = "calendly_bookings",
) -> None:
    """
    Save a booking record to the bookings table.

    This stores the full booking information along with matched lead metadata
    for analytics purposes.

    Args:
        supabase: Supabase client instance
        booking: Booking information dictionary
        lead: Matched lead record (or None if no match)
        table: Table to insert into
    """
    # Extract persona and variant info from matched lead
    generated_email = lead.get("generated_email", {}) if lead else {}

    record = {
        "invitee_email": booking.get("invitee_email"),
        "invitee_name": booking.get("invitee_name"),
        "event_uri": booking.get("event_uri"),
        "event_uuid": booking.get("event_uuid"),
        "event_name": booking.get("event_name"),
        "event_start_time": booking.get("event_start_time"),
        "event_end_time": booking.get("event_end_time"),
        "event_status": booking.get("event_status"),
        "invitee_status": booking.get("invitee_status"),
        # Lead matching info
        "matched_lead_id": lead.get("id") if lead else None,
        "matched_domain": lead.get("domain") if lead else None,
        # Persona/variant tracking for analytics
        "persona": generated_email.get("persona") if generated_email else None,
        "persona_email": generated_email.get("persona_email") if generated_email else None,
        "variant_id": generated_email.get("variant_id") if generated_email else None,
        "main_tech": generated_email.get("main_tech") if generated_email else None,
        # Timestamps
        "calendly_created_at": booking.get("created_at"),
    }

    # Upsert to avoid duplicates
    supabase.table(table).upsert(
        record, on_conflict="event_uuid,invitee_email"
    ).execute()
    logger.debug(f"Saved booking record for {booking.get('invitee_email')}")


def sync_calendly_bookings(
    calendly_token: str,
    supabase_url: str,
    supabase_key: str,
    leads_table: str = "tech_scans",
    bookings_table: str = "calendly_bookings",
    lookback_days: int = 7,
) -> dict[str, int]:
    """
    Sync Calendly bookings with Supabase leads.

    This is the main entry point for the sync process:
    1. Fetches recent scheduled events from Calendly
    2. Gets invitees for each event
    3. Matches invitee emails to leads in Supabase
    4. Updates matched leads with booking status
    5. Saves booking records for analytics

    Args:
        calendly_token: Calendly API token
        supabase_url: Supabase project URL
        supabase_key: Supabase service role key
        leads_table: Table containing leads (default: tech_scans)
        bookings_table: Table for booking records (default: calendly_bookings)
        lookback_days: Days to look back for events (default: 7)

    Returns:
        Statistics dictionary with counts
    """
    logger.info("=" * 60)
    logger.info("STARTING CALENDLY SYNC")
    logger.info("=" * 60)

    # Initialize clients
    logger.info("Initializing clients...")
    calendly = CalendlyClient(calendly_token)
    supabase = create_client(supabase_url, supabase_key)

    # Get user info to confirm connection
    user = calendly.get_current_user()
    logger.info(f"Connected to Calendly as: {user.get('name', 'Unknown')}")
    logger.info(f"Email: {user.get('email', 'Unknown')}")

    # Calculate time range
    now = datetime.now(timezone.utc)
    min_start_time = now - timedelta(days=lookback_days)

    logger.info(f"Fetching events from {min_start_time.isoformat()} to now...")

    # Fetch scheduled events
    events = calendly.list_scheduled_events(
        min_start_time=min_start_time,
        max_start_time=now,
        status="active",
    )

    logger.info(f"Found {len(events)} scheduled events")

    stats = {
        "events_processed": 0,
        "bookings_found": 0,
        "leads_matched": 0,
        "leads_updated": 0,
        "new_bookings": 0,
    }

    # Process each event
    for event in events:
        event_uri = event.get("uri", "")
        event_name = event.get("name", "Unknown")
        logger.debug(f"Processing event: {event_name}")

        try:
            # Get invitees for this event
            invitees = calendly.get_event_invitees(event_uri)
            logger.debug(f"  Found {len(invitees)} invitees")

            # Extract booking info
            bookings = extract_booking_info(event, invitees)
            stats["bookings_found"] += len(bookings)

            for booking in bookings:
                email = booking["invitee_email"]
                logger.info(f"  Processing booking: {email}")

                # Try to match with a lead
                lead = match_booking_to_lead(supabase, email, leads_table)

                if lead:
                    stats["leads_matched"] += 1
                    lead_id = lead.get("id")
                    domain = lead.get("domain", "unknown")
                    logger.info(f"    ✓ Matched lead: {domain} (ID: {lead_id})")

                    # Update lead if not already marked as booked
                    if not lead.get("booked"):
                        update_lead_with_booking(supabase, lead_id, booking, leads_table)
                        stats["leads_updated"] += 1
                        logger.info(f"    ✓ Updated lead with booking info")
                    else:
                        logger.debug(f"    Lead already marked as booked")
                else:
                    logger.debug(f"    No matching lead found for {email}")

                # Save booking record for analytics
                save_booking_record(supabase, booking, lead, bookings_table)
                stats["new_bookings"] += 1

            stats["events_processed"] += 1

        except Exception as e:
            logger.error(f"  Error processing event {event_name}: {e}")
            continue

        # Small delay to avoid rate limiting
        time.sleep(0.5)

    logger.info("=" * 60)
    logger.info("CALENDLY SYNC COMPLETE")
    logger.info("=" * 60)
    logger.info(f"  Events processed: {stats['events_processed']}")
    logger.info(f"  Bookings found: {stats['bookings_found']}")
    logger.info(f"  Leads matched: {stats['leads_matched']}")
    logger.info(f"  Leads updated: {stats['leads_updated']}")

    return stats


def get_booking_analytics(
    supabase_url: str,
    supabase_key: str,
    bookings_table: str = "calendly_bookings",
) -> dict[str, Any]:
    """
    Get analytics on Calendly bookings.

    Returns aggregate statistics on:
    - Bookings by persona
    - Bookings by email variant
    - Bookings by technology
    - Conversion rates

    Args:
        supabase_url: Supabase project URL
        supabase_key: Supabase service role key
        bookings_table: Table containing booking records

    Returns:
        Analytics dictionary with breakdowns
    """
    supabase = create_client(supabase_url, supabase_key)

    # Fetch all booking records
    response = supabase.table(bookings_table).select("*").execute()
    bookings = response.data or []

    # Aggregate statistics
    stats = {
        "total_bookings": len(bookings),
        "matched_bookings": 0,
        "by_persona": {},
        "by_variant": {},
        "by_tech": {},
    }

    for booking in bookings:
        # Count matched bookings
        if booking.get("matched_lead_id"):
            stats["matched_bookings"] += 1

        # Aggregate by persona
        persona = booking.get("persona") or "Unknown"
        stats["by_persona"][persona] = stats["by_persona"].get(persona, 0) + 1

        # Aggregate by variant
        variant = booking.get("variant_id") or "Unknown"
        stats["by_variant"][variant] = stats["by_variant"].get(variant, 0) + 1

        # Aggregate by technology
        tech = booking.get("main_tech") or "Unknown"
        stats["by_tech"][tech] = stats["by_tech"].get(tech, 0) + 1

    # Calculate conversion rates (if we have email stats)
    try:
        email_stats = supabase.table("email_stats").select("*").execute()
        total_sends = sum(r.get("send_count", 0) for r in (email_stats.data or []))
        if total_sends > 0:
            stats["overall_conversion_rate"] = stats["matched_bookings"] / total_sends
    except Exception:
        # email_stats table might not exist
        pass

    return stats


if __name__ == "__main__":
    # For testing, run sync directly
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    token = os.getenv("CALENDLY_API_TOKEN")
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")

    if not all([token, url, key]):
        print("Missing required environment variables:")
        print("  CALENDLY_API_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_KEY")
        sys.exit(1)

    stats = sync_calendly_bookings(token, url, key)
    print(f"\nSync complete: {stats}")

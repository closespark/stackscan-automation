-- Supabase Schema for HubSpot Domain Pipeline
-- Run this in the Supabase SQL editor to set up the required tables

-- Enable pgcrypto for UUID generation
create extension if not exists "pgcrypto";

-- Table for storing HubSpot scan results
create table if not exists hubspot_scans (
    id uuid primary key default gen_random_uuid(),
    domain text not null,
    hubspot_detected boolean default false,
    confidence_score float default 0,
    portal_ids jsonb default '[]'::jsonb,
    hubspot_signals jsonb default '[]'::jsonb,
    emails jsonb default '[]'::jsonb,
    category text,
    created_at timestamptz default now(),
    error text,
    -- Outreach tracking fields
    emailed boolean,
    emailed_at timestamptz
);

-- Table for tracking processed domains (deduplication)
create table if not exists domains_seen (
    domain text primary key,
    category text,
    first_seen timestamptz default now(),
    last_scanned timestamptz default now(),
    times_scanned int default 1
);

-- Indexes for better query performance
create index if not exists idx_hubspot_scans_domain on hubspot_scans(domain);
create index if not exists idx_hubspot_scans_created_at on hubspot_scans(created_at);
create index if not exists idx_hubspot_scans_hubspot_detected on hubspot_scans(hubspot_detected);
create index if not exists idx_hubspot_scans_emailed on hubspot_scans(emailed);
create index if not exists idx_domains_seen_domain on domains_seen(domain);
create index if not exists idx_domains_seen_category on domains_seen(category);

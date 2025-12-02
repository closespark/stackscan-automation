-- Supabase Schema for Tech Stack Scanner Pipeline
-- Run this in the Supabase SQL editor to set up the required tables

-- Enable pgcrypto for UUID generation
create extension if not exists "pgcrypto";

-- Table for storing technology scan results
create table if not exists tech_scans (
    id uuid primary key default gen_random_uuid(),
    domain text not null,
    -- Technology detection fields
    technologies jsonb default '[]'::jsonb,
    scored_technologies jsonb default '[]'::jsonb,
    top_technology jsonb,
    -- Email extraction
    emails jsonb default '[]'::jsonb,
    -- Generated email with persona and variant tracking
    -- Contains: subject, body, main_tech, supporting_techs, persona, persona_email, persona_role, variant_id
    generated_email jsonb,
    -- Categorization
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
create index if not exists idx_tech_scans_domain on tech_scans(domain);
create index if not exists idx_tech_scans_created_at on tech_scans(created_at);
create index if not exists idx_tech_scans_emailed on tech_scans(emailed);
create index if not exists idx_tech_scans_top_technology on tech_scans using gin(top_technology);
create index if not exists idx_domains_seen_domain on domains_seen(domain);
create index if not exists idx_domains_seen_category on domains_seen(category);

-- Optional: Migration from hubspot_scans to tech_scans
-- Uncomment if you need to migrate existing data
-- insert into tech_scans (domain, category, emails, created_at, emailed, emailed_at, error)
-- select domain, category, emails, created_at, emailed, emailed_at, error
-- from hubspot_scans
-- where hubspot_detected = true;

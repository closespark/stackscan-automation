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

-- ============================================================================
-- EMAIL STATS: Per-Variant Analytics
-- ============================================================================
-- Tracks performance metrics for each combination of persona, tech, and variant.
-- Populated automatically by a trigger when emails are sent.

create table if not exists email_stats (
    id uuid primary key default gen_random_uuid(),
    -- Dimensions for analysis
    persona text not null,                    -- Scott, Tracy, Willa
    persona_email text not null,              -- scott@closespark.co, etc.
    main_tech text not null,                  -- Shopify, Salesforce, etc.
    variant_id text not null,                 -- shopify_v1, salesforce_v2, etc.
    subject text,                             -- Actual subject line used
    smtp_inbox text,                          -- SMTP inbox = persona_email (same in this system)
    -- Counters
    send_count int default 0,                 -- Number of times this combo was used
    -- Timestamps
    first_sent_at timestamptz default now(),
    last_sent_at timestamptz default now(),
    -- Unique constraint for upsert
    constraint email_stats_unique unique (persona_email, main_tech, variant_id)
);

-- Indexes for email_stats queries
create index if not exists idx_email_stats_persona on email_stats(persona);
create index if not exists idx_email_stats_main_tech on email_stats(main_tech);
create index if not exists idx_email_stats_variant_id on email_stats(variant_id);
create index if not exists idx_email_stats_send_count on email_stats(send_count desc);

-- ============================================================================
-- DOMAIN EMAIL HISTORY: Variant Suppression Support
-- ============================================================================
-- Tracks which persona/variant combinations have been sent to each domain.
-- Used to avoid sending the same variant or persona twice to the same domain.

create table if not exists domain_email_history (
    id uuid primary key default gen_random_uuid(),
    domain text not null,
    persona text not null,
    persona_email text not null,
    variant_id text not null,
    main_tech text not null,
    sent_at timestamptz default now(),
    -- Unique constraint to prevent duplicates
    constraint domain_email_history_unique unique (domain, persona_email, variant_id)
);

-- Indexes for variant suppression queries
create index if not exists idx_domain_email_history_domain on domain_email_history(domain);
create index if not exists idx_domain_email_history_persona on domain_email_history(persona_email);

-- ============================================================================
-- TRIGGER: Auto-populate email_stats when tech_scans.emailed becomes true
-- ============================================================================

create or replace function update_email_stats()
returns trigger as $$
declare
    v_persona text;
    v_persona_email text;
    v_main_tech text;
    v_variant_id text;
    v_subject text;
begin
    -- Only run when emailed changes from null/false to true
    if NEW.emailed = true and (OLD.emailed is null or OLD.emailed = false) then
        -- Extract fields from generated_email JSONB
        v_persona := NEW.generated_email->>'persona';
        v_persona_email := NEW.generated_email->>'persona_email';
        v_main_tech := NEW.generated_email->>'main_tech';
        v_variant_id := NEW.generated_email->>'variant_id';
        v_subject := NEW.generated_email->>'subject';
        
        -- Only proceed if we have the required fields
        if v_persona is not null and v_main_tech is not null and v_variant_id is not null then
            -- Upsert into email_stats
            insert into email_stats (persona, persona_email, main_tech, variant_id, subject, smtp_inbox, send_count, first_sent_at, last_sent_at)
            values (v_persona, coalesce(v_persona_email, ''), v_main_tech, v_variant_id, v_subject, coalesce(v_persona_email, ''), 1, now(), now())
            on conflict (persona_email, main_tech, variant_id)
            do update set
                send_count = email_stats.send_count + 1,
                last_sent_at = now(),
                subject = coalesce(excluded.subject, email_stats.subject);
            
            -- Also insert into domain_email_history for variant suppression
            insert into domain_email_history (domain, persona, persona_email, variant_id, main_tech, sent_at)
            values (NEW.domain, v_persona, coalesce(v_persona_email, ''), v_variant_id, v_main_tech, now())
            on conflict (domain, persona_email, variant_id) do nothing;
        end if;
    end if;
    
    return NEW;
end;
$$ language plpgsql;

-- Create the trigger
drop trigger if exists trigger_update_email_stats on tech_scans;
create trigger trigger_update_email_stats
    after update on tech_scans
    for each row
    execute function update_email_stats();

-- ============================================================================
-- INDEXES for tech_scans
-- ============================================================================

create index if not exists idx_tech_scans_domain on tech_scans(domain);
create index if not exists idx_tech_scans_created_at on tech_scans(created_at);
create index if not exists idx_tech_scans_emailed on tech_scans(emailed);
create index if not exists idx_tech_scans_top_technology on tech_scans using gin(top_technology);
create index if not exists idx_domains_seen_domain on domains_seen(domain);
create index if not exists idx_domains_seen_category on domains_seen(category);

-- ============================================================================
-- HELPER VIEWS for Analytics
-- ============================================================================

-- View: Top performing variants by send count
create or replace view v_top_variants as
select 
    main_tech,
    variant_id,
    persona,
    send_count,
    first_sent_at,
    last_sent_at
from email_stats
order by send_count desc;

-- View: Persona performance summary
create or replace view v_persona_stats as
select 
    persona,
    persona_email,
    count(distinct main_tech) as tech_count,
    count(distinct variant_id) as variant_count,
    sum(send_count) as total_sends,
    min(first_sent_at) as first_send,
    max(last_sent_at) as last_send
from email_stats
group by persona, persona_email
order by total_sends desc;

-- View: Tech performance summary
create or replace view v_tech_stats as
select 
    main_tech,
    count(distinct variant_id) as variant_count,
    count(distinct persona) as persona_count,
    sum(send_count) as total_sends
from email_stats
group by main_tech
order by total_sends desc;

-- ============================================================================
-- Optional: Migration from hubspot_scans to tech_scans
-- ============================================================================
-- Uncomment if you need to migrate existing data
-- insert into tech_scans (domain, category, emails, created_at, emailed, emailed_at, error)
-- select domain, category, emails, created_at, emailed, emailed_at, error
-- from hubspot_scans
-- where hubspot_detected = true;

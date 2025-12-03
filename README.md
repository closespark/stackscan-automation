# 🚀 ProspectPilot

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-beta-yellow)

**Autonomous AI-Powered Outbound Engine for Technical Consultants, Agencies & RevOps Teams**

ProspectPilot is a fully automated outbound pipeline that discovers new businesses, analyzes their website tech stack, extracts verified contact emails, generates personalized outreach, rotates SMTP inboxes, and tracks Calendly bookings — every single day, without human involvement.

Point ProspectPilot at any business category (dentists, accountants, e-commerce stores, landscapers, professional services, etc.) and it will:

1. **Scrape Google Places** for business websites in your target category
2. **Scan each website's tech stack** — detecting 40+ technologies (CRMs, CMSs, ecommerce platforms, analytics tools, marketing systems, payment processors, and more)
3. **Score each detected technology** 1–5 based on value and specialization
4. **Extract real business email addresses** (filtering out generic ones like info@, support@, admin@)
5. **Generate personalized outreach emails** tailored to their tech stack
6. **Use persona-based writing** with multiple variants for A/B testing
7. **Send emails automatically** through rotating SMTP inboxes
8. **Log all activity** to Supabase
9. **Track meeting conversions** by syncing Calendly bookings

You deploy ProspectPilot once, set your environment variables, and it runs daily as an autonomous cron job.

---

## 💸 Cost Advantage (Why ProspectPilot Is So Cheap to Run)

One of the biggest strengths of ProspectPilot is how cost-efficient the entire pipeline is:

| Service | Cost |
|---------|------|
| **Apify (Google Places data)** | $4 per 1,000 businesses — Even scraping 150 businesses per week costs under $1/month |
| **Render hosting** | FREE — Runs daily cron jobs and background workers at zero cost |
| **Calendly sync** | FREE — All meeting analytics pull from the free plan |
| **OpenAI rewriting (optional)** | ~$1–$5/month — $5 in API credits powers 80–200 rewritten emails |
| **Zapmail pre-warmed inboxes** | $33 total — Includes 1 domain, 3 fully pre-warmed mailboxes, SMTP credentials |

With ProspectPilot's rotation engine, 3 inboxes can handle **2,700–3,600 outbound emails/month**.

**Total monthly spend: $2–$6** (after initial $33 inbox purchase).

This makes ProspectPilot one of the lowest-cost fully autonomous outbound systems available.

---

## ⭐ Features

- **Multi-Technology Detection**: Detects 40+ technologies including HubSpot, Salesforce, Shopify, Stripe, WooCommerce, Klaviyo, Mailchimp, Segment, WordPress, Mixpanel, and more
- **Technology Scoring**: Scores each detected technology from 1–5 based on enterprise demand & project value
- **Persona-Driven AI Email Generation**: Emails are tailored to the detected technologies, your persona's tone & role, A/B test variants, and company details (name, hourly rate, location, Calendly)
- **Multiple Variant Support**: Each technology offers 2–3 email variants for real A/B testing
- **Email & Domain Extraction**: Crawler detects real business emails and automatically filters generic inboxes
- **JSON Output**: All scans produce structured JSON with stack data, scores, emails, and variant metadata
- **CLI + Python Library**: Run ProspectPilot via CLI or import it into your own Python automation workflows
- **Automated Daily Pipeline**: Daily cron handles Google Places scrape, tech stack scan, email extraction, persona emails, outreach, and Calendly sync
- **Calendly Integration**: Tracks which emails → which meetings → which personas → which variants → which tech stacks
- **Fully Configurable**: All settings are environment-driven

---

## 📚 Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Complete Setup Guide](#complete-setup-guide)
   - [1. Supabase Setup](#1-supabase-setup)
   - [2. Apify Setup (Google Places Scraping)](#2-apify-setup-google-places-scraping)
   - [3. Zapmail/SMTP Setup](#3-zapmailsmtp-setup)
   - [4. Calendly Setup](#4-calendly-setup)
   - [5. OpenAI Setup (Optional)](#5-openai-setup-optional)
   - [6. Company Profile & Personas](#6-company-profile--personas)
   - [7. Render Deployment](#7-render-deployment)
4. [Environment Variables Reference](#environment-variables-reference)
5. [Architecture](#architecture)
6. [Email Generation](#email-generation)
7. [Analytics & Tracking](#analytics--tracking)
8. [CLI Reference](#cli-reference)
9. [Examples](#examples)
10. [Supported Technologies](#supported-technologies)
11. [Technology Scoring](#technology-scoring)
12. [Requirements](#requirements)
13. [License](#license)

---

## 🛠 Installation

```bash
git clone https://github.com/closespark/prospectpilot.git
cd prospectpilot

pip install -r requirements.txt
pip install -e .
```

## ⚡ Quick Start

### Command Line

```bash
# Scan a domain
prospectpilot scan example.com

# Scan multiple domains
prospectpilot scan shopify.com stripe.com

# Save JSON output
prospectpilot scan -f domains.txt -o results.json

# Skip email generation
prospectpilot scan example.com --no-email
```

### Python Library

```python
from prospectpilot import scan_technologies, generate_persona_outreach_email

result = scan_technologies("example.com")
print(result.technologies)
print(result.top_technology["name"])

email = generate_persona_outreach_email(
    domain="example.com",
    main_tech="Shopify",
    supporting_techs=["Stripe", "Klaviyo"],
    from_email="john@yourdomain.com"
)

print(email.subject)
print(email.body)
```

---

## 🔧 Complete Setup Guide

This guide walks you through setting up all the external services needed to run the full automated pipeline.

### 1. Supabase Setup

Supabase is used to store scan results, track domains, and manage analytics.

#### Create a Supabase Project

1. Go to [supabase.com](https://supabase.com) and create a new project
2. Note your **Project URL** and **Service Role Key** (found in Settings > API)

#### Create the Required Tables

Open the Supabase SQL Editor and run the full schema from `config/supabase_schema.sql`, or use these essential tables:

```sql
-- Enable UUID extension
create extension if not exists "pgcrypto";

-- Main table for technology scan results
create table if not exists tech_scans (
    id uuid primary key default gen_random_uuid(),
    domain text not null,
    technologies jsonb default '[]'::jsonb,
    scored_technologies jsonb default '[]'::jsonb,
    top_technology jsonb,
    emails jsonb default '[]'::jsonb,
    generated_email jsonb,
    category text,
    created_at timestamptz default now(),
    error text,
    emailed boolean,
    emailed_at timestamptz,
    -- Calendly booking tracking
    booked boolean,
    booked_at timestamptz,
    calendly_event_uri text,
    calendly_invitee_email text,
    calendly_event_name text
);

-- Track processed domains to avoid duplicates
create table if not exists domains_seen (
    domain text primary key,
    category text,
    first_seen timestamptz default now(),
    last_scanned timestamptz default now(),
    times_scanned int default 1
);

-- Track which categories have been processed for rotation
create table if not exists categories_used (
    id uuid primary key default gen_random_uuid(),
    category text not null,
    used_date date not null default current_date,
    domains_found int default 0,
    domains_new int default 0,
    created_at timestamptz default now(),
    constraint categories_used_unique unique (category, used_date)
);

-- Calendly booking records for conversion analytics
create table if not exists calendly_bookings (
    id uuid primary key default gen_random_uuid(),
    invitee_email text not null,
    invitee_name text,
    event_uri text not null,
    event_uuid text not null,
    event_name text,
    event_start_time timestamptz,
    event_end_time timestamptz,
    event_status text,
    invitee_status text,
    matched_lead_id uuid references tech_scans(id),
    matched_domain text,
    persona text,
    persona_email text,
    variant_id text,
    main_tech text,
    calendly_created_at timestamptz,
    synced_at timestamptz default now(),
    constraint calendly_bookings_unique unique (event_uuid, invitee_email)
);

-- Email statistics for A/B testing analytics
create table if not exists email_stats (
    id uuid primary key default gen_random_uuid(),
    persona text not null,
    persona_email text not null,
    main_tech text not null,
    variant_id text not null,
    subject text,
    smtp_inbox text,
    send_count int default 0,
    first_sent_at timestamptz default now(),
    last_sent_at timestamptz default now(),
    constraint email_stats_unique unique (persona_email, main_tech, variant_id)
);

-- Create indexes for performance
create index if not exists idx_tech_scans_domain on tech_scans(domain);
create index if not exists idx_tech_scans_emailed on tech_scans(emailed);
create index if not exists idx_tech_scans_booked on tech_scans(booked);
create index if not exists idx_domains_seen_domain on domains_seen(domain);
create index if not exists idx_categories_used_date on categories_used(used_date desc);
```

See `config/supabase_schema.sql` for the complete schema including analytics views and triggers.

---

### 2. Apify Setup (Google Places Scraping)

Apify is used to scrape Google Places for business listings by category.

#### Get Your Apify API Token

1. Create an account at [apify.com](https://apify.com)
2. Go to **Settings** > **Integrations**
3. Copy your **API Token**

#### How It Works

The pipeline uses the `compass/crawler-google-places` actor to:
- Search for businesses by category (e.g., "accountant", "dentist", "restaurant")
- Extract business websites/domains
- Rotate through 250 categories daily

Set the environment variable:
```bash
APIFY_TOKEN=your_apify_api_token_here
```

---

### 3. Zapmail/SMTP Setup

The outreach worker sends emails through SMTP. You can use Zapmail (pre-warmed inboxes) or any SMTP provider.

#### Exporting from Zapmail

If using Zapmail for email deliverability:

1. Log into [Zapmail](https://zapmail.app)
2. Go to **Settings** > **Inboxes**
3. Click **Export Configuration** for each inbox
4. Combine into a JSON array

#### SMTP Configuration Format

Set the `SMTP_ACCOUNTS_JSON` environment variable with your inbox configurations:

```json
{
  "inboxes": [
    {
      "email": "john@yourdomain.com",
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "smtp_user": "john@yourdomain.com",
      "smtp_password": "your_app_password"
    },
    {
      "email": "jane@yourdomain.com",
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "smtp_user": "jane@yourdomain.com",
      "smtp_password": "your_app_password"
    }
  ]
}
```

#### Gmail App Passwords

If using Gmail:
1. Enable 2-factor authentication on your Google account
2. Go to Google Account > Security > App Passwords
3. Generate an app password for "Mail"
4. Use this password in `smtp_password`

---

### 4. Calendly Setup

Calendly integration tracks which outreach emails led to booked meetings.

#### Get Your Personal Access Token

1. Log into [Calendly](https://calendly.com)
2. Go to **Integrations** > **API & Webhooks**
3. Click **Generate New Token**
4. Copy the token (it won't be shown again)

Set the environment variable:
```bash
CALENDLY_API_TOKEN=your_personal_access_token
```

---

### 5. OpenAI Setup (Optional)

OpenAI is used to rewrite outreach emails for improved deliverability and clarity. This is optional but recommended for better email performance.

#### How It Works

When configured, the system sends your generated email templates through OpenAI's API to:
- Optimize emails for deliverability (avoiding spam triggers)
- Keep tone professional, friendly, and conversational
- Maintain the original intent and all key details (links, names, rates)
- Keep emails concise (80-140 words)

If OpenAI is not configured, emails are sent using the original templates without modification.

#### Get Your OpenAI API Key

1. Go to [platform.openai.com](https://platform.openai.com)
2. Navigate to **API Keys** in your account settings
3. Click **Create new secret key**
4. Copy the key (it won't be shown again)

Set the environment variable:
```bash
OPENAI_API_KEY=your_openai_api_key_here
```

#### Optional Configuration

You can customize the OpenAI behavior with these optional variables:

```bash
# Model to use (default: gpt-4.1-mini)
OPENAI_MODEL=gpt-4.1-mini

# Temperature for generation (default: 0.4, lower = more consistent)
OPENAI_EMAIL_TEMPERATURE=0.4

# Max tokens for response (default: 400)
OPENAI_EMAIL_MAX_TOKENS=400
```

---

### 6. Company Profile & Personas

All company details and persona information are configured via environment variables.

#### Company Profile Variables

These values appear in your generated outreach emails:

```bash
COMPANY_NAME=Your Company Name
COMPANY_LOCATION=Your City, State
COMPANY_HOURLY_RATE=$85/hr
COMPANY_GITHUB=https://github.com/yourcompany/
COMPANY_CALENDLY=https://calendly.com/your-link
```

> **Note**: For `COMPANY_HOURLY_RATE`, include the full formatted string exactly as you want it to appear in emails (e.g., `$85/hr`, `$125/hr`, or `€100/hr`). This value is inserted directly into outreach emails without modification.

#### Persona Map

The `PERSONA_MAP_JSON` variable maps SMTP email addresses to persona details. Each persona has a name, role, and tone that affects the email style:

```bash
PERSONA_MAP_JSON='{
  "john@yourdomain.com": {
    "name": "John",
    "role": "Systems Engineer",
    "tone": "concise, technical, straight to the point"
  },
  "jane@yourdomain.com": {
    "name": "Jane",
    "role": "Technical Lead",
    "tone": "structured, slightly more formal"
  },
  "alex@yourdomain.com": {
    "name": "Alex",
    "role": "Automation Specialist",
    "tone": "friendly but still professional"
  }
}'
```

**Important**: The email addresses in `PERSONA_MAP_JSON` must match the emails in `SMTP_ACCOUNTS_JSON`.

---

### 7. Render Deployment

Deploy to Render using the included `render.yaml` configuration.

#### Steps

1. **Fork this repository** to your GitHub account

2. **Create a new Render account** at [render.com](https://render.com)

3. **Connect your GitHub** and import the repository

4. **Create a new Blueprint** and select `render.yaml`

5. **Set environment variables** in the Render dashboard:

   | Variable | Type | Description |
   |----------|------|-------------|
   | `SUPABASE_URL` | Secret | Your Supabase project URL |
   | `SUPABASE_SERVICE_KEY` | Secret | Supabase service role key |
   | `APIFY_TOKEN` | Secret | Apify API token |
   | `SMTP_ACCOUNTS_JSON` | Secret | JSON with SMTP inbox configs |
   | `CALENDLY_API_TOKEN` | Secret | Calendly Personal Access Token |
   | `COMPANY_NAME` | Env Var | Your company name |
   | `COMPANY_LOCATION` | Env Var | Your city/location |
   | `COMPANY_HOURLY_RATE` | Env Var | Your hourly rate (include `$` sign, e.g., `$85/hr`) |
   | `COMPANY_GITHUB` | Env Var | Your GitHub URL |
   | `COMPANY_CALENDLY` | Env Var | Your Calendly booking link |
   | `PERSONA_MAP_JSON` | Secret | Persona configurations |

6. **Deploy!** The cron job runs daily at 6:00 AM UTC

#### Docker Command Execution

The Dockerfile is configured to run `daily_worker.py` by default, which executes all workers sequentially (pipeline → outreach → calendly). 

**Important**: When running chained commands on Render (e.g., `python pipeline_worker.py && python outreach_worker.py`), always wrap them with `bash -c`:

```bash
# ✅ Correct - uses bash to interpret &&
bash -c "python pipeline_worker.py && python outreach_worker.py"

# ❌ Wrong - && may not be interpreted correctly
python pipeline_worker.py && python outreach_worker.py
```

For most use cases, simply use `python daily_worker.py` which handles all workers properly.

#### Local Development

```bash
# Copy environment template
cp .env.example .env

# Edit with your values
nano .env

# Run all workers (same as Render cron)
python daily_worker.py

# Or run individual workers:
python pipeline_worker.py    # Scan domains
python outreach_worker.py    # Send emails
python calendly_worker.py    # Sync Calendly bookings

# Preview emails without sending
python scripts/preview_email.py --tech Shopify --from john@yourdomain.com
```

---

## Environment Variables Reference

### Required Variables

| Variable | Description |
|----------|-------------|
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Supabase service role key |
| `APIFY_TOKEN` | Apify API token |
| `SMTP_ACCOUNTS_JSON` | JSON with SMTP inbox configurations |
| `COMPANY_NAME` | Your company name for emails |
| `COMPANY_LOCATION` | Your city/location for emails |
| `COMPANY_HOURLY_RATE` | Hourly rate shown in emails (include currency symbol, e.g., `$85/hr`) |
| `COMPANY_CALENDLY` | Your Calendly booking link |

### Optional Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `COMPANY_GITHUB` | `""` | GitHub/portfolio URL |
| `PERSONA_MAP_JSON` | `""` | JSON mapping emails to personas |
| `CALENDLY_API_TOKEN` | `""` | Calendly token for booking sync |
| `OPENAI_API_KEY` | `""` | OpenAI API key for email rewriting |
| `OPENAI_MODEL` | `gpt-4.1-mini` | OpenAI model to use |
| `OPENAI_EMAIL_TEMPERATURE` | `0.4` | Temperature for email generation |
| `OPENAI_EMAIL_MAX_TOKENS` | `400` | Max tokens for email response |
| `LOG_LEVEL` | `INFO` | Logging verbosity |
| `APIFY_MAX_PLACES` | `1000` | Max places per search |
| `APIFY_ACTOR` | `compass/crawler-google-places` | Apify actor ID |
| `OUTREACH_DAILY_LIMIT` | `500` | Max emails per day |
| `OUTREACH_PER_INBOX_LIMIT` | `50` | Max emails per inbox |
| `SMTP_SEND_DELAY_SECONDS` | `4` | Delay between emails |
| `SCANNER_MAX_EMAIL_PAGES` | `10` | Max pages to crawl for emails |
| `CATEGORY_COOLDOWN_DAYS` | `7` | Days before a category can be reused |
| `SUPABASE_CATEGORIES_TABLE` | `categories_used` | Table for tracking used categories |

---

## 🧠 Architecture

The system runs as a daily cron job (`daily_worker.py`) executing three workers sequentially:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Daily Pipeline (6:00 AM UTC)                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. PIPELINE WORKER (pipeline_worker.py)                        │
│     ├── Scrape Google Places via Apify                          │
│     ├── Extract & normalize domains                             │
│     ├── Deduplicate against Supabase                            │
│     ├── Scan each domain for tech stack                         │
│     ├── Extract contact emails                                  │
│     ├── Generate persona-based emails                           │
│     └── Store results in Supabase                               │
│                                                                  │
│  2. OUTREACH WORKER (outreach_worker.py)                        │
│     ├── Pull leads with tech + valid emails                     │
│     ├── Rotate through SMTP inboxes                             │
│     ├── Send personalized emails (350-500/day)                  │
│     └── Mark leads as emailed                                   │
│                                                                  │
│  3. CALENDLY SYNC (calendly_worker.py)                          │
│     ├── Fetch recent Calendly events                            │
│     ├── Match invitee emails to leads                           │
│     ├── Update booking status                                   │
│     └── Save conversion analytics                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Deduplication Mechanisms

The system includes three layers of deduplication to prevent redundant work:

#### 1. Domain Deduplication

The `domains_seen` table tracks every domain that has been crawled. Before scanning a domain:
- The pipeline checks if the domain exists in `domains_seen`
- Only new (never-seen) domains are scanned
- Each domain is added to the table after first crawl

This ensures **no domain is ever crawled twice**.

#### 2. Email Deduplication

The `emailed` field on each lead record tracks email status:
- Leads are only fetched if `emailed = null`
- After sending, the lead is marked `emailed = true` with timestamp
- This is checked at query time for efficiency

This ensures **no lead receives duplicate emails**.

#### 3. Category Rotation & Cooldown

The `categories_used` table tracks which business categories have been processed:
- Before selecting a category, the pipeline checks recently used categories
- Categories used within the cooldown period (`CATEGORY_COOLDOWN_DAYS`, default: 7) are skipped
- Falls back to deterministic selection if all categories are in cooldown

This ensures **diverse lead generation across business categories**.

```sql
-- View recently used categories
SELECT category, used_date, domains_found, domains_new
FROM categories_used
ORDER BY used_date DESC
LIMIT 10;
```

---

## 📝 Email Generation

### Email Variants

Each technology has 2-3 email variants for A/B testing:

```python
EMAIL_VARIANTS = {
    "Shopify": [
        {"id": "shopify_v1", "subject_template": "Shopify integration issue on {{domain}}?"},
        {"id": "shopify_v2", "subject_template": "Quick Shopify improvement idea for {{domain}}"},
        {"id": "shopify_v3", "subject_template": "Saw something in your Shopify setup"},
    ],
    "Salesforce": [...],
    "WordPress": [...],
    # ... 14 technologies with variants
}
```

### Supported Technologies with Variants

- **Ecommerce**: Shopify, WooCommerce, Magento
- **CRM/Marketing**: Salesforce, HubSpot
- **CMS**: WordPress
- **Payments**: Stripe
- **Email Marketing**: Klaviyo, Mailchimp
- **Analytics**: Google Analytics, GA4, Mixpanel
- **CDP**: Segment
- **Live Chat**: Intercom

### Example Generated Email

```
Subject: Shopify integration issue on sample-shop.com?

Hi — I'm John from Acme Consulting in New York, NY.

I saw that sample-shop.com is running Shopify + Stripe, Klaviyo, and I specialize in short-term technical fixes for stacks like yours.

• Checkout or webhook issues affecting orders
• Payment + analytics events not lining up (GA4, Klaviyo, etc.)
• Small automation gaps that slow down the team

Hourly: $95/hr, strictly short-term — no long-term commitment.

If it would help to have a specialist jump in, you can grab time here:
https://calendly.com/acme/consultation

– John
Systems Engineer, Acme Consulting
https://github.com/acmeconsulting/
```

---

## 📊 Analytics & Tracking

### Per-Variant Analytics

The system tracks performance for each persona/technology/variant combination:

```sql
-- Top performing variants
SELECT * FROM v_top_variants LIMIT 10;

-- Persona performance
SELECT * FROM v_persona_stats;

-- Conversion funnel (emails → bookings)
SELECT * FROM v_conversion_funnel ORDER BY conversion_rate_pct DESC;
```

### Variant Suppression

The system avoids sending repetitive emails:
- Don't send the same variant twice to a domain
- Don't send the same persona twice to a domain
- Prefer new combinations first

---

## 🔌 CLI Reference

### prospectpilot

```
usage: prospectpilot [-h] [-f DOMAINS_FILE] [-o OUTPUT_FILE] [-t TIMEOUT]
                     [--no-email] [-v]
                     [domains ...]

Options:
  domains               Domain(s) to scan
  -f, --file            File containing domains (one per line)
  -o, --output          Output file for JSON results
  -t, --timeout         Request timeout in seconds (default: 10)
  --no-email            Skip outreach email generation
  -v, --version         Show version
```

### Email Preview CLI

```bash
# Preview email without sending
python scripts/preview_email.py --tech Shopify --from john@yourdomain.com

# Preview with custom domain
python scripts/preview_email.py --tech HubSpot --from john@yourdomain.com --domain acme.com

# Generate multiple variants
python scripts/preview_email.py --tech Klaviyo --from john@yourdomain.com --count 3

# List available personas and technologies
python scripts/preview_email.py --list
```

---

## 📦 Examples

See the `examples/` directory:

- `basic_usage.py` - Single domain scanning
- `batch_scan.py` - Multiple domain scanning
- `category_scraper.py` - Google Places scraping via Apify
- `daily_pipeline.py` - Full automated pipeline
- `tech_scanner_usage.py` - Technology scanner examples
- `domains.txt` - Sample domain list
- `sample_output.json` - Example output format

---

## 🔧 Supported Technologies

### Marketing & Sales
- **CRM**: Salesforce, Zoho, Pipedrive
- **Marketing Automation**: HubSpot, Marketo, Pardot, ActiveCampaign
- **Email**: Klaviyo, Mailchimp, SendGrid
- **Live Chat**: Intercom, Drift, Zendesk Chat, Freshchat

### Ecommerce
- **Platforms**: Shopify, WooCommerce, Magento, BigCommerce
- **Payments**: Stripe, PayPal, Braintree, Square

### Analytics & Testing
- **Analytics**: Google Analytics, Mixpanel, Amplitude, Heap, Hotjar
- **A/B Testing**: Optimizely, VWO, Google Optimize
- **CDP**: Segment

### Infrastructure
- **CMS**: WordPress, Webflow
- **Hosting**: AWS, Vercel, Netlify, Cloudflare

---

## 🧮 Technology Scoring

Technologies are scored 1-5 based on value/specialization:

| Score | Category | Examples |
|-------|----------|----------|
| 5 | Enterprise | Salesforce, HubSpot, Marketo, Segment |
| 4 | Ecommerce + Payments | Shopify, BigCommerce, Stripe, Klaviyo |
| 3 | Mainstream CMS | WordPress, WooCommerce, Mailchimp |
| 2 | Infrastructure | AWS, Vercel, Netlify, Cloudflare |
| 1 | Basic Analytics | Google Analytics, GA4, Heap, Hotjar |

---

## 📋 Requirements

- Python 3.10+
- requests
- beautifulsoup4
- lxml
- apify-client
- supabase
- python-dotenv
- openai (optional, for email rewriting)

## 📜 License

MIT License

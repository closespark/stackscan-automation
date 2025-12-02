# CloseSpark Tech Stack Scanner

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-beta-yellow)

Wappalyzer-style domain scanner that detects **40+ technologies** across business websites — including CRM systems, marketing automation, ecommerce platforms, payment processors, analytics tools, and more. Scores each technology by value and generates **persona-based outreach emails** with variant tracking. Built for consultants, revops teams, and lead generation workflows.

> **Note**: This project is in **beta** status. The core scanning functionality is stable and actively used in production for lead generation workflows.

## Features

- **Multi-Technology Detection**: Detects 40+ technologies including HubSpot, Salesforce, Shopify, Stripe, and more
- **Technology Scoring**: Scores each technology 1-5 based on value and specialization
- **Persona-Based Email Generation**: Generates personalized outreach emails with distinct personas (Scott, Tracy, Willa)
- **Multiple Email Variants**: 2-3 email variants per technology for A/B testing
- **Variant Tracking**: Full metadata tracking for analytics (variant_id, persona, persona_email)
- **Email Extraction**: Crawls sites to find non-generic business email addresses
- **Generic Email Filtering**: Automatically excludes info@, support@, admin@, hello@, sales@, etc.
- **JSON Output**: Structured output with technologies, scores, and generated emails
- **CLI & Library**: Use as command-line tool or import as Python library

## Installation

```bash
# Clone the repository
git clone https://github.com/closespark/stackscan-automation.git
cd stackscan-automation

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

## Quick Start

### Command Line

```bash
# Scan a domain for all technologies
tech-scanner example.com

# Scan multiple domains
tech-scanner shopify.com hubspot.com stripe.com

# Save results with email generation
tech-scanner -f domains.txt -o results.json

# Skip email generation
tech-scanner example.com --no-email
```

### Python Library

```python
from hubspot_scanner import scan_technologies, generate_persona_outreach_email

# Scan a domain for technologies
result = scan_technologies("example.com")
print(f"Technologies: {result.technologies}")
print(f"Top tech: {result.top_technology['name']} (score: {result.top_technology['score']})")

# Generate persona-based email
email = generate_persona_outreach_email(
    domain="example.com",
    main_tech="Shopify",
    supporting_techs=["Stripe", "Klaviyo"],
    from_email="scott@closespark.co"
)
print(f"Subject: {email.subject}")
print(f"Persona: {email.persona} ({email.persona_role})")
print(f"Variant: {email.variant_id}")
print(f"Body:\n{email.body}")
```

## Persona System

The outreach system uses three distinct personas tied to SMTP sender addresses:

| Email | Persona | Role | Tone |
|-------|---------|------|------|
| `scott@closespark.co` | Scott | Systems Engineer | Concise, technical, straight to the point |
| `tracy@closespark.co` | Tracy | Technical Project Lead | Structured, slightly more formal |
| `willa@closespark.co` | Willa | Automation Specialist | Friendly but still professional |

Each email includes:
- **CloseSpark branding** with Richmond, VA location
- **Hourly rate**: $85/hr
- **Calendly link**: https://calendly.com/closespark/technical-systems-consultation
- **GitHub link**: https://github.com/closespark/

## Email Variants

Each technology has 2-3 email variants for A/B testing:

```python
EMAIL_VARIANTS = {
    "Shopify": [
        {"id": "shopify_v1", "subject_template": "Shopify integration issue on {{domain}}?", ...},
        {"id": "shopify_v2", "subject_template": "Quick Shopify improvement idea for {{domain}}", ...},
        {"id": "shopify_v3", "subject_template": "Saw something in your Shopify setup", ...},
    ],
    "Salesforce": [...],
    "WordPress": [...],
    # ... 14 technologies with variants
}
```

### Supported Technologies with Variants

- Shopify, WooCommerce, Magento (Ecommerce)
- Salesforce, HubSpot (CRM/Marketing)
- WordPress (CMS)
- Stripe (Payments)
- Klaviyo, Mailchimp (Email Marketing)
- Google Analytics, GA4, Mixpanel (Analytics)
- Segment (CDP)
- Intercom (Live Chat)

## Technology Scoring

Technologies are scored by value/specialization (1-5 scale):

| Score | Category | Examples |
|-------|----------|----------|
| 5 | Enterprise | Salesforce, HubSpot, Marketo, Segment, Magento, Pardot |
| 4 | Ecommerce + Payments | Shopify, BigCommerce, Stripe, Klaviyo, Mixpanel |
| 3 | Mainstream CMS + Marketing | WordPress, WooCommerce, Mailchimp, Intercom, Drift |
| 2 | Infrastructure | AWS, Vercel, Netlify, Cloudflare |
| 1 | Basic Analytics | Google Analytics, GA4, Heap, Hotjar |

## Email Generation Output

```json
{
  "subject": "Shopify integration issue on sample-shop.com?",
  "body": "Hi — I'm Scott from CloseSpark in Richmond, VA...",
  "main_tech": "Shopify",
  "supporting_techs": ["Stripe", "Klaviyo"],
  "persona": "Scott",
  "persona_email": "scott@closespark.co",
  "persona_role": "Systems Engineer",
  "variant_id": "shopify_v1",
  "domain": "sample-shop.com"
}
```

### Example Generated Email

```
Subject: Shopify integration issue on sample-shop.com?

Hi — I'm Scott from CloseSpark in Richmond, VA.

I saw that sample-shop.com is running Shopify + Stripe, Klaviyo, and I specialize in short-term technical fixes for stacks like yours.

• Checkout or webhook issues affecting orders
• Payment + analytics events not lining up (GA4, Klaviyo, etc.)
• Small automation gaps that slow down the team

Hourly: $85/hr, strictly short-term — no long-term commitment.

If it would help to have a specialist jump in, you can grab time here:
https://calendly.com/closespark/technical-systems-consultation

– Scott
Systems Engineer, CloseSpark
https://github.com/closespark/
```

## Render Deployment

This repository includes a complete Render deployment kit for running a fully automated daily pipeline.

### Architecture

The system consists of two workers running as daily cron jobs:

1. **Pipeline Worker** (`pipeline_worker.py`) - Runs at 6:00 AM UTC
   - Scrapes Google Places for one business category (rotates through 250 categories)
   - Extracts and normalizes domains
   - Deduplicates against Supabase history
   - Scans each domain for technology stack
   - Extracts non-generic contact emails
   - Generates persona-based outreach emails with variant tracking
   - Stores results in Supabase

2. **Outreach Worker** (`outreach_worker.py`) - Runs at 2:00 PM UTC
   - Pulls leads with detected technologies and valid emails
   - Rotates through SMTP inboxes (each inbox = different persona)
   - Generates persona-specific emails based on detected tech stack
   - Sends personalized outreach emails (350-500/day)
   - Marks leads as emailed in Supabase

### SMTP Configuration

The `SMTP_ACCOUNTS_JSON` environment variable must use this format:

```json
{
  "inboxes": [
    {
      "email": "scott@closespark.co",
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "smtp_user": "scott@closespark.co",
      "smtp_password": "your_app_password"
    },
    {
      "email": "tracy@closespark.co",
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "smtp_user": "tracy@closespark.co",
      "smtp_password": "your_app_password"
    },
    {
      "email": "willa@closespark.co",
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "smtp_user": "willa@closespark.co",
      "smtp_password": "your_app_password"
    }
  ]
}
```

**Important**: The `email` field determines which persona sends the email. Use the exact email addresses shown above to match the persona map.

### Supabase Setup

Run the SQL schema in your Supabase SQL editor:

```sql
-- See config/supabase_schema.sql for full schema
create table tech_scans (
    id uuid primary key default gen_random_uuid(),
    domain text not null,
    technologies jsonb default '[]'::jsonb,
    scored_technologies jsonb default '[]'::jsonb,
    top_technology jsonb,
    emails jsonb default '[]'::jsonb,
    generated_email jsonb,  -- Contains persona, variant_id, subject, body
    category text,
    created_at timestamptz default now(),
    error text,
    emailed boolean,
    emailed_at timestamptz
);

create table domains_seen (
    domain text primary key,
    category text,
    first_seen timestamptz default now()
);
```

### Environment Variables

Set these in the Render dashboard:

#### Required Variables
| Variable | Description |
|----------|-------------|
| `SUPABASE_URL` | Your Supabase project URL |
| `SUPABASE_SERVICE_KEY` | Supabase service role key (secret) |
| `APIFY_TOKEN` | Apify API token (secret) |
| `SMTP_ACCOUNTS_JSON` | JSON object with "inboxes" array (secret) - see format above |

#### Optional Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Logging verbosity (DEBUG, INFO, WARNING, ERROR) |
| `APIFY_MAX_PLACES` | `1000` | Max places to crawl per Google Places search |
| `APIFY_ACTOR` | `compass/crawler-google-places` | Apify actor ID |
| `APIFY_POLL_INTERVAL` | `30` | Seconds between Apify run status polls |
| `APIFY_RUN_TIMEOUT` | `3600` | Maximum seconds to wait for Apify run (1 hour) |
| `SUPABASE_TABLE` | `tech_scans` | Table for scan results |
| `SUPABASE_DOMAIN_TABLE` | `domains_seen` | Table for domain tracking |
| `CATEGORIES_FILE` | `config/categories-250.json` | Path to categories JSON |
| `SCANNER_MAX_EMAIL_PAGES` | `10` | Max pages to crawl for emails per domain |
| `SCANNER_DISABLE_EMAILS` | `false` | Set to 'true' to skip email extraction |
| `CATEGORY_OVERRIDE` | - | Override the daily category selection |
| `OUTREACH_TABLE` | `tech_scans` | Table with leads |
| `OUTREACH_DAILY_LIMIT` | `500` | Max emails per day |
| `OUTREACH_PER_INBOX_LIMIT` | `50` | Max emails per SMTP inbox |
| `SMTP_SEND_DELAY_SECONDS` | `4` | Delay between emails in seconds |

### Deploy to Render

1. Fork this repository
2. Connect to Render
3. Import `render.yaml` (Infrastructure as Code)
4. Set secret environment variables in Render dashboard
5. Deploy!

### Local Development

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
nano .env

# Run pipeline worker
python pipeline_worker.py

# Run outreach worker
python outreach_worker.py

# Test SMTP sending
python scripts/smtp_test_send.py

# Preview emails (QA tool)
python scripts/preview_email.py --tech Shopify --from scott@closespark.co
```

## Email Preview CLI

The preview CLI lets you generate and view outreach emails without sending them — useful for QA and testing different persona/tech/variant combinations.

```bash
# Basic preview with Shopify and Scott persona
python scripts/preview_email.py --tech Shopify --from scott@closespark.co

# Preview with custom domain and supporting techs
python scripts/preview_email.py --tech HubSpot --from tracy@closespark.co --domain acme-corp.com --supporting Salesforce Stripe

# Generate multiple variants for comparison
python scripts/preview_email.py --tech Klaviyo --from willa@closespark.co --count 3

# List all available personas and technologies
python scripts/preview_email.py --list

# Output as JSON
python scripts/preview_email.py --tech Shopify --from scott@closespark.co --json
```

## Per-Variant Analytics

The system automatically tracks performance metrics for each combination of persona, technology, and variant. This data is stored in the `email_stats` table and populated by a Supabase trigger when emails are marked as sent.

### Tables

| Table | Purpose |
|-------|---------|
| `email_stats` | Aggregate counters per persona/tech/variant combo |
| `domain_email_history` | Per-domain history for variant suppression |

### Analytics Views

```sql
-- Top performing variants by send count
SELECT * FROM v_top_variants LIMIT 10;

-- Persona performance summary
SELECT * FROM v_persona_stats;

-- Tech performance summary
SELECT * FROM v_tech_stats;
```

## Variant Suppression

The system includes logic to avoid sending repetitive emails to the same domain:

- **Don't send the same variant twice** to the same domain
- **Don't send the same persona twice** to the same domain
- **Prefer new combinations first** before recycling

### Usage

```python
from hubspot_scanner import generate_persona_outreach_email

# Suppress previously used variants
email = generate_persona_outreach_email(
    domain="example.com",
    main_tech="Shopify",
    supporting_techs=["Stripe"],
    from_email="scott@closespark.co",
    domain_history={
        "used_variant_ids": ["shopify_v1", "shopify_v2"],  # Will select shopify_v3
    },
)
```

### Helper Functions

```python
from hubspot_scanner import (
    get_variant_for_tech,
    select_variant_with_suppression,
    get_unused_persona_for_domain,
)

# Get a variant excluding certain IDs
variant = get_variant_for_tech("Shopify", exclude_variant_ids=["shopify_v1"])

# Select variant with full suppression logic
variant = select_variant_with_suppression(
    main_tech="Shopify",
    from_email="scott@closespark.co",
    domain_history={"used_variant_ids": ["shopify_v1", "shopify_v2"]},
)

# Find an unused persona for a domain
persona = get_unused_persona_for_domain(
    domain="example.com",
    available_personas=["scott@closespark.co", "tracy@closespark.co", "willa@closespark.co"],
    used_personas=["scott@closespark.co"],
)
```

## Email Filtering

When scanning sites, the scanner crawls for email addresses and automatically filters out non-valuable contacts:

### Generic Email Prefixes
- info@, support@, admin@
- hello@, sales@, contact@
- help@, noreply@, webmaster@
- office@, team@, general@

### Disposable/Honeypot Domains
The scanner filters out emails from disposable and honeypot email services (e.g., mailinator.com, guerrillamail.com, tempmail.net). The blocklist contains 5,500+ domains from multiple community-maintained sources.

To update the blocklist:
```bash
python scripts/update_disposable_blocklist.py
```

## CLI Reference

### tech-scanner

```
usage: tech-scanner [-h] [-f DOMAINS_FILE] [-o OUTPUT_FILE] [-t TIMEOUT]
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

## Examples

See the `examples/` directory for more usage examples:

- `basic_usage.py` - Single domain scanning
- `batch_scan.py` - Multiple domain scanning with progress
- `category_scraper.py` - Scrape Google Places by business category using Apify
- `daily_pipeline.py` - Full automated lead generation pipeline
- `tech_scanner_usage.py` - Technology scanner with email generation examples
- `domains.txt` - Sample domain list
- `sample_output.json` - Example output format

## Supported Technologies

**Marketing & Sales:**
- CRM: Salesforce, Zoho, Pipedrive
- Marketing Automation: HubSpot, Marketo, Pardot, ActiveCampaign
- Email: Klaviyo, Mailchimp, SendGrid
- Live Chat: Intercom, Drift, Zendesk Chat, Freshchat

**Ecommerce:**
- Platforms: Shopify, WooCommerce, Magento, BigCommerce
- Payments: Stripe, PayPal, Braintree, Square

**Analytics & Testing:**
- Analytics: Google Analytics, Mixpanel, Amplitude, Heap, Hotjar
- A/B Testing: Optimizely, VWO, Google Optimize
- CDP: Segment

**Infrastructure:**
- CMS: WordPress, Webflow
- Hosting: AWS, Vercel, Netlify, Cloudflare

## Use Cases

- **Lead Generation**: Identify businesses using specific technologies and extract contact emails
- **Technology Profiling**: Discover the complete tech stack of target companies
- **Competitive Analysis**: Survey technology adoption across industries or competitors
- **Partnership Targeting**: Find companies using complementary technologies
- **RevOps Workflows**: Automate technology-based lead qualification
- **Automated Outreach**: Daily pipeline for discovering and contacting technology-matched prospects
- **A/B Testing**: Track which email variants perform best with variant_id metadata

## Requirements

- Python 3.10+
- requests
- beautifulsoup4
- lxml
- apify-client (for Google Places scraping)
- supabase (for data persistence)
- python-dotenv (for local development)

## License

MIT License

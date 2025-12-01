# HubSpot Presence Scanner

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-beta-yellow)

Lightweight domain crawler that detects HubSpot usage across business websites by scanning for tracking codes, COS signatures, embedded forms, script tags, and API endpoints. When HubSpot is detected, it also crawls the site to find non-generic email addresses. Built for consultants, revops teams, and automation workflows that need to identify HubSpot-powered organizations at scale.

> **Note**: This project is in **beta** status. The core scanning functionality is stable and actively used in production for lead generation workflows.

## Features

- **HubSpot Detection**: Scans HTML, script tags, metadata, and HTTP headers for HubSpot signatures
- **Confidence Scoring**: Returns a 0-100 confidence score based on detected signals
- **Portal ID Extraction**: Identifies HubSpot portal IDs when available
- **Email Extraction**: Crawls sites with HubSpot to find non-generic business emails
- **Generic Email Filtering**: Automatically excludes info@, support@, admin@, hello@, sales@, etc.
- **JSON Output**: Structured output with domain, signals, emails, and confidence scores
- **CLI & Library**: Use as command-line tool or import as Python library

## Installation

```bash
# Clone the repository
git clone https://github.com/inevitablesale/hubspot-presence-scanner.git
cd hubspot-presence-scanner

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

## Quick Start

### Command Line

```bash
# Scan a single domain
hubspot-scanner hubspot.com

# Scan multiple domains
hubspot-scanner hubspot.com drift.com example.com

# Scan domains from a file
hubspot-scanner -f examples/domains.txt

# Save results to JSON file
hubspot-scanner -f examples/domains.txt -o results.json

# Skip email extraction (faster)
hubspot-scanner hubspot.com --no-emails

# Increase pages crawled for emails
hubspot-scanner hubspot.com --max-pages 20
```

### Python Library

```python
from hubspot_scanner import scan_domain, scan_domains

# Scan a single domain
result = scan_domain("hubspot.com")
print(f"HubSpot detected: {result.hubspot_detected}")
print(f"Confidence: {result.confidence_score}%")
print(f"Emails: {result.emails}")

# Scan multiple domains
results = scan_domains(["hubspot.com", "drift.com", "example.com"])
for r in results:
    if r["hubspot_detected"]:
        print(f"{r['domain']}: {r['confidence_score']}% - Emails: {r['emails']}")
```

## Output Format

The scanner outputs structured JSON with the following fields:

```json
{
  "domain": "example.com",
  "hubspot_detected": true,
  "confidence_score": 95.0,
  "hubspot_signals": [
    {
      "name": "hs-script-loader",
      "description": "HubSpot tracking script loader",
      "weight": 30,
      "portal_id": "12345"
    }
  ],
  "portal_ids": ["12345"],
  "emails": ["john.smith@example.com", "jane.doe@example.com"],
  "error": null
}
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `domain` | string | The scanned domain |
| `hubspot_detected` | boolean | Whether HubSpot was detected |
| `confidence_score` | float | Confidence score (0-100) |
| `hubspot_signals` | array | List of detected HubSpot signatures |
| `portal_ids` | array | HubSpot portal IDs found |
| `emails` | array | Non-generic emails found (only if HubSpot detected) |
| `error` | string/null | Error message if scan failed |

## Detection Signals

The scanner looks for these HubSpot signatures:

### Script Tags
- `js.hs-scripts.com` - HubSpot tracking script loader
- `js.hs-analytics.net` - HubSpot analytics
- `track.hubspot.com` - Tracking endpoint
- `js.hsforms.net` - HubSpot forms
- `js.hscta.net` - Call-to-action scripts

### COS (Content Optimization System)
- `cdn2.hubspot.net` - HubSpot CDN
- `/hubfs/` - HubSpot File System
- `hs-cos-wrapper` - COS wrapper classes
- `hs-menu-wrapper` - Menu components

### API & Endpoints
- `api.hubspot.com` - API calls
- `forms.hubspot.com` - Form submissions

### Inline JavaScript
- `_hsq` - HubSpot tracking queue
- `hbspt.` - HubSpot JavaScript object

## Email Filtering

When HubSpot is detected, the scanner crawls the site for email addresses. The following emails are automatically excluded:

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

## CLI Options

```
usage: hubspot-scanner [-h] [-f DOMAINS_FILE] [-o OUTPUT_FILE] [-t TIMEOUT]
                       [--user-agent USER_AGENT] [-q] [--no-summary]
                       [--compact] [--no-emails] [--max-pages MAX_PAGES] [-v]
                       [domains ...]

Options:
  domains               Domain(s) to scan
  -f, --file            File containing domains (one per line)
  -o, --output          Output file for JSON results
  -t, --timeout         Request timeout in seconds (default: 10)
  --user-agent          Custom user agent string
  -q, --quiet           Suppress progress output
  --no-summary          Suppress summary output
  --compact             Output compact JSON
  --no-emails           Skip email extraction
  --max-pages           Max pages to crawl for emails (default: 10)
  -v, --version         Show version
```

## Examples

See the `examples/` directory for more usage examples:

- `basic_usage.py` - Single domain scanning
- `batch_scan.py` - Multiple domain scanning with progress
- `category_scraper.py` - Scrape Google Places by business category using Apify
- `daily_pipeline.py` - Full automated lead generation pipeline
- `zapmail_config.sample.json` - Sample Zapmail inbox configuration
- `domains.txt` - Sample domain list
- `sample_output.json` - Example output format

## Daily Pipeline

The `daily_pipeline.py` script implements an end-to-end automated pipeline that:

1. **Scrapes Google Places** - Uses Apify's Compass crawler to find businesses by category
2. **Deduplicates domains** - Tracks processed domains to avoid rescanning
3. **Scans for HubSpot** - Detects HubSpot presence and extracts portal IDs
4. **Extracts emails** - Crawls sites with HubSpot to find non-generic contact emails
5. **Saves structured leads** - Outputs JSON records with all lead data
6. **Sends outreach** - Optionally sends emails through Zapmail pre-warmed inboxes

### Pipeline Usage

```bash
# Set required environment variables
export APIFY_TOKEN="your_apify_token"

# Run the daily pipeline (uses today's category based on day of year)
python examples/daily_pipeline.py

# Run with a specific category
python examples/daily_pipeline.py --category "accountant"

# Dry run to see what would be processed
python examples/daily_pipeline.py --dry-run

# Enable email sending
export ZAPMAIL_CONFIG="path/to/zapmail_config.json"
python examples/daily_pipeline.py --send-emails
```

### Zapmail Configuration

To enable email sending, create a `zapmail_config.json` file (see `zapmail_config.sample.json`):

```json
{
  "inboxes": [
    {
      "email": "sender@warmeddomain.com",
      "smtp_host": "smtp.gmail.com",
      "smtp_port": 587,
      "smtp_user": "sender@warmeddomain.com",
      "smtp_password": "your_app_password",
      "daily_limit": 40
    }
  ]
}
```

## Use Cases

- **Lead Generation**: Find HubSpot users and extract contact emails
- **Competitive Analysis**: Identify which competitors use HubSpot
- **Market Research**: Survey HubSpot adoption across industries
- **Integration Planning**: Identify potential integration partners
- **RevOps Workflows**: Automate HubSpot user identification
- **Automated Outreach**: Daily pipeline for discovering and contacting prospects

## Render Deployment

This repository includes a complete Render deployment kit for running a fully automated daily pipeline.

### Architecture

The system consists of two workers running as daily cron jobs:

1. **Pipeline Worker** (`pipeline_worker.py`) - Runs at 6:00 AM UTC
   - Scrapes Google Places for one business category (rotates through 250 categories)
   - Extracts and normalizes domains
   - Deduplicates against Supabase history
   - Scans each domain for HubSpot presence
   - Extracts non-generic contact emails
   - Stores results in Supabase

2. **Outreach Worker** (`outreach_worker.py`) - Runs at 2:00 PM UTC
   - Pulls HubSpot-detected leads with emails
   - Rotates through Zapmail pre-warmed SMTP inboxes
   - Sends personalized outreach emails (350-500/day)
   - Marks leads as emailed in Supabase

### 📩 Automated Outreach Email (What the System Sends)

When the outreach worker runs, it selects HubSpot-detected domains with valid non-generic contact emails and sends a personalized outreach message through your Zapmail warmed inbox fleet.

The outreach email is intentionally simple, friendly, and relevant to the signal the scanner found.

**Default Outreach Template (`templates/outreach_email.txt`):**

```
Hey there,

I was reviewing {{domain}} and noticed that your website is running HubSpot — specifically a few tracking and COS (Content Optimization System) components that usually indicate there may be hidden optimization opportunities.

I run a daily HubSpot Presence Scanner that identifies technical gaps, workflow friction points, and automation leaks. If you'd like, I can share a quick breakdown of what the scanner found for your domain along with a couple of improvements that typically move the needle fast.

No pressure at all — happy to point you in the right direction.

– Chris
```

**Personalization Logic:**

- `{{domain}}` is inserted dynamically
- Sender rotates across your Zapmail warm inbox pool
- Limits are enforced per inbox (35–50/day)
- Once emailed, the lead is marked with `emailed = true` in Supabase

**Why this works:**

The email references:

- HubSpot usage (already confirmed by your scanner)
- Specific findings (signals + portal IDs)
- A clear value proposition
- A real person (Chris) offering expertise

This results in reply rates far above generic cold email because the email is tied directly to the scanner's findings.

### Supabase Setup

Run the SQL schema in your Supabase SQL editor:

```sql
-- See config/supabase_schema.sql for full schema
create table hubspot_scans (
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
| `SMTP_ACCOUNTS_JSON` | JSON array of SMTP inbox configs (secret) |

#### Optional Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Logging verbosity (DEBUG, INFO, WARNING, ERROR) |
| `APIFY_MAX_PLACES` | `1000` | Max places to crawl per Google Places search |
| `APIFY_ACTOR` | `compass/crawler-google-places` | Apify actor ID |
| `APIFY_POLL_INTERVAL` | `30` | Seconds between Apify run status polls |
| `APIFY_RUN_TIMEOUT` | `3600` | Maximum seconds to wait for Apify run (1 hour) |
| `SUPABASE_TABLE` | `hubspot_scans` | Table for scan results |
| `SUPABASE_DOMAIN_TABLE` | `domains_seen` | Table for domain tracking |
| `CATEGORIES_FILE` | `config/categories-250.json` | Path to categories JSON |
| `SCANNER_MAX_EMAIL_PAGES` | `10` | Max pages to crawl for emails per domain |
| `SCANNER_DISABLE_EMAILS` | `false` | Set to 'true' to skip email extraction |
| `CATEGORY_OVERRIDE` | - | Override the daily category selection |
| `OUTREACH_TABLE` | `hubspot_scans` | Table with leads |
| `OUTREACH_DAILY_LIMIT` | `500` | Max emails per day |
| `OUTREACH_PER_INBOX_LIMIT` | `50` | Max emails per SMTP inbox |
| `OUTREACH_EMAIL_TEMPLATE` | `templates/outreach_email.txt` | Path to email template |
| `OUTREACH_SUBJECT` | `Quick question about your website` | Email subject line |
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
```

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

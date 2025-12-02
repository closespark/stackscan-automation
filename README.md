# Tech Stack Scanner

![Python Version](https://img.shields.io/badge/python-3.10%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-beta-yellow)

Wappalyzer-style domain scanner that detects **40+ technologies** across business websites — including CRM systems, marketing automation, ecommerce platforms, payment processors, analytics tools, and more. Scores each technology by value and optionally generates personalized outreach emails. Built for consultants, revops teams, and lead generation workflows. See [Supported Technologies](#supported-technologies) for the full list.

> **Note**: This project is in **beta** status. The core scanning functionality is stable and actively used in production for lead generation workflows.

## Features

- **Multi-Technology Detection**: Detects 40+ technologies including HubSpot, Salesforce, Shopify, Stripe, and more
- **Technology Scoring**: Scores each technology 1-5 based on value and specialization
- **Automated Email Generation**: Generates personalized outreach emails based on detected stack
- **Email Extraction**: Crawls sites to find non-generic business email addresses
- **Generic Email Filtering**: Automatically excludes info@, support@, admin@, hello@, sales@, etc.
- **JSON Output**: Structured output with technologies, scores, and generated emails
- **CLI & Library**: Use as command-line tool or import as Python library

## Installation

```bash
# Clone the repository
git clone https://github.com/inevitablesale/stackscan-automation.git
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

# Custom consultant profile for emails
tech-scanner example.com --name "Alex" --location "Austin, TX" --rate "\$100/hr"
```

### Python Library

```python
from hubspot_scanner import scan_technologies, score_technologies, generate_outreach_email

# Scan a domain for technologies
result = scan_technologies("example.com")
print(f"Technologies: {result.technologies}")
print(f"Top tech: {result.top_technology['name']} (score: {result.top_technology['score']})")

# The generated email is ready to use
if result.generated_email:
    print(f"Subject: {result.generated_email['subject_lines'][0]}")
    print(f"Body: {result.generated_email['email_body']}")
```

### Technology Scoring

Technologies are scored by value/specialization (1-5 scale):

| Score | Category | Examples |
|-------|----------|----------|
| 5 | Enterprise | Salesforce, HubSpot, Marketo, Segment, Magento, Pardot |
| 4 | Ecommerce + Payments | Shopify, BigCommerce, Stripe, Klaviyo, Mixpanel |
| 3 | Mainstream CMS + Marketing | WordPress, WooCommerce, Mailchimp, Intercom, Drift |
| 2 | Infrastructure | AWS, Vercel, Netlify, Cloudflare |
| 1 | Basic Analytics | Google Analytics, GA4, Heap, Hotjar |

### Email Generation Output

```json
{
  "domain": "example.com",
  "technologies": ["Shopify", "Stripe", "Klaviyo", "Google Analytics"],
  "scored_technologies": [
    {"name": "Shopify", "score": 4, "category": "Ecommerce"},
    {"name": "Stripe", "score": 4, "category": "Payment Processor"},
    {"name": "Klaviyo", "score": 4, "category": "Email Marketing"},
    {"name": "Google Analytics", "score": 1, "category": "Analytics"}
  ],
  "top_technology": {
    "name": "Shopify",
    "score": 4,
    "category": "Ecommerce",
    "recent_project": "rebuilt a Shopify checkout flow and fixed server-side tracking for Stripe + Klaviyo events."
  },
  "generated_email": {
    "selected_technology": "Shopify",
    "subject_lines": [
      "Quick Shopify question",
      "Your Shopify setup",
      "Short-term Shopify help?"
    ],
    "email_body": "Hey there,\n\nI was looking at example.com and noticed you're using Shopify..."
  }
}
```

### Supported Technologies

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
                    [--no-email] [--name NAME] [--location LOCATION]
                    [--rate RATE] [-v]
                    [domains ...]

Options:
  domains               Domain(s) to scan
  -f, --file            File containing domains (one per line)
  -o, --output          Output file for JSON results
  -t, --timeout         Request timeout in seconds (default: 10)
  --no-email            Skip outreach email generation
  --name                Consultant name for email personalization
  --location            Consultant location for email personalization
  --rate                Consultant rate for email personalization
  -v, --version         Show version
```

## Examples

See the `examples/` directory for more usage examples:

- `basic_usage.py` - Single domain scanning
- `batch_scan.py` - Multiple domain scanning with progress
- `category_scraper.py` - Scrape Google Places by business category using Apify
- `daily_pipeline.py` - Full automated lead generation pipeline
- `tech_scanner_usage.py` - Technology scanner with email generation examples
- `zapmail_config.sample.json` - Sample Zapmail inbox configuration
- `domains.txt` - Sample domain list
- `sample_output.json` - Example output format

## Daily Pipeline

The `daily_pipeline.py` script implements an end-to-end automated pipeline that:

1. **Scrapes Google Places** - Uses Apify's Compass crawler to find businesses by category
2. **Deduplicates domains** - Tracks processed domains to avoid rescanning
3. **Scans for technologies** - Detects technology stack and extracts relevant signals
4. **Extracts emails** - Crawls sites to find non-generic contact emails
5. **Saves structured leads** - Outputs JSON records with all lead data
6. **Sends outreach** - Optionally sends personalized emails through Zapmail pre-warmed inboxes

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

- **Lead Generation**: Identify businesses using specific technologies and extract contact emails
- **Technology Profiling**: Discover the complete tech stack of target companies
- **Competitive Analysis**: Survey technology adoption across industries or competitors
- **Partnership Targeting**: Find companies using complementary technologies
- **RevOps Workflows**: Automate technology-based lead qualification
- **Automated Outreach**: Daily pipeline for discovering and contacting technology-matched prospects

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
   - Stores results in Supabase

2. **Outreach Worker** (`outreach_worker.py`) - Runs at 2:00 PM UTC
   - Pulls leads with detected technologies and valid emails
   - Rotates through Zapmail pre-warmed SMTP inboxes
   - Sends personalized outreach emails (350-500/day)
   - Marks leads as emailed in Supabase

### 📩 Automated Outreach Email (What the System Sends)

When the outreach worker runs, it selects domains with detected technologies and valid non-generic contact emails, then sends a personalized outreach message through your Zapmail warmed inbox fleet.

The outreach email is intentionally simple, friendly, and relevant to the technology signals the scanner found.

**Default Outreach Template (`templates/outreach_email.txt`):**

```
Hey there!

I was reviewing {{domain}} and noticed something interesting.

We run a daily scanner that looks at HubSpot implementations across local businesses. If you're open to it, I can share what we found and some suggestions that usually improve leadflow + automations within days.

Let me know if you'd like a quick breakdown.

– Chris
```

**Personalization Logic:**

- `{{domain}}` is inserted dynamically
- Sender rotates across your Zapmail warm inbox pool
- Limits are enforced per inbox (35–50/day)
- Once emailed, the lead is marked with `emailed = true` in Supabase

**Why this works:**

The email references:

- Detected technologies (confirmed by your scanner)
- Specific findings (signals + tech details)
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

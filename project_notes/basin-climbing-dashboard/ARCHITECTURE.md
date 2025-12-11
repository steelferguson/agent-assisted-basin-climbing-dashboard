# Basin Climbing Dashboard - System Architecture

## Project Overview

Customer Data Platform (CDP) and lifecycle automation system for Basin Climbing gym. Integrates data from multiple sources (Stripe, Square, Capitan, Mailchimp, Instagram) into unified customer profiles with event timelines and business rule automation.

## Core Components

### 1. Data Pipeline (`data_pipeline/`)

**Purpose:** Fetch, normalize, and aggregate data from all sources

**Key Modules:**
- `pipeline_handler.py` - Main orchestration and S3 upload functions
- `fetch_*.py` - API integrations for each data source
- `customer_deduplication.py` - Identity resolution across sources
- `customer_events_builder.py` - Event aggregation into timeline
- `business_rules.py` - Customer flagging and lifecycle automation
- `email_templates.py` - Email offer tracking with LLM analysis

**Data Flow:**
```
External APIs → Fetch modules → Normalization → Deduplication → Events → Business Rules → S3
```

### 2. Customer Identity Resolution

**Problem:** Same customer exists across multiple systems with different identifiers

**Solution:** Three-tier confidence matching system

**Files:**
- `customer_deduplication.py` - Core deduplication logic
- Output: `customer_master.csv` + `customer_identifiers.csv`

**Confidence Levels:**
- **Exact:** Deterministic matches (email, phone, membership ID)
- **Medium:** High-probability matches (name + context)
- **Low:** Fuzzy matches requiring review

**Approach:**
1. Collect all identifiers from all sources (emails, phones, names, IDs)
2. Build similarity graph using multiple matching strategies
3. Find connected components (clusters of identifiers belonging to same person)
4. Assign single `customer_id` to each cluster
5. Track confidence level for each identifier

### 3. Customer Event Timeline

**Purpose:** Unified chronological log of all customer interactions

**Event Types:**
- `day_pass_purchase` - Day pass transaction
- `membership_purchase` - New membership signup
- `membership_renewal` - Recurring membership payment
- `retail_purchase` - Retail/merchandise purchase
- `programming_purchase` - Fitness programming purchase
- `event_booking` - Event/class booking
- `checkin` - Gym check-in from Capitan
- `email_sent` - Email campaign sent (with offer details)
- `email_opened` - Email opened (future)
- `email_clicked` - Email link clicked (future)

**Schema:**
```
customer_id, event_date, event_type, event_source, source_confidence, event_details (JSON)
```

**Critical Implementation Detail:**
Dates MUST be parsed immediately when extracted from source DataFrames:
```python
date = pd.to_datetime(row.get('Date'), errors='coerce')
if pd.isna(date):
    continue  # Skip invalid dates
```

This prevents mixed date format issues that cause silent failures in pandas.

### 4. Business Rules Engine (Sprint 3)

**Purpose:** Automated customer lifecycle management

**Architecture:**
- Base class: `FlagRule` with `check()` method
- Each rule implements its own logic
- Rules can access customer master, events, identifiers

**Current Rules:**
- `NoEmailRule` - Flag customers without email addresses
- `LapsedMemberRule` - Flag members who haven't visited in 30+ days
- `PurchaseWithoutAccountRule` - Day pass buyers without Capitan accounts
- `ChurnRiskRule` - Members with declining visit frequency
- Future: Add more rules as needed (extensible design)

**Output:**
- `customer_flags.csv` with columns: customer_id, flag_type, flag_date, flag_details (JSON)

### 5. Offer Tracking System (Sprint 5)

**Purpose:** Track which customers received which marketing offers for conversion analysis

**Architecture Insight:**
"Analyze each email template once, track which customers received it"

**Components:**

**A. Email Template Analysis** (`email_templates.py`)
- Analyzes campaign HTML with Claude Haiku to extract offer details
- Caches results forever in `data/outputs/email_templates.json`
- Cost: ~$0.0007 per template (~$0.01 for 20 templates one-time)

**B. Mailchimp Integration** (`fetch_mailchimp_data.py`)
- Fetches campaign metadata and recipient lists from Mailchimp API
- Paginated recipient fetching with full email activity support

**C. Event Creation** (`customer_events_builder.py`)
- Creates `email_sent` events for each campaign recipient
- Inherits offer metadata from cached template analysis
- Matches recipients to customers via email lookup

**Offer Metadata Structure:**
```json
{
  "has_offer": true,
  "offer_type": "membership_discount",
  "offer_amount": "20%",
  "offer_code": "CLIMB20",
  "offer_expires": "2025-12-31",
  "offer_description": "20% off first month",
  "email_category": "welcome"
}
```

**Use Cases:**
- Conversion tracking: Did customers who received X offer convert?
- Duplicate prevention: Don't send same offer twice in 30 days
- Offer effectiveness: Which offers/amounts convert best?
- Attribution: Connect purchases to specific offers via promo codes

### 6. Daily Pipeline (`run_daily_pipeline.py`)

**Purpose:** Automated daily data refresh

**Schedule:** Designed to run as cron job (currently manual)

**Tasks:**
1. Fetch Stripe & Square transactions (last 2 days)
2. Fetch Capitan membership data (current state)
3. Fetch Capitan check-ins (last 7 days)
4. Fetch Instagram posts (last 30 days with AI vision analysis)
5. Fetch Mailchimp campaigns (last 90 days with AI content analysis)
6. Fetch Capitan associations & events (all)

**Output:** All data uploaded to S3 with monthly snapshots on 1st of month

## Data Storage

### S3 Structure

```
s3://basin-business-performance-dashboard/
├── transactions/
│   ├── combined_transactions.csv (current)
│   └── monthly_snapshots/
│       ├── 2025-01-01_combined_transactions.csv
│       └── 2025-02-01_combined_transactions.csv
├── capitan/
│   ├── memberships.csv
│   ├── checkins.csv
│   └── monthly_snapshots/
├── mailchimp/
│   ├── campaigns.csv
│   └── monthly_snapshots/
├── instagram/
│   ├── posts.csv
│   └── monthly_snapshots/
├── customers/
│   ├── customer_master.csv
│   ├── customer_identifiers.csv
│   ├── customer_events.csv
│   └── customer_flags.csv
└── facebook_ads/
    └── ads_data.csv
```

### Local Outputs (`data/outputs/`)

- Development and testing outputs
- Email template cache: `email_templates.json`
- Optional local copies when `save_local=True`

## Key Technical Patterns

### 1. Incremental Data Updates

**Pattern:** Merge new data with existing S3 data to avoid full refetch

```python
# Download existing data from S3
existing_df = download_from_s3(...)

# Fetch new data from API (e.g., last 7 days)
new_df = fetch_from_api(days_back=7)

# Merge: keep existing + add new (deduplicate on ID)
combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset='id')

# Upload back to S3
upload_to_s3(combined_df, ...)
```

### 2. Smart Caching for LLM Analysis

**Problem:** Re-analyzing same content wastes money and time

**Solution:** Content-based caching with hash detection

```python
# Check if content already analyzed
content_hash = hashlib.md5(html_content.encode()).hexdigest()
if content_hash in cache:
    return cache[content_hash]

# Analyze with LLM if new
analysis = call_claude(content)
cache[content_hash] = analysis
```

Used for:
- Instagram vision analysis
- Email template offer extraction
- Future: Any other LLM-based content analysis

### 3. Date Parsing Anti-Pattern

**CRITICAL:** Never parse dates on entire DataFrame columns with mixed formats

**Wrong:**
```python
# This fails silently with mixed timestamp/date strings
df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')
```

**Right:**
```python
# Parse immediately during extraction
for _, row in df.iterrows():
    date = pd.to_datetime(row.get('date_field'), errors='coerce')
    if pd.isna(date):
        continue  # Skip invalid dates
    events.append({'event_date': date, ...})
```

### 4. Confidence-Based Matching

**Pattern:** Track match quality for data quality monitoring

All customer identifiers and events include `source_confidence`:
- `exact` - Deterministic match (email, phone, ID)
- `medium` - High probability (name match with context)
- `low` - Fuzzy match requiring review
- `unmatched` - No customer match found

## Data Quality Monitoring

### Validation Checks

Run regularly to ensure data integrity:

1. **No duplicate customer IDs** in customer_master.csv
2. **All identifiers reference valid customers** (foreign key check)
3. **All events reference valid customers** (foreign key check)
4. **No future-dated events** (date sanity check)
5. **Time series consistency** (event dates within reasonable range)
6. **Customer coverage** (what % of customers have events?)
7. **Date quality** (no NULL/NaT dates in event timeline)

### Current Metrics (as of 2025-11-24)

- Total customers: 7,400
- Customers with events: 4,223 (57.1% coverage)
- Total events: 37,229
- Date quality: 0% NULL (all events have valid dates)
- Confidence distribution:
  - Exact: 85%
  - Medium: 12%
  - Low: 3%

## Sprint History

### Sprint 1-2: Data Pipeline Foundation
- Multi-source data integration
- S3 storage architecture
- Basic customer deduplication

### Sprint 3: Customer Lifecycle Automation (COMPLETED)
- Business rules engine
- Customer flagging system
- Event timeline foundation

### Sprint 4: (Future)
- TBD

### Sprint 5: Offer Tracking System (COMPLETED 2025-11-24)
- Email template analysis with LLM
- Mailchimp recipient tracking
- Offer-attributed events
- Conversion analysis foundation

## Future Enhancements

### Near-term
- Dashboard visualizations in Streamlit
- Customer cohort analysis
- Conversion funnel metrics
- Email open/click tracking

### Long-term
- Real-time event streaming
- Predictive churn modeling
- Automated offer optimization
- SMS integration with offer tracking
- Advanced segmentation engine

## Configuration

### API Keys Required
- Stripe API key
- Square access token
- Capitan API token
- Mailchimp API key + server prefix
- Instagram Graph API token
- Anthropic API key (for LLM analysis)
- AWS credentials (for S3)

### Configuration File
`data_pipeline/config.py` - All settings and credentials

## Testing

### Test Files
- `test_offer_tracking.py` - End-to-end Sprint 5 validation
- Individual module tests in `data_pipeline/*.py` `__main__` blocks

### Test Pattern
```bash
# Test specific module
python data_pipeline/email_templates.py

# Test full pipeline
python data_pipeline/customer_events_builder.py

# Test daily pipeline
python run_daily_pipeline.py
```

## Known Issues & Gotchas

1. **Mixed date formats** - Always parse dates immediately during extraction
2. **Email case sensitivity** - Normalize emails to lowercase before matching
3. **Mailchimp pagination** - Recipient lists can be large, use pagination
4. **LLM costs** - Cache aggressively to avoid re-analysis
5. **S3 eventual consistency** - Account for slight delays in updates

## Key Files Reference

| File | Purpose | Critical Sections |
|------|---------|-------------------|
| `pipeline_handler.py` | Main orchestration | Lines 574-750 (update_customer_master) |
| `customer_events_builder.py` | Event aggregation | Lines 212-331 (add_mailchimp_events) |
| `email_templates.py` | LLM offer extraction | Lines 15-80 (analyze_email_with_claude) |
| `business_rules.py` | Lifecycle automation | Lines 100-200 (FlagRule classes) |
| `customer_deduplication.py` | Identity resolution | Lines 50-300 (deduplication logic) |
| `run_daily_pipeline.py` | Daily automation | Entire file (orchestration) |

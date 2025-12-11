# Sprint 5: Offer Tracking Implementation

**Status:** ✅ COMPLETED (2025-11-24)
**Git Commits:** ded4c01, 6d4bb30

## Business Goal

Track which customers received which marketing offers to enable:
1. **Conversion attribution** - Did customers who received offer X convert?
2. **Duplicate prevention** - Don't send same offer twice in 30 days
3. **Offer optimization** - Which offers/amounts convert best?
4. **ROI analysis** - Cost per acquisition for each campaign

## Architecture Decision

### Key Insight
"Analyze each email template once, track which customers received it"

### Why This Approach?

**Cost Analysis:**
- Claude Haiku: ~$0.0007 per email analysis
- 20 unique email templates = ~$0.01 one-time cost
- Ongoing cost after initial analysis: $0.00

**Alternative Approaches Rejected:**
1. **Analyze every email sent** - 800 customers × 3 emails/week × $0.0007 = $100+/month ❌
2. **Manual logging** - Requires manual work for every campaign ❌
3. **No tracking** - Can't measure offer effectiveness ❌

**Chosen Approach:** Template-based analysis ✅
- Analyze template once with LLM
- Cache result forever
- Track which customers received which template
- Near-zero ongoing cost

## System Components

### 1. Email Template Analysis (`data_pipeline/email_templates.py`)

**Purpose:** Extract offer details from email HTML using Claude

**Key Functions:**

#### `analyze_email_with_claude()`
```python
def analyze_email_with_claude(
    campaign_id: str,
    campaign_title: str,
    email_subject: str,
    email_html: str,
    anthropic_api_key: str
) -> Dict
```

**What it does:**
- Sends email HTML to Claude Haiku
- Extracts structured offer information
- Returns metadata dictionary

**Output Schema:**
```json
{
  "campaign_id": "abc123",
  "campaign_title": "Welcome to Basin",
  "email_subject": "Welcome! Here's 20% off",
  "has_offer": true,
  "offer_type": "membership_discount",
  "offer_amount": "20%",
  "offer_code": "CLIMB20",
  "offer_expires": "2025-12-31",
  "offer_description": "20% off first month membership",
  "email_category": "welcome"
}
```

**Offer Types:**
- `membership_discount` - Discount on membership
- `day_pass_discount` - Discount on day pass
- `retail_discount` - Discount on retail items
- `free_trial` - Free trial period
- `other` - Other promotional offers
- `null` - No offer present

**Email Categories:**
- `welcome` - Welcome/onboarding emails
- `promotional` - Sales and offers
- `newsletter` - Regular updates
- `transactional` - Receipts, confirmations
- `re-engagement` - Win-back campaigns
- `other` - Uncategorized

#### `get_campaign_template()`
```python
def get_campaign_template(
    campaign_id: str,
    campaign_title: str,
    email_subject: str,
    email_html: str,
    anthropic_api_key: str,
    force_reanalyze: bool = False
) -> Dict
```

**What it does:**
- Checks if campaign already analyzed (cache lookup)
- If cached: Returns immediately (FREE)
- If new: Analyzes with Claude (~$0.0007)
- Saves to cache for future use

**Cache File:** `data/outputs/email_templates.json`

**Cache Structure:**
```json
{
  "campaign_abc123": {
    "campaign_id": "abc123",
    "campaign_title": "Welcome",
    "has_offer": true,
    "offer_type": "membership_discount",
    ...
  },
  "campaign_xyz789": {
    ...
  }
}
```

### 2. Mailchimp Recipient Fetching (`data_pipeline/fetch_mailchimp_data.py`)

**New Method Added:** `get_campaign_recipients()`

```python
def get_campaign_recipients(self, campaign_id: str, count: int = 1000) -> List[Dict]
```

**What it does:**
- Fetches full recipient list for a campaign from Mailchimp API
- Handles pagination (API returns max 1000 per page)
- Returns list of recipient dictionaries with email addresses

**Example Output:**
```python
[
  {
    "email_address": "customer1@example.com",
    "status": "sent",
    ...
  },
  {
    "email_address": "customer2@example.com",
    "status": "sent",
    ...
  }
]
```

### 3. Customer Event Creation (`data_pipeline/customer_events_builder.py`)

**Completely Rewrote:** `add_mailchimp_events()` method (lines 212-331)

**Process Flow:**

```
1. Load Mailchimp campaigns from S3 (df_mailchimp)
   ↓
2. For each campaign:
   ↓
3. Fetch campaign HTML content
   ↓
4. Analyze template with Claude (cached if seen before)
   ↓
5. Fetch campaign recipients from Mailchimp API
   ↓
6. For each recipient:
   ↓
7. Look up customer_id from email
   ↓
8. Create email_sent event with offer details
   ↓
9. Append to events list
```

**Event Details Structure:**
```python
{
  'customer_id': 'cust_123',
  'event_date': datetime(2025, 11, 23, 9, 0, 0),
  'event_type': 'email_sent',
  'event_source': 'mailchimp',
  'source_confidence': 'exact',  # Based on email match
  'event_details': json.dumps({
    'campaign_id': 'abc123',
    'campaign_title': 'Welcome to Basin',
    'email_subject': 'Welcome! Here's 20% off',
    'recipient_email': 'customer@example.com',
    'has_offer': True,
    'offer_type': 'membership_discount',
    'offer_amount': '20%',
    'offer_code': 'CLIMB20',
    'offer_expires': '2025-12-31',
    'offer_description': '20% off first month',
    'email_category': 'welcome'
  })
}
```

**Key Implementation Details:**

1. **Template Analysis (Lines 260-270)**
```python
# Get campaign content for template analysis
content = mailchimp_fetcher.get_campaign_content(campaign_id)
subject_line = campaign_row.get('subject_line', '')

# Analyze template with Claude (cached if seen before)
template_metadata = get_campaign_template(
    campaign_id=campaign_id,
    campaign_title=campaign_title,
    email_subject=subject_line,
    email_html=content.get('html', ''),
    anthropic_api_key=anthropic_api_key
)
```

2. **Recipient Fetching (Lines 272-277)**
```python
# Get recipients for this campaign
recipients = mailchimp_fetcher.get_campaign_recipients(campaign_id)

if not recipients:
    print(f"    ⚠️  No recipients found")
    continue
```

3. **Customer Matching (Lines 280-295)**
```python
for recipient in recipients:
    recipient_email = recipient.get('email_address', '').lower().strip()

    if not recipient_email:
        continue

    # Look up customer_id from email
    customer_match = self._lookup_customer(recipient_email)

    if not customer_match:
        recipients_unmatched += 1
        continue

    customer_id = customer_match['customer_id']
    confidence = customer_match['confidence']
    recipients_matched += 1
```

4. **Event Creation with Offer Details (Lines 297-326)**
```python
# Build event details with template metadata
event_details = {
    'campaign_id': campaign_id,
    'campaign_title': campaign_title,
    'email_subject': subject_line,
    'recipient_email': recipient_email
}

# Add offer details if present
if template_metadata.get('has_offer'):
    event_details['has_offer'] = True
    event_details['offer_type'] = template_metadata.get('offer_type')
    event_details['offer_amount'] = template_metadata.get('offer_amount')
    event_details['offer_code'] = template_metadata.get('offer_code')
    event_details['offer_expires'] = template_metadata.get('offer_expires')
    event_details['offer_description'] = template_metadata.get('offer_description')
    event_details['email_category'] = template_metadata.get('email_category')
else:
    event_details['has_offer'] = False
    event_details['email_category'] = template_metadata.get('email_category')

self.events.append({
    'customer_id': customer_id,
    'event_date': send_date,
    'event_type': 'email_sent',
    'event_source': 'mailchimp',
    'source_confidence': confidence,
    'event_details': json.dumps(event_details)
})
```

### 4. Pipeline Integration (`data_pipeline/pipeline_handler.py`)

**Modified:** `update_customer_master()` function (lines 638-671)

**Changes Made:**

1. **Load Mailchimp Campaign Data**
```python
# Load Mailchimp campaign data for email event tracking
df_mailchimp = pd.DataFrame()
mailchimp_fetcher = None
try:
    from data_pipeline.fetch_mailchimp_data import MailchimpDataFetcher

    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_mailchimp_campaigns)
    df_mailchimp = uploader.convert_csv_to_df(csv_content)
    print(f"📥 Loaded {len(df_mailchimp)} Mailchimp campaigns for event building")
```

2. **Initialize Mailchimp Fetcher**
```python
    # Initialize Mailchimp fetcher for recipient-level data
    if config.mailchimp_api_key and config.mailchimp_server_prefix:
        mailchimp_fetcher = MailchimpDataFetcher(
            api_key=config.mailchimp_api_key,
            server_prefix=config.mailchimp_server_prefix,
            anthropic_api_key=config.anthropic_api_key
        )
        print("✅ Mailchimp fetcher initialized for recipient tracking")
    else:
        print("⚠️  Mailchimp API credentials not configured - skipping email events")
```

3. **Pass to Event Builder**
```python
# Build customer events
df_events = customer_events_builder.build_customer_events(
    df_master,
    df_identifiers,
    df_transactions=df_transactions,
    df_checkins=df_checkins,
    df_mailchimp=df_mailchimp,
    mailchimp_fetcher=mailchimp_fetcher,
    anthropic_api_key=config.anthropic_api_key
)
```

### 5. Daily Pipeline Integration

**File:** `run_daily_pipeline.py`

**Existing Integration (Lines 71-81):**
```python
# 5. Update Mailchimp data (last 90 days with AI content analysis)
print("5. Fetching Mailchimp campaigns (last 90 days with AI content analysis)...")
try:
    upload_new_mailchimp_data(
        save_local=False,
        enable_content_analysis=True,  # Enable AI content analysis for new campaigns
        days_to_fetch=90
    )
    print("✅ Mailchimp data updated successfully\n")
except Exception as e:
    print(f"❌ Error updating Mailchimp data: {e}\n")
```

**How It Works:**
1. Daily pipeline calls `upload_new_mailchimp_data()`
2. Function fetches campaigns from Mailchimp API (last 90 days)
3. For NEW campaigns, analyzes with Claude and caches results
4. For EXISTING campaigns, skips analysis (uses cache)
5. Uploads campaign metadata to S3
6. When `update_customer_master()` runs, it:
   - Loads campaign metadata from S3
   - Fetches recipients from Mailchimp API
   - Creates events with cached offer analysis

**Cost Model:**
- Day 1: 3 new campaigns × $0.0007 = $0.0021
- Day 2: 1 new campaign × $0.0007 = $0.0007
- Day 3: 0 new campaigns × $0.0007 = $0.0000
- Month 1 total: ~20 campaigns = ~$0.01
- Month 2 onwards: ~$0.002/month (only new templates)

## Testing

### Test Script: `test_offer_tracking.py`

**What It Tests:**
1. Loading customer data from S3
2. Loading Mailchimp campaigns from S3
3. Initializing Mailchimp fetcher with API credentials
4. Building customer events with Mailchimp offer tracking
5. Template caching functionality
6. Offer detection and extraction

**Test Results (2025-11-24):**
```
✅ Loaded 7,400 customers
✅ Loaded 3 campaigns
✅ Mailchimp fetcher initialized
✅ 6,295 email_sent events created
✅ 2,102 unique customers reached
✅ Template caching working (3 templates cached)
✅ 2 campaigns with offers identified
```

**Example Output:**
```
📧 Email Events Created: 6,295
   Unique customers reached: 2,102

📋 Sample Events:

   Event 1:
      Date: 2025-11-15 09:00:00
      Customer: cust_abc123
      Campaign: Welcome to Basin
      Subject: Welcome! Here's 20% off
      Has Offer: True
      Offer Type: membership_discount
      Offer Amount: 20%
      Offer Code: WELCOME20

🗂️  Template Cache Status:
   Total templates cached: 3
   Templates with offers: 2

   Campaigns with offers:
      - Welcome to Basin: 20% off first month membership
      - Holiday Special: $10 off day passes
```

## Use Cases & Queries

### 1. Conversion Attribution

**Question:** Did customers who received the WELCOME20 offer convert to memberships?

**Query:**
```python
# Get customers who received WELCOME20 offer
offer_recipients = df_events[
    (df_events['event_type'] == 'email_sent') &
    (df_events['event_details'].str.contains('WELCOME20'))
]['customer_id'].unique()

# Get their membership purchases
conversions = df_events[
    (df_events['customer_id'].isin(offer_recipients)) &
    (df_events['event_type'] == 'membership_purchase')
]

conversion_rate = len(conversions) / len(offer_recipients)
print(f"Conversion rate: {conversion_rate:.1%}")
```

### 2. Duplicate Prevention

**Question:** Which customers already received WELCOME20 in the last 30 days?

**Query:**
```python
from datetime import datetime, timedelta

recent_offers = df_events[
    (df_events['event_type'] == 'email_sent') &
    (df_events['event_date'] > datetime.now() - timedelta(days=30)) &
    (df_events['event_details'].str.contains('WELCOME20'))
]['customer_id'].unique()

# Filter out these customers from next campaign
eligible_customers = df_master[
    ~df_master['customer_id'].isin(recent_offers)
]
```

### 3. Offer Effectiveness

**Question:** Which offer type has the best conversion rate?

**Query:**
```python
import json

# Extract offer type and measure conversion
for offer_type in ['membership_discount', 'day_pass_discount', 'retail_discount']:
    # Get recipients of this offer type
    recipients = []
    for _, row in df_events[df_events['event_type'] == 'email_sent'].iterrows():
        details = json.loads(row['event_details'])
        if details.get('offer_type') == offer_type:
            recipients.append(row['customer_id'])

    # Get conversions
    conversions = df_events[
        (df_events['customer_id'].isin(recipients)) &
        (df_events['event_type'] == 'membership_purchase')
    ]

    rate = len(conversions) / len(recipients) if recipients else 0
    print(f"{offer_type}: {rate:.1%} conversion rate")
```

### 4. Time to Conversion

**Question:** How long after receiving an offer do customers convert?

**Query:**
```python
# For each customer who converted
for customer_id in conversions['customer_id'].unique():
    # Find offer email date
    offer_date = df_events[
        (df_events['customer_id'] == customer_id) &
        (df_events['event_type'] == 'email_sent') &
        (df_events['event_details'].str.contains('has_offer": true'))
    ]['event_date'].min()

    # Find conversion date
    conversion_date = df_events[
        (df_events['customer_id'] == customer_id) &
        (df_events['event_type'] == 'membership_purchase')
    ]['event_date'].min()

    if offer_date and conversion_date:
        days_to_convert = (conversion_date - offer_date).days
        print(f"Customer {customer_id}: {days_to_convert} days to convert")
```

## Future Enhancements

### Phase 2 (Future Sprint)
- Email open tracking (Mailchimp API supports this)
- Email click tracking (track link clicks)
- Create `email_opened` and `email_clicked` events

### Phase 3 (Future)
- SMS offer tracking (integrate with Twilio)
- A/B testing framework for offers
- Automated offer optimization based on conversion rates
- Predictive modeling: Which customers most likely to convert with offer?

## Documentation

- Architecture: `ARCHITECTURE.md`
- Implementation details: This file
- Offer tracking overview: `data_pipeline/OFFER_TRACKING.md`

## Files Created/Modified

**Created:**
- `data_pipeline/email_templates.py` - Template analysis and caching
- `data_pipeline/OFFER_TRACKING.md` - Architecture documentation
- `test_offer_tracking.py` - End-to-end testing

**Modified:**
- `data_pipeline/customer_events_builder.py` - Added Mailchimp event creation
- `data_pipeline/fetch_mailchimp_data.py` - Added recipient fetching
- `data_pipeline/pipeline_handler.py` - Integrated Mailchimp into events pipeline

## Success Metrics

✅ System complete and operational
✅ 0 NULL dates in event timeline
✅ 6,295 email events created from 3 campaigns
✅ 2,102 customers matched to email recipients
✅ Template caching working (avoiding re-analysis costs)
✅ Daily pipeline fetches Mailchimp campaigns automatically
✅ Conversion analysis queries ready to use

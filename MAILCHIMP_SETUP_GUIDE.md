# Mailchimp Integration Setup Guide

## Overview

This guide walks you through connecting Mailchimp email campaign and landing page data to your analytics system using the Mailchimp Marketing API v3.0.

## What Data Can We Pull from Mailchimp?

### Email Campaign Metrics
- **Campaign Performance**: Opens, clicks, bounces, unsubscribes
- **Open Rate**: Percentage of delivered emails that were opened
- **Click Rate**: Percentage of delivered emails with at least 1 click
- **Click-Through Rate (CTR)**: Clicks as percentage of opens
- **Engagement Over Time**: Track performance trends across campaigns
- **Top Performing Campaigns**: Identify what content resonates
- **Link-Level Analytics**: See which specific links get clicked most
- **Geographic Data**: Where your subscribers are located
- **Device/Client Data**: What devices/email clients people use

### Landing Page Metrics
- **Page Views**: Total visits to your landing pages
- **Unique Visitors**: Unique people who visited
- **Signups**: Form submissions on landing pages
- **Conversion Rate**: Signups divided by unique visitors
- **Revenue**: E-commerce data from product blocks on pages
- **Traffic Sources**: Where visitors came from (if tracked)

### Audience/Subscriber Metrics
- **List Growth**: Track subscriber additions over time
- **Churn Rate**: Unsubscribe trends
- **Engagement Segments**: Active vs. inactive subscribers
- **Demographics**: Location, signup source

## Prerequisites

**What You Need:**
1. Mailchimp account (Basin Climbing's account)
2. Admin access to the Mailchimp account
3. API key (we'll generate this together)
4. Data center prefix (found in your Mailchimp account URL)

## Step-by-Step Setup Process

### Phase 1: Generate Mailchimp API Key (5 minutes)

1. Log into Basin Climbing's Mailchimp account
2. Click your profile icon in the top right
3. Select **"Account & Billing"** from the dropdown
4. Navigate to **"Extras"** → **"API keys"**
5. Click **"Create A Key"**
6. **Copy the key immediately!** You won't be able to see it again
7. Give it a descriptive name like "Basin Analytics Dashboard"

**Find Your Data Center Prefix:**
- Look at your Mailchimp account URL
- Example: If URL is `https://us19.admin.mailchimp.com/`, your prefix is **us19**
- Save this - you'll need it for API calls

### Phase 2: Test API Access (5 minutes)

We'll test the API connection using Python:

```bash
# Install the Mailchimp Python library
pip install mailchimp-marketing
```

**Test Script:**
```python
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError

# Set up client
client = MailchimpMarketing.Client()
client.set_config({
    "api_key": "your-api-key-here",
    "server": "us19"  # Replace with your data center prefix
})

# Test connection
try:
    response = client.ping.get()
    print("✅ API connection successful!")
    print(response)
except ApiClientError as error:
    print("❌ Error:", error.text)
```

If this works, you're ready to integrate!

---

## Available Mailchimp API Endpoints

### Campaigns API
- **GET /campaigns** - List all campaigns
- **GET /campaigns/{campaign_id}** - Get specific campaign details
- **GET /reports/{campaign_id}** - Get campaign performance report

### Reports API (Most Important for Analytics)
- **GET /reports** - List all campaign reports
- **GET /reports/{campaign_id}** - Full campaign report with all metrics
- **GET /reports/{campaign_id}/email-activity** - Individual subscriber activity
- **GET /reports/{campaign_id}/click-details** - Link-level click data
- **GET /reports/{campaign_id}/locations** - Geographic breakdown
- **GET /reports/{campaign_id}/sent-to** - Who received the campaign

### Landing Pages API
- **GET /landing-pages** - List all landing pages
- **GET /landing-pages/{page_id}** - Get specific page details
- **GET /reports/landing-pages/{page_id}** - Landing page performance report

### Lists (Audiences) API
- **GET /lists** - All audience lists
- **GET /lists/{list_id}** - Specific list details
- **GET /lists/{list_id}/growth-history** - Subscriber growth over time
- **GET /lists/{list_id}/members** - All subscribers in a list

---

## Key Metrics Returned by Reports API

### Campaign Report Response (GET /reports/{campaign_id})

```json
{
  "id": "campaign_id_here",
  "campaign_title": "January Newsletter",
  "type": "regular",
  "emails_sent": 3375,
  "abuse_reports": 0,
  "unsubscribed": 5,
  "send_time": "2024-01-15T10:00:00+00:00",
  "bounces": {
    "hard_bounces": 2,
    "soft_bounces": 3,
    "syntax_errors": 0
  },
  "opens": {
    "opens_total": 1250,
    "unique_opens": 980,
    "open_rate": 0.29,
    "last_open": "2024-01-16T15:30:00+00:00"
  },
  "clicks": {
    "clicks_total": 420,
    "unique_clicks": 315,
    "unique_subscriber_clicks": 310,
    "click_rate": 0.093,
    "last_click": "2024-01-16T18:45:00+00:00"
  },
  "ecommerce": {
    "total_orders": 12,
    "total_spent": 1245.50,
    "total_revenue": 1245.50
  }
}
```

### Landing Page Report Response

```json
{
  "id": "page_id_here",
  "name": "Free Trial Signup",
  "title": "Start Your Free Trial",
  "url": "https://mailchi.mp/yourdomain/free-trial",
  "published_at": "2024-01-01T00:00:00+00:00",
  "visits": 1250,
  "unique_visits": 980,
  "subscribes": 145,
  "conversion_rate": 0.148,
  "ecommerce": {
    "total_revenue": 850.00,
    "total_orders": 8
  }
}
```

---

## Integration Plan for Your Dashboard

### Architecture (Same Pattern as Instagram & Revenue Data)

**Data Flow:**
```
Mailchimp API → fetch_mailchimp_data.py → S3 Storage → Analytics Agent → Dashboard
```

### Implementation Steps

#### Step 1: Create Mailchimp Data Fetcher

Create `data_pipeline/fetch_mailchimp_data.py`:

**Class Structure:**
```python
class MailchimpDataFetcher:
    """Fetch campaign and landing page data from Mailchimp API"""

    def __init__(self, api_key: str, server_prefix: str):
        """Initialize with API credentials"""

    def get_campaigns(self, since: Optional[datetime] = None) -> List[Dict]:
        """Fetch all campaigns since a given date"""

    def get_campaign_report(self, campaign_id: str) -> Dict:
        """Get detailed report for a specific campaign"""

    def get_all_campaign_reports(self, since: Optional[datetime] = None) -> pd.DataFrame:
        """Fetch reports for all campaigns and return as DataFrame"""

    def get_landing_pages(self) -> List[Dict]:
        """Fetch all landing pages"""

    def get_landing_page_reports(self) -> pd.DataFrame:
        """Fetch performance data for all landing pages"""

    def get_list_growth_history(self, list_id: str) -> pd.DataFrame:
        """Fetch subscriber growth history for a list"""
```

**Data Schema (campaigns.csv):**
```csv
campaign_id,campaign_title,type,send_time,emails_sent,delivered,opens_total,unique_opens,open_rate,clicks_total,unique_clicks,click_rate,unsubscribed,bounces_hard,bounces_soft,revenue,orders
```

**Data Schema (landing_pages.csv):**
```csv
page_id,page_name,page_title,page_url,published_at,visits,unique_visits,subscribes,conversion_rate,revenue,orders
```

**Data Schema (audience_growth.csv):**
```csv
date,list_id,list_name,total_subscribers,new_subscribers,unsubscribed,cleaned,net_growth
```

#### Step 2: Add to pipeline_handler.py

Add function similar to Instagram and Capitan:

```python
def upload_new_mailchimp_data(save_local=False, days_to_fetch=90):
    """
    Fetch Mailchimp campaign and landing page data and upload to S3.

    Args:
        save_local: If True, also save CSV files locally
        days_to_fetch: How many days back to fetch campaigns (default 90)
    """
    print("\n=== Fetching Mailchimp Data ===")

    # Initialize fetcher
    fetcher = MailchimpDataFetcher(
        api_key=config.mailchimp_api_key,
        server_prefix=config.mailchimp_server_prefix
    )

    # 1. Fetch campaign reports (last X days)
    # 2. Download existing data from S3
    # 3. Merge new + existing (remove duplicates)
    # 4. Upload back to S3
    # 5. Do the same for landing pages and audience growth
```

Add to daily pipeline in `replace_days_in_transaction_df_in_s3()`:

```python
# Update Mailchimp data (last 90 days)
print("\n=== Updating Mailchimp Data ===")
try:
    upload_new_mailchimp_data(
        save_local=False,
        days_to_fetch=90  # 3 months of campaign data
    )
    print("✅ Mailchimp data updated successfully")
except Exception as e:
    print(f"❌ Error updating Mailchimp data: {e}")
```

#### Step 3: Add Mailchimp Tools to Analytics Agent

Add to `agent/analytics_tools.py`:

**Potential Tools:**
- `get_mailchimp_campaign_performance` - Top/worst performing campaigns
- `get_mailchimp_engagement_trends` - Open/click rate trends over time
- `get_mailchimp_landing_page_performance` - Landing page conversion rates
- `get_mailchimp_audience_growth` - Subscriber growth analysis
- `get_mailchimp_best_send_times` - When campaigns perform best
- `create_mailchimp_campaign_chart` - Visualize campaign performance
- `create_mailchimp_funnel_chart` - Email → Open → Click → Purchase funnel

---

## Configuration Setup

### Add to data_pipeline/config.py

```python
# Mailchimp API Configuration
mailchimp_api_key = os.getenv('MAILCHIMP_API_KEY')
mailchimp_server_prefix = os.getenv('MAILCHIMP_SERVER_PREFIX', 'us19')  # Default to us19

# S3 paths for Mailchimp data
s3_path_mailchimp_campaigns = 'mailchimp/campaigns.csv'
s3_path_mailchimp_landing_pages = 'mailchimp/landing_pages.csv'
s3_path_mailchimp_audience_growth = 'mailchimp/audience_growth.csv'
```

### Add to Environment Variables (~/.zshrc)

```bash
export MAILCHIMP_API_KEY="your-api-key-here"
export MAILCHIMP_SERVER_PREFIX="us19"  # Your data center prefix
```

---

## API Rate Limits

- **10 simultaneous connections** maximum
- No strict rate limit per hour, but be reasonable
- Use pagination (count/offset) for large datasets
- Consider caching data in S3 to reduce API calls

---

## Security Notes

- API key provides **full account access**
- Never commit API key to Git
- Keep it secure like a password
- Use environment variables only
- Regenerate if compromised

---

## Useful Analytics Questions Mailchimp Can Answer

1. **Campaign Performance**
   - "Which email campaigns had the highest open rates in the last 3 months?"
   - "What's our average click-through rate compared to industry benchmarks?"
   - "Which campaigns drove the most revenue?"

2. **Content Optimization**
   - "What subject lines performed best?"
   - "Which links in our emails get clicked most?"
   - "What day/time do our emails perform best?"

3. **Audience Insights**
   - "How is our subscriber list growing over time?"
   - "What's our monthly churn rate (unsubscribes)?"
   - "Where are our most engaged subscribers located?"

4. **Landing Pages**
   - "Which landing pages have the highest conversion rates?"
   - "How much revenue came from landing page signups?"
   - "Which pages get the most traffic?"

5. **ROI & Revenue**
   - "How much revenue did email campaigns generate this month?"
   - "What's the revenue per email sent?"
   - "Which campaigns had the best ROI?"

---

## Example API Calls (Python)

### Get All Campaigns from Last 90 Days

```python
import mailchimp_marketing as MailchimpMarketing
from datetime import datetime, timedelta

client = MailchimpMarketing.Client()
client.set_config({
    "api_key": "your-api-key",
    "server": "us19"
})

# Calculate date 90 days ago
ninety_days_ago = datetime.now() - timedelta(days=90)

# Fetch campaigns
response = client.campaigns.list(
    count=1000,
    since_send_time=ninety_days_ago.isoformat()
)

campaigns = response['campaigns']
print(f"Found {len(campaigns)} campaigns")
```

### Get Campaign Report

```python
campaign_id = "abc123xyz"

report = client.reports.get_campaign_report(campaign_id)

print(f"Campaign: {report['campaign_title']}")
print(f"Sent to: {report['emails_sent']} subscribers")
print(f"Open rate: {report['opens']['open_rate']*100:.1f}%")
print(f"Click rate: {report['clicks']['click_rate']*100:.1f}%")
print(f"Revenue: ${report.get('ecommerce', {}).get('total_revenue', 0):.2f}")
```

### Get Landing Page Reports

```python
# List all landing pages
pages = client.landing_pages.get_all_landing_pages(count=100)

for page in pages['landing_pages']:
    print(f"Page: {page['name']}")
    print(f"  URL: {page['url']}")
    print(f"  Visits: {page.get('visits', 0)}")
    print(f"  Conversion Rate: {page.get('conversion_rate', 0)*100:.1f}%")
```

---

## Next Steps

1. **Generate API Key** (5 minutes)
   - Log into Mailchimp
   - Navigate to Account → API Keys
   - Create new key
   - Save key and data center prefix

2. **Test API Connection** (5 minutes)
   - Install `mailchimp-marketing` library
   - Run test script to verify access
   - Confirm you can fetch campaigns and reports

3. **Share Credentials** so I can:
   - Create the data fetcher script
   - Add Mailchimp analytics tools
   - Integrate with existing pipeline

4. **Decide What to Track**
   - Which metrics are most important?
   - How far back to fetch historical data?
   - Should we track individual subscriber activity?

---

## Resources

- [Mailchimp Marketing API Documentation](https://mailchimp.com/developer/marketing/)
- [API Quick Start Guide](https://mailchimp.com/developer/marketing/guides/quick-start/)
- [Reports API Reference](https://mailchimp.com/developer/marketing/api/reports/)
- [Landing Pages API Reference](https://mailchimp.com/developer/marketing/api/landing-pages/)
- [Python SDK on PyPI](https://pypi.org/project/mailchimp-marketing/)

---

## Questions to Consider

Before we start building, let's discuss:

1. **Historical Data**: How far back should we pull campaign data? (Suggest: 1 year)
2. **Update Frequency**: Daily updates in the pipeline, or less often?
3. **Priority Metrics**: Which metrics matter most for Basin's decision-making?
4. **Landing Pages**: Do you actively use Mailchimp landing pages, or mainly email campaigns?
5. **Audience Lists**: Do you have multiple lists, or one main subscriber list?
6. **E-commerce**: Are you tracking purchases from email campaigns?

Once you generate the API key and answer these questions, I can build the complete integration!

# Mailchimp Integration Plan - Basin Climbing Analytics

## Business Requirements

### Core Goals
1. **Journey Mapping**: Understand which email sequences/automations are working
2. **Segmentation Analysis**: See who gets what emails and how they respond
3. **Content Analysis**: LLM-powered summaries of email content, tone, CTAs
4. **Landing Page Performance**: Track visitor volume and engagement
5. **Link Tracking**: Capture CTAs and destination URLs from emails

### Data Scope
- **Historical Data**: 14 months backfill
- **Update Frequency**: Daily
- **Key Metrics**: Sends, open rates, clicks (not e-commerce)
- **Landing Pages**: High priority - volume and engagement tracking

---

## Data Architecture

### Data Sources

#### 1. Campaign Data (Regular Campaigns)
**API Endpoint**: `GET /campaigns`

**What We'll Capture**:
```
- campaign_id
- campaign_title
- subject_line
- preview_text
- send_time
- emails_sent
- list_id (which audience segment)
- segment_id (if targeted segment)
- campaign_type (regular, plaintext, etc.)
```

#### 2. Campaign Reports (Performance Metrics)
**API Endpoint**: `GET /reports/{campaign_id}`

**What We'll Capture**:
```
- opens_total
- unique_opens
- open_rate
- clicks_total
- unique_clicks
- click_rate
- bounces (hard/soft)
- unsubscribed
- forwards
- abuse_reports
```

#### 3. Campaign Content (For LLM Analysis)
**API Endpoint**: `GET /campaigns/{campaign_id}/content`

**What We'll Capture**:
```
- html (full email HTML)
- plain_text (plaintext version)
```

**LLM Analysis Fields** (using Claude):
- `content_summary`: 2-3 sentence summary of email
- `tone`: Formal, casual, urgent, friendly, promotional, educational
- `primary_cta`: Main call-to-action text
- `secondary_ctas`: Other CTAs if present
- `has_link`: Boolean if contains links
- `link_destinations`: List of destination URLs
- `content_type`: Newsletter, promotion, announcement, event, class schedule

#### 4. Link Click Details
**API Endpoint**: `GET /reports/{campaign_id}/click-details`

**What We'll Capture**:
```
- url (destination URL)
- total_clicks
- unique_clicks
- click_percentage
- last_click
```

#### 5. Automation Workflows (Journeys)
**API Endpoint**: `GET /automations`

**What We'll Capture**:
```
- automation_id
- automation_title
- status (active, paused)
- trigger_type (signup, date-based, etc.)
- create_time
- start_time
- emails_sent (total across all emails in workflow)
- list_id (audience)
```

#### 6. Automation Email Reports
**API Endpoint**: `GET /reports/{automation_id}/emails/{workflow_email_id}`

**What We'll Capture**:
```
- workflow_email_id
- email_title
- position_in_workflow (1, 2, 3, etc.)
- emails_sent
- unique_opens
- open_rate
- unique_clicks
- click_rate
```

#### 7. Audience Lists & Segments
**API Endpoint**: `GET /lists` and `GET /lists/{list_id}/segments`

**What We'll Capture**:
```
- list_id
- list_name
- member_count
- unsubscribe_count
- cleaned_count
- segment_id
- segment_name
- segment_type (saved, static, fuzzy)
- member_count_in_segment
```

#### 8. Landing Pages
**API Endpoint**: `GET /landing-pages` and `GET /reports/landing-pages/{page_id}`

**What We'll Capture**:
```
- page_id
- page_name
- page_title
- page_url
- status (published, unpublished)
- created_at
- published_at
- visits
- unique_visits
- subscribes
- conversion_rate
```

---

## Data Schemas

### campaigns.csv
```csv
campaign_id,campaign_title,subject_line,preview_text,send_time,campaign_type,emails_sent,list_id,list_name,segment_id,segment_name,open_rate,click_rate,unique_opens,unique_clicks,bounces_hard,bounces_soft,unsubscribed,forwards,abuse_reports,content_summary,tone,primary_cta,secondary_ctas,has_link,link_count,content_type
```

### campaign_links.csv
```csv
campaign_id,campaign_title,send_time,url,total_clicks,unique_clicks,click_percentage,last_click
```

### automations.csv
```csv
automation_id,automation_title,status,trigger_type,create_time,start_time,total_emails_sent,list_id,list_name,email_count_in_workflow
```

### automation_emails.csv
```csv
automation_id,automation_title,workflow_email_id,email_title,position_in_workflow,emails_sent,unique_opens,open_rate,unique_clicks,click_rate,content_summary,tone,primary_cta,content_type
```

### audience_segments.csv
```csv
list_id,list_name,total_members,segment_id,segment_name,segment_type,segment_member_count,segment_percentage
```

### landing_pages.csv
```csv
page_id,page_name,page_title,page_url,status,created_at,published_at,visits,unique_visits,subscribes,conversion_rate
```

---

## LLM Content Analysis Implementation

### Content Analysis Function

```python
def analyze_email_content_with_llm(html_content: str, subject_line: str) -> Dict:
    """
    Use Claude to analyze email content and extract insights.

    Args:
        html_content: HTML of the email
        subject_line: Email subject line

    Returns:
        Dict with: content_summary, tone, primary_cta, secondary_ctas,
                   has_link, link_destinations, content_type
    """

    # Strip HTML to get clean text
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract all links
    links = [a.get('href') for a in soup.find_all('a', href=True)]

    # Get clean text
    text_content = soup.get_text(separator=' ', strip=True)

    # Prepare prompt for Claude
    prompt = f"""Analyze this email campaign from a climbing gym.

**Subject Line:** {subject_line}

**Email Content:**
{text_content[:3000]}  # Limit to first 3000 chars

Please provide:
1. **Summary** (2-3 sentences): What is this email about?
2. **Tone** (1-2 words): Choose from: Formal, Casual, Urgent, Friendly, Promotional, Educational, Inspiring, Informational
3. **Primary CTA** (exact text): What's the main call-to-action?
4. **Secondary CTAs** (comma-separated): Any other calls-to-action?
5. **Content Type** (1-2 words): Choose from: Newsletter, Promotion, Announcement, Event, Class Schedule, Member Update, New Feature, Community

Format your response as:
SUMMARY: [your summary]
TONE: [tone]
PRIMARY_CTA: [main CTA text or "None"]
SECONDARY_CTAS: [other CTAs or "None"]
CONTENT_TYPE: [type]
"""

    # Call Claude API
    message = anthropic_client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=300,
        messages=[{
            "role": "user",
            "content": prompt
        }]
    )

    response_text = message.content[0].text

    # Parse response
    result = {
        'content_summary': None,
        'tone': None,
        'primary_cta': None,
        'secondary_ctas': None,
        'has_link': len(links) > 0,
        'link_count': len(links),
        'link_destinations': ', '.join(links[:5]),  # First 5 links
        'content_type': None
    }

    # Extract fields from response
    for line in response_text.split('\n'):
        if line.startswith('SUMMARY:'):
            result['content_summary'] = line.replace('SUMMARY:', '').strip()
        elif line.startswith('TONE:'):
            result['tone'] = line.replace('TONE:', '').strip()
        elif line.startswith('PRIMARY_CTA:'):
            result['primary_cta'] = line.replace('PRIMARY_CTA:', '').strip()
        elif line.startswith('SECONDARY_CTAS:'):
            result['secondary_ctas'] = line.replace('SECONDARY_CTAS:', '').strip()
        elif line.startswith('CONTENT_TYPE:'):
            result['content_type'] = line.replace('CONTENT_TYPE:', '').strip()

    return result
```

### Smart Caching Strategy

**Problem**: 14 months of historical data could mean 100+ campaigns to analyze.

**Solution**:
1. Only run LLM analysis ONCE per campaign (check if already analyzed)
2. Store analysis results in S3
3. On subsequent runs, skip campaigns that already have analysis
4. Similar pattern to Instagram AI vision

---

## Journey Analysis Capabilities

### Customer Journey Tracking

With the data above, we can answer:

1. **Automation Performance**:
   - "Which automation workflows have the best open rates?"
   - "At what step in the onboarding journey do people drop off?"
   - "Compare welcome series performance vs. re-engagement campaigns"

2. **Segment Performance**:
   - "How do new members respond to emails vs. long-time members?"
   - "Which audience segment has the highest click-through rate?"
   - "Show me all emails sent to [specific segment] and their performance"

3. **Content Effectiveness**:
   - "Which CTAs get clicked most?"
   - "Do promotional emails or educational emails perform better?"
   - "What tone of email gets the most engagement?"
   - "Which subject line styles get opened most?"

4. **Link Analysis**:
   - "Which links in our emails get clicked most?"
   - "Do links to class schedules perform better than links to memberships?"
   - "Track which external vs. internal links drive more engagement"

5. **Landing Page Funnel**:
   - "How many people visit landing pages from emails?"
   - "Which landing pages convert best?"
   - "Email → Landing Page → Signup conversion rates"

---

## Implementation Plan

### Phase 1: Data Fetcher (2-3 hours)

Create `data_pipeline/fetch_mailchimp_data.py`:

```python
class MailchimpDataFetcher:

    def __init__(self, api_key: str, server_prefix: str, anthropic_api_key: str = None):
        """Initialize with Mailchimp and Anthropic credentials"""

    def get_campaigns(self, since: datetime) -> List[Dict]:
        """Fetch all campaigns since date"""

    def get_campaign_report(self, campaign_id: str) -> Dict:
        """Get performance report for campaign"""

    def get_campaign_content(self, campaign_id: str) -> Dict:
        """Get HTML/text content of campaign"""

    def get_campaign_click_details(self, campaign_id: str) -> List[Dict]:
        """Get all clicked links in campaign"""

    def get_automations(self) -> List[Dict]:
        """Fetch all automation workflows"""

    def get_automation_emails(self, automation_id: str) -> List[Dict]:
        """Get all emails in an automation workflow"""

    def get_lists_and_segments(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Fetch audience lists and their segments"""

    def get_landing_pages(self) -> pd.DataFrame:
        """Fetch all landing pages with performance data"""

    def analyze_email_content_with_llm(self, html: str, subject: str) -> Dict:
        """Use Claude to analyze email content"""

    def fetch_all_mailchimp_data(
        self,
        since: datetime,
        enable_content_analysis: bool = True,
        existing_campaigns_df: Optional[pd.DataFrame] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Main function: fetch all Mailchimp data and return as DataFrames.

        Returns:
            {
                'campaigns': campaigns_df,
                'campaign_links': links_df,
                'automations': automations_df,
                'automation_emails': automation_emails_df,
                'audience_segments': segments_df,
                'landing_pages': pages_df
            }
        """
```

### Phase 2: Pipeline Integration (30 mins)

Add to `data_pipeline/pipeline_handler.py`:

```python
def upload_new_mailchimp_data(save_local=False, enable_content_analysis=True):
    """
    Fetch Mailchimp data and upload to S3.

    - Fetches last 14 months on first run
    - Then daily incremental updates
    - LLM content analysis runs once per campaign (cached)
    - Uploads 6 CSV files to S3
    """

    # Calculate date range (14 months back)
    since_date = datetime.now() - timedelta(days=425)

    # Download existing data from S3 (for smart caching)
    existing_campaigns = download_from_s3('mailchimp/campaigns.csv')

    # Fetch new data
    fetcher = MailchimpDataFetcher(
        api_key=config.mailchimp_api_key,
        server_prefix=config.mailchimp_server_prefix,
        anthropic_api_key=config.anthropic_api_key
    )

    data = fetcher.fetch_all_mailchimp_data(
        since=since_date,
        enable_content_analysis=enable_content_analysis,
        existing_campaigns_df=existing_campaigns
    )

    # Merge with existing and upload to S3
    for dataset_name, df in data.items():
        # Merge, dedupe, upload logic
        pass
```

Add to daily pipeline in `replace_days_in_transaction_df_in_s3()`:

```python
# Update Mailchimp data (daily incremental)
print("\n=== Updating Mailchimp Data ===")
try:
    upload_new_mailchimp_data(
        save_local=False,
        enable_content_analysis=True  # Smart caching: only new campaigns
    )
    print("✅ Mailchimp data updated successfully")
except Exception as e:
    print(f"❌ Error updating Mailchimp data: {e}")
```

### Phase 3: Analytics Tools (1-2 hours)

Add to `agent/analytics_tools.py`:

**Journey Analysis Tools**:
- `get_automation_performance`: Compare automation workflows
- `get_automation_email_sequence`: See step-by-step performance
- `analyze_customer_journey_dropoff`: Find where people stop engaging

**Segment Analysis Tools**:
- `get_segment_email_performance`: Compare segments' responses
- `get_campaigns_by_segment`: See what each segment receives

**Content Analysis Tools**:
- `get_campaigns_by_cta`: Group by call-to-action type
- `get_campaigns_by_tone`: Group by email tone
- `get_campaigns_by_content_type`: Newsletter vs. promo vs. event
- `get_most_clicked_links`: Top performing links across campaigns

**Landing Page Tools**:
- `get_landing_page_performance`: Conversion rates, traffic
- `analyze_email_to_landing_page_funnel`: Track full journey

### Phase 4: Initial Backfill (15-30 mins runtime)

One-time script to fetch 14 months of historical data:

```python
# Run once to populate historical data
upload_new_mailchimp_data(
    save_local=True,
    enable_content_analysis=True
)
```

**Estimated Runtime**:
- ~100 campaigns over 14 months
- ~5-10 automations
- ~10-20 landing pages
- LLM analysis: ~2-3 seconds per email
- Total: ~15-30 minutes

---

## Cost Estimates

### Mailchimp API
- **Rate Limit**: 10 simultaneous connections, no hard hourly limit
- **Cost**: Included in your Mailchimp subscription

### Claude API (LLM Content Analysis)
- **Model**: Claude 3 Haiku (`claude-3-haiku-20240307`)
- **Cost**: $0.25 per million input tokens, $1.25 per million output tokens
- **Per Email Analysis**: ~1,000 input tokens + ~200 output tokens = ~$0.0003/email
- **Initial Backfill** (100 emails): ~$0.03
- **Daily Updates** (1-2 new emails): ~$0.0006/day = ~$0.20/year

**Total Annual LLM Cost**: ~$0.23 (negligible)

---

## Example Analytics Questions We Can Answer

1. **Journey Optimization**:
   - "Show me the 5-email welcome series. At which email do most people drop off?"
   - "Compare our 'new member onboarding' automation to 'lapsed member re-engagement'"
   - "Which automation workflow has the best conversion rate?"

2. **Content Strategy**:
   - "Do emails with 'urgent' tone perform better than 'friendly' tone?"
   - "Which types of CTAs get clicked most: 'Sign Up', 'Learn More', or 'Book Now'?"
   - "Are educational emails or promotional emails more effective?"

3. **Segmentation Insights**:
   - "How do new members respond to emails compared to members who've been here 6+ months?"
   - "Which segment has the highest click-through rate?"
   - "Show me all emails sent to 'Active Climbers' segment and their open rates"

4. **Link Performance**:
   - "What are the top 10 most-clicked links across all our emails?"
   - "Do links to class schedules get more clicks than links to membership pages?"
   - "Which external links (blog, social media) get engagement?"

5. **Landing Page Funnel**:
   - "Which landing pages have the highest conversion rates?"
   - "How many email opens lead to landing page visits?"
   - "Email → Landing Page → Signup conversion funnel"

6. **Campaign Effectiveness**:
   - "Which campaigns in the last 3 months had the best open rates?"
   - "Show me all 'promotion' type emails and their click rates"
   - "What subject line patterns perform best?"

---

## Next Steps

1. **You Generate Mailchimp API Key** (~5 mins):
   - Log into Mailchimp
   - Account → Extras → API Keys → Create Key
   - Note your data center prefix (e.g., "us19")

2. **Share Credentials**:
   - API key
   - Data center prefix
   - Confirm: Do you want 14-month backfill or shorter?

3. **I Build the Integration** (~3 hours):
   - Create `fetch_mailchimp_data.py` with all data fetchers
   - Add LLM content analysis function
   - Integrate into pipeline
   - Run initial backfill
   - Add analytics tools

4. **Test & Validate** (~30 mins):
   - Verify data in S3
   - Test sample queries
   - Ensure LLM summaries are accurate

---

## Questions Before I Start Building

1. **API Key**: Ready to generate it now?
2. **Backfill Scope**: 14 months confirmed, or adjust?
3. **Automation Workflows**: Do you know how many you have? (Want to list them?)
4. **Priority Questions**: Which of the analytics questions above are most important to you?
5. **Audience Segments**: Do you have specific segments you want to track (e.g., "New Members", "Lapsed Members")?

Once you provide the API key and answer these, I can start building immediately!

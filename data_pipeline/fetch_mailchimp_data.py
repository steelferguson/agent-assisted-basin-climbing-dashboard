"""
Mailchimp Data Fetcher

Fetches Mailchimp email campaign, automation, landing page, and audience data.
Includes AI-powered content analysis using Claude to extract insights from emails.
"""

import pandas as pd
import mailchimp_marketing as MailchimpMarketing
from mailchimp_marketing.api_client import ApiClientError
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from anthropic import Anthropic
import re
from html import unescape
from bs4 import BeautifulSoup


class MailchimpDataFetcher:
    """
    A class for fetching and processing Mailchimp marketing data.

    Features:
    - Fetches campaigns with detailed performance metrics
    - Fetches automations (automated email sequences)
    - Fetches landing pages with conversion data
    - Fetches audience growth history
    - AI content analysis to extract themes, tone, and CTAs from emails
    - Smart incremental updates (only analyze new campaigns to save costs)
    """

    def __init__(self, api_key: str, server_prefix: str, anthropic_api_key: Optional[str] = None):
        """
        Initialize the Mailchimp data fetcher.

        Args:
            api_key: Mailchimp API key (format: key-us9)
            server_prefix: Mailchimp server prefix (e.g., 'us9')
            anthropic_api_key: Anthropic API key for content analysis (optional)
        """
        self.client = MailchimpMarketing.Client()
        self.client.set_config({
            "api_key": api_key,
            "server": server_prefix
        })
        self.anthropic_api_key = anthropic_api_key

        if self.anthropic_api_key:
            self.anthropic_client = Anthropic(api_key=self.anthropic_api_key)
        else:
            self.anthropic_client = None
            print("Warning: No Anthropic API key provided. Content analysis will be skipped.")

    def _clean_html_to_text(self, html_content: str) -> str:
        """
        Convert HTML content to clean text for AI analysis.

        Args:
            html_content: HTML string

        Returns:
            Clean text with HTML tags removed
        """
        if not html_content:
            return ""

        try:
            # Parse HTML
            soup = BeautifulSoup(html_content, 'html.parser')

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Get text
            text = soup.get_text()

            # Break into lines and remove leading/trailing space
            lines = (line.strip() for line in text.splitlines())

            # Break multi-headlines into a line each
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))

            # Drop blank lines
            text = ' '.join(chunk for chunk in chunks if chunk)

            # Unescape HTML entities
            text = unescape(text)

            return text[:3000]  # Limit to 3000 chars for AI analysis

        except Exception as e:
            print(f"Error cleaning HTML: {e}")
            return ""

    def analyze_email_content_with_ai(self, subject_line: str, html_content: str,
                                       campaign_title: str = "") -> Dict:
        """
        Use Claude AI to analyze email content and extract insights.

        Args:
            subject_line: Email subject line
            html_content: Email HTML content
            campaign_title: Internal campaign title (optional)

        Returns:
            Dictionary with AI-generated insights
        """
        if not self.anthropic_client:
            return {
                'ai_summary': None,
                'ai_tone': None,
                'ai_content_type': None,
                'ai_ctas': None,
                'ai_themes': None
            }

        try:
            # Clean HTML to text
            email_text = self._clean_html_to_text(html_content)

            if not email_text or len(email_text) < 50:
                print(f"Skipping AI analysis - insufficient email content")
                return {
                    'ai_summary': None,
                    'ai_tone': None,
                    'ai_content_type': None,
                    'ai_ctas': None,
                    'ai_themes': None
                }

            prompt = f"""Analyze this email campaign from Basin Climbing & Fitness gym.

Campaign Title: "{campaign_title}"
Subject Line: "{subject_line}"

Email Content:
{email_text}

Please provide:
1. **Summary** (1-2 sentences): What is this email about?
2. **Tone** (1-2 words): What is the tone? (e.g., promotional, educational, friendly, urgent, informative, etc.)
3. **Content Type** (1-2 words): What type of content? (e.g., newsletter, announcement, promotion, event, class schedule, etc.)
4. **CTAs** (comma-separated): What are the main calls-to-action? (e.g., "Sign up", "Book now", "Learn more", etc.)
5. **Themes** (comma-separated): Key themes or topics (e.g., climbing, fitness, community, membership, events, classes, etc.)

Format your response as:
SUMMARY: [your summary]
TONE: [tone]
CONTENT_TYPE: [content type]
CTAS: [comma-separated CTAs]
THEMES: [comma-separated themes]"""

            message = self.anthropic_client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=400,
                messages=[
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
            )

            # Parse response
            response_text = message.content[0].text

            summary = None
            tone = None
            content_type = None
            ctas = None
            themes = None

            for line in response_text.split('\n'):
                if line.startswith('SUMMARY:'):
                    summary = line.replace('SUMMARY:', '').strip()
                elif line.startswith('TONE:'):
                    tone = line.replace('TONE:', '').strip()
                elif line.startswith('CONTENT_TYPE:'):
                    content_type = line.replace('CONTENT_TYPE:', '').strip()
                elif line.startswith('CTAS:'):
                    ctas = line.replace('CTAS:', '').strip()
                elif line.startswith('THEMES:'):
                    themes = line.replace('THEMES:', '').strip()

            return {
                'ai_summary': summary,
                'ai_tone': tone,
                'ai_content_type': content_type,
                'ai_ctas': ctas,
                'ai_themes': themes
            }

        except Exception as e:
            print(f"Error analyzing email content: {e}")
            return {
                'ai_summary': None,
                'ai_tone': None,
                'ai_content_type': None,
                'ai_ctas': None,
                'ai_themes': None
            }

    def get_campaigns(self, since: Optional[datetime] = None, status: str = "sent",
                      count: int = 1000) -> List[Dict]:
        """
        Fetch email campaigns from Mailchimp.

        Args:
            since: Only fetch campaigns sent after this date
            status: Campaign status filter (sent, scheduled, paused, etc.)
            count: Maximum number of campaigns to fetch

        Returns:
            List of campaign dictionaries
        """
        try:
            params = {
                'count': min(count, 1000),  # API max per page
                'status': status
            }

            if since:
                params['since_send_time'] = since.isoformat()

            response = self.client.campaigns.list(**params)
            campaigns = response.get('campaigns', [])

            print(f"Fetched {len(campaigns)} campaigns")
            return campaigns

        except ApiClientError as error:
            print(f"Error fetching campaigns: {error.text}")
            return []

    def get_campaign_content(self, campaign_id: str) -> Dict:
        """
        Fetch the HTML content and subject line for a campaign.

        Args:
            campaign_id: Campaign ID

        Returns:
            Dictionary with subject_line and html content
        """
        try:
            content = self.client.campaigns.get_content(campaign_id)
            return {
                'html': content.get('html', ''),
                'plain_text': content.get('plain_text', '')
            }
        except ApiClientError as error:
            print(f"Error fetching content for campaign {campaign_id}: {error.text}")
            return {'html': '', 'plain_text': ''}

    def get_campaign_report(self, campaign_id: str) -> Optional[Dict]:
        """
        Fetch detailed report for a specific campaign.

        Args:
            campaign_id: Campaign ID

        Returns:
            Dictionary with campaign metrics
        """
        try:
            report = self.client.reports.get_campaign_report(campaign_id)
            return report
        except ApiClientError as error:
            print(f"Error fetching report for campaign {campaign_id}: {error.text}")
            return None

    def get_campaign_click_details(self, campaign_id: str) -> List[Dict]:
        """
        Fetch link-level click details for a campaign.

        Args:
            campaign_id: Campaign ID

        Returns:
            List of dictionaries with link URLs and click counts
        """
        try:
            response = self.client.reports.get_campaign_click_details(campaign_id, count=100)
            return response.get('urls_clicked', [])
        except ApiClientError as error:
            print(f"Error fetching click details for campaign {campaign_id}: {error.text}")
            return []

    def get_campaign_recipients(self, campaign_id: str, count: int = 1000) -> List[Dict]:
        """
        Fetch recipients who were sent a specific campaign.

        Args:
            campaign_id: Campaign ID
            count: Maximum number of recipients to fetch per request

        Returns:
            List of recipient dictionaries with email addresses
        """
        try:
            all_recipients = []
            offset = 0
            batch_size = min(count, 1000)  # API max per page

            while True:
                response = self.client.reports.get_email_activity_for_campaign(
                    campaign_id,
                    count=batch_size,
                    offset=offset
                )

                recipients = response.get('emails', [])
                if not recipients:
                    break

                all_recipients.extend(recipients)
                offset += len(recipients)

                # Check if we've fetched all recipients
                total_items = response.get('total_items', 0)
                if offset >= total_items:
                    break

            # Removed verbose output - summary will be printed at the end
            return all_recipients

        except ApiClientError as error:
            print(f"Error fetching recipients for campaign {campaign_id}: {error.text}")
            return []

    def fetch_all_campaign_data(self, since: Optional[datetime] = None,
                                 enable_content_analysis: bool = True,
                                 existing_campaigns_df: Optional[pd.DataFrame] = None) -> tuple:
        """
        Fetch all campaign data with reports and AI analysis.

        Args:
            since: Only fetch campaigns sent after this date
            enable_content_analysis: Whether to run AI analysis on email content
            existing_campaigns_df: Existing campaigns DataFrame for smart caching

        Returns:
            Tuple of (campaigns_df, campaign_links_df)
        """
        print("\n=== Fetching Mailchimp Campaign Data ===")

        # Fetch campaigns
        campaigns = self.get_campaigns(since=since)

        if not campaigns:
            print("No campaigns found")
            return pd.DataFrame(), pd.DataFrame()

        # Process each campaign
        campaigns_data = []
        links_data = []

        for i, campaign in enumerate(campaigns):
            campaign_id = campaign['id']
            campaign_title = campaign.get('settings', {}).get('title', 'Untitled')

            print(f"\n[{i+1}/{len(campaigns)}] Processing: {campaign_title}")

            # Get campaign report (metrics)
            report = self.get_campaign_report(campaign_id)

            if not report:
                print(f"  ⚠️  No report data available")
                continue

            # Check if we already have AI analysis for this campaign
            needs_ai_analysis = True
            if existing_campaigns_df is not None and not existing_campaigns_df.empty:
                existing_row = existing_campaigns_df[existing_campaigns_df['campaign_id'] == campaign_id]
                if not existing_row.empty and pd.notna(existing_row.iloc[0].get('ai_summary')):
                    needs_ai_analysis = False
                    print(f"  ✓ Using cached AI analysis")

            # Get content and run AI analysis if needed
            ai_analysis = {
                'ai_summary': None,
                'ai_tone': None,
                'ai_content_type': None,
                'ai_ctas': None,
                'ai_themes': None
            }

            if enable_content_analysis and needs_ai_analysis:
                content = self.get_campaign_content(campaign_id)
                subject_line = campaign.get('settings', {}).get('subject_line', '')
                ai_analysis = self.analyze_email_content_with_ai(
                    subject_line,
                    content.get('html', ''),
                    campaign_title
                )
                print(f"  ✓ AI analysis complete")
            elif not needs_ai_analysis and existing_campaigns_df is not None:
                # Use existing AI analysis
                existing_row = existing_campaigns_df[existing_campaigns_df['campaign_id'] == campaign_id]
                if not existing_row.empty:
                    for key in ai_analysis.keys():
                        if key in existing_row.columns:
                            ai_analysis[key] = existing_row.iloc[0][key]

            # Extract campaign data
            settings = campaign.get('settings', {})
            bounces = report.get('bounces', {})
            opens = report.get('opens', {})
            clicks = report.get('clicks', {})
            ecommerce = report.get('ecommerce', {})

            campaign_data = {
                'campaign_id': campaign_id,
                'campaign_title': campaign_title,
                'subject_line': settings.get('subject_line', ''),
                'type': campaign.get('type', ''),
                'status': campaign.get('status', ''),
                'send_time': report.get('send_time', ''),
                'emails_sent': report.get('emails_sent', 0),
                'delivered': report.get('emails_sent', 0) - bounces.get('hard_bounces', 0) - bounces.get('soft_bounces', 0),
                'opens_total': opens.get('opens_total', 0),
                'unique_opens': opens.get('unique_opens', 0),
                'open_rate': opens.get('open_rate', 0),
                'clicks_total': clicks.get('clicks_total', 0),
                'unique_clicks': clicks.get('unique_clicks', 0),
                'subscriber_clicks': clicks.get('unique_subscriber_clicks', 0),
                'click_rate': clicks.get('click_rate', 0),
                'unsubscribed': report.get('unsubscribed', 0),
                'bounces_hard': bounces.get('hard_bounces', 0),
                'bounces_soft': bounces.get('soft_bounces', 0),
                'abuse_reports': report.get('abuse_reports', 0),
                'revenue': ecommerce.get('total_revenue', 0),
                'orders': ecommerce.get('total_orders', 0),
                'list_id': campaign.get('recipients', {}).get('list_id', ''),
                'segment_id': campaign.get('recipients', {}).get('segment_opts', {}).get('saved_segment_id'),
                **ai_analysis  # Add AI analysis fields
            }

            campaigns_data.append(campaign_data)

            # Get link-level click data
            click_details = self.get_campaign_click_details(campaign_id)
            for link in click_details:
                links_data.append({
                    'campaign_id': campaign_id,
                    'campaign_title': campaign_title,
                    'url': link.get('url', ''),
                    'total_clicks': link.get('total_clicks', 0),
                    'unique_clicks': link.get('unique_clicks', 0),
                    'unique_subscriber_clicks': link.get('unique_subscriber_clicks', 0),
                    'click_percentage': link.get('click_percentage', 0)
                })

        # Create DataFrames
        campaigns_df = pd.DataFrame(campaigns_data)
        links_df = pd.DataFrame(links_data)

        print(f"\n✅ Processed {len(campaigns_df)} campaigns")
        print(f"✅ Extracted {len(links_df)} campaign links")

        return campaigns_df, links_df

    def get_automations(self) -> List[Dict]:
        """
        Fetch all automations (automated email sequences).

        Returns:
            List of automation dictionaries
        """
        try:
            response = self.client.automations.list(count=100)
            automations = response.get('automations', [])
            print(f"Fetched {len(automations)} automations")
            return automations
        except ApiClientError as error:
            print(f"Error fetching automations: {error.text}")
            return []

    def get_automation_emails(self, automation_id: str) -> List[Dict]:
        """
        Fetch all emails within an automation sequence.

        Args:
            automation_id: Workflow ID

        Returns:
            List of email dictionaries
        """
        try:
            response = self.client.automations.list_all_workflow_emails(automation_id)
            emails = response.get('emails', [])
            return emails
        except ApiClientError as error:
            print(f"Error fetching automation emails for {automation_id}: {error.text}")
            return []

    def fetch_all_automation_data(self) -> tuple:
        """
        Fetch all automation data including individual email performance.

        Returns:
            Tuple of (automations_df, automation_emails_df)
        """
        print("\n=== Fetching Mailchimp Automation Data ===")

        automations = self.get_automations()

        if not automations:
            print("No automations found")
            return pd.DataFrame(), pd.DataFrame()

        automations_data = []
        emails_data = []

        for automation in automations:
            automation_id = automation['id']
            automation_title = automation.get('settings', {}).get('title', 'Untitled')

            print(f"Processing automation: {automation_title}")

            # Get automation-level data
            recipients = automation.get('recipients', {})
            report_summary = automation.get('report_summary', {})

            automation_data = {
                'automation_id': automation_id,
                'title': automation_title,
                'status': automation.get('status', ''),
                'create_time': automation.get('create_time', ''),
                'start_time': automation.get('start_time', ''),
                'list_id': recipients.get('list_id', ''),
                'emails_sent': report_summary.get('emails_sent', 0),
                'opens_total': report_summary.get('opens', 0),
                'unique_opens': report_summary.get('unique_opens', 0),
                'open_rate': report_summary.get('open_rate', 0),
                'clicks_total': report_summary.get('clicks', 0),
                'subscriber_clicks': report_summary.get('subscriber_clicks', 0),
                'click_rate': report_summary.get('click_rate', 0),
                'revenue': report_summary.get('revenue', 0)
            }

            automations_data.append(automation_data)

            # Get individual emails in automation
            emails = self.get_automation_emails(automation_id)

            for email in emails:
                email_id = email['id']
                email_data = {
                    'automation_id': automation_id,
                    'automation_title': automation_title,
                    'email_id': email_id,
                    'position': email.get('position', 0),
                    'subject_line': email.get('settings', {}).get('subject_line', ''),
                    'status': email.get('status', ''),
                    'create_time': email.get('create_time', ''),
                    'start_time': email.get('start_time', ''),
                    'emails_sent': email.get('emails_sent', 0),
                    'delay_action': email.get('delay', {}).get('action', ''),
                    'delay_amount': email.get('delay', {}).get('amount', 0),
                    'delay_type': email.get('delay', {}).get('type', '')
                }
                emails_data.append(email_data)

        automations_df = pd.DataFrame(automations_data)
        emails_df = pd.DataFrame(emails_data)

        print(f"✅ Processed {len(automations_df)} automations")
        print(f"✅ Extracted {len(emails_df)} automation emails")

        return automations_df, emails_df

    def get_landing_pages(self) -> List[Dict]:
        """
        Fetch all landing pages.

        Returns:
            List of landing page dictionaries
        """
        try:
            response = self.client.landingPages.get_all(count=100)
            pages = response.get('landing_pages', [])
            print(f"Fetched {len(pages)} landing pages")
            return pages
        except ApiClientError as error:
            print(f"Error fetching landing pages: {error.text}")
            return []

    def fetch_all_landing_page_data(self) -> pd.DataFrame:
        """
        Fetch all landing page data with performance metrics.

        Returns:
            DataFrame with landing page data
        """
        print("\n=== Fetching Mailchimp Landing Page Data ===")

        pages = self.get_landing_pages()

        if not pages:
            print("No landing pages found")
            return pd.DataFrame()

        pages_data = []

        for page in pages:
            page_id = page['id']
            page_name = page.get('name', 'Untitled')

            print(f"Processing landing page: {page_name}")

            page_data = {
                'page_id': page_id,
                'name': page_name,
                'title': page.get('title', ''),
                'url': page.get('url', ''),
                'status': page.get('status', ''),
                'published_at': page.get('published_at', ''),
                'unpublished_at': page.get('unpublished_at', ''),
                'created_at': page.get('created_at', ''),
                'updated_at': page.get('updated_at', ''),
                'list_id': page.get('list_id', ''),
                'visits': page.get('visits', 0),
                'unique_visits': page.get('unique_visits', 0),
                'subscribes': page.get('subscribes', 0),
                'clicks': page.get('clicks', 0),
                'conversion_rate': page.get('conversion_rate', 0)
            }

            pages_data.append(page_data)

        pages_df = pd.DataFrame(pages_data)

        print(f"✅ Processed {len(pages_df)} landing pages")

        return pages_df

    def get_audience_growth_history(self, list_id: str, count: int = 180) -> List[Dict]:
        """
        Fetch audience growth history for a list (up to 180 days).

        Args:
            list_id: Mailchimp list ID
            count: Number of days to fetch (max 180)

        Returns:
            List of growth history entries
        """
        try:
            response = self.client.lists.get_list_growth_history(list_id, count=min(count, 180))
            history = response.get('history', [])
            print(f"Fetched {len(history)} days of growth history")
            return history
        except ApiClientError as error:
            print(f"Error fetching growth history: {error.text}")
            return []

    def fetch_audience_growth_data(self, list_id: str) -> pd.DataFrame:
        """
        Fetch audience growth data.

        Args:
            list_id: Mailchimp list/audience ID

        Returns:
            DataFrame with daily growth metrics
        """
        print("\n=== Fetching Mailchimp Audience Growth Data ===")

        history = self.get_audience_growth_history(list_id)

        if not history:
            print("No growth history found")
            return pd.DataFrame()

        growth_data = []

        for entry in history:
            growth_data.append({
                'list_id': list_id,
                'month': entry.get('month', ''),
                'existing': entry.get('existing', 0),
                'imports': entry.get('imports', 0),
                'optins': entry.get('optins', 0),
                'unsubscribes': entry.get('unsubscribes', 0),
                'reconfirms': entry.get('reconfirm', 0),
                'cleaned': entry.get('cleaned', 0),
                'pending': entry.get('pending', 0),
                'deleted': entry.get('deleted', 0),
                'transactional': entry.get('transactional', 0)
            })

        growth_df = pd.DataFrame(growth_data)

        print(f"✅ Processed {len(growth_df)} growth history records")

        return growth_df

    def fetch_recipient_activity(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch recipient-level email activity for all recent campaigns.

        Returns data suitable for adding to customer event timeline:
        - email_address
        - campaign_id
        - campaign_title
        - sent_date
        - opened (bool)
        - clicked (bool)

        Args:
            days_back: Number of days back to fetch campaigns (default 30)

        Returns:
            DataFrame with recipient activity
        """
        print("\n=== Fetching Mailchimp Recipient Activity ===")

        # Get campaigns from last N days
        since_date = datetime.now() - timedelta(days=days_back)
        campaigns = self.get_campaigns(since=since_date, status="sent")

        if not campaigns:
            print("No campaigns found")
            return pd.DataFrame()

        print(f"Found {len(campaigns)} campaigns in last {days_back} days")

        all_recipients = []

        for campaign in campaigns:
            campaign_id = campaign['id']
            campaign_title = campaign.get('settings', {}).get('title', 'Untitled')
            send_time = campaign.get('send_time', '')

            # Fetch recipients for this campaign (reduced verbosity)
            recipients = self.get_campaign_recipients(campaign_id)

            if not recipients:
                continue

            # Only print summary instead of per-campaign details
            if len(all_recipients) % 1000 == 0:  # Print progress every 1000 recipients
                print(f"  ... processed {len(all_recipients)} recipients so far")

            # Process each recipient
            for recipient in recipients:
                email = recipient.get('email_address', '').lower().strip()
                if not email:
                    continue

                # Extract activity data
                activity = recipient.get('activity', [])
                opened = any(a.get('action') == 'open' for a in activity)
                clicked = any(a.get('action') == 'click' for a in activity)

                all_recipients.append({
                    'email_address': email,
                    'campaign_id': campaign_id,
                    'campaign_title': campaign_title,
                    'sent_date': send_time,
                    'opened': opened,
                    'clicked': clicked
                })

        if not all_recipients:
            print("\n✅ No recipient activity found")
            return pd.DataFrame()

        df = pd.DataFrame(all_recipients)
        df['sent_date'] = pd.to_datetime(df['sent_date'])

        print(f"\n✅ Processed {len(df)} recipient records")
        print(f"   Unique recipients: {df['email_address'].nunique()}")
        print(f"   Total opens: {df['opened'].sum()}")
        print(f"   Total clicks: {df['clicked'].sum()}")

        return df

"""
Fetch email activity data from SendGrid API.

Tracks emails sent, delivered, opened, and clicked for experiment tracking.
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import boto3
from io import StringIO


class SendGridDataFetcher:
    """Fetch email activity data from SendGrid."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize SendGrid data fetcher.

        Args:
            api_key: SendGrid API key (defaults to SENDGRID_API_KEY env var)
        """
        self.api_key = api_key or os.getenv("SENDGRID_API_KEY")

        if not self.api_key:
            raise ValueError("SENDGRID_API_KEY must be set")

        self.base_url = "https://api.sendgrid.com/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        print("✅ SendGrid Data Fetcher initialized")

    def fetch_email_activity(
        self,
        days_back: int = 7,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch email activity events from SendGrid.

        Args:
            days_back: Number of days to fetch (default 7, max 30 on free tier)
            limit: Max events to fetch per request (default 1000, max 1000)

        Returns:
            DataFrame with email activity events
        """
        print(f"\n📧 Fetching SendGrid email activity (last {days_back} days)...")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        # Format dates for API (Unix timestamp)
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        # API endpoint - Email Activity Feed
        url = f"{self.base_url}/messages"

        params = {
            "limit": limit,
            "query": f"last_event_time BETWEEN TIMESTAMP \"{start_date.isoformat()}\" AND TIMESTAMP \"{end_date.isoformat()}\""
        }

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            messages = data.get('messages', [])

            if not messages:
                print("   No email activity found")
                return pd.DataFrame()

            # Parse messages into DataFrame
            rows = []
            for msg in messages:
                row = {
                    'msg_id': msg.get('msg_id'),
                    'from_email': msg.get('from_email'),
                    'to_email': msg.get('to_email'),
                    'subject': msg.get('subject'),
                    'status': msg.get('status'),  # processed, delivered, opened, clicked, etc.
                    'template_id': msg.get('template_id'),
                    'template_name': msg.get('template_name'),
                    'last_event_time': msg.get('last_event_time'),
                    'opens_count': msg.get('opens_count', 0),
                    'clicks_count': msg.get('clicks_count', 0),
                }
                rows.append(row)

            df = pd.DataFrame(rows)

            # Convert timestamp to datetime
            df['last_event_time'] = pd.to_datetime(df['last_event_time'])

            print(f"   ✅ Fetched {len(df)} email activity events")
            return df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                print(f"   ❌ Authentication failed - check SENDGRID_API_KEY")
            else:
                print(f"   ❌ HTTP error: {e}")
            return pd.DataFrame()
        except Exception as e:
            print(f"   ❌ Error fetching SendGrid data: {e}")
            return pd.DataFrame()

    def fetch_stats(self, days_back: int = 30) -> pd.DataFrame:
        """
        Fetch email statistics from SendGrid Stats API.

        Args:
            days_back: Number of days to fetch (default 30)

        Returns:
            DataFrame with daily email stats
        """
        print(f"\n📊 Fetching SendGrid stats (last {days_back} days)...")

        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        url = f"{self.base_url}/stats"

        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregated_by": "day"
        }

        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if not data:
                print("   No stats found")
                return pd.DataFrame()

            # Parse stats into DataFrame
            rows = []
            for day_stat in data:
                row = {
                    'date': day_stat.get('date'),
                    'requests': day_stat['stats'][0].get('metrics', {}).get('requests', 0),
                    'delivered': day_stat['stats'][0].get('metrics', {}).get('delivered', 0),
                    'opens': day_stat['stats'][0].get('metrics', {}).get('opens', 0),
                    'unique_opens': day_stat['stats'][0].get('metrics', {}).get('unique_opens', 0),
                    'clicks': day_stat['stats'][0].get('metrics', {}).get('clicks', 0),
                    'unique_clicks': day_stat['stats'][0].get('metrics', {}).get('unique_clicks', 0),
                    'bounces': day_stat['stats'][0].get('metrics', {}).get('bounces', 0),
                    'spam_reports': day_stat['stats'][0].get('metrics', {}).get('spam_reports', 0),
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            df['date'] = pd.to_datetime(df['date'])

            print(f"   ✅ Fetched stats for {len(df)} days")
            return df

        except Exception as e:
            print(f"   ❌ Error fetching stats: {e}")
            return pd.DataFrame()

    def save_email_activity(
        self,
        save_local: bool = True,
        days_back: int = 7
    ):
        """
        Fetch email activity and save to local + S3.

        Args:
            save_local: Whether to save locally (default True)
            days_back: Number of days to fetch
        """
        df = self.fetch_email_activity(days_back=days_back)

        if df.empty:
            print("   ⚠️  No email activity to save")
            return

        # Save locally
        if save_local:
            output_path = "data/sendgrid/emails_sent.csv"
            os.makedirs("data/sendgrid", exist_ok=True)

            # Load existing data if it exists
            try:
                existing_df = pd.read_csv(output_path)
                existing_df['last_event_time'] = pd.to_datetime(existing_df['last_event_time'])

                # Merge with new data (de-duplicate by msg_id)
                combined_df = pd.concat([existing_df, df])
                combined_df = combined_df.drop_duplicates(subset=['msg_id'], keep='last')
                combined_df = combined_df.sort_values('last_event_time')

                df = combined_df
            except FileNotFoundError:
                pass

            df.to_csv(output_path, index=False)
            print(f"   ✅ Saved {len(df)} email activity records locally")

        # Save to S3
        try:
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )

                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)

                s3_client.put_object(
                    Bucket='basin-climbing-data-prod',
                    Key='sendgrid/emails_sent.csv',
                    Body=csv_buffer.getvalue()
                )
                print(f"   ✅ Uploaded email activity to S3")
        except Exception as e:
            print(f"   ⚠️  Could not upload to S3: {e}")


def main():
    """Run SendGrid data fetch."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    fetcher = SendGridDataFetcher()

    # Fetch and save email activity
    fetcher.save_email_activity(save_local=True, days_back=7)

    # Fetch and display stats
    stats_df = fetcher.fetch_stats(days_back=30)
    if not stats_df.empty:
        print("\n📊 Recent email stats:")
        print(stats_df.tail(7).to_string(index=False))


if __name__ == "__main__":
    main()

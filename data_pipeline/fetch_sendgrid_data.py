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
        Fetch email activity events from SendGrid Stats API.

        Note: The Email Activity Feed API requires additional permissions.
        Using Stats API for aggregate daily metrics instead.

        Args:
            days_back: Number of days to fetch (default 7)
            limit: Not used for Stats API (kept for compatibility)

        Returns:
            DataFrame with daily email statistics
        """
        print(f"\n📧 Fetching SendGrid email statistics (last {days_back} days)...")

        # Use the stats API instead of email activity feed
        # The activity feed requires special permissions
        return self.fetch_stats(days_back=days_back)

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
        Fetch email statistics and save to local + S3.

        Args:
            save_local: Whether to save locally (default True)
            days_back: Number of days to fetch
        """
        df = self.fetch_email_activity(days_back=days_back)

        if df.empty:
            print("   ⚠️  No email statistics to save")
            return

        # Save locally
        if save_local:
            output_path = "data/sendgrid/email_stats.csv"
            os.makedirs("data/sendgrid", exist_ok=True)

            # Load existing data if it exists
            try:
                existing_df = pd.read_csv(output_path)
                existing_df['date'] = pd.to_datetime(existing_df['date'])

                # Merge with new data (de-duplicate by date)
                combined_df = pd.concat([existing_df, df])
                combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
                combined_df = combined_df.sort_values('date')

                df = combined_df
            except FileNotFoundError:
                pass

            df.to_csv(output_path, index=False)
            print(f"   ✅ Saved {len(df)} email statistics records locally")

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
                    Key='sendgrid/email_stats.csv',
                    Body=csv_buffer.getvalue()
                )
                print(f"   ✅ Uploaded email statistics to S3")
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

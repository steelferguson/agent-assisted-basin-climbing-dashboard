"""
Fetch SendGrid Recipient Activity

Fetches individual recipient-level email activity from SendGrid Email Activity Feed.
This data is used to track which customers received which emails, enabling:
1. Verifying flagged customers received automated journey emails
2. Preventing duplicate offers
3. Adding email events to customer timelines
4. Analyzing email engagement per customer

Storage:
- S3: sendgrid/recipient_activity.csv
- Local: data/sendgrid/recipient_activity.csv

Requirements:
- SendGrid Email Activity Feed API access (requires additional permissions)
- See: https://docs.sendgrid.com/api-reference/e-mail-activity/filter-all-messages
"""

import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
from typing import Optional
import time


class SendGridRecipientActivityFetcher:
    """Fetch and manage SendGrid recipient-level email activity."""

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize SendGrid recipient activity fetcher.

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

        # AWS credentials
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"
        self.s3_key = "sendgrid/recipient_activity.csv"

        # S3 client
        if self.aws_access_key_id and self.aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
        else:
            self.s3_client = None

        print("✅ SendGrid Recipient Activity Fetcher initialized")

    def load_existing_from_s3(self) -> pd.DataFrame:
        """
        Load existing recipient activity from S3.

        Returns:
            DataFrame with existing activity
        """
        if not self.s3_client:
            print("⚠️  No S3 client available")
            return pd.DataFrame()

        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(df)} existing records from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No existing recipient activity found in S3 (first run)")
            return pd.DataFrame()
        except Exception as e:
            print(f"⚠️  Error loading from S3: {e}")
            return pd.DataFrame()

    def fetch_recipient_activity(
        self,
        days_back: int = 7,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch recipient-level email activity from SendGrid Email Activity Feed API.

        NOTE: This requires the Email Activity Feed API to be enabled in your
        SendGrid account. This is a premium feature that may require contacting
        SendGrid support to enable.

        Args:
            days_back: Number of days to fetch (default 7, max 30)
            limit: Results per page (max 1000)

        Returns:
            DataFrame with recipient activity records
        """
        print(f"\n📧 Fetching SendGrid recipient activity (last {days_back} days)...")

        # Calculate date range (SendGrid uses Unix timestamps)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        # Convert to Unix timestamps
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())

        url = f"{self.base_url}/messages"

        all_messages = []
        page = 0

        while True:
            params = {
                "limit": limit,
                "query": f"last_event_time BETWEEN TIMESTAMP \"{start_date.strftime('%Y-%m-%d')}\" AND TIMESTAMP \"{end_date.strftime('%Y-%m-%d')}\""
            }

            try:
                print(f"   Fetching page {page + 1}...")
                response = requests.get(url, headers=self.headers, params=params, timeout=30)

                # Check for permission error
                if response.status_code == 403:
                    print("   ⚠️  Email Activity Feed API not enabled or insufficient permissions")
                    print("   ℹ️  This requires additional SendGrid permissions")
                    print("   ℹ️  Contact SendGrid support to enable Email Activity Feed API")
                    return pd.DataFrame()

                response.raise_for_status()
                data = response.json()

                messages = data.get('messages', [])
                if not messages:
                    print(f"   No more messages (page {page + 1})")
                    break

                all_messages.extend(messages)
                print(f"   Found {len(messages)} messages")

                # Check if there are more pages
                # SendGrid uses cursor-based pagination
                if len(messages) < limit:
                    break

                page += 1
                time.sleep(0.2)  # Rate limiting

            except requests.exceptions.RequestException as e:
                print(f"   ❌ Error fetching messages: {e}")
                if hasattr(e.response, 'text'):
                    print(f"   Response: {e.response.text}")
                break

        if not all_messages:
            print("   No recipient activity found")
            return pd.DataFrame()

        # Parse messages into DataFrame
        rows = []
        for msg in all_messages:
            # Extract basic message info
            msg_id = msg.get('msg_id')
            from_email = msg.get('from_email')
            subject = msg.get('subject')
            to_email = msg.get('to_email')
            status = msg.get('status')
            last_event_time = msg.get('last_event_time')

            # Extract events (opens, clicks, etc.)
            events = msg.get('events', [])

            # Create a row for this message
            row = {
                'msg_id': msg_id,
                'from_email': from_email,
                'subject': subject,
                'to_email': to_email,
                'status': status,
                'last_event_time': last_event_time,
                'delivered': any(e.get('event_name') == 'delivered' for e in events),
                'opened': any(e.get('event_name') == 'open' for e in events),
                'clicked': any(e.get('event_name') == 'click' for e in events),
                'bounced': any(e.get('event_name') == 'bounce' for e in events),
                'spam_report': any(e.get('event_name') == 'spamreport' for e in events),
            }

            # Get earliest send time
            delivered_events = [e for e in events if e.get('event_name') == 'delivered']
            if delivered_events:
                # Convert timestamp to datetime
                timestamp = delivered_events[0].get('timestamp')
                if timestamp:
                    row['sent_date'] = datetime.fromtimestamp(timestamp).isoformat()
                else:
                    row['sent_date'] = None
            else:
                row['sent_date'] = last_event_time

            rows.append(row)

        df = pd.DataFrame(rows)

        # Normalize email addresses
        df['to_email'] = df['to_email'].str.lower().str.strip()

        print(f"\n✅ Fetched {len(df)} recipient activity records")
        print(f"   Unique recipients: {df['to_email'].nunique()}")
        print(f"   Unique subjects: {df['subject'].nunique()}")

        return df

    def save_to_s3(self, df: pd.DataFrame):
        """
        Save recipient activity DataFrame to S3.

        Args:
            df: DataFrame to save
        """
        if not self.s3_client:
            print("⚠️  No S3 client available")
            return

        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.s3_key,
                Body=csv_buffer.getvalue()
            )
            print(f"✅ Uploaded {len(df)} records to S3: {self.s3_key}")
        except Exception as e:
            print(f"❌ Error uploading to S3: {e}")

    def fetch_and_save(self, days_back: int = 7, save_local: bool = True):
        """
        Fetch recipient activity and save to S3 and optionally local.

        Args:
            days_back: Number of days back to fetch
            save_local: Whether to save a local copy
        """
        print("\n" + "="*80)
        print("SENDGRID RECIPIENT ACTIVITY SYNC")
        print("="*80)

        # Fetch new recipient activity
        new_df = self.fetch_recipient_activity(days_back=days_back)

        if new_df.empty:
            print("\n⚠️  No new recipient activity found")
            return

        # Load existing data from S3
        existing_df = self.load_existing_from_s3()

        # Merge with existing data
        if not existing_df.empty:
            print(f"\nMerging with {len(existing_df)} existing records...")

            # Combine and deduplicate by msg_id
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['msg_id'], keep='last')
            combined_df = combined_df.sort_values('sent_date', ascending=False)

            print(f"   Combined: {len(combined_df)} total records")
            df = combined_df
        else:
            df = new_df

        # Save locally if requested
        if save_local:
            output_path = "data/sendgrid/recipient_activity.csv"
            os.makedirs("data/sendgrid", exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"\n✅ Saved {len(df)} records locally: {output_path}")

        # Upload to S3
        self.save_to_s3(df)

        print("\n" + "="*80)
        print("SENDGRID RECIPIENT ACTIVITY SYNC COMPLETE")
        print("="*80)


def main():
    """Run SendGrid recipient activity fetch."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    fetcher = SendGridRecipientActivityFetcher()
    fetcher.fetch_and_save(days_back=7, save_local=True)


if __name__ == "__main__":
    main()

"""
Fetch SendGrid Webhook Events from S3

Processes SendGrid webhook events stored in S3 and creates a consolidated
recipient activity CSV. This replaces the Email Activity Feed API approach.

Webhook events are stored at:
- S3: sendgrid/events/date=YYYY-MM-DD/*.jsonl
- Format: NDJSON (newline-delimited JSON)

Each event contains:
- received_at: When webhook was received
- source: "sendgrid"
- raw_event: The actual SendGrid event data (delivered, open, click, etc.)
"""

import boto3
import json
import os
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from typing import List, Dict


class SendGridWebhookProcessor:
    """Process SendGrid webhook events from S3."""

    def __init__(self):
        """Initialize S3 client and configuration."""
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"
        self.events_prefix = "sendgrid/events/date="
        self.output_key = "sendgrid/recipient_activity.csv"

        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise ValueError("AWS credentials not set")

        self.s3_client = boto3.client(
            's3',
            region_name='us-west-2',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        print("✅ SendGrid Webhook Processor initialized")

    def get_available_dates(self, days_back: int = 30) -> List[str]:
        """
        Get list of dates with webhook events available in S3.

        Args:
            days_back: Number of days to look back (default 30)

        Returns:
            List of date strings (YYYY-MM-DD format)
        """
        # List all date prefixes
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=self.events_prefix,
            Delimiter='/'
        )

        dates = []
        for prefix in response.get('CommonPrefixes', []):
            # Extract date from prefix: sendgrid/events/date=YYYY-MM-DD/
            date_str = prefix['Prefix'].replace(self.events_prefix, '').rstrip('/')
            dates.append(date_str)

        # Filter to last N days
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        dates = [d for d in dates if d >= cutoff_date]
        dates.sort()

        return dates

    def fetch_events_for_date(self, date: str) -> List[Dict]:
        """
        Fetch all webhook events for a specific date.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            List of event dictionaries
        """
        prefix = f"{self.events_prefix}{date}/"

        # List all files for this date
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )

        events = []
        for obj in response.get('Contents', []):
            try:
                # Read file
                data = self.s3_client.get_object(Bucket=self.bucket_name, Key=obj['Key'])
                content = data['Body'].read().decode('utf-8')

                # Parse NDJSON (each line is a JSON object)
                for line in content.strip().split('\n'):
                    if line:
                        event = json.loads(line)
                        events.append(event)

            except Exception as e:
                print(f"   ⚠️  Error reading {obj['Key']}: {e}")
                continue

        return events

    def process_events_to_dataframe(self, events: List[Dict]) -> pd.DataFrame:
        """
        Convert webhook events to structured DataFrame.

        Args:
            events: List of webhook event dictionaries

        Returns:
            DataFrame with recipient activity
        """
        rows = []

        for event in events:
            raw_event = event.get('raw_event', {})

            # Extract key fields
            row = {
                'received_at': event.get('received_at'),
                'event_type': raw_event.get('event'),
                'email': raw_event.get('email'),
                'timestamp': raw_event.get('timestamp'),
                'sg_message_id': raw_event.get('sg_message_id'),
                'sg_event_id': raw_event.get('sg_event_id'),
                'useragent': raw_event.get('useragent'),
                'url': raw_event.get('url'),  # For click events
                'reason': raw_event.get('reason'),  # For bounce/deferred
                'status': raw_event.get('status'),  # For bounce
                'response': raw_event.get('response'),  # For bounce
            }

            rows.append(row)

        df = pd.DataFrame(rows)

        if not df.empty:
            # Convert timestamps
            df['received_at'] = pd.to_datetime(df['received_at'])
            df['event_datetime'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')

            # Normalize emails
            df['email'] = df['email'].str.lower().str.strip()

            # Sort by timestamp
            df = df.sort_values('event_datetime', ascending=False)

        return df

    def aggregate_recipient_activity(self, df_events: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate events by recipient and message to create recipient activity summary.

        Args:
            df_events: DataFrame of individual events

        Returns:
            DataFrame with one row per recipient per message
        """
        if df_events.empty:
            return pd.DataFrame()

        # Group by message and recipient
        activity = []

        for (msg_id, email), group in df_events.groupby(['sg_message_id', 'email']):
            # Get event types
            event_types = set(group['event_type'].dropna())

            # Get earliest timestamp (sent time)
            sent_time = group['event_datetime'].min()

            activity.append({
                'sg_message_id': msg_id,
                'email': email,
                'sent_datetime': sent_time,
                'delivered': 'delivered' in event_types,
                'opened': 'open' in event_types,
                'clicked': 'click' in event_types,
                'bounced': 'bounce' in event_types,
                'dropped': 'dropped' in event_types,
                'deferred': 'deferred' in event_types,
                'unsubscribed': 'unsubscribe' in event_types,
                'spam_report': 'spamreport' in event_types,
            })

        df_activity = pd.DataFrame(activity)
        return df_activity

    def fetch_and_save(self, days_back: int = 7, save_local: bool = True):
        """
        Fetch webhook events, process, and save to S3.

        Args:
            days_back: Number of days to fetch (default 7)
            save_local: Whether to save a local copy
        """
        print("\n" + "="*80)
        print("SENDGRID WEBHOOK EVENTS PROCESSING")
        print("="*80)

        # Get available dates
        print(f"\n📅 Finding webhook events (last {days_back} days)...")
        dates = self.get_available_dates(days_back=days_back)

        if not dates:
            print("   ⚠️  No webhook events found")
            return

        print(f"   ✅ Found {len(dates)} dates with events: {dates[0]} to {dates[-1]}")

        # Fetch events for all dates
        print(f"\n📥 Fetching events...")
        all_events = []
        for date in dates:
            events = self.fetch_events_for_date(date)
            all_events.extend(events)
            print(f"   {date}: {len(events)} events")

        if not all_events:
            print("\n   ⚠️  No events found")
            return

        print(f"\n   ✅ Total: {len(all_events)} webhook events")

        # Process to DataFrame
        print("\n🔄 Processing events...")
        df_events = self.process_events_to_dataframe(all_events)
        print(f"   ✅ Processed {len(df_events)} events")

        # Show event type breakdown
        event_counts = df_events['event_type'].value_counts()
        print("\n📊 Event breakdown:")
        for event_type, count in event_counts.items():
            print(f"   {event_type}: {count}")

        # Aggregate to recipient activity
        print("\n📧 Aggregating recipient activity...")
        df_activity = self.aggregate_recipient_activity(df_events)
        print(f"   ✅ {len(df_activity)} unique recipient-message pairs")
        print(f"   ✅ {df_activity['email'].nunique()} unique recipients")

        # Summary stats
        if not df_activity.empty:
            print("\n📈 Activity summary:")
            print(f"   Delivered: {df_activity['delivered'].sum()}")
            print(f"   Opened: {df_activity['opened'].sum()} ({df_activity['opened'].mean()*100:.1f}%)")
            print(f"   Clicked: {df_activity['clicked'].sum()} ({df_activity['clicked'].mean()*100:.1f}%)")
            print(f"   Bounced: {df_activity['bounced'].sum()}")

        # Load existing data from S3 and merge
        print("\n🔄 Loading existing data from S3...")
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.output_key)
            existing_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            existing_df['sent_datetime'] = pd.to_datetime(existing_df['sent_datetime'])
            print(f"   ✅ Loaded {len(existing_df)} existing records")

            # Merge with new data
            combined_df = pd.concat([existing_df, df_activity], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['sg_message_id', 'email'], keep='last')
            combined_df = combined_df.sort_values('sent_datetime', ascending=False)

            print(f"   ✅ Combined: {len(combined_df)} total records")
            df_activity = combined_df

        except self.s3_client.exceptions.NoSuchKey:
            print("   ℹ️  No existing data found (first run)")
        except Exception as e:
            print(f"   ⚠️  Error loading existing data: {e}")

        # Save locally if requested
        if save_local:
            output_path = "data/sendgrid/recipient_activity.csv"
            os.makedirs("data/sendgrid", exist_ok=True)
            df_activity.to_csv(output_path, index=False)
            print(f"\n💾 Saved locally: {output_path}")

        # Upload to S3
        print("\n☁️  Uploading to S3...")
        try:
            csv_buffer = StringIO()
            df_activity.to_csv(csv_buffer, index=False)

            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.output_key,
                Body=csv_buffer.getvalue()
            )
            print(f"   ✅ Uploaded to s3://{self.bucket_name}/{self.output_key}")
        except Exception as e:
            print(f"   ❌ Error uploading to S3: {e}")

        print("\n" + "="*80)
        print("SENDGRID WEBHOOK EVENTS PROCESSING COMPLETE")
        print("="*80)


def main():
    """Run SendGrid webhook event processing."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    processor = SendGridWebhookProcessor()
    processor.fetch_and_save(days_back=7, save_local=True)


if __name__ == "__main__":
    main()

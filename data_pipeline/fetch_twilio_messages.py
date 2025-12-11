"""
Fetch and Store Twilio Message History

Fetches all Twilio SMS messages and stores them for analysis.
Tracks all inbound and outbound messages with full details.

Storage:
- s3://basin-climbing-data-prod/twilio/messages.csv (all messages)
- Local: data/outputs/twilio_messages.csv (optional)
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from twilio.rest import Client
import boto3
from io import StringIO
from typing import Optional


class TwilioMessageFetcher:
    """
    Fetch and store all Twilio SMS messages.
    """

    def __init__(self):
        # Twilio credentials
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.from_number = os.getenv("TWILIO_PHONE_NUMBER")

        # AWS credentials
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"

        # S3 key
        self.s3_key = "twilio/messages.csv"

        # Initialize clients
        self.twilio_client = Client(self.account_sid, self.auth_token)
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        print("✅ Twilio Message Fetcher initialized")

    def fetch_messages(self, days_back: Optional[int] = None, limit: int = 5000):
        """
        Fetch Twilio messages.

        Args:
            days_back: Only fetch messages from last N days (default: all via limit)
            limit: Maximum messages to fetch (default: 5000)

        Returns:
            DataFrame with messages
        """
        print(f"\nFetching Twilio messages...")
        if days_back:
            date_sent_after = datetime.utcnow() - timedelta(days=days_back)
            print(f"  Date range: last {days_back} days (since {date_sent_after.date()})")
            messages = self.twilio_client.messages.list(
                date_sent_after=date_sent_after,
                limit=limit
            )
        else:
            print(f"  Fetching up to {limit} messages")
            messages = self.twilio_client.messages.list(limit=limit)

        print(f"✅ Fetched {len(messages)} messages from Twilio")

        # Convert to DataFrame
        message_data = []
        for msg in messages:
            message_data.append({
                'message_sid': msg.sid,
                'date_sent': msg.date_sent.isoformat() if msg.date_sent else None,
                'date_created': msg.date_created.isoformat() if msg.date_created else None,
                'direction': msg.direction,  # inbound, outbound-api, outbound-call, outbound-reply
                'from_number': self._normalize_phone(msg.from_),
                'to_number': self._normalize_phone(msg.to),
                'body': msg.body,
                'status': msg.status,  # delivered, sent, failed, etc.
                'error_code': msg.error_code,
                'error_message': msg.error_message,
                'num_segments': msg.num_segments,
                'price': msg.price,
                'price_unit': msg.price_unit
            })

        df = pd.DataFrame(message_data)

        # Add parsed fields
        if len(df) > 0:
            df['body_lower'] = df['body'].fillna('').str.lower().str.strip()

            # Identify message types
            df['is_waiver_request'] = df['body_lower'].str.contains('waiver', na=False)
            df['is_opt_in'] = df['body_lower'].apply(
                lambda x: any(kw in x for kw in ['basin', 'yes', 'y', 'opt in', 'optin', 'start', 'unstop'])
            )
            df['is_opt_out'] = df['body_lower'].apply(
                lambda x: x in ['stop', 'stopall', 'unsubscribe', 'cancel', 'end', 'quit']
            )

            # Sort by date
            df = df.sort_values('date_sent', ascending=False)

        return df

    def load_existing_messages(self):
        """
        Load existing messages from S3.

        Returns:
            DataFrame with existing messages (empty if none exist)
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Convert boolean columns
            bool_columns = ['is_waiver_request', 'is_opt_in', 'is_opt_out']
            for col in bool_columns:
                if col in df.columns:
                    df[col] = df[col].fillna(False).astype(bool)

            print(f"✅ Loaded {len(df)} existing messages from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No existing messages found (first run)")
            return pd.DataFrame()

    def merge_and_save(self, new_messages: pd.DataFrame, save_local: bool = False):
        """
        Merge new messages with existing and save to S3.

        Args:
            new_messages: DataFrame with new messages
            save_local: Whether to save local copy
        """
        print("\n💾 Merging and saving messages...")

        # Load existing
        existing = self.load_existing_messages()

        # Merge
        if len(existing) > 0 and len(new_messages) > 0:
            all_messages = pd.concat([existing, new_messages], ignore_index=True)

            # Deduplicate by message_sid
            before_count = len(all_messages)
            all_messages = all_messages.drop_duplicates(subset=['message_sid'], keep='last')
            after_count = len(all_messages)

            if before_count > after_count:
                print(f"   Removed {before_count - after_count} duplicate messages")

            # Sort by date
            all_messages = all_messages.sort_values('date_sent', ascending=False)
        elif len(new_messages) > 0:
            all_messages = new_messages
        else:
            all_messages = existing

        # Save to S3 (keep phone numbers as strings with dtype)
        csv_buffer = StringIO()
        all_messages.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.s3_key,
            Body=csv_buffer.getvalue()
        )

        print(f"✅ Saved {len(all_messages)} total messages to S3")
        print(f"   Location: s3://{self.bucket_name}/{self.s3_key}")

        # Save local copy if requested
        if save_local:
            local_path = 'data/outputs/twilio_messages.csv'
            all_messages.to_csv(local_path, index=False)
            print(f"✅ Saved local copy to {local_path}")

        # Print summary
        print("\n📊 Message Summary:")
        print(f"   Total messages: {len(all_messages)}")
        if len(all_messages) > 0:
            print(f"   Inbound: {len(all_messages[all_messages['direction'] == 'inbound'])}")
            print(f"   Outbound: {len(all_messages[all_messages['direction'].str.startswith('outbound')])}")
            print(f"   WAIVER requests: {all_messages['is_waiver_request'].sum()}")
            print(f"   Opt-ins: {all_messages['is_opt_in'].sum()}")
            print(f"   Opt-outs: {all_messages['is_opt_out'].sum()}")

        return all_messages

    def fetch_and_save(self, days_back: Optional[int] = None, limit: int = 5000, save_local: bool = False):
        """
        Convenience method: Fetch messages and save to S3.

        Args:
            days_back: Only fetch messages from last N days
            limit: Maximum messages to fetch
            save_local: Whether to save local copy

        Returns:
            DataFrame with all messages (merged)
        """
        print("="*80)
        print("TWILIO MESSAGE SYNC")
        print("="*80)

        # Fetch new messages
        new_messages = self.fetch_messages(days_back=days_back, limit=limit)

        # Merge and save
        all_messages = self.merge_and_save(new_messages, save_local=save_local)

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)

        return all_messages

    def _normalize_phone(self, phone_number):
        """Normalize phone number to E.164 format."""
        if not phone_number:
            return None

        # Remove all non-digit characters
        digits = ''.join(filter(str.isdigit, str(phone_number)))

        # Add +1 if not present (assuming US)
        if len(digits) == 10:
            return f"+1{digits}"
        elif len(digits) == 11 and digits[0] == '1':
            return f"+{digits}"
        else:
            if str(phone_number).startswith('+'):
                return phone_number
            return f"+{digits}"


def main():
    """Run message fetch and save."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    fetcher = TwilioMessageFetcher()

    # Fetch last 30 days of messages (or up to 5000)
    messages = fetcher.fetch_and_save(days_back=30, limit=5000, save_local=True)

    # Show sample
    if len(messages) > 0:
        print("\n" + "="*80)
        print("SAMPLE MESSAGES")
        print("="*80)
        print("\nFirst 5 messages:")
        print(messages[['date_sent', 'direction', 'from_number', 'to_number', 'body']].head().to_string())

        print("\n\nWAIVER requests:")
        waiver_msgs = messages[messages['is_waiver_request'] == True]
        if len(waiver_msgs) > 0:
            print(waiver_msgs[['date_sent', 'from_number', 'body']].head().to_string())
        else:
            print("None found")


if __name__ == "__main__":
    main()

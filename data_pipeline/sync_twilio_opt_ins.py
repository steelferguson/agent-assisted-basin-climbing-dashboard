"""
Sync Twilio Opt-ins and Opt-outs

Fetches Twilio message history and tracks:
1. All opt-in/opt-out actions (history table)
2. Current opt-in status per phone number (status table)

Storage:
- s3://basin-climbing-data-prod/twilio/sms_opt_in_history.csv (all actions)
- s3://basin-climbing-data-prod/twilio/sms_opt_in_status.csv (current status)
"""

import os
import pandas as pd
from datetime import datetime
from twilio.rest import Client
import boto3
from io import StringIO


class TwilioOptInTracker:
    """
    Track SMS opt-ins and opt-outs from Twilio message history.
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

        # S3 keys
        self.history_key = "twilio/sms_opt_in_history.csv"
        self.status_key = "twilio/sms_opt_in_status.csv"

        # Initialize clients
        self.twilio_client = Client(self.account_sid, self.auth_token)
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        print("✅ Twilio Opt-In Tracker initialized")

    def fetch_messages(self, limit=1000):
        """
        Fetch recent Twilio messages.

        Args:
            limit: Number of messages to fetch (default 1000)

        Returns:
            List of message objects
        """
        print(f"\nFetching last {limit} Twilio messages...")
        messages = self.twilio_client.messages.list(limit=limit)
        print(f"✅ Fetched {len(messages)} messages")
        return messages

    def extract_opt_in_actions(self, messages):
        """
        Extract opt-in and opt-out actions from messages.

        Opt-in flows:
        1. Customer texts keyword (BASIN, Y, YES, OPT IN, etc.) → We reply with confirmation
        2. We send welcome message asking "Reply Y or N" → They reply Y

        Opt-out:
        - Customer texts STOP (any capitalization)

        Args:
            messages: List of Twilio message objects

        Returns:
            DataFrame with opt-in/opt-out actions
        """
        print("\n📊 Extracting opt-in/opt-out actions...")

        actions = []

        for msg in messages:
            phone_number = self._normalize_phone(msg.from_ if msg.direction == 'inbound' else msg.to)
            timestamp = msg.date_created.isoformat() if msg.date_created else None
            message_body = (msg.body or "").strip()
            message_body_lower = message_body.lower()

            # Inbound messages (customer to us)
            if msg.direction == 'inbound':
                # Check for opt-out
                if message_body_lower in ['stop', 'stopall', 'unsubscribe', 'cancel', 'end', 'quit']:
                    actions.append({
                        'phone_number': phone_number,
                        'timestamp': timestamp,
                        'action': 'opt_out',
                        'method': 'keyword',
                        'message_body': message_body,
                        'message_sid': msg.sid,
                        'notes': f'Customer texted: {message_body}'
                    })

                # Check for opt-in keywords
                elif any(keyword in message_body_lower for keyword in ['basin', 'y', 'yes', 'opt in', 'optin', 'start', 'unstop']):
                    # Determine which flow
                    if message_body_lower in ['y', 'yes']:
                        flow = 'reply_to_welcome'
                    elif 'basin' in message_body_lower:
                        flow = 'keyword_basin'
                    elif 'opt in' in message_body_lower or 'optin' in message_body_lower:
                        flow = 'keyword_opt_in'
                    else:
                        flow = 'keyword_start'

                    actions.append({
                        'phone_number': phone_number,
                        'timestamp': timestamp,
                        'action': 'opt_in',
                        'method': flow,
                        'message_body': message_body,
                        'message_sid': msg.sid,
                        'notes': f'Customer texted: {message_body}'
                    })

            # Outbound messages (us to customer)
            # We can infer opt-ins from our welcome messages
            elif msg.direction == 'outbound-api':
                # If we sent them a discount offer, they must have opted in
                if 'day pass offer' in message_body_lower or 'discount' in message_body_lower:
                    # Don't add action here - the opt-in should have been captured from their inbound message
                    pass

        # Convert to DataFrame
        actions_df = pd.DataFrame(actions)

        if len(actions_df) > 0:
            print(f"   Found {len(actions_df)} opt-in/opt-out actions")
            print(f"   Opt-ins: {len(actions_df[actions_df['action'] == 'opt_in'])}")
            print(f"   Opt-outs: {len(actions_df[actions_df['action'] == 'opt_out'])}")
        else:
            print("   No opt-in/opt-out actions found")

        return actions_df

    def load_existing_history(self):
        """
        Load existing opt-in history from S3.

        Returns:
            DataFrame with existing history
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.history_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(df)} existing history records from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No existing history found (first run)")
            return pd.DataFrame(columns=[
                'phone_number', 'timestamp', 'action', 'method',
                'message_body', 'message_sid', 'notes'
            ])

    def update_history(self, new_actions):
        """
        Update opt-in history table with new actions.

        Args:
            new_actions: DataFrame with new opt-in/opt-out actions
        """
        print("\n📝 Updating opt-in history...")

        # Load existing history
        existing_history = self.load_existing_history()

        # Combine with new actions
        if len(new_actions) > 0:
            all_history = pd.concat([existing_history, new_actions], ignore_index=True)

            # Deduplicate by message_sid (same action shouldn't be recorded twice)
            before_count = len(all_history)
            all_history = all_history.drop_duplicates(subset=['message_sid'], keep='last')
            after_count = len(all_history)

            if before_count > after_count:
                print(f"   Removed {before_count - after_count} duplicate actions")

            # Sort by timestamp
            all_history = all_history.sort_values('timestamp', ascending=False)
        else:
            all_history = existing_history

        # Upload to S3
        csv_buffer = StringIO()
        all_history.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.history_key,
            Body=csv_buffer.getvalue()
        )

        print(f"✅ Saved {len(all_history)} total history records to S3")
        print(f"   Location: s3://{self.bucket_name}/{self.history_key}")

        return all_history

    def calculate_current_status(self, history):
        """
        Calculate current opt-in status for each phone number.

        Args:
            history: DataFrame with all opt-in/opt-out history

        Returns:
            DataFrame with current status per phone number
        """
        print("\n📊 Calculating current opt-in status...")

        if len(history) == 0:
            print("   No history to process")
            return pd.DataFrame(columns=[
                'phone_number', 'current_status', 'last_action_timestamp',
                'last_action', 'last_method', 'total_opt_ins', 'total_opt_outs'
            ])

        # Sort by timestamp (most recent first)
        history_sorted = history.sort_values('timestamp', ascending=False)

        # Get most recent action per phone number
        latest_action = history_sorted.groupby('phone_number').first().reset_index()

        # Count total opt-ins and opt-outs
        action_counts = history.groupby(['phone_number', 'action']).size().unstack(fill_value=0).reset_index()

        # Ensure both columns exist
        if 'opt_in' not in action_counts.columns:
            action_counts['opt_in'] = 0
        if 'opt_out' not in action_counts.columns:
            action_counts['opt_out'] = 0

        # Rename columns
        action_counts = action_counts.rename(columns={
            'opt_in': 'total_opt_ins',
            'opt_out': 'total_opt_outs'
        })

        # Merge
        status_df = latest_action.merge(action_counts, on='phone_number', how='left')

        # Set current status based on last action
        status_df['current_status'] = status_df['action'].map({
            'opt_in': 'opted_in',
            'opt_out': 'opted_out'
        })

        # Select and rename columns
        status_df = status_df[[
            'phone_number',
            'current_status',
            'timestamp',
            'action',
            'method',
            'total_opt_ins',
            'total_opt_outs'
        ]].rename(columns={
            'timestamp': 'last_action_timestamp',
            'action': 'last_action',
            'method': 'last_method'
        })

        # Count statuses
        opted_in_count = len(status_df[status_df['current_status'] == 'opted_in'])
        opted_out_count = len(status_df[status_df['current_status'] == 'opted_out'])

        print(f"   Total unique phone numbers: {len(status_df)}")
        print(f"   ✅ Currently opted in: {opted_in_count}")
        print(f"   ❌ Currently opted out: {opted_out_count}")

        return status_df

    def save_current_status(self, status_df):
        """
        Save current opt-in status to S3.

        Args:
            status_df: DataFrame with current status
        """
        print("\n💾 Saving current status...")

        csv_buffer = StringIO()
        status_df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.status_key,
            Body=csv_buffer.getvalue()
        )

        print(f"✅ Saved current status to S3")
        print(f"   Location: s3://{self.bucket_name}/{self.status_key}")

    def sync(self, message_limit=1000):
        """
        Full sync: Fetch messages, update history, calculate status.

        Args:
            message_limit: Number of Twilio messages to fetch
        """
        print("="*80)
        print("TWILIO OPT-IN SYNC")
        print("="*80)

        # 1. Fetch messages from Twilio
        messages = self.fetch_messages(limit=message_limit)

        # 2. Extract opt-in/opt-out actions
        new_actions = self.extract_opt_in_actions(messages)

        # 3. Update history table
        full_history = self.update_history(new_actions)

        # 4. Calculate current status
        current_status = self.calculate_current_status(full_history)

        # 5. Save current status
        self.save_current_status(current_status)

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)

        return {
            'history': full_history,
            'status': current_status
        }

    def get_opted_in_numbers(self):
        """
        Get list of currently opted-in phone numbers.

        Returns:
            List of phone numbers
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.status_key)
            status_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            opted_in = status_df[status_df['current_status'] == 'opted_in']
            return opted_in['phone_number'].tolist()
        except Exception as e:
            print(f"❌ Error loading opted-in numbers: {e}")
            return []

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
    """Run full sync."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    tracker = TwilioOptInTracker()
    results = tracker.sync(message_limit=1000)

    # Show sample of results
    print("\n" + "="*80)
    print("SAMPLE RESULTS")
    print("="*80)

    print("\nCurrent Status (first 10):")
    print(results['status'].head(10).to_string())

    print("\n\nRecent History (first 10):")
    print(results['history'].head(10).to_string())


if __name__ == "__main__":
    main()

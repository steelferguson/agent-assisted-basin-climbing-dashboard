"""
Customer flagging engine.

Evaluates business rules against customer event timelines to identify
customers who need outreach or automated actions.
"""

import pandas as pd
import json
import os
from datetime import datetime
from typing import List, Dict
from data_pipeline import customer_flags_config
from data_pipeline import experiment_tracking
import boto3
from io import StringIO


class CustomerFlagsEngine:
    """Engine for evaluating customer flagging rules."""

    def __init__(self, rules: List = None):
        """
        Initialize the flagging engine.

        Args:
            rules: List of FlagRule objects. If None, uses all active rules from config.
        """
        self.rules = rules if rules is not None else customer_flags_config.get_active_rules()
        self.customer_emails = {}  # Cache for customer emails
        self.customer_phones = {}  # Cache for customer phones

    def load_customer_contact_info(self):
        """Load customer emails and phones from S3 for AB group assignment."""
        try:
            # Try S3 first
            aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

            if aws_access_key_id and aws_secret_access_key:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key
                )

                obj = s3_client.get_object(
                    Bucket='basin-climbing-data-prod',
                    Key='capitan/customers.csv'
                )
                df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            else:
                # Fall back to local file
                df = pd.read_csv('data/outputs/capitan_customers.csv')

            # Build email and phone lookup dicts
            self.customer_emails = df.set_index('customer_id')['email'].to_dict()
            self.customer_phones = df.set_index('customer_id')['phone'].to_dict()
            print(f"   Loaded contact info for {len(self.customer_emails)} customers")
            print(f"   - {sum(1 for e in self.customer_emails.values() if pd.notna(e))} with emails")
            print(f"   - {sum(1 for p in self.customer_phones.values() if pd.notna(p))} with phones")

        except Exception as e:
            print(f"   ⚠️  Could not load customer contact info: {e}")
            self.customer_emails = {}
            self.customer_phones = {}

    def evaluate_customer(
        self,
        customer_id: str,
        events: List[Dict],
        today: datetime = None
    ) -> List[Dict]:
        """
        Evaluate all rules for a single customer.

        Args:
            customer_id: Customer UUID
            events: List of event dicts for this customer
            today: Reference date (defaults to now)

        Returns:
            List of flag dicts for any rules that triggered
        """
        if today is None:
            today = datetime.now()

        # Convert event dates to datetime if they're strings
        for event in events:
            if isinstance(event['event_date'], str):
                event['event_date'] = pd.to_datetime(event['event_date'])

        # Sort events by date
        events_sorted = sorted(events, key=lambda e: e['event_date'])

        # Get customer email and phone for AB group assignment
        email = self.customer_emails.get(customer_id)
        phone = self.customer_phones.get(customer_id)

        # Evaluate each rule
        flags = []
        for rule in self.rules:
            # Pass email and phone if the rule accepts them (AB test flags)
            try:
                flag = rule.evaluate(customer_id, events_sorted, today, email=email, phone=phone)
            except TypeError:
                # Rule doesn't accept email/phone parameters (older flags)
                try:
                    flag = rule.evaluate(customer_id, events_sorted, today, email=email)
                except TypeError:
                    flag = rule.evaluate(customer_id, events_sorted, today)

            if flag:
                flags.append(flag)

        return flags

    def evaluate_all_customers(
        self,
        df_events: pd.DataFrame,
        today: datetime = None
    ) -> pd.DataFrame:
        """
        Evaluate rules for all customers.

        Args:
            df_events: DataFrame of customer events
            today: Reference date (defaults to now)

        Returns:
            DataFrame of customer flags
        """
        if today is None:
            today = datetime.now()

        print("=" * 60)
        print("Evaluating Customer Flagging Rules")
        print("=" * 60)
        print(f"Evaluation date: {today.date()}")
        print(f"Active rules: {len(self.rules)}")
        for rule in self.rules:
            print(f"  - {rule.flag_type}: {rule.description}")

        # Load customer contact info for AB group assignment
        print("\n📧 Loading customer contact info for household grouping...")
        self.load_customer_contact_info()

        if df_events.empty:
            print("\n⚠️  No events to evaluate")
            return pd.DataFrame(columns=[
                'customer_id', 'flag_type', 'triggered_date',
                'flag_data', 'priority'
            ])

        # Convert event_date to datetime
        df_events['event_date'] = pd.to_datetime(df_events['event_date'])

        # Group events by customer
        print(f"\n📊 Processing {df_events['customer_id'].nunique()} customers...")

        all_flags = []
        customers_processed = 0
        customers_flagged = 0

        for customer_id, customer_events in df_events.groupby('customer_id'):
            customers_processed += 1

            # Convert to list of dicts
            events_list = customer_events.to_dict('records')

            # Evaluate rules
            flags = self.evaluate_customer(customer_id, events_list, today)

            if flags:
                all_flags.extend(flags)
                customers_flagged += 1

                # Log experiment entries for AB test flags
                for flag in flags:
                    flag_type = flag['flag_type']
                    flag_data = flag['flag_data'] if isinstance(flag['flag_data'], dict) else json.loads(flag['flag_data'])

                    # Check if this is an AB test flag (has experiment_id)
                    if 'experiment_id' in flag_data and 'ab_group' in flag_data:
                        experiment_id = flag_data['experiment_id']
                        ab_group = flag_data['ab_group']

                        # Log experiment entry
                        experiment_tracking.log_experiment_entry(
                            customer_id=customer_id,
                            experiment_id=experiment_id,
                            group=ab_group,
                            entry_flag=flag_type,
                            entry_date=flag['triggered_date'],
                            save_local=True
                        )

        # Build DataFrame
        if not all_flags:
            print(f"\n✅ Evaluated {customers_processed} customers")
            print("   No customers matched any rules")
            return pd.DataFrame(columns=[
                'customer_id', 'flag_type', 'triggered_date',
                'flag_data', 'priority'
            ])

        df_flags = pd.DataFrame(all_flags)

        # Convert flag_data dict to JSON string for storage
        df_flags['flag_data'] = df_flags['flag_data'].apply(json.dumps)

        # Convert dates
        df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date'])

        # Sort by priority and date
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        df_flags['priority_sort'] = df_flags['priority'].map(priority_order)
        df_flags = df_flags.sort_values(['priority_sort', 'triggered_date'])
        df_flags = df_flags.drop('priority_sort', axis=1)

        print(f"\n✅ Evaluated {customers_processed} customers")
        print(f"   {customers_flagged} customers flagged ({len(all_flags)} total flags)")

        # Print summary by flag type
        print(f"\n🚩 Flags by type:")
        for flag_type, count in df_flags['flag_type'].value_counts().items():
            priority = df_flags[df_flags['flag_type'] == flag_type]['priority'].iloc[0]
            print(f"  {flag_type:30} {count:4} customers ({priority} priority)")

        # Remove expired flags (older than 14 days)
        print(f"\n🗑️  Removing expired flags (older than 14 days)...")
        df_flags = self.remove_expired_flags(df_flags, today, days_until_expiration=14)

        return df_flags

    def remove_expired_flags(
        self,
        df_flags: pd.DataFrame,
        today: datetime,
        days_until_expiration: int = 14
    ) -> pd.DataFrame:
        """
        Remove flags that are older than the expiration period.

        Args:
            df_flags: DataFrame of flags
            today: Current date
            days_until_expiration: Number of days until a flag expires (default: 14)

        Returns:
            DataFrame with expired flags removed
        """
        if df_flags.empty:
            return df_flags

        initial_count = len(df_flags)

        # Calculate expiration date
        from datetime import timedelta
        expiration_date = today - timedelta(days=days_until_expiration)

        # Filter out expired flags
        df_flags = df_flags[df_flags['triggered_date'] >= expiration_date].copy()

        expired_count = initial_count - len(df_flags)
        if expired_count > 0:
            print(f"   Removed {expired_count} expired flags")
        else:
            print(f"   No expired flags found")

        return df_flags


def build_customer_flags(df_events: pd.DataFrame, today: datetime = None) -> pd.DataFrame:
    """
    Main function to build customer flags from event timeline.

    Args:
        df_events: DataFrame of customer events
        today: Reference date (defaults to now)

    Returns:
        DataFrame of customer flags
    """
    engine = CustomerFlagsEngine()
    return engine.evaluate_all_customers(df_events, today)


if __name__ == "__main__":
    # Test the flagging engine
    from data_pipeline import upload_data, config
    import pandas as pd

    print("Testing Customer Flagging Engine")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer events from S3
    print("\nLoading customer events from S3...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_events)
    df_events = uploader.convert_csv_to_df(csv_content)
    print(f"Loaded {len(df_events)} events for {df_events['customer_id'].nunique()} customers")

    # Build flags
    df_flags = build_customer_flags(df_events)

    # Show sample
    if not df_flags.empty:
        print("\n" + "=" * 60)
        print("Sample Flagged Customers")
        print("=" * 60)
        print(df_flags.head(10).to_string(index=False))

        # Save locally
        df_flags.to_csv('data/outputs/customer_flags.csv', index=False)
        print("\n✅ Saved to data/outputs/customer_flags.csv")

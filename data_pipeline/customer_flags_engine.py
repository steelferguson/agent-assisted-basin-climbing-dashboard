"""
Customer flagging engine.

Evaluates business rules against customer event timelines to identify
customers who need outreach or automated actions.
"""

import pandas as pd
import json
from datetime import datetime
from typing import List, Dict
from data_pipeline import customer_flags_config


class CustomerFlagsEngine:
    """Engine for evaluating customer flagging rules."""

    def __init__(self, rules: List = None):
        """
        Initialize the flagging engine.

        Args:
            rules: List of FlagRule objects. If None, uses all active rules from config.
        """
        self.rules = rules if rules is not None else customer_flags_config.get_active_rules()

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

        # Evaluate each rule
        flags = []
        for rule in self.rules:
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

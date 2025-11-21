"""
Customer event aggregation system.

Converts all data sources (transactions, check-ins, emails, etc.) into a unified
customer event timeline for business logic and customer lifecycle automation.
"""

import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Optional


class CustomerEventsBuilder:
    """
    Aggregates events from multiple data sources into a unified customer timeline.
    """

    def __init__(self, customers_master: pd.DataFrame, customer_identifiers: pd.DataFrame):
        """
        Initialize with customer master and identifier data.

        Args:
            customers_master: DataFrame with deduplicated customer records
            customer_identifiers: DataFrame with all customer identifiers and confidence levels
        """
        self.customers_master = customers_master
        self.customer_identifiers = customer_identifiers
        self.events = []

        # Build email -> customer_id lookup for fast matching
        self.email_to_customer = {}
        for _, row in customer_identifiers[customer_identifiers['identifier_type'] == 'email'].iterrows():
            email = row['normalized_value']
            customer_id = row['customer_id']
            confidence = row['match_confidence']
            if email:
                self.email_to_customer[email] = {
                    'customer_id': customer_id,
                    'confidence': confidence
                }

    def _lookup_customer(self, email: Optional[str]) -> Optional[Dict]:
        """
        Look up customer_id and confidence from email.

        Returns:
            {'customer_id': str, 'confidence': str} or None
        """
        if not email or pd.isna(email):
            return None

        # Normalize email (lowercase, strip)
        email = str(email).lower().strip()

        return self.email_to_customer.get(email)

    def add_transaction_events(self, df_transactions: pd.DataFrame):
        """
        Add events from Stripe/Square transaction data.

        Event types:
        - day_pass_purchase
        - membership_purchase
        - membership_renewal
        - retail_purchase
        - programming_purchase
        """
        print(f"\n💳 Processing transaction events ({len(df_transactions)} records)...")

        if df_transactions.empty:
            print("⚠️  No transaction data")
            return

        # Build name -> customer lookup from customers_master
        name_to_customer = {}
        for _, row in self.customers_master.iterrows():
            name = row.get('primary_name')
            if name and not pd.isna(name):
                # Normalize name (lowercase, strip)
                normalized = str(name).lower().strip()
                if normalized and normalized != 'no name':
                    name_to_customer[normalized] = row.get('customer_id')

        events_added = 0
        matched = 0
        unmatched = 0

        for _, row in df_transactions.iterrows():
            # Get event details
            date = row.get('Date')
            category = row.get('revenue_category', '')
            amount = row.get('Total Amount', 0)
            description = row.get('Description', '')
            source = row.get('Data Source', '').lower()
            customer_name = row.get('Name', '')
            transaction_id = row.get('transaction_id', '')

            # Determine event type based on revenue category
            event_type = None
            if category == 'Day Pass':
                event_type = 'day_pass_purchase'
            elif category == 'New Membership':
                event_type = 'membership_purchase'
            elif category == 'Membership Renewal':
                event_type = 'membership_renewal'
            elif category == 'Retail':
                event_type = 'retail_purchase'
            elif category == 'Programming':
                event_type = 'programming_purchase'
            elif category == 'Event Booking':
                event_type = 'event_booking'

            if not event_type:
                continue

            # Try to match customer by name
            customer_id = None
            confidence = 'unmatched'

            if customer_name and not pd.isna(customer_name):
                normalized_name = str(customer_name).lower().strip()
                if normalized_name in name_to_customer:
                    customer_id = name_to_customer[normalized_name]
                    confidence = 'medium'  # Name match is medium confidence
                    matched += 1

            if not customer_id:
                # Skip events we can't match to customers
                unmatched += 1
                continue

            self.events.append({
                'customer_id': customer_id,
                'event_date': date,
                'event_type': event_type,
                'event_source': source,
                'source_confidence': confidence,
                'event_details': json.dumps({
                    'transaction_id': transaction_id,
                    'amount': float(amount) if amount else 0,
                    'description': description,
                    'category': category,
                    'customer_name': customer_name
                })
            })
            events_added += 1

        print(f"✅ Added {events_added} transaction events ({matched} matched, {unmatched} unmatched)")

    def add_checkin_events(self, df_checkins: pd.DataFrame):
        """
        Add check-in events from Capitan.

        Event type: checkin
        """
        print(f"\n🚪 Processing check-in events ({len(df_checkins)} records)...")

        if df_checkins.empty:
            print("⚠️  No check-in data")
            return

        # Check-ins have customer_id directly from Capitan
        # Need to map Capitan customer_id to our unified customer_id

        events_added = 0
        for _, row in df_checkins.iterrows():
            capitan_customer_id = row.get('customer_id')
            checkin_date = row.get('created_at')

            # Look up unified customer_id from Capitan customer_id
            # Match via customer_identifiers where source='capitan' and source_id contains customer_id
            customer_match = self.customer_identifiers[
                (self.customer_identifiers['source'] == 'capitan') &
                (self.customer_identifiers['source_id'].str.contains(str(capitan_customer_id), na=False))
            ]

            if customer_match.empty:
                continue

            customer_id = customer_match.iloc[0]['customer_id']
            confidence = customer_match.iloc[0]['match_confidence']

            self.events.append({
                'customer_id': customer_id,
                'event_date': checkin_date,
                'event_type': 'checkin',
                'event_source': 'capitan',
                'source_confidence': confidence,
                'event_details': json.dumps({
                    'checkin_id': row.get('id'),
                    'association': row.get('association_name', '')
                })
            })
            events_added += 1

        print(f"✅ Added {events_added} check-in events")

    def add_mailchimp_events(self, df_mailchimp: pd.DataFrame):
        """
        Add Mailchimp campaign events.

        Event types:
        - email_sent
        - email_opened
        - email_clicked
        """
        print(f"\n📧 Processing Mailchimp events ({len(df_mailchimp)} records)...")

        if df_mailchimp.empty:
            print("⚠️  No Mailchimp data")
            return

        # Mailchimp campaigns don't have individual recipient data in our current export
        # TODO: Fetch Mailchimp campaign recipient data with open/click tracking
        print("⚠️  Mailchimp recipient-level data not yet available")
        print("   Need to fetch campaign recipients with email addresses")

    def build_events_dataframe(self) -> pd.DataFrame:
        """
        Build final customer events DataFrame.

        Returns:
            DataFrame with columns: customer_id, event_date, event_type,
            event_source, source_confidence, event_details
        """
        if not self.events:
            return pd.DataFrame(columns=[
                'customer_id', 'event_date', 'event_type',
                'event_source', 'source_confidence', 'event_details'
            ])

        df = pd.DataFrame(self.events)

        # Convert dates
        df['event_date'] = pd.to_datetime(df['event_date'], errors='coerce')

        # Sort by customer and date
        df = df.sort_values(['customer_id', 'event_date'])

        return df

    def print_summary(self, df_events: pd.DataFrame):
        """Print summary of events built."""
        print("\n" + "=" * 60)
        print("Customer Events Summary")
        print("=" * 60)

        if df_events.empty:
            print("⚠️  No events found")
            return

        print(f"Total events: {len(df_events)}")
        print(f"Unique customers with events: {df_events['customer_id'].nunique()}")
        print(f"Date range: {df_events['event_date'].min()} to {df_events['event_date'].max()}")

        print(f"\n📊 Events by type:")
        for event_type, count in df_events['event_type'].value_counts().items():
            print(f"  {event_type:25} {count:6} events")

        print(f"\n📍 Events by source:")
        for source, count in df_events['event_source'].value_counts().items():
            print(f"  {source:15} {count:6} events")

        print(f"\n🎯 Events by confidence:")
        for conf, count in df_events['source_confidence'].value_counts().items():
            pct = (count / len(df_events)) * 100
            print(f"  {conf:10} {count:6} events ({pct:.1f}%)")


def build_customer_events(
    customers_master: pd.DataFrame,
    customer_identifiers: pd.DataFrame,
    df_transactions: pd.DataFrame = None,
    df_checkins: pd.DataFrame = None,
    df_mailchimp: pd.DataFrame = None
) -> pd.DataFrame:
    """
    Main function to build customer events from all data sources.

    Args:
        customers_master: Deduplicated customer records
        customer_identifiers: Customer identifiers with confidence
        df_transactions: Stripe/Square transaction data
        df_checkins: Capitan check-in data
        df_mailchimp: Mailchimp campaign data

    Returns:
        DataFrame of customer events
    """
    print("=" * 60)
    print("Building Customer Event Timeline")
    print("=" * 60)

    builder = CustomerEventsBuilder(customers_master, customer_identifiers)

    # Add events from each source
    if df_transactions is not None and not df_transactions.empty:
        builder.add_transaction_events(df_transactions)

    if df_checkins is not None and not df_checkins.empty:
        builder.add_checkin_events(df_checkins)

    if df_mailchimp is not None and not df_mailchimp.empty:
        builder.add_mailchimp_events(df_mailchimp)

    # Build final DataFrame
    df_events = builder.build_events_dataframe()
    builder.print_summary(df_events)

    return df_events


if __name__ == "__main__":
    # Test the event builder
    from data_pipeline import upload_data, config
    import pandas as pd

    print("Testing Customer Events Builder")
    print("=" * 60)

    uploader = upload_data.DataUploader()

    # Load customer master and identifiers
    print("\nLoading customer data from S3...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master)
    df_master = uploader.convert_csv_to_df(csv_content)

    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_identifiers)
    df_identifiers = uploader.convert_csv_to_df(csv_content)

    print(f"Loaded {len(df_master)} customers, {len(df_identifiers)} identifiers")

    # Load check-ins
    print("\nLoading check-in data from S3...")
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_checkins = uploader.convert_csv_to_df(csv_content)
    print(f"Loaded {len(df_checkins)} check-ins")

    # Build events
    df_events = build_customer_events(
        df_master,
        df_identifiers,
        df_transactions=pd.DataFrame(),  # Skip for now
        df_checkins=df_checkins,
        df_mailchimp=pd.DataFrame()  # Skip for now
    )

    # Show sample
    if not df_events.empty:
        print("\n" + "=" * 60)
        print("Sample Events")
        print("=" * 60)
        print(df_events.head(20).to_string(index=False))

        # Save
        df_events.to_csv('data/outputs/customer_events.csv', index=False)
        print("\n✅ Saved to data/outputs/customer_events.csv")

"""
Customer Flag Engine

Evaluates customer flag rules and sets/clears flags based on business logic criteria.
Runs daily to identify customers who meet specific conditions.

Process:
1. Load customer flag rules (from customer_flag_rules.py)
2. For each enabled rule, evaluate criteria against all customers
3. Set flags for customers who meet criteria
4. Clear flags for customers who no longer meet criteria
5. Execute actions (log events, set Shopify metafields, etc.)

Storage:
- S3: customers/customer_flags.csv (current flag status)
- S3: customers/customer_flag_history.csv (historical flag changes)
"""

import os
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta
import json
from typing import Dict, List, Set

try:
    from data_pipeline import customer_flag_rules
except ImportError:
    import customer_flag_rules


class CustomerFlagEngine:
    """
    Evaluate flag rules and manage customer flags.
    """

    def __init__(self):
        # AWS
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"

        # S3 keys
        self.flags_key = "customers/customer_flags.csv"
        self.history_key = "customers/customer_flag_history.csv"

        # S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        print("✅ Customer Flag Engine initialized")

    def load_data_sources(self) -> Dict[str, pd.DataFrame]:
        """
        Load all data sources needed for rule evaluation.

        Returns:
            Dict with DataFrames: messages, customers, checkins, memberships, events, current_flags
        """
        print("\n📂 Loading data sources...")

        data = {}

        # Twilio messages
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="twilio/messages.csv")
            data['messages'] = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   ✅ Messages: {len(data['messages'])}")
        except Exception as e:
            print(f"   ⚠️  Messages: Error - {e}")
            data['messages'] = pd.DataFrame()

        # Capitan customers
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="capitan/customers.csv")
            data['customers'] = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   ✅ Customers: {len(data['customers'])}")
        except Exception as e:
            print(f"   ⚠️  Customers: Error - {e}")
            data['customers'] = pd.DataFrame()

        # Capitan check-ins
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="capitan/checkins.csv")
            data['checkins'] = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   ✅ Check-ins: {len(data['checkins'])}")
        except Exception as e:
            print(f"   ⚠️  Check-ins: Error - {e}")
            data['checkins'] = pd.DataFrame()

        # Capitan memberships
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="capitan/memberships.csv")
            data['memberships'] = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   ✅ Memberships: {len(data['memberships'])}")
        except Exception as e:
            print(f"   ⚠️  Memberships: Error - {e}")
            data['memberships'] = pd.DataFrame()

        # Customer events
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key="customers/customer_events.csv")
            data['events'] = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   ✅ Events: {len(data['events'])}")
        except Exception as e:
            print(f"   ⚠️  Events: Error - {e}")
            data['events'] = pd.DataFrame()

        # Current flags
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.flags_key)
            data['current_flags'] = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"   ✅ Current flags: {len(data['current_flags'])}")
        except:
            print(f"   ℹ️  Current flags: None (first run)")
            data['current_flags'] = pd.DataFrame(columns=['customer_id', 'flag_name', 'flagged_at', 'criteria_met'])

        return data

    def evaluate_second_visit_offer_rule(self, rule: Dict, data: Dict[str, pd.DataFrame]) -> Set[str]:
        """
        Evaluate the second-visit offer rule.

        Args:
            rule: Rule configuration dict
            data: Dict of data sources

        Returns:
            Set of customer_ids who meet criteria
        """
        print(f"\n🔍 Evaluating rule: {rule['flag_name']}")
        print(f"   Description: {rule['description']}")

        criteria = rule['criteria']
        customers = data['customers'].copy()
        messages = data['messages'].copy()
        checkins = data['checkins'].copy()
        memberships = data['memberships'].copy()
        events = data['events'].copy()

        # Start with all customers
        eligible_ids = set(customers['customer_id'].unique())
        print(f"   Starting with {len(eligible_ids)} total customers")

        # Criterion 1: Texted keyword "WAIVER"
        if 'texted_keyword' in criteria:
            keyword = criteria['texted_keyword']
            print(f"\n   ✓ Checking: Texted '{keyword}'...")

            # Find inbound messages with keyword
            keyword_texts = messages[
                (messages['direction'] == 'inbound') &
                (messages['body'].str.contains(keyword, case=False, na=False))
            ].copy()

            # Normalize phone numbers and match to customers
            keyword_texts['phone_normalized'] = keyword_texts['from_number'].apply(self._normalize_phone)
            customers['phone_normalized'] = customers['phone'].astype(str).apply(self._normalize_phone)

            matched = customers.merge(
                keyword_texts[['phone_normalized']].drop_duplicates(),
                on='phone_normalized',
                how='inner'
            )

            keyword_customer_ids = set(matched['customer_id'])
            print(f"     Found {len(keyword_customer_ids)} customers who texted '{keyword}'")

            eligible_ids = eligible_ids.intersection(keyword_customer_ids)
            print(f"     {len(eligible_ids)} customers remaining")

        # Criterion 2: Has day pass check-in
        if criteria.get('has_day_pass_checkin'):
            print(f"\n   ✓ Checking: Has day pass check-in...")

            day_pass_checkins = checkins[
                checkins['entry_method_description'].str.contains(
                    'Day Pass|Entry Pass',
                    case=False,
                    na=False
                )
            ]

            day_pass_customer_ids = set(day_pass_checkins['customer_id'].unique())
            print(f"     Found {len(day_pass_customer_ids)} customers with day pass check-ins")

            eligible_ids = eligible_ids.intersection(day_pass_customer_ids)
            print(f"     {len(eligible_ids)} customers remaining")

        # Criterion 3: Not an active member
        if criteria.get('is_not_active_member'):
            print(f"\n   ✓ Checking: Not an active member...")

            if len(memberships) > 0:
                active_members = memberships[
                    memberships['status'].isin(['ACT', 'ACTIVE'])
                ]['owner_id'].unique()  # Note: memberships use 'owner_id' not 'customer_id'

                # Convert to set
                active_member_ids = set(active_members)
                print(f"     Found {len(active_member_ids)} active members")

                # Remove active members from eligible
                before = len(eligible_ids)
                eligible_ids = eligible_ids - active_member_ids
                removed = before - len(eligible_ids)
                print(f"     Excluded {removed} active members")
                print(f"     {len(eligible_ids)} customers remaining")
            else:
                print(f"     No membership data available")

        # Criterion 4: No recent flag (within N days)
        if 'no_recent_flag_days' in criteria:
            days = criteria['no_recent_flag_days']
            print(f"\n   ✓ Checking: No flag in last {days} days...")

            cutoff_date = datetime.now() - timedelta(days=days)

            # Check flag history from events
            if len(events) > 0:
                events['event_date'] = pd.to_datetime(events['event_date'], errors='coerce')

                recent_flags = events[
                    (events['event_type'].str.contains('flag_set_second_visit_offer', na=False)) &
                    (events['event_date'] >= cutoff_date)
                ]['customer_id'].unique()

                recent_flag_ids = set(recent_flags)
                print(f"     Found {len(recent_flag_ids)} customers flagged in last {days} days")

                before = len(eligible_ids)
                eligible_ids = eligible_ids - recent_flag_ids
                removed = before - len(eligible_ids)
                print(f"     Excluded {removed} recently flagged customers")
                print(f"     {len(eligible_ids)} customers remaining")
            else:
                print(f"     No event history available")

        print(f"\n   ✅ Final result: {len(eligible_ids)} customers meet ALL criteria")

        return eligible_ids

    def evaluate_all_rules(self, data: Dict[str, pd.DataFrame]) -> Dict[str, Set[str]]:
        """
        Evaluate all enabled rules.

        Args:
            data: Dict of data sources

        Returns:
            Dict mapping flag_name -> set of customer_ids who should have the flag
        """
        print("\n" + "="*80)
        print("EVALUATING FLAG RULES")
        print("="*80)

        rules = customer_flag_rules.get_enabled_rules()
        print(f"\nFound {len(rules)} enabled rules")

        results = {}

        for rule in rules:
            flag_name = rule['flag_name']

            # Evaluate based on flag type
            if flag_name == 'second_visit_offer_eligible':
                eligible_ids = self.evaluate_second_visit_offer_rule(rule, data)
                results[flag_name] = eligible_ids
            else:
                print(f"\n⚠️  Unknown rule type: {flag_name} - skipping")

        return results

    def update_flags(self, evaluation_results: Dict[str, Set[str]], data: Dict[str, pd.DataFrame]):
        """
        Update customer flags based on evaluation results.

        Args:
            evaluation_results: Dict mapping flag_name -> set of customer_ids
            data: Dict of data sources
        """
        print("\n" + "="*80)
        print("UPDATING FLAGS")
        print("="*80)

        current_flags = data['current_flags']
        customers = data['customers']

        flag_changes = []

        for flag_name, should_have_flag_ids in evaluation_results.items():
            print(f"\n🏁 Processing flag: {flag_name}")

            # Get current state (handle empty DataFrame)
            if len(current_flags) > 0 and 'flag_name' in current_flags.columns:
                currently_flagged = set(
                    current_flags[current_flags['flag_name'] == flag_name]['customer_id']
                )
            else:
                currently_flagged = set()

            # Calculate changes
            to_set = should_have_flag_ids - currently_flagged  # New flags
            to_clear = currently_flagged - should_have_flag_ids  # Remove flags

            print(f"   Currently flagged: {len(currently_flagged)}")
            print(f"   Should be flagged: {len(should_have_flag_ids)}")
            print(f"   To SET: {len(to_set)}")
            print(f"   To CLEAR: {len(to_clear)}")

            # Record changes
            timestamp = datetime.now().isoformat()

            for customer_id in to_set:
                flag_changes.append({
                    'customer_id': customer_id,
                    'flag_name': flag_name,
                    'action': 'set',
                    'timestamp': timestamp
                })

            for customer_id in to_clear:
                flag_changes.append({
                    'customer_id': customer_id,
                    'flag_name': flag_name,
                    'action': 'clear',
                    'timestamp': timestamp
                })

        # Apply changes and save
        if len(flag_changes) > 0:
            self._apply_flag_changes(flag_changes, current_flags, customers)
        else:
            print("\n   No flag changes needed")

    def _apply_flag_changes(self, changes: List[Dict], current_flags: pd.DataFrame, customers: pd.DataFrame):
        """
        Apply flag changes and save to S3.

        Args:
            changes: List of flag change dicts
            current_flags: Current flags DataFrame
            customers: Customers DataFrame
        """
        print(f"\n💾 Applying {len(changes)} flag changes...")

        # Update current flags
        updated_flags = current_flags.copy()

        for change in changes:
            customer_id = change['customer_id']
            flag_name = change['flag_name']
            action = change['action']

            if action == 'set':
                # Add or update flag
                mask = (updated_flags['customer_id'] == customer_id) & (updated_flags['flag_name'] == flag_name)
                if mask.any():
                    updated_flags.loc[mask, 'flagged_at'] = change['timestamp']
                else:
                    new_row = pd.DataFrame([{
                        'customer_id': customer_id,
                        'flag_name': flag_name,
                        'flagged_at': change['timestamp'],
                        'criteria_met': True
                    }])
                    updated_flags = pd.concat([updated_flags, new_row], ignore_index=True)

            elif action == 'clear':
                # Remove flag
                updated_flags = updated_flags[
                    ~((updated_flags['customer_id'] == customer_id) & (updated_flags['flag_name'] == flag_name))
                ]

        # Save current flags to S3
        csv_buffer = StringIO()
        updated_flags.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.flags_key,
            Body=csv_buffer.getvalue()
        )
        print(f"   ✅ Saved updated flags to S3: {self.flags_key}")

        # Save changes to history
        changes_df = pd.DataFrame(changes)
        self._append_to_history(changes_df)

        # Show summary of changes by flag
        for flag_name in changes_df['flag_name'].unique():
            flag_changes = changes_df[changes_df['flag_name'] == flag_name]
            set_count = len(flag_changes[flag_changes['action'] == 'set'])
            clear_count = len(flag_changes[flag_changes['action'] == 'clear'])
            print(f"\n   {flag_name}:")
            print(f"     SET: {set_count} customers")
            print(f"     CLEARED: {clear_count} customers")

            # Show sample of SET customers
            if set_count > 0:
                set_customers = flag_changes[flag_changes['action'] == 'set']['customer_id'].head(5)
                for cust_id in set_customers:
                    cust = customers[customers['customer_id'] == cust_id].iloc[0]
                    print(f"       ✅ {cust['first_name']} {cust['last_name']} ({cust['email']})")
                if set_count > 5:
                    print(f"       ... and {set_count - 5} more")

    def _append_to_history(self, changes: pd.DataFrame):
        """Append flag changes to history."""
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.history_key)
            existing_history = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            all_history = pd.concat([existing_history, changes], ignore_index=True)
        except:
            all_history = changes

        csv_buffer = StringIO()
        all_history.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=self.history_key,
            Body=csv_buffer.getvalue()
        )
        print(f"   ✅ Saved flag history to S3: {self.history_key}")

    def run(self):
        """Run the customer flag engine."""
        print("\n" + "="*80)
        print("CUSTOMER FLAG ENGINE")
        print("="*80)

        # 1. Load data
        data = self.load_data_sources()

        # 2. Evaluate rules
        results = self.evaluate_all_rules(data)

        # 3. Update flags
        self.update_flags(results, data)

        print("\n" + "="*80)
        print("✅ FLAG ENGINE COMPLETE")
        print("="*80)


    def _normalize_phone(self, phone_number):
        """
        Normalize phone number to just digits (last 10 digits).

        Capitan stores phones as digits-only (e.g. "2817998018")
        Twilio stores with +1 prefix (e.g. "+12817998018" or saved as int 12817998018)

        We'll normalize to last 10 digits for matching.
        """
        if pd.isna(phone_number) or not phone_number:
            return None

        # Remove all non-digit characters
        digits = ''.join(filter(str.isdigit, str(phone_number)))

        # Return last 10 digits (removes country code if present)
        if len(digits) >= 10:
            return digits[-10:]

        return digits if digits else None


def main():
    """Run the flag engine."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    engine = CustomerFlagEngine()
    engine.run()


if __name__ == "__main__":
    main()

"""
Sync Customer Flags to Mailchimp Audiences

Adds/updates flagged customers in Mailchimp with appropriate tags for the AB test.

Usage:
    python sync_flags_to_mailchimp.py

Environment Variables:
    MAILCHIMP_API_KEY: Your Mailchimp API key
    MAILCHIMP_SERVER_PREFIX: Your Mailchimp server prefix (e.g., 'us9')
    AWS_ACCESS_KEY_ID: AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
"""

import os
import pandas as pd
import boto3
import requests
import time
from io import StringIO
from typing import Dict, List, Optional
import hashlib


class MailchimpFlagSyncer:
    """
    Sync customer flags from S3 to Mailchimp audience.
    """

    def __init__(self, audience_id: str = None):
        # Mailchimp credentials
        self.api_key = os.getenv("MAILCHIMP_API_KEY")
        self.server_prefix = os.getenv("MAILCHIMP_SERVER_PREFIX")

        if not self.api_key or not self.server_prefix:
            raise ValueError("MAILCHIMP_API_KEY and MAILCHIMP_SERVER_PREFIX must be set")

        # Default audience - you'll need to set this to your actual audience ID
        # You can find it in Mailchimp under Audience > Settings > Audience name and defaults
        self.audience_id = audience_id or os.getenv("MAILCHIMP_AUDIENCE_ID")
        if not self.audience_id:
            raise ValueError("MAILCHIMP_AUDIENCE_ID must be set or passed to constructor")

        self.base_url = f"https://{self.server_prefix}.api.mailchimp.com/3.0"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        # AWS credentials
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"

        # S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )

        # Rate limiting: Mailchimp allows 10 requests/second
        self.min_delay_between_calls = 0.11  # 110ms = ~9 calls/sec (safe margin)
        self.last_api_call_time = 0

        print("✅ Mailchimp Flag Syncer initialized")
        print(f"   Server: {self.server_prefix}")
        print(f"   Audience ID: {self.audience_id}")

    def _rate_limit(self):
        """
        Enforce rate limiting to stay under Mailchimp's 10 calls/second limit.
        """
        current_time = time.time()
        time_since_last_call = current_time - self.last_api_call_time

        if time_since_last_call < self.min_delay_between_calls:
            sleep_time = self.min_delay_between_calls - time_since_last_call
            time.sleep(sleep_time)

        self.last_api_call_time = time.time()

    def _get_subscriber_hash(self, email: str) -> str:
        """
        Get MD5 hash of lowercase email for Mailchimp API.

        Args:
            email: Email address

        Returns:
            MD5 hash of lowercase email
        """
        return hashlib.md5(email.lower().encode()).hexdigest()

    def load_flags_from_s3(self) -> pd.DataFrame:
        """
        Load customer flags from S3.

        Returns:
            DataFrame of customer flags
        """
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key='customers/customer_flags.csv')
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df

    def load_customers_from_s3(self) -> pd.DataFrame:
        """
        Load customer data from S3.

        Returns:
            DataFrame of customers
        """
        obj = self.s3_client.get_object(Bucket=self.bucket_name, Key='capitan/customers.csv')
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df

    def check_subscriber_exists(self, email: str) -> bool:
        """
        Check if a subscriber exists in the audience.

        Args:
            email: Email address to check

        Returns:
            True if subscriber exists, False otherwise
        """
        subscriber_hash = self._get_subscriber_hash(email)
        url = f"{self.base_url}/lists/{self.audience_id}/members/{subscriber_hash}"

        self._rate_limit()
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def add_or_update_subscriber(
        self,
        email: str,
        first_name: str = "",
        last_name: str = "",
        tags: List[str] = None,
        merge_fields: Dict[str, str] = None
    ) -> bool:
        """
        Add or update a subscriber in the Mailchimp audience.

        Args:
            email: Subscriber email
            first_name: First name
            last_name: Last name
            tags: List of tags to apply
            merge_fields: Additional merge fields

        Returns:
            True if successful, False otherwise
        """
        subscriber_hash = self._get_subscriber_hash(email)
        url = f"{self.base_url}/lists/{self.audience_id}/members/{subscriber_hash}"

        # Build subscriber data
        data = {
            "email_address": email,
            "status_if_new": "subscribed",  # Only applies if new
            "merge_fields": {
                "FNAME": first_name if first_name and pd.notna(first_name) else "",
                "LNAME": last_name if last_name and pd.notna(last_name) else ""
            }
        }

        # Add custom merge fields
        if merge_fields:
            data["merge_fields"].update(merge_fields)

        # Add tags if provided
        if tags:
            data["tags"] = [{"name": tag, "status": "active"} for tag in tags]

        self._rate_limit()
        try:
            # Use PUT to add or update (upsert)
            response = requests.put(url, headers=self.headers, json=data, timeout=10)
            return response.status_code in [200, 201]
        except Exception as e:
            print(f"   ⚠️  Error adding/updating subscriber {email}: {e}")
            return False

    def sync_flags_to_mailchimp(self, dry_run: bool = False):
        """
        Sync customer flags to Mailchimp audience.

        Args:
            dry_run: If True, only print what would be done without making changes
        """
        print("\n" + "="*80)
        print("SYNC CUSTOMER FLAGS TO MAILCHIMP")
        print("="*80)

        # Load flags and customer data
        flags_df = self.load_flags_from_s3()
        print(f"✅ Loaded {len(flags_df)} flags from S3")

        customers_df = self.load_customers_from_s3()
        print(f"✅ Loaded {len(customers_df)} customers from S3")

        # Filter to only AB test flags (not tracking-only flags)
        ab_test_flags = [
            'first_time_day_pass_2wk_offer',
            'second_visit_offer_eligible',
            'second_visit_2wk_offer'
        ]
        flags_to_sync = flags_df[flags_df['flag_type'].isin(ab_test_flags)]
        print(f"📊 Found {len(flags_to_sync)} AB test flags to sync")

        if len(flags_to_sync) == 0:
            print("   No flags to sync")
            return

        # Merge with customer data to get email addresses
        flags_with_customers = flags_to_sync.merge(
            customers_df[['customer_id', 'email', 'first_name', 'last_name']],
            on='customer_id',
            how='left'
        )

        # Filter to only customers with email addresses
        flags_with_email = flags_with_customers[flags_with_customers['email'].notna()]
        print(f"📧 {len(flags_with_email)} flags have email addresses")

        # Sync each flagged customer to Mailchimp
        added = 0
        updated = 0
        skipped = 0
        errors = 0

        for _, flag in flags_with_email.iterrows():
            email = flag['email']
            first_name = flag.get('first_name', '')
            last_name = flag.get('last_name', '')
            flag_type = flag['flag_type']
            customer_id = flag['customer_id']

            # Convert flag type to Mailchimp tag (replace underscores with hyphens)
            tag = flag_type.replace('_', '-')

            print(f"\n   Syncing {customer_id} ({first_name} {last_name}) - {email}")
            print(f"      Tag: {tag}")

            if dry_run:
                print(f"      [DRY RUN] Would add/update subscriber with tag '{tag}'")
                added += 1
                continue

            # Check if subscriber exists
            exists = self.check_subscriber_exists(email)

            # Add or update subscriber with tag
            success = self.add_or_update_subscriber(
                email=email,
                first_name=first_name,
                last_name=last_name,
                tags=[tag],
                merge_fields={"CAPTID": str(customer_id)}  # Store Capitan ID
            )

            if success:
                if exists:
                    updated += 1
                    print(f"      ✅ Updated subscriber and added tag '{tag}'")
                else:
                    added += 1
                    print(f"      ✅ Added new subscriber with tag '{tag}'")
            else:
                errors += 1
                print(f"      ❌ Failed to sync subscriber")

        print(f"\n{'='*80}")
        print("SYNC SUMMARY")
        print(f"{'='*80}")
        print(f"   New subscribers added: {added}")
        print(f"   Existing subscribers updated: {updated}")
        print(f"   Skipped (no email): {len(flags_to_sync) - len(flags_with_email)}")
        print(f"   Errors: {errors}")
        print(f"{'='*80}\n")


def main():
    """Run the sync."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    # You need to set MAILCHIMP_AUDIENCE_ID in your .env file
    # Or pass it directly here
    syncer = MailchimpFlagSyncer()

    # Run sync (set dry_run=True to test without making changes)
    syncer.sync_flags_to_mailchimp(dry_run=False)


if __name__ == "__main__":
    main()

"""
Sync Customer Flags to Shopify Metafields

Reads customer flags from S3 and syncs them to Shopify customer metafields.
Shopify Flows can then trigger on metafield changes to send offers, create discounts, etc.

Usage:
    python sync_flags_to_shopify.py

Environment Variables:
    SHOPIFY_STORE_DOMAIN: Your Shopify store domain (e.g., basin-climbing.myshopify.com)
    SHOPIFY_ADMIN_TOKEN: Shopify Admin API access token
    AWS_ACCESS_KEY_ID: AWS access key
    AWS_SECRET_ACCESS_KEY: AWS secret key
"""

import os
import pandas as pd
import boto3
import requests
from io import StringIO
from typing import Dict, List, Optional
from datetime import datetime


class ShopifyFlagSyncer:
    """
    Sync customer flags from S3 to Shopify metafields.
    """

    def __init__(self):
        # Shopify credentials
        self.store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
        self.admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")

        if not self.store_domain or not self.admin_token:
            raise ValueError("SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_TOKEN must be set")

        self.base_url = f"https://{self.store_domain}/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": self.admin_token,
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

        print("✅ Shopify Flag Syncer initialized")
        print(f"   Store: {self.store_domain}")

    def load_flags_from_s3(self) -> pd.DataFrame:
        """
        Load customer flags from S3.

        Returns:
            DataFrame with columns: customer_id, flag_name, flagged_at, criteria_met
        """
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="customers/customer_flags.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(df)} flags from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No flags found in S3")
            return pd.DataFrame(columns=['customer_id', 'flag_name', 'flagged_at', 'criteria_met'])

    def load_customers_from_s3(self) -> pd.DataFrame:
        """
        Load customer data from S3 to get email/phone for matching.

        Returns:
            DataFrame with customer_id, email, phone
        """
        try:
            obj = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key="capitan/customers.csv"
            )
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
            print(f"✅ Loaded {len(df)} customers from S3")
            return df[['customer_id', 'email', 'phone', 'first_name', 'last_name']]
        except Exception as e:
            print(f"⚠️  Error loading customers: {e}")
            return pd.DataFrame()

    def search_shopify_customer(self, email: Optional[str] = None, phone: Optional[str] = None) -> Optional[str]:
        """
        Search for a customer in Shopify by email or phone.

        Args:
            email: Customer email
            phone: Customer phone

        Returns:
            Shopify customer ID (as string) or None if not found
        """
        # Try email first (more reliable)
        if email and pd.notna(email):
            url = f"{self.base_url}/customers/search.json?query=email:{email}"
            try:
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    customers = response.json().get('customers', [])
                    if len(customers) > 0:
                        return str(customers[0]['id'])
            except Exception as e:
                print(f"   Error searching by email: {e}")

        # Try phone if email didn't work
        if phone and pd.notna(phone):
            # Normalize phone for search
            phone_digits = ''.join(filter(str.isdigit, str(phone)))
            if len(phone_digits) >= 10:
                phone_normalized = phone_digits[-10:]  # Last 10 digits
                url = f"{self.base_url}/customers/search.json?query=phone:+1{phone_normalized}"
                try:
                    response = requests.get(url, headers=self.headers, timeout=10)
                    if response.status_code == 200:
                        customers = response.json().get('customers', [])
                        if len(customers) > 0:
                            return str(customers[0]['id'])
                except Exception as e:
                    print(f"   Error searching by phone: {e}")

        return None

    def set_customer_metafield(self, shopify_customer_id: str, namespace: str, key: str,
                              value: str, value_type: str = "boolean") -> bool:
        """
        Set a metafield on a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            namespace: Metafield namespace (e.g., "custom")
            key: Metafield key (e.g., "second_visit_offer_eligible")
            value: Metafield value (e.g., "true")
            value_type: Metafield type (e.g., "boolean", "string")

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}/metafields.json"

        payload = {
            "metafield": {
                "namespace": namespace,
                "key": key,
                "value": value,
                "type": value_type
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code in [200, 201]:
                return True
            else:
                print(f"   ⚠️  Failed to set metafield: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error setting metafield: {e}")
            return False

    def delete_customer_metafield(self, shopify_customer_id: str, metafield_id: str) -> bool:
        """
        Delete a metafield from a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            metafield_id: Metafield ID to delete

        Returns:
            True if successful, False otherwise
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}/metafields/{metafield_id}.json"

        try:
            response = requests.delete(url, headers=self.headers, timeout=10)
            return response.status_code == 200
        except Exception as e:
            print(f"   ⚠️  Error deleting metafield: {e}")
            return False

    def get_customer_metafield(self, shopify_customer_id: str, namespace: str, key: str) -> Optional[Dict]:
        """
        Get a specific metafield from a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            namespace: Metafield namespace
            key: Metafield key

        Returns:
            Metafield dict or None if not found
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}/metafields.json"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                metafields = response.json().get('metafields', [])
                for mf in metafields:
                    if mf['namespace'] == namespace and mf['key'] == key:
                        return mf
        except Exception as e:
            print(f"   ⚠️  Error getting metafield: {e}")

        return None

    def get_customer_tags(self, shopify_customer_id: str) -> List[str]:
        """
        Get all tags for a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID

        Returns:
            List of tag strings
        """
        url = f"{self.base_url}/customers/{shopify_customer_id}.json"

        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                customer = response.json().get('customer', {})
                tags_str = customer.get('tags', '')
                # Tags are comma-separated
                return [tag.strip() for tag in tags_str.split(',') if tag.strip()]
        except Exception as e:
            print(f"   ⚠️  Error getting customer tags: {e}")

        return []

    def add_customer_tag(self, shopify_customer_id: str, tag: str) -> bool:
        """
        Add a tag to a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            tag: Tag to add

        Returns:
            True if successful, False otherwise
        """
        # Get current tags
        current_tags = self.get_customer_tags(shopify_customer_id)

        # Check if tag already exists
        if tag in current_tags:
            return True  # Already has tag, no need to add

        # Add new tag
        current_tags.append(tag)
        tags_str = ', '.join(current_tags)

        url = f"{self.base_url}/customers/{shopify_customer_id}.json"

        payload = {
            "customer": {
                "id": int(shopify_customer_id),
                "tags": tags_str
            }
        }

        try:
            response = requests.put(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code == 200:
                return True
            else:
                print(f"   ⚠️  Failed to add tag: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error adding tag: {e}")
            return False

    def remove_customer_tag(self, shopify_customer_id: str, tag: str) -> bool:
        """
        Remove a tag from a Shopify customer.

        Args:
            shopify_customer_id: Shopify customer ID
            tag: Tag to remove

        Returns:
            True if successful, False otherwise
        """
        # Get current tags
        current_tags = self.get_customer_tags(shopify_customer_id)

        # Check if tag exists
        if tag not in current_tags:
            return True  # Tag doesn't exist, nothing to remove

        # Remove tag
        current_tags.remove(tag)
        tags_str = ', '.join(current_tags)

        url = f"{self.base_url}/customers/{shopify_customer_id}.json"

        payload = {
            "customer": {
                "id": int(shopify_customer_id),
                "tags": tags_str
            }
        }

        try:
            response = requests.put(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code == 200:
                return True
            else:
                print(f"   ⚠️  Failed to remove tag: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error removing tag: {e}")
            return False

    def sync_flags_to_shopify(self, dry_run: bool = False):
        """
        Main sync function: Read flags from S3 and update Shopify customer tags.

        Args:
            dry_run: If True, only print what would be done without making changes
        """
        print("\n" + "="*80)
        print("SYNC CUSTOMER FLAGS TO SHOPIFY TAGS")
        print("="*80)

        if dry_run:
            print("🔍 DRY RUN MODE - No changes will be made")

        # Load data
        flags_df = self.load_flags_from_s3()
        customers_df = self.load_customers_from_s3()

        if len(flags_df) == 0:
            print("\nℹ️  No flags to sync")
            return

        # Merge flags with customer data
        flags_with_contact = flags_df.merge(
            customers_df,
            on='customer_id',
            how='left'
        )

        print(f"\n📊 Found {len(flags_with_contact)} flags to sync")

        # Group by flag type
        flag_types = flags_with_contact['flag_name'].unique()

        for flag_name in flag_types:
            print(f"\n🏁 Syncing flag: {flag_name}")
            flag_subset = flags_with_contact[flags_with_contact['flag_name'] == flag_name]

            # Convert flag name to tag format (replace underscores with hyphens)
            tag_name = flag_name.replace('_', '-')

            synced = 0
            not_found = 0
            errors = 0

            for _, row in flag_subset.iterrows():
                capitan_id = row['customer_id']
                email = row.get('email')
                phone = row.get('phone')
                first_name = row.get('first_name', 'Unknown')
                last_name = row.get('last_name', 'Unknown')

                # Search for customer in Shopify
                shopify_id = self.search_shopify_customer(email=email, phone=phone)

                if not shopify_id:
                    not_found += 1
                    print(f"   ⚠️  Customer {capitan_id} ({first_name} {last_name}) not found in Shopify")
                    continue

                # Add tag
                if not dry_run:
                    success = self.add_customer_tag(
                        shopify_customer_id=shopify_id,
                        tag=tag_name
                    )
                    if success:
                        synced += 1
                        print(f"   ✅ Added tag '{tag_name}' to customer {capitan_id} (Shopify ID: {shopify_id})")
                    else:
                        errors += 1
                else:
                    synced += 1
                    print(f"   [DRY RUN] Would add tag '{tag_name}' to customer {capitan_id} (Shopify ID: {shopify_id})")

            print(f"\n   Summary for {flag_name} -> tag '{tag_name}':")
            print(f"      Synced: {synced}")
            print(f"      Not found in Shopify: {not_found}")
            print(f"      Errors: {errors}")

        # Clean up stale tags (tags that exist in Shopify but not in current flags)
        print(f"\n🧹 Cleaning up stale tags...")
        self.cleanup_stale_tags(flags_with_contact, dry_run=dry_run)

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)

    def cleanup_stale_tags(self, active_flags_df: pd.DataFrame, dry_run: bool = False):
        """
        Remove flag tags from Shopify customers who no longer have active flags.

        Args:
            active_flags_df: DataFrame of currently active flags
            dry_run: If True, only print what would be done
        """
        # Get list of flag tag prefixes to look for
        # Note: Tags are flag names with underscores replaced by hyphens (NO "flag-" prefix)
        flag_tag_prefixes = [
            'ready-for-membership',
            'first-time-day-pass-2wk-offer',
            'second-visit-offer-eligible',
            'second-visit-2wk-offer',
            'used-2-week-pass'
        ]

        # Create set of customers with active flags
        active_customer_ids = set(active_flags_df['customer_id'].unique())

        # Load all customers to check their Shopify tags
        customers_df = self.load_customers_from_s3()

        removed_count = 0
        checked_count = 0

        for _, customer in customers_df.iterrows():
            customer_id = customer['customer_id']
            email = customer.get('email')
            phone = customer.get('phone')

            # Skip if customer has active flags
            if customer_id in active_customer_ids:
                continue

            # Search for customer in Shopify
            shopify_id = self.search_shopify_customer(email=email, phone=phone)
            if not shopify_id:
                continue

            checked_count += 1

            # Get customer's current tags
            current_tags = self.get_customer_tags(shopify_id)

            # Check if customer has any flag tags
            flag_tags_to_remove = [tag for tag in current_tags if any(tag.startswith(prefix) for prefix in flag_tag_prefixes)]

            if flag_tags_to_remove:
                for tag in flag_tags_to_remove:
                    if not dry_run:
                        success = self.remove_customer_tag(shopify_id, tag)
                        if success:
                            removed_count += 1
                            print(f"   🗑️  Removed stale tag '{tag}' from customer {customer_id}")
                    else:
                        removed_count += 1
                        print(f"   [DRY RUN] Would remove stale tag '{tag}' from customer {customer_id}")

        print(f"\n   Cleanup summary:")
        print(f"      Customers checked: {checked_count}")
        print(f"      Stale tags removed: {removed_count}")


def main():
    """Run the sync."""
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    syncer = ShopifyFlagSyncer()

    # Run sync (set dry_run=True to test without making changes)
    syncer.sync_flags_to_shopify(dry_run=False)


if __name__ == "__main__":
    main()

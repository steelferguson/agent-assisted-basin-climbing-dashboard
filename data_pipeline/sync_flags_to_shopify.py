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

    def sync_flags_to_shopify(self, dry_run: bool = False):
        """
        Main sync function: Read flags from S3 and update Shopify metafields.

        Args:
            dry_run: If True, only print what would be done without making changes
        """
        print("\n" + "="*80)
        print("SYNC CUSTOMER FLAGS TO SHOPIFY")
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

                # Set metafield
                if not dry_run:
                    success = self.set_customer_metafield(
                        shopify_customer_id=shopify_id,
                        namespace="custom",
                        key=flag_name,
                        value="true",
                        value_type="boolean"
                    )
                    if success:
                        synced += 1
                        print(f"   ✅ Set {flag_name} for customer {capitan_id} (Shopify ID: {shopify_id})")
                    else:
                        errors += 1
                else:
                    synced += 1
                    print(f"   [DRY RUN] Would set {flag_name} for customer {capitan_id} (Shopify ID: {shopify_id})")

            print(f"\n   Summary for {flag_name}:")
            print(f"      Synced: {synced}")
            print(f"      Not found in Shopify: {not_found}")
            print(f"      Errors: {errors}")

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)


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

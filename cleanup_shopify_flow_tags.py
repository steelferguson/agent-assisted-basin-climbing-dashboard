"""
Cleanup Shopify Flow Tags

Removes specific tags from ALL Shopify customers to prepare for a clean test.
Tags to remove:
- first-time-day-pass-2wk-offer
- second-visit-2wk-offer
- 2-week-pass-purchase

Usage:
    python cleanup_shopify_flow_tags.py [--send]

Without --send: Dry run (shows what would be removed)
With --send: Actually removes the tags
"""

import os
import sys
import requests
import time
from typing import List, Optional

# Load environment
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Tags to remove
TAGS_TO_REMOVE = [
    'first-time-day-pass-2wk-offer',
    'second-visit-2wk-offer',
    '2-week-pass-purchase',
    # Also remove the "-sent" versions
    'first-time-day-pass-2wk-offer-sent',
    'second-visit-2wk-offer-sent',
    '2-week-pass-purchase-sent',
]


class ShopifyTagCleaner:
    def __init__(self):
        self.store_domain = os.getenv("SHOPIFY_STORE_DOMAIN")
        self.admin_token = os.getenv("SHOPIFY_ADMIN_TOKEN")

        if not self.store_domain or not self.admin_token:
            raise ValueError("SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_TOKEN must be set")

        self.base_url = f"https://{self.store_domain}/admin/api/2024-01"
        self.headers = {
            "X-Shopify-Access-Token": self.admin_token,
            "Content-Type": "application/json"
        }

        # Rate limiting
        self.min_delay = 0.6
        self.last_call_time = 0

    def _rate_limit(self):
        """Enforce rate limiting."""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time
        if time_since_last < self.min_delay:
            time.sleep(self.min_delay - time_since_last)
        self.last_call_time = time.time()

    def get_all_customers(self) -> List[dict]:
        """Get all customers from Shopify using pagination."""
        customers = []
        url = f"{self.base_url}/customers.json?limit=250"

        while url:
            self._rate_limit()
            response = requests.get(url, headers=self.headers, timeout=30)

            if response.status_code != 200:
                print(f"Error fetching customers: {response.status_code}")
                break

            data = response.json()
            batch = data.get('customers', [])
            customers.extend(batch)
            print(f"   Fetched {len(customers)} customers...", end='\r')

            # Check for next page via Link header
            link_header = response.headers.get('Link', '')
            if 'rel="next"' in link_header:
                # Parse next URL from Link header
                parts = link_header.split(',')
                for part in parts:
                    if 'rel="next"' in part:
                        url = part.split(';')[0].strip().strip('<>')
                        break
            else:
                url = None

        print(f"\n   ✓ Fetched {len(customers)} total customers")
        return customers

    def remove_tags_from_customer(self, customer_id: str, current_tags: List[str],
                                   tags_to_remove: List[str], dry_run: bool = True) -> int:
        """Remove specified tags from a customer. Returns count of tags removed."""
        # Find which tags need to be removed
        tags_to_actually_remove = [t for t in tags_to_remove if t in current_tags]

        if not tags_to_actually_remove:
            return 0

        # Build new tags list
        new_tags = [t for t in current_tags if t not in tags_to_remove]
        tags_str = ', '.join(new_tags)

        if dry_run:
            return len(tags_to_actually_remove)

        # Update customer
        url = f"{self.base_url}/customers/{customer_id}.json"
        payload = {
            "customer": {
                "id": int(customer_id),
                "tags": tags_str
            }
        }

        self._rate_limit()
        response = requests.put(url, headers=self.headers, json=payload, timeout=10)

        if response.status_code == 200:
            return len(tags_to_actually_remove)
        else:
            print(f"   ⚠️  Failed to update customer {customer_id}: {response.status_code}")
            return 0

    def cleanup_tags(self, dry_run: bool = True):
        """Main cleanup function."""
        print("\n" + "=" * 70)
        print("SHOPIFY FLOW TAG CLEANUP")
        print("=" * 70)

        if dry_run:
            print("🔍 DRY RUN MODE - No changes will be made\n")
        else:
            print("⚠️  LIVE MODE - Tags will be removed\n")

        print("Tags to remove:")
        for tag in TAGS_TO_REMOVE:
            print(f"   - {tag}")

        print("\n📥 Fetching all customers from Shopify...")
        customers = self.get_all_customers()

        print("\n🔍 Scanning for tags to remove...")

        total_removed = 0
        customers_affected = 0

        for customer in customers:
            customer_id = str(customer['id'])
            email = customer.get('email', 'N/A')
            name = f"{customer.get('first_name', '')} {customer.get('last_name', '')}".strip() or 'Unknown'

            # Parse tags
            tags_str = customer.get('tags', '')
            current_tags = [t.strip() for t in tags_str.split(',') if t.strip()]

            # Check for tags to remove
            removed = self.remove_tags_from_customer(
                customer_id, current_tags, TAGS_TO_REMOVE, dry_run=dry_run
            )

            if removed > 0:
                customers_affected += 1
                total_removed += removed
                matching_tags = [t for t in TAGS_TO_REMOVE if t in current_tags]
                action = "Would remove" if dry_run else "Removed"
                print(f"   {action}: {name} ({email}) - tags: {matching_tags}")

        print("\n" + "=" * 70)
        print("CLEANUP SUMMARY")
        print("=" * 70)
        print(f"   Customers scanned: {len(customers)}")
        print(f"   Customers affected: {customers_affected}")
        print(f"   Tags {'to remove' if dry_run else 'removed'}: {total_removed}")

        if dry_run:
            print("\n💡 Run with --send to actually remove tags")
        else:
            print("\n✅ Tag cleanup complete!")


def main():
    dry_run = '--send' not in sys.argv

    cleaner = ShopifyTagCleaner()
    cleaner.cleanup_tags(dry_run=dry_run)


if __name__ == "__main__":
    main()

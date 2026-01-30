"""
Setup Klaviyo Lists for Flow Triggers

Creates lists that correspond to Shopify tags, so flows can trigger on list entry.

Lists to create:
- "Day Pass - 2 Week Offer" (for first-time-day-pass-2wk-offer and second-visit-2wk-offer tags)
- "2 Week Pass - Membership Offer" (for 2-week-pass-purchase tag)

Usage:
    python setup_klaviyo_flow_lists.py
"""

import os
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")
KLAVIYO_HEADERS = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2025-01-15",
    "Content-Type": "application/json",
    "Accept": "application/json"
}


def get_all_lists():
    """Get all existing Klaviyo lists."""
    url = "https://a.klaviyo.com/api/lists"
    response = requests.get(url, headers=KLAVIYO_HEADERS, timeout=30)

    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        print(f"Error fetching lists: {response.status_code}")
        return []


def create_list(name: str) -> str:
    """Create a Klaviyo list and return its ID."""
    url = "https://a.klaviyo.com/api/lists"

    payload = {
        "data": {
            "type": "list",
            "attributes": {
                "name": name
            }
        }
    }

    response = requests.post(url, headers=KLAVIYO_HEADERS, json=payload, timeout=30)

    if response.status_code in [200, 201]:
        list_id = response.json()['data']['id']
        print(f"   ✅ Created list: {name} (ID: {list_id})")
        return list_id
    else:
        print(f"   ❌ Failed to create list '{name}': {response.status_code}")
        print(f"      {response.text[:200]}")
        return None


def main():
    print("\n" + "=" * 60)
    print("SETUP KLAVIYO FLOW LISTS")
    print("=" * 60)

    # Lists to create with their purposes
    lists_to_create = [
        {
            "name": "Day Pass - 2 Week Offer",
            "description": "Customers tagged with first-time-day-pass-2wk-offer or second-visit-2wk-offer",
            "tags": ["first-time-day-pass-2wk-offer", "second-visit-2wk-offer"]
        },
        {
            "name": "2 Week Pass - Membership Offer",
            "description": "Customers tagged with 2-week-pass-purchase",
            "tags": ["2-week-pass-purchase"]
        }
    ]

    # Check existing lists
    print("\n📋 Checking existing lists...")
    existing_lists = get_all_lists()
    existing_names = {lst['attributes']['name']: lst['id'] for lst in existing_lists}

    print(f"   Found {len(existing_lists)} existing lists")

    # Create or find lists
    list_mapping = {}

    print("\n📝 Creating/finding lists...")
    for list_config in lists_to_create:
        name = list_config['name']

        if name in existing_names:
            list_id = existing_names[name]
            print(f"   ✓ List exists: {name} (ID: {list_id})")
        else:
            list_id = create_list(name)

        if list_id:
            for tag in list_config['tags']:
                list_mapping[tag] = list_id

    # Print mapping for use in sync_flags_to_shopify.py
    print("\n" + "=" * 60)
    print("LIST MAPPING FOR sync_flags_to_shopify.py")
    print("=" * 60)
    print("\nAdd this to klaviyo_flag_list_map in ShopifyFlagSyncer.__init__:")
    print()
    print("self.klaviyo_flag_list_map = {")
    for tag, list_id in list_mapping.items():
        print(f"    '{tag.replace('-', '_')}': '{list_id}',")
    print("}")

    print("\n" + "=" * 60)
    print("✅ SETUP COMPLETE")
    print("=" * 60)
    print("\nNext steps:")
    print("1. Update sync_flags_to_shopify.py with the list mapping above")
    print("2. In Klaviyo, set each flow to trigger on 'Added to List'")
    print("   - Day Pass flow → 'Day Pass - 2 Week Offer' list")
    print("   - 2 Week Pass flow → '2 Week Pass - Membership Offer' list")


if __name__ == "__main__":
    main()

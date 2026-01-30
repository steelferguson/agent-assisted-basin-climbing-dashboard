"""
Add a profile to a Klaviyo list (for testing flow triggers).

Usage:
    python add_to_klaviyo_list.py --email user@example.com --list LIST_ID
    python add_to_klaviyo_list.py --email user@example.com --list day-pass
    python add_to_klaviyo_list.py --email user@example.com --list 2-week
"""

import os
import sys
import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")
KLAVIYO_HEADERS = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2025-01-15",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# List shortcuts
LIST_SHORTCUTS = {
    'day-pass': 'RX9TsQ',      # Day Pass - 2 Week Offer
    '2-week': 'VxZEtN',        # 2 Week Pass - Membership Offer
    'winback': 'VbbZSy',       # Membership Win-Back
}


def get_or_create_profile(email: str) -> str:
    """Get or create a Klaviyo profile and return its ID."""
    # First try to find existing profile
    url = f"https://a.klaviyo.com/api/profiles?filter=equals(email,\"{email}\")"
    response = requests.get(url, headers=KLAVIYO_HEADERS, timeout=30)

    if response.status_code == 200:
        data = response.json().get('data', [])
        if data:
            return data[0]['id']

    # Create new profile if not found
    create_url = "https://a.klaviyo.com/api/profiles"
    payload = {
        "data": {
            "type": "profile",
            "attributes": {
                "email": email
            }
        }
    }
    response = requests.post(create_url, headers=KLAVIYO_HEADERS, json=payload, timeout=30)

    if response.status_code in [200, 201]:
        return response.json()['data']['id']

    print(f"   Error creating profile: {response.status_code} - {response.text[:200]}")
    return None


def add_to_list(email: str, list_id: str):
    """Add a profile to a Klaviyo list."""
    # First get the profile ID
    profile_id = get_or_create_profile(email)
    if not profile_id:
        print(f"❌ Could not find or create profile for {email}")
        return False

    url = f"https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles"

    payload = {
        "data": [
            {
                "type": "profile",
                "id": profile_id
            }
        ]
    }

    response = requests.post(url, headers=KLAVIYO_HEADERS, json=payload, timeout=30)

    if response.status_code in [200, 201, 202, 204]:
        print(f"✅ Added {email} to list {list_id}")
        return True
    else:
        print(f"❌ Failed: {response.status_code}")
        print(f"   {response.text[:300]}")
        return False


def main():
    email = None
    list_id = None

    # Parse args
    for i, arg in enumerate(sys.argv):
        if arg == '--email' and i + 1 < len(sys.argv):
            email = sys.argv[i + 1]
        if arg == '--list' and i + 1 < len(sys.argv):
            list_arg = sys.argv[i + 1]
            list_id = LIST_SHORTCUTS.get(list_arg, list_arg)

    if not email or not list_id:
        print(__doc__)
        print("\nAvailable list shortcuts:")
        for shortcut, lid in LIST_SHORTCUTS.items():
            print(f"  {shortcut} -> {lid}")
        return

    add_to_list(email, list_id)


if __name__ == "__main__":
    main()

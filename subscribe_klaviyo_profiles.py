"""
Subscribe profiles to Klaviyo email and SMS marketing.

Usage:
    python subscribe_klaviyo_profiles.py --email user@example.com [--phone +15551234567]
    python subscribe_klaviyo_profiles.py --search "Name" (searches Shopify for email/phone)
    python subscribe_klaviyo_profiles.py --test (subscribes 4 test users)
"""

import os
import sys
import requests
import time

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Klaviyo API setup
KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")
KLAVIYO_HEADERS = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2025-01-15",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# Shopify API setup
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN")
SHOPIFY_ADMIN_TOKEN = os.getenv("SHOPIFY_ADMIN_TOKEN")
SHOPIFY_HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ADMIN_TOKEN,
    "Content-Type": "application/json"
}


def search_shopify_customer(name: str) -> dict:
    """Search for a customer by name in Shopify."""
    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/2024-01/customers/search.json?query={name}"
    response = requests.get(url, headers=SHOPIFY_HEADERS, timeout=10)

    if response.status_code == 200:
        customers = response.json().get('customers', [])
        if customers:
            c = customers[0]
            return {
                'name': f"{c.get('first_name', '')} {c.get('last_name', '')}".strip(),
                'email': c.get('email'),
                'phone': c.get('phone')
            }
    return None


def subscribe_profile(email: str = None, phone: str = None, name: str = ""):
    """Subscribe a profile to email and SMS marketing in Klaviyo."""
    if not email and not phone:
        print(f"   ⚠️  No email or phone for {name}")
        return False

    profile_data = {}
    subscriptions = {}

    if email:
        profile_data['email'] = email
        subscriptions['email'] = {'marketing': {'consent': 'SUBSCRIBED'}}

    if phone:
        # Normalize phone to E.164
        phone_digits = ''.join(c for c in phone if c.isdigit())
        if len(phone_digits) == 10:
            phone = f"+1{phone_digits}"
        elif len(phone_digits) == 11 and phone_digits[0] == '1':
            phone = f"+{phone_digits}"

        profile_data['phone_number'] = phone
        subscriptions['sms'] = {'marketing': {'consent': 'SUBSCRIBED'}}

    profile_data['subscriptions'] = subscriptions

    payload = {
        "data": {
            "type": "profile-subscription-bulk-create-job",
            "attributes": {
                "profiles": {
                    "data": [
                        {
                            "type": "profile",
                            "attributes": profile_data
                        }
                    ]
                }
            }
        }
    }

    url = "https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs"
    response = requests.post(url, headers=KLAVIYO_HEADERS, json=payload, timeout=30)

    if response.status_code in [200, 201, 202]:
        print(f"   ✅ Subscribed: {name or email}")
        if email:
            print(f"      Email: {email}")
        if phone:
            print(f"      SMS: {phone}")
        return True
    else:
        print(f"   ❌ Failed to subscribe {name or email}: {response.status_code}")
        print(f"      {response.text[:200]}")
        return False


def subscribe_test_users():
    """Subscribe the 4 test users."""
    test_names = [
        "Trinity Robb",
        "Steel Ferguson",
        "Walker Farrar",
        "Vicky Galasso"
    ]

    print("\n" + "=" * 60)
    print("SUBSCRIBE TEST USERS TO KLAVIYO")
    print("=" * 60)

    print("\n🔍 Searching Shopify for test users...")

    users_to_subscribe = []

    for name in test_names:
        print(f"\n   Searching: {name}")
        customer = search_shopify_customer(name)

        if customer:
            print(f"   ✓ Found: {customer['name']}")
            print(f"     Email: {customer['email']}")
            print(f"     Phone: {customer['phone']}")
            users_to_subscribe.append(customer)
        else:
            print(f"   ⚠️  Not found in Shopify: {name}")

    if not users_to_subscribe:
        print("\n❌ No users found to subscribe")
        return

    print(f"\n📧 Subscribing {len(users_to_subscribe)} users to Klaviyo...")

    success = 0
    for user in users_to_subscribe:
        time.sleep(0.5)  # Rate limiting
        if subscribe_profile(
            email=user.get('email'),
            phone=user.get('phone'),
            name=user.get('name')
        ):
            success += 1

    print(f"\n" + "=" * 60)
    print(f"✅ Subscribed {success}/{len(users_to_subscribe)} users")
    print("=" * 60)


def main():
    if '--test' in sys.argv:
        subscribe_test_users()
    elif '--search' in sys.argv:
        idx = sys.argv.index('--search')
        if idx + 1 < len(sys.argv):
            name = sys.argv[idx + 1]
            customer = search_shopify_customer(name)
            if customer:
                print(f"Found: {customer}")
                subscribe_profile(
                    email=customer.get('email'),
                    phone=customer.get('phone'),
                    name=customer.get('name')
                )
            else:
                print(f"Not found: {name}")
    elif '--email' in sys.argv:
        idx = sys.argv.index('--email')
        email = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
        phone = None
        if '--phone' in sys.argv:
            phone_idx = sys.argv.index('--phone')
            phone = sys.argv[phone_idx + 1] if phone_idx + 1 < len(sys.argv) else None
        if email:
            subscribe_profile(email=email, phone=phone)
    else:
        print(__doc__)


if __name__ == "__main__":
    main()

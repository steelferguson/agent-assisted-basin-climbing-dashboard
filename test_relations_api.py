"""
Test the Capitan relations API to understand its structure.

This will help us understand what data is returned so we can implement
parent contact info lookup properly.
"""

import requests
import json
from dotenv import load_dotenv
import os

load_dotenv('.env')

def test_relations_api():
    """Test relations API with a known customer who has relations."""

    capitan_token = os.getenv('CAPITAN_API_TOKEN')

    if not capitan_token:
        print("❌ CAPITAN_API_TOKEN not found in .env")
        return

    headers = {"Authorization": f"token {capitan_token}"}

    # Test with Lane family - check both parent and kids
    test_customers = [
        {"id": "1709965", "name": "Emyris Lane (Parent)"},
        {"id": "1709966", "name": "Lucian Lane (Child)"},
        {"id": "1709967", "name": "Aiden Lane (Child)"},
        {"id": "1709968", "name": "Malachi Lane (Child)"},
    ]

    print("="*80)
    print("TESTING CAPITAN RELATIONS API")
    print("="*80)

    for customer in test_customers:
        print(f"\n📇 Testing Customer {customer['id']}: {customer['name']}")
        print("-"*60)

        # Build relations URL
        relations_url = f"https://api.hellocapitan.com/api/customers/{customer['id']}/relations/"
        emergency_url = f"https://api.hellocapitan.com/api/customers/{customer['id']}/emergency-contacts/"

        # Try relations first
        try:
            response = requests.get(relations_url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()

                print(f"✅ Relations API returned:")
                print(json.dumps(data, indent=2))

                # Check if we have results
                if 'results' in data and len(data['results']) > 0:
                    print(f"\n📊 Found {len(data['results'])} relation(s):")

                    for relation in data['results']:
                        print(f"\n  Relation:")
                        print(f"    Related Customer ID: {relation.get('related_customer')}")
                        print(f"    Relationship Type: {relation.get('relationship')}")
                        print(f"    Notes: {relation.get('notes', 'N/A')}")

                        # Now fetch the related customer's details
                        related_id = relation.get('related_customer')
                        if related_id:
                            print(f"\n  Fetching related customer {related_id} details...")
                            customer_url = f"https://api.hellocapitan.com/api/customers/{related_id}/"

                            try:
                                customer_response = requests.get(customer_url, headers=headers, timeout=10)
                                if customer_response.status_code == 200:
                                    customer_data = customer_response.json()
                                    print(f"    Name: {customer_data.get('first_name')} {customer_data.get('last_name')}")
                                    print(f"    Email: {customer_data.get('email', 'N/A')}")
                                    print(f"    Phone: {customer_data.get('telephone', 'N/A')}")
                                else:
                                    print(f"    ⚠️  Failed to fetch customer details: {customer_response.status_code}")
                            except Exception as e:
                                print(f"    ❌ Error fetching customer: {e}")
                else:
                    print("  ⚠️  No relations found")

            elif response.status_code == 404:
                print(f"⚠️  Relations endpoint not found (404) - customer may not have relations")
            else:
                print(f"❌ API returned status code: {response.status_code}")
                print(f"   Response: {response.text[:200]}")

        except Exception as e:
            print(f"❌ Error calling relations API: {e}")

        # Also try emergency contacts
        print(f"\n📞 Checking Emergency Contacts...")
        try:
            response = requests.get(emergency_url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                print(f"✅ Emergency Contacts API returned:")
                print(json.dumps(data, indent=2))

                if 'results' in data and len(data['results']) > 0:
                    print(f"\n📊 Found {len(data['results'])} emergency contact(s):")

                    for contact in data['results']:
                        print(f"\n  Emergency Contact:")
                        print(f"    Name: {contact.get('first_name')} {contact.get('last_name')}")
                        print(f"    Email: {contact.get('email', 'N/A')}")
                        print(f"    Phone: {contact.get('telephone', 'N/A')}")
                        print(f"    Relationship: {contact.get('relationship', 'N/A')}")
                else:
                    print("  ⚠️  No emergency contacts found")
            else:
                print(f"⚠️  Emergency contacts returned status: {response.status_code}")

        except Exception as e:
            print(f"❌ Error calling emergency contacts API: {e}")

    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)


if __name__ == "__main__":
    test_relations_api()

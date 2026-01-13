"""
Comprehensively explore Capitan API to see what family relationship data is available.

This will test multiple endpoints and save responses for analysis.
"""

import requests
import json
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv('.env')

def save_response(data, filename):
    """Save API response to file for analysis."""
    os.makedirs('data/api_exploration', exist_ok=True)
    filepath = f'data/api_exploration/{filename}.json'
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"   💾 Saved to {filepath}")

def test_endpoint(base_url, endpoint, headers, params=None):
    """Test an API endpoint and return response."""
    url = f"{base_url}{endpoint}"
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"   ⚠️  Status {response.status_code}: {response.text[:200]}")
            return None
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

def explore_capitan_api():
    """Explore all Capitan API endpoints to understand family relationships."""

    capitan_token = os.getenv('CAPITAN_API_TOKEN')
    if not capitan_token:
        print("❌ CAPITAN_API_TOKEN not found in .env")
        return

    base_url = "https://api.hellocapitan.com/api/"
    headers = {"Authorization": f"token {capitan_token}"}

    print("="*80)
    print("EXPLORING CAPITAN API FOR FAMILY RELATIONSHIP DATA")
    print("="*80)
    print()

    # ==================================================================
    # 1. WAIVERS - Does this endpoint exist?
    # ==================================================================
    print("📋 1. TESTING WAIVERS ENDPOINT")
    print("-"*60)

    for endpoint in ['waivers', 'customer-waivers', 'waiver', 'waiver-records']:
        print(f"\n   Trying: {endpoint}")
        data = test_endpoint(base_url, endpoint, headers, params={'page_size': 5})
        if data:
            print(f"   ✅ SUCCESS! Found {len(data.get('results', []))} records")
            save_response(data, f'{endpoint}_sample')

            # Show structure of first record
            if data.get('results'):
                first = data['results'][0]
                print(f"\n   Sample Record Structure:")
                for key in first.keys():
                    value = first[key]
                    if isinstance(value, dict):
                        print(f"     {key}: {{dict with {len(value)} keys}}")
                    elif isinstance(value, list):
                        print(f"     {key}: [list with {len(value)} items]")
                    else:
                        print(f"     {key}: {value}")
            break

    # ==================================================================
    # 2. CUSTOMER-MEMBERSHIPS - Get full structure with all_customers
    # ==================================================================
    print("\n\n👥 2. TESTING CUSTOMER-MEMBERSHIPS (Family Rosters)")
    print("-"*60)

    print("\n   Fetching sample family membership...")
    data = test_endpoint(base_url, 'customer-memberships', headers, params={'page_size': 100})

    if data:
        # Find a family membership (one with multiple customers)
        family_membership = None
        for membership in data.get('results', []):
            if len(membership.get('all_customers', [])) > 2:
                family_membership = membership
                break

        if family_membership:
            print(f"   ✅ Found family membership!")
            print(f"\n   Membership ID: {family_membership.get('id')}")
            print(f"   Name: {family_membership.get('name')}")
            print(f"   Owner ID: {family_membership.get('owner_id')}")
            print(f"   Number of members: {len(family_membership.get('all_customers', []))}")

            print(f"\n   Family Members:")
            for member in family_membership.get('all_customers', []):
                print(f"     - Customer ID: {member.get('id')}, Name: {member.get('first_name')} {member.get('last_name')}")

            save_response(family_membership, 'family_membership_example')
        else:
            print("   ⚠️  No family memberships found in sample")

    # ==================================================================
    # 3. RELATIONS - Test the relations_url for specific customers
    # ==================================================================
    print("\n\n👨‍👩‍👧‍👦 3. TESTING RELATIONS ENDPOINT")
    print("-"*60)

    # Test with a few different customer types
    test_customer_ids = [
        '1709965',  # Emyris Lane (parent)
        '1709966',  # Lucian Lane (child)
        '1379167',  # Stephanie Hodnett (parent from family membership)
        '2412318',  # Brigham Hodnett (child from family membership)
    ]

    for customer_id in test_customer_ids:
        print(f"\n   Testing customer {customer_id}...")
        relations_url = f"customers/{customer_id}/relations/"
        data = test_endpoint(base_url, relations_url, headers)

        if data:
            results = data.get('results', [])
            print(f"   Relations found: {len(results)}")
            if results:
                save_response(data, f'relations_customer_{customer_id}')
                print(f"   Sample relation:")
                for key, value in results[0].items():
                    print(f"     {key}: {value}")

    # ==================================================================
    # 4. EMERGENCY CONTACTS
    # ==================================================================
    print("\n\n📞 4. TESTING EMERGENCY CONTACTS")
    print("-"*60)

    for customer_id in test_customer_ids:
        print(f"\n   Testing customer {customer_id}...")
        contacts_url = f"customers/{customer_id}/emergency-contacts/"
        data = test_endpoint(base_url, contacts_url, headers)

        if data:
            results = data.get('results', [])
            print(f"   Emergency contacts found: {len(results)}")
            if results:
                save_response(data, f'emergency_contacts_customer_{customer_id}')
                for idx, contact in enumerate(results):
                    print(f"\n   Contact {idx+1}:")
                    print(f"     Name: {contact.get('first_name')} {contact.get('last_name')}")
                    print(f"     Phone: {contact.get('telephone')}")
                    print(f"     Email: {contact.get('email')}")
                    print(f"     Relationship: {contact.get('relation')}")

    # ==================================================================
    # 5. CUSTOMERS - Get full customer record to see all fields
    # ==================================================================
    print("\n\n👤 5. TESTING CUSTOMER DETAIL ENDPOINT")
    print("-"*60)

    print(f"\n   Fetching full customer record for 1709966 (minor)...")
    data = test_endpoint(base_url, 'customers/1709966/', headers)

    if data:
        print(f"   ✅ Retrieved customer record")
        print(f"\n   All Available Fields:")
        for key in sorted(data.keys()):
            value = data[key]
            if isinstance(value, dict):
                print(f"     {key}: {{dict}}")
            elif isinstance(value, list):
                print(f"     {key}: [list]")
            elif value:
                print(f"     {key}: {value}")

        save_response(data, 'customer_full_record_minor')

    print(f"\n   Fetching full customer record for 1379167 (adult)...")
    data = test_endpoint(base_url, 'customers/1379167/', headers)

    if data:
        save_response(data, 'customer_full_record_adult')

    # ==================================================================
    # 6. CHECK-INS - Sample to see all fields
    # ==================================================================
    print("\n\n🚪 6. TESTING CHECK-INS ENDPOINT")
    print("-"*60)

    print(f"\n   Fetching sample check-ins...")
    data = test_endpoint(base_url, 'check-ins', headers, params={'page_size': 5})

    if data and data.get('results'):
        print(f"   ✅ Retrieved {len(data['results'])} check-ins")

        first_checkin = data['results'][0]
        print(f"\n   Sample Check-in Fields:")
        for key in sorted(first_checkin.keys()):
            value = first_checkin[key]
            print(f"     {key}: {value}")

        save_response(data, 'checkins_sample')

    # ==================================================================
    # 7. LIST ALL AVAILABLE ENDPOINTS
    # ==================================================================
    print("\n\n🔍 7. EXPLORING ROOT API")
    print("-"*60)

    print(f"\n   Fetching API root to see available endpoints...")
    data = test_endpoint(base_url, '', headers)

    if data:
        print(f"   ✅ Available endpoints:")
        for key in sorted(data.keys()):
            print(f"     - {key}: {data[key]}")

        save_response(data, 'api_root')

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n\n" + "="*80)
    print("EXPLORATION COMPLETE")
    print("="*80)
    print("\nAll API responses saved to data/api_exploration/")
    print("\nNext steps:")
    print("1. Review the JSON files in data/api_exploration/")
    print("2. Identify which endpoints have parent-child relationship data")
    print("3. Update fetch_capitan_membership_data.py to pull this data")
    print()

if __name__ == "__main__":
    explore_capitan_api()

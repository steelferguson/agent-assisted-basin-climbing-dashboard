"""
QuickBooks API Test Script

Run this AFTER you've completed the OAuth flow (setup_quickbooks_oauth.py)
and have tokens saved in INTUIT_QUICKBOOKS_CREDENTIALS.md

This script will:
1. Read your access token and realm ID from the credentials file
2. Test basic API connectivity
3. Fetch sample expense data to understand the data structure
"""

from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
import requests
import json
import re

# Your credentials
CLIENT_ID = 'ABGH71AmkTpOcNVtSYtBAGG0EQ3TN48EAkUNYlO3lwt5giEpQE'
CLIENT_SECRET = 'FCmxDTLD1RY1bCDN5kRkapKlJHKGYw85RPxdfYjH'
REDIRECT_URI = 'http://localhost:8000/callback'
ENVIRONMENT = 'production'


def extract_token_from_credentials():
    """Read the credentials file and extract the access token and realm ID."""
    with open('INTUIT_QUICKBOOKS_CREDENTIALS.md', 'r') as f:
        content = f.read()

    # Extract access token (between ```  after "**Access Token:**")
    access_token_match = re.search(r'\*\*Access Token:\*\*\s*```\s*([^\s`]+)', content)
    realm_id_match = re.search(r'\*\*Company ID \(Realm ID\):\*\*\s*```\s*([^\s`]+)', content)
    refresh_token_match = re.search(r'\*\*Refresh Token:\*\*\s*```\s*([^\s`]+)', content)

    if not access_token_match or not realm_id_match:
        raise ValueError("Could not find access token or realm ID in credentials file. Have you run setup_quickbooks_oauth.py?")

    return {
        'access_token': access_token_match.group(1),
        'realm_id': realm_id_match.group(1),
        'refresh_token': refresh_token_match.group(1) if refresh_token_match else None
    }


def test_company_info(access_token, realm_id):
    """Test basic API connectivity by fetching company info."""
    base_url = "https://quickbooks.api.intuit.com"

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    url = f"{base_url}/v3/company/{realm_id}/companyinfo/{realm_id}"

    print("Testing API connectivity...")
    print(f"GET {url}")
    print()

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        company = data.get('CompanyInfo', {})
        print("✅ API Connection Successful!")
        print()
        print(f"Company Name: {company.get('CompanyName')}")
        print(f"Legal Name: {company.get('LegalName')}")
        print(f"Email: {company.get('Email', {}).get('Address')}")
        print()
        return True
    else:
        print(f"❌ API Error: {response.status_code}")
        print(response.text)
        return False


def fetch_sample_expenses(access_token, realm_id, max_results=5):
    """Fetch a few recent purchases/expenses to see the data structure."""
    base_url = "https://quickbooks.api.intuit.com"

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    # Query for recent Purchase transactions (expenses)
    query = f"SELECT * FROM Purchase MAXRESULTS {max_results}"
    url = f"{base_url}/v3/company/{realm_id}/query"

    print("Fetching sample expenses...")
    print(f"Query: {query}")
    print()

    response = requests.get(url, headers=headers, params={'query': query})

    if response.status_code == 200:
        data = response.json()
        purchases = data.get('QueryResponse', {}).get('Purchase', [])

        if not purchases:
            print("No purchase transactions found.")
            return

        print(f"✅ Found {len(purchases)} purchase transactions")
        print()

        for i, purchase in enumerate(purchases, 1):
            print(f"Purchase #{i}:")
            print(f"  ID: {purchase.get('Id')}")
            print(f"  Date: {purchase.get('TxnDate')}")
            print(f"  Total: ${purchase.get('TotalAmt', 0):.2f}")
            print(f"  Payment Type: {purchase.get('PaymentType')}")

            # Show account details if available
            account_ref = purchase.get('AccountRef', {})
            print(f"  Account: {account_ref.get('name', 'N/A')}")

            # Show line items
            lines = purchase.get('Line', [])
            print(f"  Line Items: {len(lines)}")
            for line in lines[:3]:  # Show first 3 line items
                if line.get('DetailType') == 'AccountBasedExpenseLineDetail':
                    detail = line.get('AccountBasedExpenseLineDetail', {})
                    account = detail.get('AccountRef', {})
                    print(f"    - {account.get('name', 'Unknown')}: ${line.get('Amount', 0):.2f}")
            print()

        # Save raw response for inspection
        with open('data/raw_data/sample_quickbooks_expenses.json', 'w') as f:
            json.dump(data, f, indent=2)
        print("💾 Full response saved to: data/raw_data/sample_quickbooks_expenses.json")
        print()

    else:
        print(f"❌ Error fetching expenses: {response.status_code}")
        print(response.text)


def fetch_expense_accounts(access_token, realm_id):
    """Fetch list of expense accounts to understand categorization."""
    base_url = "https://quickbooks.api.intuit.com"

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    # Query for expense accounts
    query = "SELECT * FROM Account WHERE AccountType = 'Expense'"
    url = f"{base_url}/v3/company/{realm_id}/query"

    print("Fetching expense accounts...")
    print(f"Query: {query}")
    print()

    response = requests.get(url, headers=headers, params={'query': query})

    if response.status_code == 200:
        data = response.json()
        accounts = data.get('QueryResponse', {}).get('Account', [])

        if not accounts:
            print("No expense accounts found.")
            return

        print(f"✅ Found {len(accounts)} expense accounts")
        print()
        print("Expense Account Categories:")
        for account in accounts[:20]:  # Show first 20
            print(f"  - {account.get('Name')} (ID: {account.get('Id')})")

        if len(accounts) > 20:
            print(f"  ... and {len(accounts) - 20} more")
        print()

    else:
        print(f"❌ Error fetching accounts: {response.status_code}")
        print(response.text)


def main():
    print("=" * 70)
    print("QuickBooks API Test")
    print("=" * 70)
    print()

    try:
        # Extract credentials
        creds = extract_token_from_credentials()
        access_token = creds['access_token']
        realm_id = creds['realm_id']

        print(f"Realm ID: {realm_id}")
        print(f"Access Token: {access_token[:20]}...{access_token[-20:]}")
        print()
        print("-" * 70)
        print()

        # Test 1: Basic connectivity
        if not test_company_info(access_token, realm_id):
            print("❌ Basic API test failed. Check your credentials.")
            return

        print("-" * 70)
        print()

        # Test 2: Fetch expense accounts
        fetch_expense_accounts(access_token, realm_id)

        print("-" * 70)
        print()

        # Test 3: Fetch sample expenses
        fetch_sample_expenses(access_token, realm_id, max_results=5)

        print("=" * 70)
        print("All tests completed!")
        print("=" * 70)
        print()
        print("Next steps:")
        print("1. Review the sample data in data/raw_data/sample_quickbooks_expenses.json")
        print("2. Decide which expense categories to track in the dashboard")
        print("3. Build the expense data fetcher")
        print("4. Add Expenses tab to the dashboard")

    except FileNotFoundError:
        print("❌ Error: INTUIT_QUICKBOOKS_CREDENTIALS.md not found")
        print()
        print("Please run setup_quickbooks_oauth.py first to get your access tokens.")

    except ValueError as e:
        print(f"❌ Error: {e}")
        print()
        print("Make sure you've completed the OAuth flow by running:")
        print("  python setup_quickbooks_oauth.py")

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

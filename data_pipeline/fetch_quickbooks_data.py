"""
QuickBooks data fetcher for Basin Climbing expense tracking.

This module handles:
- OAuth token management and automatic refresh
- Fetching Purchase transactions (expenses) from QuickBooks
- Processing expense data for dashboard consumption
"""

import requests
import json
import os
import datetime
import pandas as pd
import base64
from typing import Optional, Dict, List


class QuickBooksFetcher:
    """
    A class for fetching and processing QuickBooks expense data.

    Handles automatic token refresh when access tokens expire.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        realm_id: str,
        access_token: str,
        refresh_token: str,
        credentials_file: str = "INTUIT_QUICKBOOKS_CREDENTIALS.md"
    ):
        """
        Initialize QuickBooks fetcher.

        Args:
            client_id: QuickBooks app client ID
            client_secret: QuickBooks app client secret
            realm_id: Company/realm ID
            access_token: OAuth access token (expires in 1 hour)
            refresh_token: OAuth refresh token (expires in 100 days)
            credentials_file: Path to credentials file for updating tokens
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.realm_id = realm_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.credentials_file = credentials_file

        # QuickBooks API endpoints
        self.base_url = f"https://quickbooks.api.intuit.com/v3/company/{realm_id}"
        self.token_endpoint = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

    def _get_auth_header(self) -> str:
        """Get base64-encoded authorization header for token refresh."""
        credentials = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded}"

    def refresh_access_token(self) -> bool:
        """
        Refresh the access token using the refresh token.

        Returns:
            True if refresh successful, False otherwise
        """
        print("🔄 Refreshing QuickBooks access token...")

        headers = {
            "Authorization": self._get_auth_header(),
            "Content-Type": "application/x-www-form-urlencoded"
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }

        try:
            response = requests.post(self.token_endpoint, headers=headers, data=data)

            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data["access_token"]
                # Note: refresh_token stays the same

                print("✅ Access token refreshed successfully")
                print(f"   New token expires in: {token_data.get('expires_in', 'unknown')} seconds")

                # Update credentials file if it exists
                self._update_credentials_file(self.access_token, self.refresh_token)

                return True
            else:
                print(f"❌ Failed to refresh token. Status: {response.status_code}")
                print(f"   Response: {response.text}")
                return False

        except Exception as e:
            print(f"❌ Error refreshing token: {e}")
            return False

    def _update_credentials_file(self, new_access_token: str, refresh_token: str):
        """Update the credentials file with new tokens."""
        try:
            if not os.path.exists(self.credentials_file):
                print(f"⚠️  Credentials file not found: {self.credentials_file}")
                return

            with open(self.credentials_file, 'r') as f:
                content = f.read()

            # Replace access token
            import re
            content = re.sub(
                r'(\*\*Access Token:\*\*\s*```\s*)([^\s]+)',
                f'\\1{new_access_token}',
                content
            )

            with open(self.credentials_file, 'w') as f:
                f.write(content)

            print(f"✅ Updated credentials file: {self.credentials_file}")

        except Exception as e:
            print(f"⚠️  Could not update credentials file: {e}")

    def _make_api_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make an API request to QuickBooks, with automatic token refresh on 401.

        Args:
            endpoint: API endpoint path (e.g., "/query")
            params: Query parameters

        Returns:
            JSON response or None if failed
        """
        url = f"{self.base_url}{endpoint}"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        }

        try:
            response = requests.get(url, headers=headers, params=params)

            # If unauthorized, try refreshing token once
            if response.status_code == 401:
                print("⚠️  Access token expired, attempting refresh...")
                if self.refresh_access_token():
                    # Retry with new token
                    headers["Authorization"] = f"Bearer {self.access_token}"
                    response = requests.get(url, headers=headers, params=params)
                else:
                    print("❌ Could not refresh token")
                    return None

            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ API request failed. Status: {response.status_code}")
                print(f"   Response: {response.text}")
                return None

        except Exception as e:
            print(f"❌ Error making API request: {e}")
            return None

    def fetch_expense_accounts(self) -> pd.DataFrame:
        """
        Fetch all expense account categories from QuickBooks.

        Returns:
            DataFrame with columns: account_id, account_name, account_type
        """
        print("Fetching expense account categories...")

        query = "SELECT * FROM Account WHERE AccountType = 'Expense'"
        response = self._make_api_request("/query", params={"query": query})

        if not response or "QueryResponse" not in response:
            print("❌ Failed to fetch expense accounts")
            return pd.DataFrame()

        accounts = response["QueryResponse"].get("Account", [])

        account_data = []
        for account in accounts:
            account_data.append({
                "account_id": account.get("Id"),
                "account_name": account.get("Name"),
                "account_type": account.get("AccountType"),
                "account_sub_type": account.get("AccountSubType"),
            })

        df = pd.DataFrame(account_data)
        print(f"✅ Retrieved {len(df)} expense account categories")

        return df

    def fetch_purchases(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        max_results: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch Purchase transactions (expenses) from QuickBooks.

        Args:
            start_date: Start date for expense query
            end_date: End date for expense query
            max_results: Maximum number of results per query

        Returns:
            DataFrame with expense data including:
            - transaction_id: Purchase transaction ID
            - date: Transaction date
            - amount: Total amount
            - vendor: Vendor name
            - account_name: Expense account/category
            - description: Transaction description
            - payment_type: Cash, Check, Credit Card, etc.
        """
        print(f"Fetching Purchase transactions from {start_date.date()} to {end_date.date()}...")

        # Format dates for QuickBooks query (YYYY-MM-DD)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        # Query for Purchase transactions in date range
        query = f"SELECT * FROM Purchase WHERE TxnDate >= '{start_str}' AND TxnDate <= '{end_str}' MAXRESULTS {max_results}"

        response = self._make_api_request("/query", params={"query": query})

        if not response or "QueryResponse" not in response:
            print("❌ Failed to fetch purchases")
            return pd.DataFrame()

        purchases = response["QueryResponse"].get("Purchase", [])
        print(f"Retrieved {len(purchases)} Purchase transactions")

        # Process purchases into structured data
        expense_data = []

        for purchase in purchases:
            # Get basic transaction info
            txn_id = purchase.get("Id")
            txn_date = purchase.get("TxnDate")
            total_amt = purchase.get("TotalAmt", 0)
            payment_type = purchase.get("PaymentType", "Unknown")

            # Get vendor info
            vendor_ref = purchase.get("EntityRef", {})
            vendor_name = vendor_ref.get("name") if vendor_ref else None

            # Get account info (where money came from)
            account_ref = purchase.get("AccountRef", {})
            payment_account = account_ref.get("name") if account_ref else None

            # Process line items (individual expenses)
            lines = purchase.get("Line", [])

            for line in lines:
                if line.get("DetailType") == "AccountBasedExpenseLineDetail":
                    line_detail = line.get("AccountBasedExpenseLineDetail", {})

                    # Get expense category
                    expense_account_ref = line_detail.get("AccountRef", {})
                    expense_category = expense_account_ref.get("name", "Uncategorized")

                    # Get line amount and description
                    amount = line.get("Amount", 0)
                    description = line.get("Description", purchase.get("PrivateNote", ""))

                    expense_data.append({
                        "transaction_id": txn_id,
                        "date": txn_date,
                        "amount": amount,
                        "vendor": vendor_name,
                        "expense_category": expense_category,
                        "description": description,
                        "payment_type": payment_type,
                        "payment_account": payment_account,
                    })

        df = pd.DataFrame(expense_data)

        if not df.empty:
            # Convert date to datetime
            df["date"] = pd.to_datetime(df["date"])

            # Sort by date
            df = df.sort_values("date", ascending=False)

            print(f"✅ Processed {len(df)} expense line items")
            print(f"   Total expenses: ${df['amount'].sum():,.2f}")
            print(f"   Date range: {df['date'].min().date()} to {df['date'].max().date()}")

            # Show top expense categories
            if len(df) > 0:
                print("\n   Top expense categories:")
                top_categories = df.groupby("expense_category")["amount"].sum().sort_values(ascending=False).head(5)
                for category, amount in top_categories.items():
                    print(f"     • {category}: ${amount:,.2f}")

        return df

    def fetch_revenue(
        self,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        max_results: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch revenue transactions (Sales Receipts, Invoices, Deposits) from QuickBooks.

        Combines all revenue sources into a single DataFrame for reconciliation.

        Args:
            start_date: Start date for revenue query
            end_date: End date for revenue query
            max_results: Maximum number of results per query

        Returns:
            DataFrame with revenue data including:
            - transaction_id: QuickBooks transaction ID
            - transaction_type: SalesReceipt, Invoice, or Deposit
            - date: Transaction date
            - amount: Total amount
            - customer: Customer name
            - revenue_category: Item/service category
            - description: Transaction description
            - payment_method: Payment method used
        """
        print(f"Fetching revenue transactions from {start_date.date()} to {end_date.date()}...")

        # Format dates for QuickBooks query
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")

        all_revenue = []

        # 1. Fetch Sales Receipts (immediate sales)
        print("\n📝 Fetching Sales Receipts...")
        query = f"SELECT * FROM SalesReceipt WHERE TxnDate >= '{start_str}' AND TxnDate <= '{end_str}' MAXRESULTS {max_results}"
        response = self._make_api_request("/query", params={"query": query})

        if response and "QueryResponse" in response:
            sales_receipts = response["QueryResponse"].get("SalesReceipt", [])
            print(f"   Retrieved {len(sales_receipts)} Sales Receipts")

            for sr in sales_receipts:
                customer_ref = sr.get("CustomerRef", {})
                payment_method_ref = sr.get("PaymentMethodRef", {})

                for line in sr.get("Line", []):
                    if line.get("DetailType") == "SalesItemLineDetail":
                        line_detail = line.get("SalesItemLineDetail", {})
                        item_ref = line_detail.get("ItemRef", {})

                        all_revenue.append({
                            "transaction_id": sr.get("Id"),
                            "transaction_type": "SalesReceipt",
                            "date": sr.get("TxnDate"),
                            "amount": line.get("Amount", 0),
                            "customer": customer_ref.get("name") if customer_ref else None,
                            "revenue_category": item_ref.get("name") if item_ref else "Uncategorized",
                            "description": line.get("Description", ""),
                            "payment_method": payment_method_ref.get("name") if payment_method_ref else None,
                        })

        # 2. Fetch Invoices (billed sales)
        print("\n📋 Fetching Invoices...")
        query = f"SELECT * FROM Invoice WHERE TxnDate >= '{start_str}' AND TxnDate <= '{end_str}' MAXRESULTS {max_results}"
        response = self._make_api_request("/query", params={"query": query})

        if response and "QueryResponse" in response:
            invoices = response["QueryResponse"].get("Invoice", [])
            print(f"   Retrieved {len(invoices)} Invoices")

            for inv in invoices:
                customer_ref = inv.get("CustomerRef", {})

                for line in inv.get("Line", []):
                    if line.get("DetailType") == "SalesItemLineDetail":
                        line_detail = line.get("SalesItemLineDetail", {})
                        item_ref = line_detail.get("ItemRef", {})

                        all_revenue.append({
                            "transaction_id": inv.get("Id"),
                            "transaction_type": "Invoice",
                            "date": inv.get("TxnDate"),
                            "amount": line.get("Amount", 0),
                            "customer": customer_ref.get("name") if customer_ref else None,
                            "revenue_category": item_ref.get("name") if item_ref else "Uncategorized",
                            "description": line.get("Description", ""),
                            "payment_method": None,  # Invoices don't have immediate payment method
                        })

        # 3. Fetch Deposits (bank deposits)
        print("\n💰 Fetching Deposits...")
        query = f"SELECT * FROM Deposit WHERE TxnDate >= '{start_str}' AND TxnDate <= '{end_str}' MAXRESULTS {max_results}"
        response = self._make_api_request("/query", params={"query": query})

        if response and "QueryResponse" in response:
            deposits = response["QueryResponse"].get("Deposit", [])
            print(f"   Retrieved {len(deposits)} Deposits")

            for dep in deposits:
                for line in dep.get("Line", []):
                    if line.get("DetailType") == "DepositLineDetail":
                        line_detail = line.get("DepositLineDetail", {})
                        entity_ref = line_detail.get("Entity", {})
                        account_ref = line_detail.get("AccountRef", {})

                        all_revenue.append({
                            "transaction_id": dep.get("Id"),
                            "transaction_type": "Deposit",
                            "date": dep.get("TxnDate"),
                            "amount": line.get("Amount", 0),
                            "customer": entity_ref.get("name") if entity_ref else None,
                            "revenue_category": account_ref.get("name") if account_ref else "Uncategorized",
                            "description": line.get("Description", dep.get("PrivateNote", "")),
                            "payment_method": None,
                        })

        df = pd.DataFrame(all_revenue)

        if not df.empty:
            # Convert date to datetime
            df["date"] = pd.to_datetime(df["date"])

            # Sort by date
            df = df.sort_values("date", ascending=False)

            print(f"\n✅ Processed {len(df)} revenue line items")
            print(f"   Total revenue: ${df['amount'].sum():,.2f}")
            print(f"   Date range: {df['date'].min().date()} to {df['date'].max().date()}")

            # Show breakdown by transaction type
            print("\n   Revenue by type:")
            type_summary = df.groupby("transaction_type")["amount"].sum().sort_values(ascending=False)
            for txn_type, amount in type_summary.items():
                count = len(df[df["transaction_type"] == txn_type])
                print(f"     • {txn_type}: ${amount:,.2f} ({count} items)")

        return df

    def save_data(self, df: pd.DataFrame, file_name: str):
        """Save DataFrame to CSV in data/outputs directory."""
        os.makedirs("data/outputs", exist_ok=True)
        filepath = f"data/outputs/{file_name}.csv"
        df.to_csv(filepath, index=False)
        print(f"💾 Saved to {filepath}")

    def save_raw_response(self, data: dict, filename: str):
        """Save raw API response to JSON file."""
        os.makedirs("data/raw_data", exist_ok=True)
        filepath = f"data/raw_data/{filename}.json"
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved raw response to {filepath}")


def load_credentials_from_file(filepath: str = "INTUIT_QUICKBOOKS_CREDENTIALS.md") -> Dict[str, str]:
    """
    Load QuickBooks credentials from markdown file.

    Returns:
        Dictionary with keys: client_id, client_secret, realm_id, access_token, refresh_token
    """
    credentials = {}

    try:
        with open(filepath, 'r') as f:
            content = f.read()

        # Extract values using simple string parsing
        import re

        # Client ID
        match = re.search(r'\*\*Client ID:\*\*\s*```\s*([^\s]+)', content)
        if match:
            credentials['client_id'] = match.group(1)

        # Client Secret
        match = re.search(r'\*\*Client Secret:\*\*\s*```\s*([^\s]+)', content)
        if match:
            credentials['client_secret'] = match.group(1)

        # Realm ID
        match = re.search(r'\*\*Company ID \(Realm ID\):\*\*\s*```\s*([^\s]+)', content)
        if match:
            credentials['realm_id'] = match.group(1)

        # Access Token
        match = re.search(r'\*\*Access Token:\*\*\s*```\s*([^\s]+)', content)
        if match:
            credentials['access_token'] = match.group(1)

        # Refresh Token
        match = re.search(r'\*\*Refresh Token:\*\*\s*```\s*([^\s]+)', content)
        if match:
            credentials['refresh_token'] = match.group(1)

        return credentials

    except Exception as e:
        print(f"❌ Error loading credentials from {filepath}: {e}")
        return {}


if __name__ == "__main__":
    # Test the fetcher
    print("=" * 60)
    print("QuickBooks Expense Fetcher Test")
    print("=" * 60)

    # Load credentials
    creds = load_credentials_from_file()

    if not creds:
        print("❌ Could not load credentials")
        exit(1)

    # Initialize fetcher
    fetcher = QuickBooksFetcher(
        client_id=creds['client_id'],
        client_secret=creds['client_secret'],
        realm_id=creds['realm_id'],
        access_token=creds['access_token'],
        refresh_token=creds['refresh_token']
    )

    # Test 1: Fetch expense accounts
    print("\n" + "=" * 60)
    print("Test 1: Fetch Expense Accounts")
    print("=" * 60)
    df_accounts = fetcher.fetch_expense_accounts()
    if not df_accounts.empty:
        print(f"\nSample accounts:")
        print(df_accounts.head(10))
        fetcher.save_data(df_accounts, "quickbooks_expense_accounts")

    # Test 2: Fetch recent purchases (last 90 days)
    print("\n" + "=" * 60)
    print("Test 2: Fetch Recent Purchases (Last 90 Days)")
    print("=" * 60)
    end_date = datetime.datetime.now()
    start_date = end_date - datetime.timedelta(days=90)

    df_purchases = fetcher.fetch_purchases(start_date, end_date)
    if not df_purchases.empty:
        print(f"\nSample purchases:")
        print(df_purchases.head(10))
        fetcher.save_data(df_purchases, "quickbooks_purchases")

    print("\n" + "=" * 60)
    print("✅ QuickBooks fetcher test complete!")
    print("=" * 60)

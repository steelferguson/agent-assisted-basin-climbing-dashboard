"""
Capitan Check-in Data Fetcher

Fetches check-in/visit data from Capitan API.
Includes customer info, check-in times, entry methods, and visit counts.
"""

import pandas as pd
import requests
from datetime import datetime
from typing import List, Dict, Optional


class CapitanCheckinFetcher:
    """
    A class for fetching and processing Capitan check-in data.

    Features:
    - Fetches all check-ins with customer details
    - Links to customer_id for membership analysis
    - Includes entry method and location info
    - Tracks lifetime check-in counts
    """

    def __init__(self, capitan_token: str):
        self.capitan_token = capitan_token
        self.base_url = "https://api.hellocapitan.com/api/"
        self.headers = {"Authorization": f"token {self.capitan_token}"}

    def _make_api_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make a request to the Capitan API with error handling."""
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                print(f"Response: {e.response.text[:200]}")
            return None

    def get_all_checkins(self, days_back: int = 90) -> List[Dict]:
        """
        Fetch all check-ins from the last N days.

        Args:
            days_back: Number of days to look back (default 90)

        Returns:
            List of check-in dictionaries
        """
        print(f"Fetching check-ins for the last {days_back} days...")

        # Calculate date filter
        from datetime import timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        url = f"{self.base_url}check-ins/"
        all_checkins = []
        page = 1
        page_size = 1000  # Fetch 1000 at a time

        while True:
            print(f"Fetching page {page}...")
            params = {
                'page': page,
                'page_size': page_size,
                'check_in_datetime__gte': start_date.strftime('%Y-%m-%d'),
                'ordering': '-check_in_datetime'  # Newest first
            }

            data = self._make_api_request(url, params)

            if not data or 'results' not in data:
                print("No data returned or error occurred")
                break

            results = data['results']
            all_checkins.extend(results)
            print(f"  Retrieved {len(results)} check-ins (Total so far: {len(all_checkins)})")

            # Check if there's a next page
            if not data.get('next'):
                break

            page += 1

        print(f"✓ Fetched {len(all_checkins)} total check-ins")
        return all_checkins

    def checkins_to_dataframe(self, checkins: List[Dict]) -> pd.DataFrame:
        """Convert check-ins list to pandas DataFrame with proper data types."""
        if not checkins:
            print("No check-ins to convert to DataFrame")
            return pd.DataFrame()

        processed_data = []

        for checkin in checkins:
            row = {
                'checkin_id': checkin.get('id'),
                'customer_id': checkin.get('customer_id'),
                'customer_first_name': checkin.get('customer_first_name'),
                'customer_last_name': checkin.get('customer_last_name'),
                'customer_email': checkin.get('customer_email'),
                'customer_birthday': checkin.get('customer_birthday'),
                'checkin_datetime': checkin.get('check_in_datetime'),
                # Note: Ignoring check_out_datetime and visit_duration as they're unreliable
                'entry_method': checkin.get('entry_method'),
                'entry_method_description': checkin.get('entry_method_description'),
                'location_id': checkin.get('location_id'),
                'location_name': checkin.get('location_name'),
                'is_free_entry': checkin.get('free_entry_reason') is not None,
                'free_entry_reason': checkin.get('free_entry_reason'),
                'lifetime_checkin_count': checkin.get('check_in_count'),  # Total check-ins for this customer
                'created_at': checkin.get('created_at'),
            }

            processed_data.append(row)

        df = pd.DataFrame(processed_data)

        # Convert datetime columns
        datetime_cols = ['checkin_datetime', 'created_at']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Convert birthday to date
        if 'customer_birthday' in df.columns:
            df['customer_birthday'] = pd.to_datetime(df['customer_birthday'], errors='coerce')

        return df

    def fetch_and_prepare_data(self, days_back: int = 90) -> pd.DataFrame:
        """
        Main method to fetch check-in data and return as DataFrame.

        Args:
            days_back: Number of days to fetch (default 90)

        Returns:
            DataFrame with check-in data
        """
        print(f"\n{'='*60}")
        print(f"Capitan Check-in Data Fetch - Last {days_back} Days")
        print(f"{'='*60}\n")

        # Fetch check-ins
        checkins = self.get_all_checkins(days_back=days_back)

        if not checkins:
            print("⚠ No check-in data fetched")
            return pd.DataFrame()

        # Convert to DataFrame
        df = self.checkins_to_dataframe(checkins)

        print(f"\n{'='*60}")
        print(f"✓ Successfully fetched {len(df)} check-in records")
        if not df.empty:
            print(f"  Date range: {df['checkin_datetime'].min()} to {df['checkin_datetime'].max()}")
            print(f"  Unique customers: {df['customer_id'].nunique():,}")
            print(f"  Entry methods: {df['entry_method_description'].value_counts().to_dict()}")
            free_entries = df['is_free_entry'].sum()
            print(f"  Free entries: {free_entries} ({free_entries/len(df)*100:.1f}%)")
        print(f"{'='*60}\n")

        return df


def main():
    """Example usage"""
    import os

    # Get token from environment
    capitan_token = os.getenv('CAPITAN_API_TOKEN')

    if not capitan_token:
        print("Error: CAPITAN_API_TOKEN environment variable not set")
        return

    # Initialize fetcher
    fetcher = CapitanCheckinFetcher(capitan_token=capitan_token)

    # Fetch last 90 days
    df = fetcher.fetch_and_prepare_data(days_back=90)

    # Display sample
    if not df.empty:
        print("\nSample data:")
        print(df.head())

        # Save to CSV
        output_path = 'data/outputs/capitan_checkins.csv'
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"\n✓ Saved to {output_path}")


if __name__ == "__main__":
    main()

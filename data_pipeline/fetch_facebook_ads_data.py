"""
Facebook Ads Data Fetcher

Fetches Facebook/Instagram ad performance data using the Facebook Marketing API.
Includes metrics like impressions, clicks, spend, conversions, CTR, CPM, and more.
"""

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional
import time


class FacebookAdsDataFetcher:
    """
    A class for fetching and processing Facebook/Instagram Ads data.

    Features:
    - Fetches ads with daily performance metrics
    - Includes impressions, clicks, spend, reach, CTR, CPM, CPP, conversions
    - Supports date range filtering
    - Smart pagination handling
    """

    def __init__(self, access_token: str, ad_account_id: str):
        """
        Initialize the Facebook Ads data fetcher.

        Args:
            access_token: Facebook Graph API access token
            ad_account_id: Facebook Ad Account ID (format: act_XXXXXXXXX)
        """
        self.access_token = access_token
        # Ensure ad_account_id has 'act_' prefix
        if not ad_account_id.startswith('act_'):
            ad_account_id = f'act_{ad_account_id}'
        self.ad_account_id = ad_account_id
        self.base_url = "https://graph.facebook.com/v21.0"

    def _make_api_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make a request to the Facebook Graph API with error handling."""
        if params is None:
            params = {}

        params['access_token'] = self.access_token

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            if hasattr(e.response, 'text'):
                print(f"Response: {e.response.text}")
            return None

    def get_ads_insights(self, days_back: int = 90, time_increment: str = '1') -> List[Dict]:
        """
        Fetch ad insights with daily breakdowns.

        Args:
            days_back: Number of days to fetch (default 90)
            time_increment: Time period for breakdown ('1' for daily, '7' for weekly, default '1')

        Returns:
            List of ad insight dictionaries with daily metrics
        """
        print(f"Fetching ads data for the last {days_back} days...")

        # Calculate date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)

        # Build request URL
        url = f"{self.base_url}/{self.ad_account_id}/insights"

        # Fields to fetch
        fields = [
            'ad_id',
            'ad_name',
            'campaign_id',
            'campaign_name',
            'adset_id',
            'adset_name',
            'impressions',
            'clicks',
            'spend',
            'reach',
            'ctr',
            'cpm',
            'cpp',
            'cpc',  # Cost per click
            'frequency',
            'actions',  # Conversions and other actions
            'cost_per_action_type',
            'action_values',
        ]

        params = {
            'level': 'ad',  # Get data at ad level
            'time_range': json.dumps({
                'since': start_date.strftime('%Y-%m-%d'),
                'until': end_date.strftime('%Y-%m-%d')
            }),
            'time_increment': time_increment,  # Daily breakdown
            'fields': ','.join(fields),
            'limit': 100  # Fetch 100 ads at a time
        }

        all_insights = []
        next_page = None
        page_count = 0

        while True:
            page_count += 1
            print(f"Fetching page {page_count}...")

            if next_page:
                # Use next page URL directly
                response = requests.get(next_page, timeout=30)
                try:
                    response.raise_for_status()
                    data = response.json()
                except requests.exceptions.RequestException as e:
                    print(f"Pagination request failed: {e}")
                    break
            else:
                data = self._make_api_request(url, params)

            if not data or 'data' not in data:
                print("No data returned or error occurred")
                break

            insights = data['data']
            all_insights.extend(insights)
            print(f"  Retrieved {len(insights)} ad insights (Total so far: {len(all_insights)})")

            # Check for next page
            if 'paging' in data and 'next' in data['paging']:
                next_page = data['paging']['next']
                time.sleep(0.5)  # Rate limit protection
            else:
                break

        print(f"✓ Fetched {len(all_insights)} total ad insights")
        return all_insights

    def _extract_conversions(self, actions: List[Dict]) -> Dict[str, int]:
        """Extract conversion metrics from actions array."""
        conversions = {}

        if not actions:
            return conversions

        # Common action types to track
        action_mapping = {
            'link_click': 'link_clicks',
            'post_engagement': 'post_engagements',
            'page_engagement': 'page_engagements',
            'post': 'post_actions',
            'lead': 'leads',
            'purchase': 'purchases',
            'add_to_cart': 'add_to_carts',
            'complete_registration': 'registrations',
        }

        for action in actions:
            action_type = action.get('action_type', '')
            value = int(action.get('value', 0))

            # Map to friendly name
            for key, friendly_name in action_mapping.items():
                if key in action_type:
                    conversions[friendly_name] = conversions.get(friendly_name, 0) + value

        return conversions

    def insights_to_dataframe(self, insights: List[Dict]) -> pd.DataFrame:
        """Convert insights list to pandas DataFrame with proper data types."""
        if not insights:
            print("No insights to convert to DataFrame")
            return pd.DataFrame()

        processed_data = []

        for insight in insights:
            row = {
                'date': insight.get('date_start'),
                'ad_id': insight.get('ad_id'),
                'ad_name': insight.get('ad_name'),
                'campaign_id': insight.get('campaign_id'),
                'campaign_name': insight.get('campaign_name'),
                'adset_id': insight.get('adset_id'),
                'adset_name': insight.get('adset_name'),
                'impressions': int(insight.get('impressions', 0)),
                'clicks': int(insight.get('clicks', 0)),
                'spend': float(insight.get('spend', 0)),
                'reach': int(insight.get('reach', 0)),
                'ctr': float(insight.get('ctr', 0)),
                'cpm': float(insight.get('cpm', 0)),
                'cpp': float(insight.get('cpp', 0)),
                'cpc': float(insight.get('cpc', 0)) if 'cpc' in insight else 0.0,
                'frequency': float(insight.get('frequency', 0)) if 'frequency' in insight else 0.0,
            }

            # Extract conversions
            actions = insight.get('actions', [])
            conversions = self._extract_conversions(actions)
            row.update(conversions)

            processed_data.append(row)

        df = pd.DataFrame(processed_data)

        # Convert date to datetime
        if 'date' in df.columns and not df.empty:
            df['date'] = pd.to_datetime(df['date'])

        # Fill NaN values with 0 for numeric columns
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)

        return df

    def fetch_and_prepare_data(self, days_back: int = 90) -> pd.DataFrame:
        """
        Main method to fetch ads data and return as DataFrame.

        Args:
            days_back: Number of days to fetch (default 90)

        Returns:
            DataFrame with ad performance data
        """
        print(f"\n{'='*60}")
        print(f"Facebook Ads Data Fetch - Last {days_back} Days")
        print(f"{'='*60}\n")

        # Fetch insights
        insights = self.get_ads_insights(days_back=days_back)

        if not insights:
            print("⚠ No insights data fetched")
            return pd.DataFrame()

        # Convert to DataFrame
        df = self.insights_to_dataframe(insights)

        print(f"\n{'='*60}")
        print(f"✓ Successfully fetched {len(df)} ad records")
        print(f"  Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"  Total spend: ${df['spend'].sum():.2f}")
        print(f"  Total impressions: {df['impressions'].sum():,}")
        print(f"  Total clicks: {df['clicks'].sum():,}")
        print(f"  Average CTR: {df['ctr'].mean():.2f}%")
        print(f"{'='*60}\n")

        return df


def main():
    """Example usage"""
    # Get credentials from environment
    access_token = os.getenv('INSTAGRAM_ACCESS_TOKEN')  # Same token as Instagram
    ad_account_id = '272120788771569'  # Basin ad account

    if not access_token:
        print("Error: INSTAGRAM_ACCESS_TOKEN environment variable not set")
        return

    # Initialize fetcher
    fetcher = FacebookAdsDataFetcher(
        access_token=access_token,
        ad_account_id=ad_account_id
    )

    # Fetch last 90 days
    df = fetcher.fetch_and_prepare_data(days_back=90)

    # Display sample
    if not df.empty:
        print("\nSample data:")
        print(df.head())

        # Save to CSV
        output_path = 'data/outputs/facebook_ads_data.csv'
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"\n✓ Saved to {output_path}")


if __name__ == "__main__":
    main()

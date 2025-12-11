"""
Google Analytics 4 Data Fetcher

Fetches page view and visitor behavior data from Google Analytics 4.

GitHub Actions Setup:
    For CI/CD, store credentials as a GitHub secret and write to temp file:

    - name: Setup GA4 Credentials
      run: |
        echo '${{ secrets.GA4_CREDENTIALS_JSON }}' > /tmp/ga4-credentials.json
        echo "GA4_CREDENTIALS_PATH=/tmp/ga4-credentials.json" >> $GITHUB_ENV
"""

import pandas as pd
from datetime import datetime, timedelta
import os
import json
import tempfile
from typing import Dict, List, Optional
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    OrderBy
)


class GA4DataFetcher:
    """
    A class for fetching and processing Google Analytics 4 data.

    Features:
    - Fetches page view data with visitor counts
    - Fetches event data (add to cart, purchases, etc.)
    - Tracks user behavior and page paths
    - Supports both file-based credentials (local) and JSON string (CI/CD)
    """

    def __init__(self, property_id: str, credentials_path: Optional[str] = None, credentials_json: Optional[str] = None):
        """
        Initialize the GA4 data fetcher.

        Args:
            property_id: GA4 Property ID (numeric)
            credentials_path: Path to service account JSON credentials (for local use)
            credentials_json: Service account JSON as string (for CI/CD use)

        Note: Provide either credentials_path OR credentials_json, not both.
              For GitHub Actions, use credentials_json with secrets.GA4_CREDENTIALS_JSON
        """
        self.property_id = property_id
        self._temp_creds_file = None

        # Handle credentials setup
        if credentials_json:
            # GitHub Actions / CI/CD mode: JSON string provided
            # Write to temporary file and use that
            self._temp_creds_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
            self._temp_creds_file.write(credentials_json)
            self._temp_creds_file.close()
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self._temp_creds_file.name
        elif credentials_path:
            # Local mode: File path provided
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        else:
            raise ValueError("Must provide either credentials_path or credentials_json")

        # Initialize GA4 client
        self.client = BetaAnalyticsDataClient()

    def __del__(self):
        """Clean up temporary credentials file if created."""
        if self._temp_creds_file and os.path.exists(self._temp_creds_file.name):
            try:
                os.unlink(self._temp_creds_file.name)
            except:
                pass

    def get_page_views(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch page view data for the specified time period.

        Args:
            days_back: Number of days of history to fetch

        Returns:
            DataFrame with columns: date, page_path, page_title, views, users, sessions
        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="pagePath"),
                Dimension(name="pageTitle")
            ],
            metrics=[
                Metric(name="screenPageViews"),
                Metric(name="activeUsers"),
                Metric(name="sessions")
            ],
            date_ranges=[DateRange(
                start_date=f"{days_back}daysAgo",
                end_date="today"
            )],
            order_bys=[
                OrderBy(dimension={'dimension_name': 'date'}),
                OrderBy(metric={'metric_name': 'screenPageViews'}, desc=True)
            ]
        )

        response = self.client.run_report(request)

        # Convert to list of dicts
        rows = []
        for row in response.rows:
            rows.append({
                'date': row.dimension_values[0].value,
                'page_path': row.dimension_values[1].value,
                'page_title': row.dimension_values[2].value,
                'views': int(row.metric_values[0].value),
                'users': int(row.metric_values[1].value),
                'sessions': int(row.metric_values[2].value)
            })

        df = pd.DataFrame(rows)

        # Convert date to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        return df

    def get_events(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch event data (add_to_cart, purchase, etc.) for the specified time period.

        Args:
            days_back: Number of days of history to fetch

        Returns:
            DataFrame with columns: date, event_name, count, users
        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="eventName")
            ],
            metrics=[
                Metric(name="eventCount"),
                Metric(name="activeUsers")
            ],
            date_ranges=[DateRange(
                start_date=f"{days_back}daysAgo",
                end_date="today"
            )],
            order_bys=[
                OrderBy(dimension={'dimension_name': 'date'}),
                OrderBy(metric={'metric_name': 'eventCount'}, desc=True)
            ]
        )

        response = self.client.run_report(request)

        # Convert to list of dicts
        rows = []
        for row in response.rows:
            rows.append({
                'date': row.dimension_values[0].value,
                'event_name': row.dimension_values[1].value,
                'event_count': int(row.metric_values[0].value),
                'users': int(row.metric_values[1].value)
            })

        df = pd.DataFrame(rows)

        # Convert date to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        return df

    def get_user_activity(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch daily user activity metrics.

        Args:
            days_back: Number of days of history to fetch

        Returns:
            DataFrame with columns: date, active_users, new_users, sessions,
                                   bounce_rate, avg_session_duration
        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date")
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="newUsers"),
                Metric(name="sessions"),
                Metric(name="bounceRate"),
                Metric(name="averageSessionDuration")
            ],
            date_ranges=[DateRange(
                start_date=f"{days_back}daysAgo",
                end_date="today"
            )],
            order_bys=[
                OrderBy(dimension={'dimension_name': 'date'})
            ]
        )

        response = self.client.run_report(request)

        # Convert to list of dicts
        rows = []
        for row in response.rows:
            rows.append({
                'date': row.dimension_values[0].value,
                'active_users': int(row.metric_values[0].value),
                'new_users': int(row.metric_values[1].value),
                'sessions': int(row.metric_values[2].value),
                'bounce_rate': float(row.metric_values[3].value),
                'avg_session_duration': float(row.metric_values[4].value)
            })

        df = pd.DataFrame(rows)

        # Convert date to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        return df

    def get_product_views(self, days_back: int = 7) -> pd.DataFrame:
        """
        Fetch e-commerce product data (items purchased/added to cart).

        Args:
            days_back: Number of days of history to fetch

        Returns:
            DataFrame with columns: date, item_name, items_added_to_cart, items_purchased
        """
        request = RunReportRequest(
            property=f"properties/{self.property_id}",
            dimensions=[
                Dimension(name="date"),
                Dimension(name="itemName")
            ],
            metrics=[
                Metric(name="itemsAddedToCart"),
                Metric(name="itemsPurchased")
            ],
            date_ranges=[DateRange(
                start_date=f"{days_back}daysAgo",
                end_date="today"
            )],
            order_bys=[
                OrderBy(dimension={'dimension_name': 'date'}),
                OrderBy(metric={'metric_name': 'itemsPurchased'}, desc=True)
            ]
        )

        response = self.client.run_report(request)

        # Convert to list of dicts
        rows = []
        for row in response.rows:
            rows.append({
                'date': row.dimension_values[0].value,
                'item_name': row.dimension_values[1].value,
                'items_added_to_cart': int(row.metric_values[0].value),
                'items_purchased': int(row.metric_values[1].value)
            })

        df = pd.DataFrame(rows)

        # Convert date to datetime
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        return df

    def fetch_all_data(self, days_back: int = 7) -> Dict[str, pd.DataFrame]:
        """
        Fetch all GA4 data at once.

        Args:
            days_back: Number of days of history to fetch

        Returns:
            Dictionary with keys: 'page_views', 'events', 'user_activity', 'product_views'
        """
        print(f"Fetching GA4 page views (last {days_back} days)...")
        page_views = self.get_page_views(days_back)
        print(f"  → {len(page_views)} page view records")

        print(f"Fetching GA4 events (last {days_back} days)...")
        events = self.get_events(days_back)
        print(f"  → {len(events)} event records")

        print(f"Fetching GA4 user activity (last {days_back} days)...")
        user_activity = self.get_user_activity(days_back)
        print(f"  → {len(user_activity)} daily activity records")

        print(f"Fetching GA4 product views (last {days_back} days)...")
        product_views = self.get_product_views(days_back)
        print(f"  → {len(product_views)} product view records")

        return {
            'page_views': page_views,
            'events': events,
            'user_activity': user_activity,
            'product_views': product_views
        }


if __name__ == "__main__":
    # Test the fetcher
    # Load .env for local testing
    try:
        from dotenv import load_dotenv
        from pathlib import Path

        # Try to find .env in project root
        current_file = Path(__file__)
        project_root = current_file.parent.parent
        dotenv_path = project_root / '.env'

        if dotenv_path.exists():
            load_dotenv(dotenv_path)
        else:
            load_dotenv()  # Try default behavior
    except ImportError:
        pass

    from data_pipeline import config

    fetcher = GA4DataFetcher(
        property_id=config.ga4_property_id,
        credentials_path=config.ga4_credentials_path
    )

    print("\n=== Testing GA4 Data Fetcher ===\n")

    data = fetcher.fetch_all_data(days_back=7)

    print("\n=== Summary ===")
    print(f"Page views: {len(data['page_views'])} records")
    print(f"Events: {len(data['events'])} records")
    print(f"User activity: {len(data['user_activity'])} records")
    print(f"Product views: {len(data['product_views'])} records")

    if not data['page_views'].empty:
        print("\n=== Sample Page Views ===")
        print(data['page_views'].head())

    if not data['events'].empty:
        print("\n=== Sample Events ===")
        print(data['events'].head())

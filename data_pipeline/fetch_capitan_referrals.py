"""
Fetch membership referrals from Capitan API.

Tracks who referred whom for new memberships.
"""

import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime
from typing import Optional, Tuple

from data_pipeline import config


class CapitanReferralFetcher:
    """Fetches membership referral data from Capitan."""

    BASE_URL = "https://api.hellocapitan.com/api"

    def __init__(self):
        self.token = config.capitan_token
        if not self.token:
            raise ValueError("CAPITAN_API_TOKEN not set")

        self.headers = {"Authorization": f"token {self.token}"}
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key
        )

    def fetch_referrals(self) -> pd.DataFrame:
        """
        Fetch all membership referrals from Capitan.

        Returns DataFrame with columns:
        - referral_id
        - referrer_customer_id
        - referrer_name
        - referrer_email
        - referred_customer_id (owner of new membership)
        - referred_name
        - referred_email
        - membership_name
        - membership_status
        - purchase_date
        - created_at
        """
        print("Fetching membership referrals from Capitan...")

        referrals = []
        url = f"{self.BASE_URL}/membership-referrals/"
        params = {"page_size": 100}

        while url:
            response = requests.get(url, headers=self.headers, params=params)

            if response.status_code != 200:
                print(f"   Error fetching referrals: {response.status_code}")
                break

            data = response.json()
            results = data.get('results', [])

            for r in results:
                referrals.append({
                    'referral_id': r.get('id'),
                    'referrer_customer_id': r.get('referring_customer_id'),
                    'referrer_name': f"{r.get('referring_customer_first_name', '')} {r.get('referring_customer_last_name', '')}".strip(),
                    'referrer_email': r.get('referring_customer_email'),
                    'referred_customer_id': r.get('owner_id'),
                    'referred_name': f"{r.get('owner_first_name', '')} {r.get('owner_last_name', '')}".strip(),
                    'referred_email': r.get('owner_email'),
                    'membership_id': r.get('membership_id'),
                    'membership_name': r.get('name'),
                    'membership_status': r.get('membership_status'),
                    'purchase_date': r.get('purchase_completed_at'),
                    'created_at': r.get('created_at'),
                })

            # Handle pagination
            url = data.get('next')
            params = {}  # Clear params for next page (included in URL)

        df = pd.DataFrame(referrals)
        print(f"   Found {len(df)} referrals")

        return df

    def build_referral_leaderboard(self, df_referrals: pd.DataFrame) -> pd.DataFrame:
        """
        Build a referral leaderboard showing top referrers.

        Returns DataFrame with:
        - referrer_customer_id
        - referrer_name
        - referrer_email
        - total_referrals
        - active_referrals (where membership is still active)
        - referred_names (list)
        - first_referral_date
        - last_referral_date
        """
        if df_referrals.empty:
            return pd.DataFrame()

        # Group by referrer
        leaderboard = df_referrals.groupby(['referrer_customer_id', 'referrer_name', 'referrer_email']).agg({
            'referral_id': 'count',
            'membership_status': lambda x: (x == 'ACT').sum(),
            'referred_name': lambda x: ', '.join(x.tolist()),
            'purchase_date': ['min', 'max']
        }).reset_index()

        # Flatten column names
        leaderboard.columns = [
            'referrer_customer_id', 'referrer_name', 'referrer_email',
            'total_referrals', 'active_referrals', 'referred_names',
            'first_referral_date', 'last_referral_date'
        ]

        # Sort by total referrals
        leaderboard = leaderboard.sort_values('total_referrals', ascending=False)

        return leaderboard

    def save_to_s3(self, df: pd.DataFrame, s3_key: str):
        """Save DataFrame to S3."""
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        self.s3_client.put_object(
            Bucket=config.aws_bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        print(f"   Saved to s3://{config.aws_bucket_name}/{s3_key}")

    def fetch_and_save(self, save_local: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch referrals and save to S3.

        Returns:
            Tuple of (referrals_df, leaderboard_df)
        """
        # Fetch referrals
        df_referrals = self.fetch_referrals()

        if df_referrals.empty:
            print("   No referrals found")
            return df_referrals, pd.DataFrame()

        # Build leaderboard
        df_leaderboard = self.build_referral_leaderboard(df_referrals)

        # Save to S3
        self.save_to_s3(df_referrals, config.s3_path_capitan_referrals)
        self.save_to_s3(df_leaderboard, config.s3_path_capitan_referral_leaderboard)

        # Optionally save locally
        if save_local:
            os.makedirs('data/outputs', exist_ok=True)
            df_referrals.to_csv('data/outputs/capitan_referrals.csv', index=False)
            df_leaderboard.to_csv('data/outputs/capitan_referral_leaderboard.csv', index=False)
            print("   Saved local copies to data/outputs/")

        return df_referrals, df_leaderboard


def fetch_capitan_referrals(save_local: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Convenience function to fetch and save referrals."""
    fetcher = CapitanReferralFetcher()
    return fetcher.fetch_and_save(save_local=save_local)


if __name__ == "__main__":
    df_referrals, df_leaderboard = fetch_capitan_referrals(save_local=True)

    print("\n=== Referral Leaderboard ===")
    print(df_leaderboard.to_string(index=False))

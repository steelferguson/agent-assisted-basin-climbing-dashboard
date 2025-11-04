"""
Family Day Pass Identifier

Identifies families (people with same last name) who bought day passes together on the same day.
Useful for targeted marketing to convert frequent day pass users to memberships.
"""

import pandas as pd
import boto3
from datetime import datetime
from typing import Dict
import os


class FamilyDayPassIdentifier:
    """
    A class for identifying families who purchase day passes together.

    Logic:
    - Groups by last name and date
    - Identifies day pass purchases (not memberships)
    - Filters to groups of 2+ people (families)
    """

    def __init__(self, df_checkins: pd.DataFrame):
        self.df_checkins = df_checkins.copy()

    def identify_family_day_passes(self, min_family_size: int = 2) -> pd.DataFrame:
        """
        Identify families who bought day passes together.

        Args:
            min_family_size: Minimum number of people to consider a "family" (default 2)

        Returns:
            DataFrame with family day pass records including emails
        """
        print(f"Identifying families with {min_family_size}+ members buying day passes...")

        # Filter to day pass entries (exclude membership check-ins)
        day_pass_keywords = ['Day Pass', 'Punch Pass', 'Pass with Gear']

        # Create a mask for day pass entries
        mask = self.df_checkins['entry_method_description'].str.contains(
            '|'.join(day_pass_keywords),
            case=False,
            na=False,
            regex=True
        )

        df_day_passes = self.df_checkins[mask].copy()
        print(f"  Found {len(df_day_passes):,} day pass check-ins")

        # Extract date (without time) for grouping
        df_day_passes['date'] = pd.to_datetime(df_day_passes['checkin_datetime']).dt.date

        # Group by last name and date to find families
        grouped = df_day_passes.groupby(['customer_last_name', 'date'])

        family_groups = []
        for (last_name, date), group in grouped:
            # Only include groups with min_family_size or more people
            if len(group) >= min_family_size:
                family_groups.append(group)

        if not family_groups:
            print("  No families found")
            return pd.DataFrame()

        # Combine all family groups
        df_families = pd.concat(family_groups, ignore_index=True)

        # Select and order columns
        df_result = df_families[[
            'customer_first_name',
            'customer_last_name',
            'date',
            'customer_id',
            'customer_email',
            'entry_method_description'
        ]].copy()

        # Rename columns for clarity
        df_result = df_result.rename(columns={
            'customer_first_name': 'first_name',
            'customer_last_name': 'last_name',
            'customer_email': 'email',
            'entry_method_description': 'pass_type'
        })

        # Sort by date (most recent first), then by last name
        df_result = df_result.sort_values(['date', 'last_name', 'first_name'],
                                          ascending=[False, True, True])

        # Get family statistics
        num_families = len(df_result.groupby(['last_name', 'date']))

        print(f"✓ Identified {num_families} families ({len(df_result)} total people)")

        return df_result


def load_checkins_from_s3(aws_access_key_id: str, aws_secret_access_key: str,
                          bucket_name: str) -> pd.DataFrame:
    """
    Load check-in data from S3.

    Args:
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        bucket_name: S3 bucket name

    Returns:
        DataFrame with check-in data
    """
    print("Loading check-ins from S3...")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    checkins_obj = s3.get_object(Bucket=bucket_name, Key='capitan/checkins.csv')
    df_checkins = pd.read_csv(checkins_obj['Body'])
    df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'])

    print(f"✓ Loaded {len(df_checkins):,} check-in records")

    return df_checkins


def main():
    """
    Main function to identify family day passes and save results.
    """
    # Get credentials from environment
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = 'basin-climbing-data-prod'

    if not aws_access_key_id or not aws_secret_access_key:
        print("Error: AWS credentials not set in environment variables")
        return

    print(f"\n{'='*60}")
    print("Family Day Pass Identification")
    print(f"{'='*60}\n")

    # Load check-ins from S3
    df_checkins = load_checkins_from_s3(aws_access_key_id, aws_secret_access_key, bucket_name)

    # Initialize identifier
    identifier = FamilyDayPassIdentifier(df_checkins=df_checkins)

    # Identify family day passes
    df_families = identifier.identify_family_day_passes(min_family_size=2)

    if df_families.empty:
        print("No family day passes found")
        return

    # Save locally
    output_path = 'data/outputs/family_day_passes.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df_families.to_csv(output_path, index=False)

    print(f"\n{'='*60}")
    print(f"✓ Saved to {output_path}")
    print(f"{'='*60}\n")

    # Display sample
    print("Sample families:")
    print(df_families.head(10)[['first_name', 'last_name', 'date', 'email', 'pass_type']])

    # Display summary statistics
    print(f"\n{'='*60}")
    print("Summary Statistics:")
    print(f"{'='*60}")

    # Count families by size
    family_sizes = df_families.groupby(['last_name', 'date']).size()
    print("\nFamily sizes:")
    print(family_sizes.value_counts().sort_index())

    # Most recent families
    print(f"\nMost recent date: {df_families['date'].max()}")
    print(f"Oldest date: {df_families['date'].min()}")

    # Families by month
    df_families['month'] = pd.to_datetime(df_families['date']).dt.to_period('M')
    monthly_counts = df_families.groupby('month')['last_name'].apply(
        lambda x: len(set(x))
    )
    print(f"\nFamilies per month (last 6 months):")
    print(monthly_counts.tail(6))


if __name__ == "__main__":
    main()

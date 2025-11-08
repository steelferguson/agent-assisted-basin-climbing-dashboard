"""
At-Risk Members Identifier

Analyzes check-in and membership data to identify members who may need attention.
Uses simple time-based categories: recent activity (2 weeks) vs medium-term activity (2 months).
"""

import pandas as pd
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os


class AtRiskMemberIdentifier:
    """
    A class for identifying at-risk members based on recent check-in patterns.

    Risk Categories:
    1. Declining Activity - No check-ins in last 2 weeks, but had check-ins in last 2 months
    2. Very Inactive - Active members with no check-ins in last 2 months
    """

    def __init__(self, df_checkins: pd.DataFrame, df_members: pd.DataFrame,
                 df_memberships: pd.DataFrame):
        """
        Initialize with check-in, member, and membership data.

        Args:
            df_checkins: Check-in records with customer_id, checkin_datetime, customer_birthday
            df_members: Member details with member_id, status, name (membership type), and is_bcf
            df_memberships: Membership details (not used - members already has membership info)
        """
        self.df_checkins = df_checkins.copy()
        self.df_members = df_members.copy()
        self.df_memberships = df_memberships.copy()  # Keep for compatibility
        self.today = datetime.now()

    def _calculate_age(self, birthday) -> Optional[int]:
        """Calculate age from birthday."""
        if pd.isna(birthday):
            return None
        try:
            birthday_dt = pd.to_datetime(birthday)
            age = (self.today - birthday_dt).days // 365
            return age if age >= 0 and age < 120 else None
        except:
            return None

    def _count_checkins_in_period(self, customer_id: int, start_date: datetime,
                                  end_date: datetime) -> int:
        """Count check-ins for a customer in a date range."""
        mask = (
            (self.df_checkins['customer_id'] == customer_id) &
            (self.df_checkins['checkin_datetime'] >= start_date) &
            (self.df_checkins['checkin_datetime'] <= end_date)
        )
        return len(self.df_checkins[mask])

    def identify_declining_activity(self) -> pd.DataFrame:
        """
        Identify members with no check-ins in last 2 weeks, but had check-ins in last 2 months.

        Returns:
            DataFrame with at-risk members in this category
        """
        print(f"Identifying members with declining activity...")

        two_weeks_ago = self.today - timedelta(weeks=2)
        two_months_ago = self.today - timedelta(days=60)

        # Get active members who have had membership for at least 2 weeks
        active_members = self.df_members[
            self.df_members['status'].isin(['ACT', 'active', 'trialing'])
        ].copy()

        # Filter to members whose start_date is at least 2 weeks ago
        active_members['start_date'] = pd.to_datetime(active_members['start_date'], errors='coerce')
        active_members = active_members[active_members['start_date'] <= two_weeks_ago]

        at_risk = []

        for _, member in active_members.iterrows():
            customer_id = member['member_id']

            # Get membership type and BCF status directly from member record
            membership_type = member.get('name', 'Unknown')
            is_bcf = member.get('is_bcf', False)

            # Skip BCF memberships
            if is_bcf:
                continue

            # Count check-ins in last 2 weeks
            checkins_2weeks = self._count_checkins_in_period(
                customer_id, two_weeks_ago, self.today
            )

            # Count check-ins in last 2 months
            checkins_2months = self._count_checkins_in_period(
                customer_id, two_months_ago, self.today
            )

            # Flag if: 0 check-ins in last 2 weeks BUT had check-ins in last 2 months
            if checkins_2weeks == 0 and checkins_2months > 0:
                # Get last check-in date
                member_checkins = self.df_checkins[
                    self.df_checkins['customer_id'] == customer_id
                ].sort_values('checkin_datetime', ascending=False)

                last_checkin = member_checkins.iloc[0]['checkin_datetime'] if len(member_checkins) > 0 else None

                # Get birthday/age from most recent check-in
                age = None
                if len(member_checkins) > 0:
                    birthday = member_checkins.iloc[0].get('customer_birthday')
                    age = self._calculate_age(birthday)

                at_risk.append({
                    'customer_id': customer_id,
                    'first_name': member.get('member_first_name', ''),
                    'last_name': member.get('member_last_name', ''),
                    'age': age,
                    'membership_type': membership_type,
                    'last_checkin_date': last_checkin,
                    'checkins_last_2_weeks': checkins_2weeks,
                    'checkins_last_2_months': checkins_2months,
                    'risk_category': 'Declining Activity',
                    'risk_description': f'No visits in 2 weeks (had {checkins_2months} in last 2 months)'
                })

        print(f"  Found {len(at_risk)} members with declining activity")
        return pd.DataFrame(at_risk)

    def identify_very_inactive(self) -> pd.DataFrame:
        """
        Identify active members with no check-ins in last 2 months.

        Returns:
            DataFrame with at-risk members in this category
        """
        print(f"Identifying very inactive members...")

        two_weeks_ago = self.today - timedelta(weeks=2)
        two_months_ago = self.today - timedelta(days=60)

        # Get active members who have had membership for at least 2 weeks
        active_members = self.df_members[
            self.df_members['status'].isin(['ACT', 'active', 'trialing'])
        ].copy()

        # Filter to members whose start_date is at least 2 weeks ago
        active_members['start_date'] = pd.to_datetime(active_members['start_date'], errors='coerce')
        active_members = active_members[active_members['start_date'] <= two_weeks_ago]

        at_risk = []

        for _, member in active_members.iterrows():
            customer_id = member['member_id']

            # Get membership type and BCF status directly from member record
            membership_type = member.get('name', 'Unknown')
            is_bcf = member.get('is_bcf', False)

            # Skip BCF memberships
            if is_bcf:
                continue

            # Count check-ins in last 2 months
            checkins_2months = self._count_checkins_in_period(
                customer_id, two_months_ago, self.today
            )

            # Flag if: 0 check-ins in last 2 months
            if checkins_2months == 0:
                # Get last check-in date (could be older than 2 months)
                member_checkins = self.df_checkins[
                    self.df_checkins['customer_id'] == customer_id
                ].sort_values('checkin_datetime', ascending=False)

                last_checkin = member_checkins.iloc[0]['checkin_datetime'] if len(member_checkins) > 0 else None

                # Get birthday/age from most recent check-in
                age = None
                if len(member_checkins) > 0:
                    birthday = member_checkins.iloc[0].get('customer_birthday')
                    age = self._calculate_age(birthday)

                # Calculate days since last check-in
                days_since_checkin = None
                if last_checkin:
                    days_since_checkin = (self.today - last_checkin).days

                at_risk.append({
                    'customer_id': customer_id,
                    'first_name': member.get('member_first_name', ''),
                    'last_name': member.get('member_last_name', ''),
                    'age': age,
                    'membership_type': membership_type,
                    'last_checkin_date': last_checkin,
                    'checkins_last_2_weeks': 0,
                    'checkins_last_2_months': 0,
                    'risk_category': 'Very Inactive',
                    'risk_description': f'No visits in 2+ months' + (f' (last visit {days_since_checkin} days ago)' if days_since_checkin else ' (never visited)')
                })

        print(f"  Found {len(at_risk)} very inactive members")
        return pd.DataFrame(at_risk)

    def identify_all_at_risk(self) -> pd.DataFrame:
        """
        Run all at-risk identification methods and combine results.

        Returns:
            Combined DataFrame with all at-risk members
        """
        print(f"\n{'='*60}")
        print("Identifying At-Risk Members")
        print(f"{'='*60}\n")

        # Run both identification methods
        declining = self.identify_declining_activity()
        very_inactive = self.identify_very_inactive()

        # Combine results
        all_at_risk = pd.concat([
            declining,
            very_inactive
        ], ignore_index=True)

        # Remove duplicates (shouldn't happen with current logic, but safety check)
        all_at_risk = all_at_risk.drop_duplicates(subset=['customer_id'], keep='first')

        # Add Capitan link
        all_at_risk['capitan_link'] = all_at_risk['customer_id'].apply(
            lambda x: f"https://app.hellocapitan.com/customers/{x}/check-ins"
        )

        # Add "as of" timestamp
        all_at_risk['generated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Sort by category, then by last check-in date (most recent first)
        category_order = ['Declining Activity', 'Very Inactive']
        all_at_risk['category_order'] = all_at_risk['risk_category'].map(
            {cat: i for i, cat in enumerate(category_order)}
        )
        all_at_risk = all_at_risk.sort_values(
            ['category_order', 'last_checkin_date'],
            ascending=[True, False]
        ).drop('category_order', axis=1)

        print(f"\n{'='*60}")
        print(f"✓ Identified {len(all_at_risk)} total at-risk members")
        print(f"\nBreakdown by category:")
        for category in category_order:
            count = len(all_at_risk[all_at_risk['risk_category'] == category])
            print(f"  {category}: {count}")
        print(f"{'='*60}\n")

        return all_at_risk


def load_data_from_s3(aws_access_key_id: str, aws_secret_access_key: str,
                      bucket_name: str) -> Dict[str, pd.DataFrame]:
    """
    Load check-in, member, and membership data from S3.

    Args:
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        bucket_name: S3 bucket name

    Returns:
        Dictionary with 'checkins', 'members', 'memberships' DataFrames
    """
    print("Loading data from S3...")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    data = {}

    # Load check-ins
    print("  Loading check-ins...")
    checkins_obj = s3.get_object(Bucket=bucket_name, Key='capitan/checkins.csv')
    data['checkins'] = pd.read_csv(checkins_obj['Body'])
    data['checkins']['checkin_datetime'] = pd.to_datetime(data['checkins']['checkin_datetime'])

    # Load members
    print("  Loading members...")
    members_obj = s3.get_object(Bucket=bucket_name, Key='capitan/members.csv')
    data['members'] = pd.read_csv(members_obj['Body'])

    # Load memberships
    print("  Loading memberships...")
    memberships_obj = s3.get_object(Bucket=bucket_name, Key='capitan/memberships.csv')
    data['memberships'] = pd.read_csv(memberships_obj['Body'])

    print(f"✓ Loaded data:")
    print(f"  Check-ins: {len(data['checkins']):,} records")
    print(f"  Members: {len(data['members']):,} records")
    print(f"  Memberships: {len(data['memberships']):,} records\n")

    return data


def main():
    """
    Main function to identify at-risk members and save results.
    Can be run as a batch job.
    """
    # Get credentials from environment
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    bucket_name = 'basin-climbing-data-prod'

    if not aws_access_key_id or not aws_secret_access_key:
        print("Error: AWS credentials not set in environment variables")
        return

    # Load data from S3
    data = load_data_from_s3(aws_access_key_id, aws_secret_access_key, bucket_name)

    # Initialize identifier
    identifier = AtRiskMemberIdentifier(
        df_checkins=data['checkins'],
        df_members=data['members'],
        df_memberships=data['memberships']
    )

    # Identify all at-risk members
    at_risk_df = identifier.identify_all_at_risk()

    # Save locally
    output_path = 'data/outputs/at_risk_members.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    at_risk_df.to_csv(output_path, index=False)
    print(f"✓ Saved to {output_path}")

    # Upload to S3
    print(f"\nUploading to S3...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_key = 'capitan/at_risk_members.csv'
    s3.upload_file(output_path, bucket_name, s3_key)
    print(f"✓ Uploaded to s3://{bucket_name}/{s3_key}")

    # Display sample
    print(f"\nSample at-risk members:")
    print(at_risk_df.head(10)[['customer_id', 'first_name', 'last_name', 'age', 'membership_type', 'risk_category', 'risk_description']])


if __name__ == "__main__":
    main()

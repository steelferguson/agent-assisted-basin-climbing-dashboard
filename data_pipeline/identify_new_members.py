"""
New Members Report

Identifies members who joined in the last 28 days.
Excludes BCF staff/family memberships.
Used for onboarding, engagement tracking, and dashboard reporting.
"""

import pandas as pd
import boto3
from datetime import datetime, timedelta
from typing import Dict, Optional
import os


class NewMemberIdentifier:
    """
    A class for identifying new members based on membership start date.
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

    def _count_checkins_total(self, customer_id: int) -> int:
        """Count total check-ins for a customer."""
        mask = (self.df_checkins['customer_id'] == customer_id)
        return len(self.df_checkins[mask])

    def identify_new_members(self, days_back: int = 28) -> pd.DataFrame:
        """
        Identify members who joined in the last N days.

        Args:
            days_back: Number of days to look back (default: 28)

        Returns:
            DataFrame with new members
        """
        print(f"Identifying members who joined in last {days_back} days...")

        cutoff_date = self.today - timedelta(days=days_back)

        # Get active members (current and trialing)
        active_members = self.df_members[
            self.df_members['status'].isin(['ACT', 'active', 'trialing'])
        ].copy()

        # Parse start_date
        active_members['start_date'] = pd.to_datetime(active_members['start_date'], errors='coerce')

        # Filter to members who started after cutoff date
        new_members = active_members[active_members['start_date'] >= cutoff_date].copy()

        print(f"  Found {len(new_members)} active members who started after {cutoff_date.strftime('%Y-%m-%d')}")

        new_member_records = []

        for _, member in new_members.iterrows():
            # Use customer_id (Capitan ID) for URLs, not member_id
            customer_id = member.get('customer_id', member['member_id'])

            # Get membership type and BCF status directly from member record
            membership_type = member.get('name', 'Unknown')
            is_bcf = member.get('is_bcf', False)

            # Skip BCF memberships
            if is_bcf:
                continue

            # Get start date
            start_date = member['start_date']

            # Calculate days since joining
            days_since_joining = (self.today - start_date).days if pd.notna(start_date) else None

            # Count total check-ins
            total_checkins = self._count_checkins_total(customer_id)

            # Get most recent check-in and age
            member_checkins = self.df_checkins[
                self.df_checkins['customer_id'] == customer_id
            ].sort_values('checkin_datetime', ascending=False)

            last_checkin = member_checkins.iloc[0]['checkin_datetime'] if len(member_checkins) > 0 else None

            # Get birthday/age from most recent check-in
            age = None
            if len(member_checkins) > 0:
                birthday = member_checkins.iloc[0].get('customer_birthday')
                age = self._calculate_age(birthday)

            new_member_records.append({
                'customer_id': customer_id,
                'first_name': member.get('member_first_name', ''),
                'last_name': member.get('member_last_name', ''),
                'age': age,
                'membership_type': membership_type,
                'start_date': start_date,
                'days_since_joining': days_since_joining,
                'total_checkins': total_checkins,
                'last_checkin_date': last_checkin,
            })

        df = pd.DataFrame(new_member_records)

        # Sort by start_date (newest first)
        if len(df) > 0:
            df = df.sort_values('start_date', ascending=False)

        print(f"  After filtering BCF memberships: {len(df)} new members")
        return df

    def generate_report(self, days_back: int = 28) -> pd.DataFrame:
        """
        Generate complete new members report.

        Args:
            days_back: Number of days to look back (default: 28)

        Returns:
            DataFrame with new members report
        """
        print(f"\n{'='*60}")
        print(f"New Members Report (Last {days_back} Days)")
        print(f"{'='*60}\n")

        # Identify new members
        new_members_df = self.identify_new_members(days_back=days_back)

        if len(new_members_df) > 0:
            # Add Capitan link
            new_members_df['capitan_link'] = new_members_df['customer_id'].apply(
                lambda x: f"https://app.hellocapitan.com/customers/{x}/check-ins"
            )

            # Add "as of" timestamp
            new_members_df['generated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print(f"\n{'='*60}")
        print(f"✓ Identified {len(new_members_df)} new members")

        if len(new_members_df) > 0:
            print(f"\nBreakdown by membership type:")
            type_counts = new_members_df['membership_type'].value_counts()
            for membership_type, count in type_counts.items():
                print(f"  {membership_type}: {count}")

            avg_checkins = new_members_df['total_checkins'].mean()
            print(f"\nAverage check-ins per new member: {avg_checkins:.1f}")

        print(f"{'='*60}\n")

        return new_members_df


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
    Main function to generate new members report and save results.
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
    identifier = NewMemberIdentifier(
        df_checkins=data['checkins'],
        df_members=data['members'],
        df_memberships=data['memberships']
    )

    # Generate report (last 28 days)
    new_members_df = identifier.generate_report(days_back=28)

    # Save locally
    output_path = 'data/outputs/new_members.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    new_members_df.to_csv(output_path, index=False)
    print(f"✓ Saved to {output_path}")

    # Upload to S3
    print(f"\nUploading to S3...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )

    s3_key = 'capitan/new_members.csv'
    s3.upload_file(output_path, bucket_name, s3_key)
    print(f"✓ Uploaded to s3://{bucket_name}/{s3_key}")

    # Display sample
    if len(new_members_df) > 0:
        print(f"\nSample new members (most recent first):")
        print(new_members_df.head(10)[['customer_id', 'first_name', 'last_name', 'age', 'membership_type', 'start_date', 'days_since_joining', 'total_checkins']])


if __name__ == "__main__":
    main()

"""
Investigate if we can detect membership sharing/pass lending
Using Capitan data to see if different people check in on the same membership
"""

import pandas as pd
import boto3
import os
from io import StringIO

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def load_csv_from_s3(bucket_name, s3_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Error loading {s3_key}: {e}")
        return None

print("\n" + "="*80)
print("INVESTIGATING MEMBERSHIP SHARING / PASS LENDING")
print("Can we detect if different people use the same membership?")
print("="*80)

# Load relevant data
df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
df_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/members.csv")
df_checkins = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/checkins.csv")
df_customers = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/customers.csv")

print("\n" + "="*80)
print("DATA AVAILABILITY")
print("="*80)

if df_memberships is not None:
    print(f"\n✓ Memberships: {len(df_memberships)} records")
    print(f"  Columns: {df_memberships.columns.tolist()}")

if df_members is not None:
    print(f"\n✓ Members: {len(df_members)} records")
    print(f"  Columns: {df_members.columns.tolist()}")

if df_checkins is not None:
    print(f"\n✓ Check-ins: {len(df_checkins)} records")
    print(f"  Columns: {df_checkins.columns.tolist()}")

if df_customers is not None:
    print(f"\n✓ Customers: {len(df_customers)} records")
    print(f"  Columns: {df_customers.columns.tolist()}")

# Analyze check-in data structure
if df_checkins is not None:
    print("\n\n" + "="*80)
    print("CHECK-IN DATA STRUCTURE")
    print("="*80)

    print("\nSample check-ins:")
    print(df_checkins.head(20).to_string())

    # Check what identifiers exist
    print("\n\nAvailable identifiers in check-ins:")
    identifier_cols = []
    for col in ['customer_id', 'member_id', 'membership_id', 'customer_name', 'member_name']:
        if col in df_checkins.columns:
            identifier_cols.append(col)
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col}")

    # If we have the right columns, analyze sharing patterns
    if 'membership_id' in df_checkins.columns and 'customer_id' in df_checkins.columns:
        print("\n\n" + "="*80)
        print("ANALYZING CHECK-IN PATTERNS")
        print("="*80)

        # Convert dates
        df_checkins['checkin_time'] = pd.to_datetime(df_checkins['checkin_time'], errors='coerce')

        # Filter to recent data (2025)
        df_checkins_2025 = df_checkins[df_checkins['checkin_time'].dt.year == 2025].copy()

        print(f"\nTotal check-ins in 2025: {len(df_checkins_2025)}")

        # For each membership, count unique customers checking in
        membership_sharing = df_checkins_2025.groupby('membership_id').agg({
            'customer_id': 'nunique',
            'checkin_id': 'count'
        }).reset_index()
        membership_sharing.columns = ['membership_id', 'unique_customers', 'total_checkins']

        # Filter to Solo memberships only (where sharing would be suspicious)
        if df_memberships is not None:
            # Get active solo memberships
            df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
            df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

            today = pd.Timestamp.today()
            active_mask = (
                (df_memberships['start_date'] <= today) &
                ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= today))
            )

            active_memberships = df_memberships[active_mask].copy()
            solo_memberships = active_memberships[active_memberships['size'] == 'solo']

            print(f"\nActive Solo memberships: {len(solo_memberships)}")

            # Join with check-in data
            solo_sharing = membership_sharing.merge(
                solo_memberships[['membership_id', 'name', 'owner_id', 'size']],
                on='membership_id',
                how='inner'
            )

            print(f"\nSolo memberships with check-ins in 2025: {len(solo_sharing)}")

            # Find suspicious cases: Solo memberships with multiple unique customers
            suspicious = solo_sharing[solo_sharing['unique_customers'] > 1].sort_values(
                'unique_customers', ascending=False
            )

            print(f"\n\nSolo memberships with MULTIPLE people checking in: {len(suspicious)}")

            if len(suspicious) > 0:
                print("\n" + "="*80)
                print("POTENTIAL SHARING DETECTED")
                print("="*80)

                print("\nTop 20 Solo memberships by number of different people checking in:")
                print("-"*80)

                for i, row in suspicious.head(20).iterrows():
                    print(f"\nMembership ID: {row['membership_id']} ({row['name']})")
                    print(f"  Different people checking in: {row['unique_customers']}")
                    print(f"  Total check-ins: {row['total_checkins']}")
                    print(f"  Avg check-ins per person: {row['total_checkins']/row['unique_customers']:.1f}")

                    # Get the specific people
                    checkins_for_this = df_checkins_2025[
                        df_checkins_2025['membership_id'] == row['membership_id']
                    ]

                    if df_customers is not None:
                        customer_ids = checkins_for_this['customer_id'].unique()
                        customers = df_customers[df_customers['customer_id'].isin(customer_ids)]

                        if len(customers) > 0:
                            print("  People checking in:")
                            for _, customer in customers.iterrows():
                                customer_checkins = len(checkins_for_this[
                                    checkins_for_this['customer_id'] == customer['customer_id']
                                ])
                                name = f"{customer.get('first_name', 'Unknown')} {customer.get('last_name', '')}"
                                print(f"    - {name} ({customer_checkins} check-ins)")

                # Summary statistics
                print("\n\n" + "="*80)
                print("SHARING STATISTICS")
                print("="*80)

                print(f"\nTotal solo memberships with check-ins: {len(solo_sharing)}")
                print(f"Solo memberships used by 1 person only: {len(solo_sharing[solo_sharing['unique_customers'] == 1])} ({len(solo_sharing[solo_sharing['unique_customers'] == 1])/len(solo_sharing)*100:.1f}%)")
                print(f"Solo memberships used by 2+ people: {len(suspicious)} ({len(suspicious)/len(solo_sharing)*100:.1f}%)")

                # Distribution
                print("\n\nDistribution of unique people per Solo membership:")
                dist = solo_sharing['unique_customers'].value_counts().sort_index()
                for num_people, count in dist.items():
                    pct = (count / len(solo_sharing) * 100)
                    print(f"  {num_people} people: {count} memberships ({pct:.1f}%)")

            else:
                print("\nNo sharing detected - all solo memberships used by single person only")

    elif 'membership_id' in df_checkins.columns:
        print("\n\nWe have membership_id but not customer_id in check-ins")
        print("Cannot determine if different people are using the same membership")

    else:
        print("\n\nInsufficient data in check-ins to detect sharing")

else:
    print("\n\nNo check-in data available")

# Check if members table can help
if df_members is not None and df_memberships is not None:
    print("\n\n" + "="*80)
    print("ANALYZING MEMBERS TABLE FOR MULTI-PERSON USAGE")
    print("="*80)

    # The members table shows individual people on memberships
    # For solo memberships, there should only be 1 member

    # Get active members
    df_members['start_date'] = pd.to_datetime(df_members['start_date'], errors='coerce')
    df_members['end_date'] = pd.to_datetime(df_members['end_date'], errors='coerce')

    today = pd.Timestamp.today()
    active_members_mask = (
        (df_members['start_date'] <= today) &
        ((df_members['end_date'].isna()) | (df_members['end_date'] >= today)) &
        (df_members['status'] == 'ACT')
    )

    df_active_members = df_members[active_members_mask].copy()

    # Count members per membership
    members_per_membership = df_active_members.groupby('membership_id').agg({
        'member_id': 'count',
        'customer_id': 'nunique'
    }).reset_index()
    members_per_membership.columns = ['membership_id', 'member_count', 'unique_customers']

    # Join with membership data
    members_with_type = members_per_membership.merge(
        df_memberships[['membership_id', 'name', 'size']],
        on='membership_id',
        how='left'
    )

    # Filter to solo
    solo_with_members = members_with_type[members_with_type['size'] == 'solo']

    print(f"\nSolo memberships in members table: {len(solo_with_members)}")

    if len(solo_with_members) > 0:
        multiple = solo_with_members[solo_with_members['member_count'] > 1]

        print(f"Solo memberships with multiple member records: {len(multiple)}")

        if len(multiple) > 0:
            print("\nSolo memberships with multiple people:")
            print(multiple.head(20).to_string(index=False))

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

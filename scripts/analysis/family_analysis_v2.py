"""
Analyze family membership composition - alternative approach
Using the members.csv which likely has individual family members linked to family memberships
"""

import pandas as pd
import boto3
import os
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

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

def main():
    print("\n" + "="*80)
    print("FAMILY MEMBERSHIP ANALYSIS - USING MEMBERS DATA")
    print("="*80)

    # Load data
    df_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/members.csv")
    df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
    df_customers = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/customers.csv")

    if df_members is None or df_memberships is None:
        print("Could not load required data")
        return

    print(f"\n✓ Loaded {len(df_members)} members")
    print(f"✓ Loaded {len(df_memberships)} memberships")
    if df_customers is not None:
        print(f"✓ Loaded {len(df_customers)} customers")

    print(f"\nMembers columns: {df_members.columns.tolist()}")

    # Get currently active family memberships
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    today = pd.Timestamp.today()
    active_mask = (
        (df_memberships['start_date'] <= today) &
        ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= today))
    )

    df_active_memberships = df_memberships[active_mask].copy()
    df_family_memberships = df_active_memberships[
        df_active_memberships['name'].str.contains('Family', case=False, na=False)
    ].copy()

    print(f"\n{'='*80}")
    print(f"Total Active Memberships: {len(df_active_memberships)}")
    print(f"Active Family Memberships: {len(df_family_memberships)}")
    print(f"{'='*80}")

    # Check if members has membership_id
    if 'membership_id' not in df_members.columns:
        print("\nERROR: No membership_id column in members data")
        return

    # For each family membership, count members
    print("\n\nAnalyzing family composition...")

    family_composition = []

    for idx, fam in df_family_memberships.iterrows():
        membership_id = fam['membership_id']

        # Get all members for this membership
        members_of_family = df_members[df_members['membership_id'] == membership_id]

        num_members = len(members_of_family)

        # Try to get age info
        kids = 0
        adults = 0

        if df_customers is not None and 'customer_id' in members_of_family.columns:
            # Merge with customer data
            members_with_cust = members_of_family.merge(
                df_customers,
                on='customer_id',
                how='left',
                suffixes=('', '_cust')
            )

            # Check for birthday/DOB columns
            dob_col = None
            for col_name in ['date_of_birth', 'birthday', 'dob', 'birth_date']:
                if col_name in members_with_cust.columns:
                    dob_col = col_name
                    break

            if dob_col:
                members_with_cust['dob_parsed'] = pd.to_datetime(members_with_cust[dob_col], errors='coerce')
                members_with_cust['age'] = (today - members_with_cust['dob_parsed']).dt.days / 365.25

                kids = len(members_with_cust[members_with_cust['age'] < 18])
                adults = len(members_with_cust[members_with_cust['age'] >= 18])

        family_composition.append({
            'membership_id': membership_id,
            'membership_name': fam['name'],
            'billing_amount': fam['billing_amount'],
            'total_members': num_members,
            'adults': adults,
            'kids': kids
        })

    df_comp = pd.DataFrame(family_composition)

    print(f"\nAnalyzed {len(df_comp)} family memberships")

    # Check how many have member data
    with_members = len(df_comp[df_comp['total_members'] > 0])
    without_members = len(df_comp[df_comp['total_members'] == 0])

    print(f"\nFamily memberships WITH member records: {with_members}")
    print(f"Family memberships WITHOUT member records: {without_members}")

    if with_members > 0:
        print("\n" + "="*80)
        print("FAMILY COMPOSITION STATISTICS (for families with member data)")
        print("="*80)

        df_with_data = df_comp[df_comp['total_members'] > 0]

        print(f"\nAverage members per family: {df_with_data['total_members'].mean():.2f}")
        print(f"Median members per family: {df_with_data['total_members'].median():.1f}")
        print(f"Min members: {df_with_data['total_members'].min():.0f}")
        print(f"Max members: {df_with_data['total_members'].max():.0f}")

        if df_with_data['kids'].sum() > 0:
            families_with_kids = len(df_with_data[df_with_data['kids'] > 0])
            print(f"\nFamilies with kids: {families_with_kids} ({families_with_kids/len(df_with_data)*100:.1f}%)")
            print(f"Average kids per family (all): {df_with_data['kids'].mean():.2f}")
            print(f"Average kids per family (only families with kids): {df_with_data[df_with_data['kids'] > 0]['kids'].mean():.2f}")
            print(f"Average adults per family: {df_with_data['adults'].mean():.2f}")

            print("\n\nDistribution of Number of Kids:")
            print("-" * 40)
            kids_dist = df_with_data['kids'].value_counts().sort_index()
            for num_kids, count in kids_dist.items():
                pct = (count / len(df_with_data) * 100)
                print(f"  {int(num_kids):2d} kid(s):  {count:3d} families ({pct:5.1f}%)")

            print("\n\nDistribution of Total Members per Family:")
            print("-" * 40)
            member_dist = df_with_data['total_members'].value_counts().sort_index()
            for num_members, count in member_dist.items():
                pct = (count / len(df_with_data) * 100)
                print(f"  {int(num_members):2d} member(s):  {count:3d} families ({pct:5.1f}%)")

        print("\n\n" + "="*80)
        print("SAMPLE FAMILY DETAILS")
        print("="*80)
        print("\nShowing families with the most members:")
        print(df_with_data.sort_values('total_members', ascending=False).head(20).to_string(index=False))

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

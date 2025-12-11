"""
FINAL Family Composition Analysis
Now that we understand the data structure correctly:
- memberships table = the membership subscriptions
- members table = individual PEOPLE on those memberships (one membership can have multiple people)
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
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    csv_content = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(csv_content))

def main():
    print("\n" + "="*80)
    print("COMPLETE MEMBERSHIP BREAKDOWN WITH FAMILY COMPOSITION")
    print("="*80)

    df_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/members.csv")
    df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
    df_customers = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/customers.csv")

    print(f"\n✓ Loaded {len(df_members)} member records (individual people)")
    print(f"✓ Loaded {len(df_memberships)} membership records (subscriptions)")
    print(f"✓ Loaded {len(df_customers)} customer records")

    # Get currently active members (people actively on a membership)
    df_members['start_date'] = pd.to_datetime(df_members['start_date'], errors='coerce')
    df_members['end_date'] = pd.to_datetime(df_members['end_date'], errors='coerce')

    today = pd.Timestamp.today()
    active_members_mask = (
        (df_members['start_date'] <= today) &
        ((df_members['end_date'].isna()) | (df_members['end_date'] >= today)) &
        (df_members['status'] == 'ACT')
    )

    df_active_members = df_members[active_members_mask].copy()

    print(f"\n{'='*80}")
    print(f"CURRENTLY ACTIVE MEMBERS (PEOPLE)")
    print(f"{'='*80}")
    print(f"Total active members (people): {len(df_active_members)}")

    # Count memberships by type
    print("\n\nACTIVE MEMBERSHIPS BY TYPE:")
    print("-"*60)

    membership_counts = df_active_members.groupby('name').agg({
        'membership_id': 'nunique',  # Unique membership subscriptions
        'member_id': 'count'  # Total people
    })
    membership_counts.columns = ['# Memberships', '# People']
    membership_counts = membership_counts.sort_values('# Memberships', ascending=False)

    print(membership_counts.to_string())

    # Summary by frequency
    print("\n\n" + "="*80)
    print("BREAKDOWN BY PAYMENT FREQUENCY")
    print("="*80)

    freq_summary = df_active_members.groupby('frequency').agg({
        'membership_id': 'nunique',
        'member_id': 'count'
    })
    freq_summary.columns = ['# Memberships', '# People']
    print("\n", freq_summary.to_string())

    # Summary by size
    print("\n\n" + "="*80)
    print("BREAKDOWN BY MEMBERSHIP SIZE")
    print("="*80)

    size_summary = df_active_members.groupby('size').agg({
        'membership_id': 'nunique',
        'member_id': 'count'
    })
    size_summary.columns = ['# Memberships', '# People']
    print("\n", size_summary.to_string())

    # Cross-tab
    print("\n\n" + "="*80)
    print("CROSS-TAB: FREQUENCY × SIZE (# of Memberships)")
    print("="*80)

    unique_memberships = df_active_members.drop_duplicates(subset=['membership_id'])
    crosstab = pd.crosstab(unique_memberships['frequency'], unique_memberships['size'], margins=True)
    print("\n", crosstab.to_string())

    # FAMILY COMPOSITION ANALYSIS
    print("\n\n" + "="*80)
    print("FAMILY MEMBERSHIP COMPOSITION ANALYSIS")
    print("="*80)

    # Get family memberships
    family_members = df_active_members[df_active_members['name'].str.contains('Family', case=False, na=False)].copy()

    print(f"\nFamily membership people: {len(family_members)}")
    print(f"Unique family memberships: {family_members['membership_id'].nunique()}")

    if len(family_members) > 0:
        # Count members per family membership
        members_per_family = family_members.groupby('membership_id').size()

        print(f"\n\nAverage people per family membership: {members_per_family.mean():.2f}")
        print(f"Median people per family: {members_per_family.median():.1f}")
        print(f"Min people: {members_per_family.min()}")
        print(f"Max people: {members_per_family.max()}")

        print("\n\nDistribution of People per Family Membership:")
        print("-"*50)
        distribution = members_per_family.value_counts().sort_index()
        for num_people, count in distribution.items():
            pct = (count / len(members_per_family) * 100)
            print(f"  {num_people:2d} people:  {count:3d} families ({pct:5.1f}%)")

        # Try to determine kids vs adults
        print("\n\n" + "="*80)
        print("KIDS VS ADULTS IN FAMILY MEMBERSHIPS")
        print("="*80)

        # Merge with customer data to get birthdates
        family_with_cust = family_members.merge(
            df_customers[['customer_id', 'first_name', 'last_name']],
            on='customer_id',
            how='left',
            suffixes=('', '_cust')
        )

        # Check for birthdays in association members
        df_assoc_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/association_members.csv")

        if df_assoc_members is not None and 'customer_birthday' in df_assoc_members.columns:
            print("\nFound birthday data in association members!")

            # Merge birthday data
            family_with_age = family_with_cust.merge(
                df_assoc_members[['customer_id', 'customer_birthday']],
                on='customer_id',
                how='left'
            )

            family_with_age['dob'] = pd.to_datetime(family_with_age['customer_birthday'], errors='coerce')
            family_with_age['age'] = (today - family_with_age['dob']).dt.days / 365.25

            # Filter to those with age data
            with_age = family_with_age[family_with_age['age'].notna()]

            if len(with_age) > 0:
                print(f"\nFamily members with age data: {len(with_age)} out of {len(family_members)}")

                kids = len(with_age[with_age['age'] < 18])
                adults = len(with_age[with_age['age'] >= 18])

                print(f"\nKids (under 18): {kids} ({kids/len(with_age)*100:.1f}%)")
                print(f"Adults (18+): {adults} ({adults/len(with_age)*100:.1f}%)")

                # Per family breakdown
                family_composition = with_age.groupby('membership_id').apply(
                    lambda x: pd.Series({
                        'total': len(x),
                        'kids': len(x[x['age'] < 18]),
                        'adults': len(x[x['age'] >= 18]),
                        'membership_type': x['name'].iloc[0]
                    })
                )

                families_with_kids = len(family_composition[family_composition['kids'] > 0])

                print(f"\n\nFamilies with kids: {families_with_kids} out of {len(family_composition)} ({families_with_kids/len(family_composition)*100:.1f}%)")
                print(f"Average kids per family (all families): {family_composition['kids'].mean():.2f}")
                print(f"Average kids per family (only families with kids): {family_composition[family_composition['kids'] > 0]['kids'].mean():.2f}")

                print("\n\nDistribution of Kids per Family:")
                print("-"*50)
                kids_dist = family_composition['kids'].value_counts().sort_index()
                for num_kids, count in kids_dist.items():
                    pct = (count / len(family_composition) * 100)
                    print(f"  {int(num_kids):2d} kid(s):  {count:3d} families ({pct:5.1f}%)")

                print("\n\nSample of Family Compositions:")
                print(family_composition.sort_values('total', ascending=False).head(20).to_string())
            else:
                print("\nNo age data found after merge")
        else:
            print("\nNo birthday data available in association members")

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

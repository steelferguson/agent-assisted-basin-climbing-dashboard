"""
Current Membership Breakdown Analysis
Breaking down memberships by frequency, size, and family composition
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

def get_current_active_memberships(df_memberships):
    """Get currently active memberships as of today"""
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    today = pd.Timestamp.today()

    # Active = started before today and (no end date OR end date is after today)
    active_mask = (
        (df_memberships['start_date'] <= today) &
        ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= today))
    )

    return df_memberships[active_mask].copy()

def analyze_by_frequency_and_size(df_active):
    """Break down by payment frequency and membership size"""
    print("\n" + "="*80)
    print("CURRENT ACTIVE MEMBERSHIPS BY FREQUENCY AND SIZE")
    print(f"As of: {pd.Timestamp.today().strftime('%Y-%m-%d')}")
    print("="*80)

    print(f"\nTotal Active Memberships: {len(df_active)}")

    # First, let's understand what frequency and size columns exist
    print(f"\nAvailable columns: {df_active.columns.tolist()}")

    # Try to identify membership characteristics from the 'name' field
    df_active['freq_extracted'] = 'Unknown'
    df_active['size_extracted'] = 'Unknown'

    # Extract frequency
    df_active.loc[df_active['name'].str.contains('Annual', case=False, na=False), 'freq_extracted'] = 'Annual'
    df_active.loc[df_active['name'].str.contains('Monthly', case=False, na=False), 'freq_extracted'] = 'Monthly'
    df_active.loc[df_active['name'].str.contains('Weekly', case=False, na=False), 'freq_extracted'] = 'Weekly'

    # Extract size
    df_active.loc[df_active['name'].str.contains('Solo', case=False, na=False), 'size_extracted'] = 'Solo'
    df_active.loc[df_active['name'].str.contains('Duo', case=False, na=False), 'size_extracted'] = 'Duo'
    df_active.loc[df_active['name'].str.contains('Family', case=False, na=False), 'size_extracted'] = 'Family'
    df_active.loc[df_active['name'].str.contains('Student', case=False, na=False) |
                  df_active['name'].str.contains('College', case=False, na=False), 'size_extracted'] = 'Student'

    # Use actual columns if they exist
    if 'frequency' in df_active.columns:
        df_active['freq_final'] = df_active['frequency'].fillna(df_active['freq_extracted'])
    else:
        df_active['freq_final'] = df_active['freq_extracted']

    if 'size' in df_active.columns:
        df_active['size_final'] = df_active['size'].fillna(df_active['size_extracted'])
    else:
        df_active['size_final'] = df_active['size_extracted']

    # Create breakdown
    print("\n" + "="*80)
    print("BREAKDOWN BY FREQUENCY")
    print("="*80)

    freq_counts = df_active['freq_final'].value_counts().sort_index()
    print("\nMemberships by Payment Frequency:")
    for freq, count in freq_counts.items():
        pct = (count / len(df_active) * 100)
        print(f"  {freq:20s}: {count:4d} ({pct:.1f}%)")

    print("\n" + "="*80)
    print("BREAKDOWN BY SIZE")
    print("="*80)

    size_counts = df_active['size_final'].value_counts().sort_index()
    print("\nMemberships by Size:")
    for size, count in size_counts.items():
        pct = (count / len(df_active) * 100)
        print(f"  {size:20s}: {count:4d} ({pct:.1f}%)")

    # Cross-tabulation
    print("\n" + "="*80)
    print("CROSS-TAB: FREQUENCY × SIZE")
    print("="*80)

    crosstab = pd.crosstab(df_active['freq_final'], df_active['size_final'], margins=True)
    print("\n", crosstab.to_string())

    return df_active

def analyze_by_detailed_membership_type(df_active):
    """Show detailed breakdown by specific membership type names"""
    print("\n" + "="*80)
    print("DETAILED MEMBERSHIP TYPE BREAKDOWN")
    print("="*80)

    type_counts = df_active['name'].value_counts()

    print(f"\nAll Active Membership Types (Total: {len(df_active)}):")
    print("-" * 60)

    for membership_type, count in type_counts.items():
        pct = (count / len(df_active) * 100)
        print(f"  {membership_type:45s} {count:4d} ({pct:5.1f}%)")

    return type_counts

def analyze_family_composition(df_memberships, df_members):
    """Analyze family composition - how many kids per family"""
    print("\n" + "="*80)
    print("FAMILY MEMBERSHIP COMPOSITION ANALYSIS")
    print("="*80)

    if df_members is None:
        print("\nMember data not available - cannot analyze family composition")
        return None

    print(f"\nMembers data columns: {df_members.columns.tolist()}")
    print(f"Total member records: {len(df_members)}")

    # Get active family memberships
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    today = pd.Timestamp.today()
    active_mask = (
        (df_memberships['start_date'] <= today) &
        ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= today))
    )

    df_active = df_memberships[active_mask].copy()

    # Filter to family memberships
    family_mask = df_active['name'].str.contains('Family', case=False, na=False)
    df_family_memberships = df_active[family_mask].copy()

    print(f"\nTotal Active Family Memberships: {len(df_family_memberships)}")

    # Try to link members to memberships
    # Look for membership_id in members data
    if 'membership_id' in df_members.columns:
        print("\nLinking members to memberships via membership_id...")

        # For each family membership, count members
        family_composition = []

        for idx, membership in df_family_memberships.iterrows():
            membership_id = membership['membership_id']

            # Get all members for this membership
            members_in_this = df_members[df_members['membership_id'] == membership_id]

            total_members = len(members_in_this)

            # Try to identify kids vs adults
            kids = 0
            adults = 0

            # Check if there's an age column
            if 'age' in members_in_this.columns:
                kids = len(members_in_this[members_in_this['age'] < 18])
                adults = len(members_in_this[members_in_this['age'] >= 18])
            elif 'date_of_birth' in members_in_this.columns or 'birthday' in members_in_this.columns:
                # Calculate age from DOB
                dob_col = 'date_of_birth' if 'date_of_birth' in members_in_this.columns else 'birthday'
                members_in_this['dob_parsed'] = pd.to_datetime(members_in_this[dob_col], errors='coerce')
                members_in_this['age_calculated'] = (today - members_in_this['dob_parsed']).dt.days / 365.25
                kids = len(members_in_this[members_in_this['age_calculated'] < 18])
                adults = len(members_in_this[members_in_this['age_calculated'] >= 18])

            family_composition.append({
                'membership_id': membership_id,
                'membership_type': membership['name'],
                'total_members': total_members,
                'adults': adults,
                'kids': kids
            })

        df_composition = pd.DataFrame(family_composition)

        if len(df_composition) > 0:
            print("\n" + "-"*60)
            print("FAMILY COMPOSITION STATISTICS")
            print("-"*60)

            print(f"\nFamilies with member data: {len(df_composition[df_composition['total_members'] > 0])}")

            if df_composition['total_members'].sum() > 0:
                print(f"\nAverage members per family: {df_composition['total_members'].mean():.2f}")

                if df_composition['kids'].sum() > 0:
                    print(f"Average kids per family: {df_composition['kids'].mean():.2f}")
                    print(f"Average adults per family: {df_composition['adults'].mean():.2f}")

                    print("\n\nDistribution of Kids per Family:")
                    kids_distribution = df_composition['kids'].value_counts().sort_index()
                    for num_kids, count in kids_distribution.items():
                        pct = (count / len(df_composition) * 100)
                        print(f"  {num_kids} kid(s): {count:4d} families ({pct:.1f}%)")

                    print("\n\nDistribution of Total Members per Family:")
                    member_distribution = df_composition['total_members'].value_counts().sort_index()
                    for num_members, count in member_distribution.items():
                        pct = (count / len(df_composition) * 100)
                        print(f"  {num_members} member(s): {count:4d} families ({pct:.1f}%)")

                # Show some examples
                print("\n\nSample Family Compositions:")
                print(df_composition.head(20).to_string(index=False))
            else:
                print("\nNo member data found for family memberships")
        else:
            print("\nCould not create family composition analysis")
    else:
        print("\nNo membership_id column found in members data")
        print(f"Available columns: {df_members.columns.tolist()}")

    return df_composition if 'df_composition' in locals() else None

def main():
    print("\n" + "="*80)
    print("CURRENT MEMBERSHIP BREAKDOWN ANALYSIS")
    print("="*80)

    # Load data
    print("\nLoading data from S3...")
    df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
    df_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/members.csv")

    if df_memberships is None:
        print("ERROR: Could not load memberships data")
        return

    print(f"✓ Loaded {len(df_memberships)} total memberships")

    if df_members is not None:
        print(f"✓ Loaded {len(df_members)} member records")
    else:
        print("! Could not load members data - family analysis will be limited")

    # Get current active memberships
    df_active = get_current_active_memberships(df_memberships)

    # Run analyses
    df_active = analyze_by_frequency_and_size(df_active)

    type_counts = analyze_by_detailed_membership_type(df_active)

    family_comp = analyze_family_composition(df_memberships, df_members)

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

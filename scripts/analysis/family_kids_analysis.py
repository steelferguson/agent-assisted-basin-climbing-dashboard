"""
Analyze family membership composition using associations data
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
    print("FAMILY COMPOSITION ANALYSIS - USING ASSOCIATIONS DATA")
    print("="*80)

    # Load associations data
    df_associations = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/associations.csv")
    df_association_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/association_members.csv")
    df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
    df_customers = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/customers.csv")

    if df_associations is not None:
        print(f"\n✓ Loaded {len(df_associations)} associations")
        print(f"Associations columns: {df_associations.columns.tolist()}")

    if df_association_members is not None:
        print(f"\n✓ Loaded {len(df_association_members)} association members")
        print(f"Association members columns: {df_association_members.columns.tolist()}")

    if df_customers is not None:
        print(f"\n✓ Loaded {len(df_customers)} customers")
        print(f"Customers columns: {df_customers.columns.tolist()}")

    # Get active family memberships
    if df_memberships is not None:
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
        df_family = df_active[family_mask].copy()

        print(f"\n\n{'='*80}")
        print(f"Active Family Memberships: {len(df_family)}")
        print(f"{'='*80}")

        # Try to link families to association members
        if df_associations is not None and df_association_members is not None:
            print("\n\nAttempting to link family memberships to association members...")

            # Associations likely represent families/groups
            # Check if membership_id exists in associations
            if 'membership_id' in df_associations.columns:
                print("\nFound membership_id in associations!")

                family_composition = []

                for idx, fam_membership in df_family.iterrows():
                    membership_id = fam_membership['membership_id']

                    # Find association for this membership
                    association = df_associations[df_associations['membership_id'] == membership_id]

                    if len(association) > 0:
                        association_id = association.iloc[0]['association_id'] if 'association_id' in association.columns else None

                        if association_id:
                            # Get members of this association
                            members = df_association_members[df_association_members['association_id'] == association_id]

                            total_members = len(members)

                            # Try to determine kids vs adults
                            if df_customers is not None and 'customer_id' in members.columns:
                                # Join with customer data to get ages
                                members_with_info = members.merge(
                                    df_customers[['customer_id', 'date_of_birth', 'age']] if 'customer_id' in df_customers.columns else df_customers,
                                    on='customer_id',
                                    how='left'
                                )

                                kids = 0
                                adults = 0

                                if 'age' in members_with_info.columns:
                                    kids = len(members_with_info[members_with_info['age'] < 18])
                                    adults = len(members_with_info[members_with_info['age'] >= 18])
                                elif 'date_of_birth' in members_with_info.columns:
                                    members_with_info['dob_parsed'] = pd.to_datetime(members_with_info['date_of_birth'], errors='coerce')
                                    members_with_info['age_calc'] = (today - members_with_info['dob_parsed']).dt.days / 365.25
                                    kids = len(members_with_info[members_with_info['age_calc'] < 18])
                                    adults = len(members_with_info[members_with_info['age_calc'] >= 18])

                                family_composition.append({
                                    'membership_id': membership_id,
                                    'membership_type': fam_membership['name'],
                                    'association_id': association_id,
                                    'total_members': total_members,
                                    'kids': kids,
                                    'adults': adults
                                })

                if len(family_composition) > 0:
                    df_comp = pd.DataFrame(family_composition)

                    print(f"\n\nSuccessfully analyzed {len(df_comp)} family memberships")

                    print("\n" + "="*80)
                    print("FAMILY COMPOSITION STATISTICS")
                    print("="*80)

                    if df_comp['total_members'].sum() > 0:
                        print(f"\nAverage total members per family: {df_comp['total_members'].mean():.2f}")

                        if df_comp['kids'].sum() > 0:
                            print(f"Average kids per family: {df_comp['kids'].mean():.2f}")
                            print(f"Average adults per family: {df_comp['adults'].mean():.2f}")

                            print("\n\nDistribution of Kids per Family:")
                            kids_dist = df_comp['kids'].value_counts().sort_index()
                            for num_kids, count in kids_dist.items():
                                pct = (count / len(df_comp) * 100)
                                print(f"  {int(num_kids):2d} kid(s):  {count:3d} families ({pct:5.1f}%)")

                            print("\n\nDistribution of Total Members per Family:")
                            member_dist = df_comp['total_members'].value_counts().sort_index()
                            for num_members, count in member_dist.items():
                                pct = (count / len(df_comp) * 100)
                                print(f"  {int(num_members):2d} member(s):  {count:3d} families ({pct:5.1f}%)")

                            # Detailed breakdown
                            print("\n\n" + "="*80)
                            print("DETAILED FAMILY BREAKDOWN")
                            print("="*80)
                            print(df_comp.sort_values('total_members', ascending=False).to_string(index=False))
                        else:
                            print("\nCould not determine kids vs adults (no age data)")
                            print(f"Total members found: {df_comp['total_members'].sum()}")
                    else:
                        print("\nNo member data found for families")
                else:
                    print("\nCould not create family composition data")
            else:
                print("\nNo membership_id in associations data")
                print(f"Associations columns: {df_associations.columns.tolist()}")
        else:
            print("\nAssociations data not available")

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

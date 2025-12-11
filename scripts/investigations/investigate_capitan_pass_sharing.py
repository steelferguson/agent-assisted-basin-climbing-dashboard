"""
Investigate if Capitan data shows who shares passes with whom
Looking for guest/buddy pass features, shared entries, or relationship data
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
print("INVESTIGATING PASS SHARING IN CAPITAN DATA")
print("Can we see who shares passes with whom?")
print("="*80)

# Load all Capitan tables
print("\n" + "="*80)
print("LOADING CAPITAN DATA")
print("="*80)

df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
df_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/members.csv")
df_checkins = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/checkins.csv")
df_customers = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/customers.csv")

print("\n✓ Data loaded")

# Examine check-ins for sharing/guest information
print("\n\n" + "="*80)
print("CHECK-IN DATA STRUCTURE")
print("="*80)

if df_checkins is not None:
    print(f"\nTotal check-ins: {len(df_checkins)}")
    print("\nColumns:")
    for col in df_checkins.columns:
        print(f"  - {col}")

    # Look for guest/buddy/sharing related columns
    print("\n\nLooking for sharing-related fields:")
    sharing_keywords = ['guest', 'buddy', 'shared', 'companion', 'plus', 'friend', 'relationship']

    found_any = False
    for keyword in sharing_keywords:
        matching_cols = [col for col in df_checkins.columns if keyword.lower() in col.lower()]
        if matching_cols:
            print(f"  ✓ Found: {matching_cols}")
            found_any = True

    if not found_any:
        print("  ✗ No obvious sharing-related columns found")

    # Check entry_method and notes
    print("\n\n" + "="*80)
    print("ENTRY METHODS (How people checked in)")
    print("="*80)

    if 'entry_method' in df_checkins.columns:
        print("\nEntry method distribution:")
        print(df_checkins['entry_method'].value_counts().to_string())

    if 'entry_method_description' in df_checkins.columns:
        print("\n\nEntry method descriptions:")
        print(df_checkins['entry_method_description'].value_counts().head(30).to_string())

    # Look at notes field for sharing patterns
    print("\n\n" + "="*80)
    print("CHECK-IN NOTES (Looking for sharing patterns)")
    print("="*80)

    if 'notes' in df_checkins.columns:
        notes_with_content = df_checkins[df_checkins['notes'].notna() & (df_checkins['notes'] != '')]
        print(f"\nCheck-ins with notes: {len(notes_with_content)} ({len(notes_with_content)/len(df_checkins)*100:.1f}%)")

        if len(notes_with_content) > 0:
            # Search for sharing-related keywords in notes
            sharing_patterns = ['guest', 'buddy', 'friend', 'companion', 'plus one', 'brought',
                              'with', 'shared', 'pass', 'other account', 'different person']

            for pattern in sharing_patterns:
                matching = notes_with_content[notes_with_content['notes'].str.contains(pattern, case=False, na=False)]
                if len(matching) > 0:
                    print(f"\n\nNotes containing '{pattern}': {len(matching)}")
                    print("Sample notes:")
                    for note in matching['notes'].head(10):
                        print(f"  - {note}")
    else:
        print("\n✗ No 'notes' field available")

    # Look at staff_notes or admin_notes
    staff_note_cols = [col for col in df_checkins.columns if 'staff' in col.lower() or 'admin' in col.lower()]
    if staff_note_cols:
        print("\n\n" + "="*80)
        print("STAFF/ADMIN NOTES")
        print("="*80)

        for col in staff_note_cols:
            notes_with_content = df_checkins[df_checkins[col].notna() & (df_checkins[col] != '')]
            print(f"\n{col}: {len(notes_with_content)} entries with content")

            if len(notes_with_content) > 0:
                print("Sample:")
                for note in notes_with_content[col].head(10):
                    print(f"  - {note}")

# Examine members table for relationships
print("\n\n" + "="*80)
print("MEMBERS TABLE - RELATIONSHIP DATA")
print("="*80)

if df_members is not None:
    print(f"\nTotal members: {len(df_members)}")
    print("\nColumns:")
    for col in df_members.columns:
        print(f"  - {col}")

    # Look for relationship fields
    print("\n\nLooking for relationship fields:")
    relationship_keywords = ['relationship', 'relation', 'role', 'type', 'guest', 'primary', 'secondary']

    found_any = False
    for keyword in relationship_keywords:
        matching_cols = [col for col in df_members.columns if keyword.lower() in col.lower()]
        if matching_cols:
            print(f"  ✓ Found: {matching_cols}")
            found_any = True

            # Show values
            for col in matching_cols:
                print(f"\n  Values in '{col}':")
                print(df_members[col].value_counts().head(20).to_string())

    if not found_any:
        print("  ✗ No relationship columns found")

    # Check if multiple members share same membership
    if 'membership_id' in df_members.columns:
        print("\n\n" + "="*80)
        print("MULTIPLE MEMBERS PER MEMBERSHIP")
        print("="*80)

        members_per_membership = df_members.groupby('membership_id').agg({
            'member_id': 'count',
            'customer_id': ['nunique', lambda x: list(x)]
        }).reset_index()
        members_per_membership.columns = ['membership_id', 'member_count', 'unique_customers', 'customer_ids']

        multi_member = members_per_membership[members_per_membership['member_count'] > 1]
        print(f"\nMemberships with multiple members: {len(multi_member)}")

        if len(multi_member) > 0:
            print("\nSample multi-member memberships:")
            for i, row in multi_member.head(10).iterrows():
                print(f"\n  Membership {row['membership_id']}:")
                print(f"    {row['member_count']} members, {row['unique_customers']} unique customers")

                # Get details of these members
                members_details = df_members[df_members['membership_id'] == row['membership_id']]
                if df_customers is not None:
                    for _, member in members_details.iterrows():
                        customer = df_customers[df_customers['customer_id'] == member['customer_id']]
                        if len(customer) > 0:
                            name = f"{customer.iloc[0].get('first_name', 'Unknown')} {customer.iloc[0].get('last_name', '')}"
                            print(f"      - {name} (customer_id: {member['customer_id']})")

# Examine membership table for guest/buddy pass features
print("\n\n" + "="*80)
print("MEMBERSHIP TABLE - GUEST PASS FEATURES")
print("="*80)

if df_memberships is not None:
    print(f"\nTotal memberships: {len(df_memberships)}")

    # Look for guest/buddy pass columns
    guest_keywords = ['guest', 'buddy', 'pass', 'visitor', 'companion', 'allowance', 'credit']

    found_cols = []
    for keyword in guest_keywords:
        matching_cols = [col for col in df_memberships.columns if keyword.lower() in col.lower()]
        for col in matching_cols:
            if col not in found_cols:
                found_cols.append(col)

    if found_cols:
        print("\n✓ Found potential guest/pass columns:")
        for col in found_cols:
            print(f"\n  {col}:")
            print(df_memberships[col].value_counts().head(20).to_string())
    else:
        print("\n✗ No guest/buddy pass columns found")

# Check customers table for relationships
print("\n\n" + "="*80)
print("CUSTOMERS TABLE - RELATIONSHIP DATA")
print("="*80)

if df_customers is not None:
    print(f"\nTotal customers: {len(df_customers)}")
    print("\nColumns:")
    for col in df_customers.columns:
        print(f"  - {col}")

    # Look for household/family/relationship fields
    relationship_keywords = ['household', 'family', 'relationship', 'emergency', 'contact', 'parent', 'guardian']

    found_any = False
    for keyword in relationship_keywords:
        matching_cols = [col for col in df_customers.columns if keyword.lower() in col.lower()]
        if matching_cols:
            print(f"\n✓ Found: {matching_cols}")
            found_any = True

            for col in matching_cols:
                non_empty = df_customers[df_customers[col].notna() & (df_customers[col] != '')]
                print(f"\n  {col}: {len(non_empty)} customers with data")
                if len(non_empty) > 0 and len(non_empty) < 100:
                    print("  Sample values:")
                    print(non_empty[col].head(10).to_string())

    if not found_any:
        print("\n✗ No relationship columns found")

# Summary
print("\n\n" + "="*80)
print("SUMMARY: CAN WE SEE WHO SHARES PASSES?")
print("="*80)

print("""
Based on the Capitan data structure:

1. CHECK-INS:
   - No direct 'shared with' or 'guest of' fields
   - Notes field might contain manual entries about sharing
   - No link between check-in and membership_id

2. MEMBERS:
   - Shows multiple people on same membership (family/duo)
   - But doesn't indicate if they share or who the primary is

3. MEMBERSHIPS:
   - May have guest pass allowances (if column exists)
   - But doesn't track who used guest passes

4. CUSTOMERS:
   - May have emergency contact or household info
   - But not structured sharing relationship data

CONCLUSION:
Without membership_id in check-ins, we CANNOT definitively track:
- Who shares their pass with whom
- When guest passes are used
- Which specific membership was used for each check-in

The system appears to track check-ins by customer_id only, not by
which membership/pass was used to gain entry.
""")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

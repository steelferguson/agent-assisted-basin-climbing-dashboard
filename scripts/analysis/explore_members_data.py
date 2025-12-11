"""
Explore the members data structure to understand what it contains
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
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    csv_content = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(csv_content))

df_members = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/members.csv")
df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")

print("="*80)
print("EXPLORING MEMBERS DATA STRUCTURE")
print("="*80)

print(f"\nTotal members records: {len(df_members)}")
print(f"\nFirst 10 rows of members data:")
print(df_members.head(10).to_string())

print(f"\n\nUnique membership_ids in members table: {df_members['membership_id'].nunique()}")
print(f"Unique membership_ids in memberships table: {df_memberships['membership_id'].nunique()}")

# Check which membership IDs from active families exist in members table
df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

today = pd.Timestamp.today()
active_mask = (
    (df_memberships['start_date'] <= today) &
    ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= today))
)

df_active = df_memberships[active_mask].copy()
df_family = df_active[df_active['name'].str.contains('Family', case=False, na=False)].copy()

print(f"\n\nActive family memberships: {len(df_family)}")
print(f"Family membership IDs that exist in members table: {len(df_family[df_family['membership_id'].isin(df_members['membership_id'])])}")

# Show sample of family membership IDs
print(f"\n\nSample family membership_ids:")
print(df_family[['membership_id', 'name']].head(10).to_string(index=False))

print(f"\n\nChecking if these IDs exist in members table:")
for mid in df_family['membership_id'].head(10):
    count = len(df_members[df_members['membership_id'] == mid])
    print(f"  Membership {mid}: {count} member records")

# Check what the members table actually represents
print("\n\nWhat does the 'members' table represent?")
print(f"\nSample of members data with membership info:")
print(df_members[['membership_id', 'member_id', 'customer_id', 'member_first_name', 'member_last_name', 'name']].head(20).to_string(index=False))

# Check if 'name' column in members is the membership type
print("\n\nMembership types in members table:")
print(df_members['name'].value_counts().head(20))

# This might be a view of memberships, not individual family members!
print("\n\nHYPOTHESIS: The 'members' table might be a duplicate/view of memberships, not individual people")
print(f"Members table rows: {len(df_members)}")
print(f"Memberships table rows: {len(df_memberships)}")
print(f"Member IDs = Membership IDs? {df_members['member_id'].equals(df_members['membership_id'])}")

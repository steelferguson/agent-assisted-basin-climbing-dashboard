"""
Investigate ENT (Entry Pass) transfers
Can we see when entry passes (day passes, punch cards, prepaid passes)
are transferred or used by different people?
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
print("INVESTIGATING ENTRY PASS TRANSFERS")
print("Can we see who entry passes are transferred to?")
print("="*80)

# Load check-ins
df_checkins = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/checkins.csv")

print(f"\nTotal check-ins: {len(df_checkins)}")

# Focus on ENT (Entry Pass) entries
print("\n" + "="*80)
print("ENT (ENTRY PASS) ENTRIES")
print("="*80)

entry_pass_checkins = df_checkins[df_checkins['entry_method'] == 'ENT'].copy()
print(f"\nTotal entry pass check-ins: {len(entry_pass_checkins)}")

# Look at entry descriptions
print("\n\nEntry pass types (by description):")
print("-"*80)
entry_types = entry_pass_checkins['entry_method_description'].value_counts().head(40)
for desc, count in entry_types.items():
    print(f"  {count:4d} - {desc}")

# Check if there are columns about pass ownership or transfer
print("\n\n" + "="*80)
print("CHECKING FOR PASS TRANSFER INFORMATION")
print("="*80)

print("\nAll columns in check-in data:")
for col in df_checkins.columns:
    print(f"  - {col}")

# Look for pass/ticket/entry ID fields
transfer_keywords = ['pass', 'ticket', 'entry', 'owner', 'purchaser', 'original', 'transferred']
found_cols = []

for keyword in transfer_keywords:
    matching = [col for col in df_checkins.columns if keyword.lower() in col.lower()]
    if matching:
        for col in matching:
            if col not in found_cols:
                found_cols.append(col)

if found_cols:
    print("\n✓ Found potential transfer-related columns:")
    for col in found_cols:
        print(f"  - {col}")
else:
    print("\n✗ No transfer-related columns found")

# Look at specific entry pass types that might be transferable
print("\n\n" + "="*80)
print("ANALYZING PREPAID/PUNCH PASS USAGE")
print("="*80)

# These are pass types that could potentially be shared
prepaid_keywords = ['5 climb', 'punch', '7 day', 'prepaid', 'multi']

entry_pass_checkins['is_prepaid'] = entry_pass_checkins['entry_method_description'].str.contains(
    '|'.join(prepaid_keywords), case=False, na=False
)

prepaid_checkins = entry_pass_checkins[entry_pass_checkins['is_prepaid']].copy()
print(f"\nPrepaid/punch pass check-ins: {len(prepaid_checkins)}")

if len(prepaid_checkins) > 0:
    print("\nPrepaid pass types:")
    print(prepaid_checkins['entry_method_description'].value_counts().to_string())

    # Look for patterns in description that might indicate pass ID or "X remaining"
    print("\n\nSample prepaid pass descriptions:")
    print("-"*80)
    for i, row in prepaid_checkins.head(20).iterrows():
        print(f"\n{row['customer_first_name']} {row['customer_last_name']}")
        print(f"  Date: {row['checkin_datetime']}")
        print(f"  Description: {row['entry_method_description']}")

# Check for "remaining" patterns in descriptions
print("\n\n" + "="*80)
print("PASSES WITH 'REMAINING' COUNTS")
print("="*80)

remaining_mask = entry_pass_checkins['entry_method_description'].str.contains('remaining', case=False, na=False)
remaining_passes = entry_pass_checkins[remaining_mask].copy()

print(f"\nEntry passes with 'remaining' in description: {len(remaining_passes)}")

if len(remaining_passes) > 0:
    print("\nSample entries:")
    print("-"*80)
    for i, row in remaining_passes.head(30).iterrows():
        print(f"\n{row['customer_first_name']} {row['customer_last_name']}")
        print(f"  {row['checkin_datetime']}")
        print(f"  {row['entry_method_description']}")

    # Extract remaining count
    import re

    def extract_remaining(desc):
        if pd.isna(desc):
            return None
        match = re.search(r'\((\d+)\s+remaining\)', str(desc))
        if match:
            return int(match.group(1))
        return None

    remaining_passes['remaining_count'] = remaining_passes['entry_method_description'].apply(extract_remaining)

    print("\n\nDistribution of remaining passes:")
    print(remaining_passes['remaining_count'].value_counts().sort_index().to_string())

# Look for pass IDs in descriptions
print("\n\n" + "="*80)
print("PASS IDs IN DESCRIPTIONS")
print("="*80)

# Check if descriptions contain pass/entry IDs
has_id_mask = entry_pass_checkins['entry_method_description'].str.contains(r'#\d+|pass \d+|entry \d+', case=False, na=False, regex=True)
has_id = entry_pass_checkins[has_id_mask].copy()

print(f"\nEntry passes with ID numbers: {len(has_id)}")

if len(has_id) > 0:
    print("\nSample entries with pass IDs:")
    for i, row in has_id.head(15).iterrows():
        print(f"  {row['entry_method_description']}")

    # Extract pass ID
    def extract_pass_id(desc):
        if pd.isna(desc):
            return None
        match = re.search(r'#(\d+)', str(desc))
        if match:
            return match.group(1)
        return None

    has_id['pass_id'] = has_id['entry_method_description'].apply(extract_pass_id)

    # Find passes used multiple times
    print("\n\n" + "="*80)
    print("TRACKING INDIVIDUAL PASS USAGE")
    print("="*80)

    pass_usage = has_id.groupby('pass_id').agg({
        'checkin_id': 'count',
        'customer_id': lambda x: list(x.unique()),
        'customer_first_name': lambda x: list(x.unique()),
        'customer_last_name': lambda x: list(x.unique()),
        'checkin_datetime': ['min', 'max']
    }).reset_index()

    pass_usage.columns = ['pass_id', 'usage_count', 'customer_ids', 'first_names', 'last_names',
                          'first_use', 'last_use']

    # Passes used multiple times
    multi_use = pass_usage[pass_usage['usage_count'] > 1].copy()
    print(f"\nPasses used multiple times: {len(multi_use)}")

    # Check for passes used by different people
    multi_use['num_different_people'] = multi_use['customer_ids'].apply(len)
    different_people = multi_use[multi_use['num_different_people'] > 1]

    print(f"Passes used by DIFFERENT people: {len(different_people)}")

    if len(different_people) > 0:
        print("\n\n🎯 PASSES TRANSFERRED BETWEEN PEOPLE:")
        print("="*80)

        for i, row in different_people.head(20).iterrows():
            print(f"\nPass #{row['pass_id']}:")
            print(f"  Used {row['usage_count']} times by {row['num_different_people']} different people")
            print(f"  Period: {row['first_use']} to {row['last_use']}")
            print(f"  People:")
            for j in range(len(row['customer_ids'])):
                cid = row['customer_ids'][j]
                fname = row['first_names'][j] if j < len(row['first_names']) else 'Unknown'
                lname = row['last_names'][j] if j < len(row['last_names']) else ''

                # Count how many times this person used this pass
                person_uses = len(has_id[(has_id['pass_id'] == row['pass_id']) &
                                         (has_id['customer_id'] == cid)])

                print(f"    - {fname} {lname} (customer_id: {cid}): {person_uses} uses")

    else:
        print("\n✗ All multi-use passes were used by the same person")
        print("    (No transfers detected)")

    # Show some single-user multi-use passes
    same_person = multi_use[multi_use['num_different_people'] == 1].head(10)
    if len(same_person) > 0:
        print("\n\nSample passes used multiple times by SAME person:")
        print("-"*80)
        for i, row in same_person.iterrows():
            fname = row['first_names'][0] if row['first_names'] else 'Unknown'
            lname = row['last_names'][0] if row['last_names'] else ''
            print(f"  Pass #{row['pass_id']}: {fname} {lname} used {row['usage_count']} times")

# Check free entry reasons for transfer mentions
print("\n\n" + "="*80)
print("FREE ENTRIES MENTIONING TRANSFERS")
print("="*80)

free_entries = df_checkins[df_checkins['entry_method'] == 'FRE'].copy()

if 'free_entry_reason' in free_entries.columns:
    transfer_keywords = ['transfer', 'moved', 'different person', 'someone else', 'not theirs']

    found_any = False
    for keyword in transfer_keywords:
        matches = free_entries[free_entries['free_entry_reason'].str.contains(keyword, case=False, na=False)]
        if len(matches) > 0:
            found_any = True
            print(f"\n'{keyword}' found ({len(matches)} entries):")
            for reason, count in matches['free_entry_reason'].value_counts().head(10).items():
                print(f"  {count}x - {reason}")

    if not found_any:
        print("\n✗ No transfer-related keywords found in free entry reasons")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

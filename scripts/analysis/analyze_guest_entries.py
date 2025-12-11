"""
Analyze GUE (Guest) entry method and free entry reasons
to understand pass sharing patterns
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

print("\n" + "="*80)
print("ANALYZING GUEST ENTRIES AND PASS SHARING")
print("="*80)

df_checkins = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/checkins.csv")

print(f"\nTotal check-ins: {len(df_checkins)}")

# Look at entry methods
print("\n" + "="*80)
print("ENTRY METHODS")
print("="*80)

print("\nEntry method breakdown:")
entry_methods = df_checkins['entry_method'].value_counts()
for method, count in entry_methods.items():
    pct = (count / len(df_checkins) * 100)
    print(f"  {method}: {count:5d} ({pct:5.1f}%)")

# Focus on GUE (Guest) entries
print("\n\n" + "="*80)
print("GUE (GUEST) ENTRIES")
print("="*80)

guest_entries = df_checkins[df_checkins['entry_method'] == 'GUE'].copy()
print(f"\nTotal guest entries: {len(guest_entries)}")

if len(guest_entries) > 0:
    print("\nGuest entry descriptions:")
    print(guest_entries['entry_method_description'].value_counts().to_string())

    # Look at free_entry_reason
    if 'free_entry_reason' in guest_entries.columns:
        print("\n\nFree entry reasons for guests:")
        print(guest_entries['free_entry_reason'].value_counts().to_string())

    # Sample guest entries
    print("\n\nSample guest check-ins:")
    print("-"*80)
    for i, row in guest_entries.head(20).iterrows():
        print(f"\nCustomer: {row['customer_first_name']} {row['customer_last_name']}")
        print(f"  Date: {row['checkin_datetime']}")
        print(f"  Description: {row['entry_method_description']}")
        if pd.notna(row['free_entry_reason']):
            print(f"  Reason: {row['free_entry_reason']}")

# Look at FRE (Free) entries
print("\n\n" + "="*80)
print("FRE (FREE) ENTRIES")
print("="*80)

free_entries = df_checkins[df_checkins['entry_method'] == 'FRE'].copy()
print(f"\nTotal free entries: {len(free_entries)}")

if len(free_entries) > 0:
    print("\nFree entry descriptions:")
    print(free_entries['entry_method_description'].value_counts().head(20).to_string())

    if 'free_entry_reason' in free_entries.columns:
        print("\n\nFree entry reasons:")
        reasons = free_entries['free_entry_reason'].value_counts()
        for reason, count in reasons.items():
            if pd.notna(reason):
                print(f"  {count:4d} - {reason}")

    # Look for sharing-related free entries
    print("\n\nSearching for sharing-related free entries:")
    if 'free_entry_reason' in free_entries.columns:
        sharing_keywords = ['pass', 'guest', 'buddy', 'friend', 'membership', 'account', 'share']

        for keyword in sharing_keywords:
            matches = free_entries[free_entries['free_entry_reason'].str.contains(keyword, case=False, na=False)]
            if len(matches) > 0:
                print(f"\n\n'{keyword}' in reason ({len(matches)} entries):")
                print("-"*60)
                for reason, count in matches['free_entry_reason'].value_counts().head(10).items():
                    print(f"  {count:3d}x - {reason}")

# Time-based analysis
print("\n\n" + "="*80)
print("GUEST/FREE ENTRIES OVER TIME")
print("="*80)

df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'])
df_checkins['month'] = df_checkins['checkin_datetime'].dt.to_period('M')

# Filter to 2025
df_2025 = df_checkins[df_checkins['checkin_datetime'].dt.year == 2025].copy()

print(f"\nTotal 2025 check-ins: {len(df_2025)}")

monthly_methods = pd.crosstab(df_2025['month'], df_2025['entry_method'], margins=True)
print("\n2025 Check-ins by entry method:")
print(monthly_methods.to_string())

# Guest entry rate by month
print("\n\nGuest entry rate by month (2025):")
monthly_totals = df_2025.groupby('month')['entry_method'].count()
monthly_guests = df_2025[df_2025['entry_method'] == 'GUE'].groupby('month')['entry_method'].count()

for month in monthly_totals.index:
    if month != 'All':
        total = monthly_totals[month]
        guests = monthly_guests.get(month, 0)
        pct = (guests / total * 100) if total > 0 else 0
        print(f"  {month}: {guests:3d} guests / {total:4d} total ({pct:4.1f}%)")

# Check who the most frequent guest pass users are
print("\n\n" + "="*80)
print("MOST FREQUENT GUEST PASS USERS")
print("="*80)

if len(guest_entries) > 0:
    guest_freq = guest_entries.groupby('customer_id').agg({
        'checkin_id': 'count',
        'customer_first_name': 'first',
        'customer_last_name': 'first',
        'checkin_datetime': ['min', 'max']
    }).reset_index()
    guest_freq.columns = ['customer_id', 'guest_visits', 'first_name', 'last_name', 'first_visit', 'last_visit']
    guest_freq = guest_freq.sort_values('guest_visits', ascending=False)

    print(f"\nTop 20 guest pass users:")
    print("-"*80)
    for i, row in guest_freq.head(20).iterrows():
        first_v = pd.to_datetime(row['first_visit'])
        last_v = pd.to_datetime(row['last_visit'])
        date_range = f"{first_v.strftime('%Y-%m-%d')} to {last_v.strftime('%Y-%m-%d')}"
        print(f"  {row['first_name']} {row['last_name']}: {row['guest_visits']} guest visits ({date_range})")

    # Multiple guest visits might indicate regular sharing
    regular_guests = guest_freq[guest_freq['guest_visits'] >= 5]
    print(f"\n\nCustomers with 5+ guest visits: {len(regular_guests)}")
    print("(These people might be regularly using someone else's guest passes)")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

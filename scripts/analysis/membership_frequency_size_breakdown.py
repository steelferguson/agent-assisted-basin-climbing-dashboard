"""
Show current active memberships broken down by frequency and size
Specifically answering: For Annual and Weekly, how many Solo/Duo/Family?
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
print("CURRENT ACTIVE MEMBERSHIPS: FREQUENCY × SIZE BREAKDOWN")
print("As of November 24, 2025")
print("="*80)

df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")

# Get currently active
df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

today = pd.Timestamp.today()
active_mask = (
    (df_memberships['start_date'] <= today) &
    ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= today))
)

df_active = df_memberships[active_mask].copy()

print(f"\nTotal Active Memberships: {len(df_active)}")

# Cross-tab by frequency and size
crosstab = pd.crosstab(
    df_active['frequency'],
    df_active['size'],
    margins=True,
    margins_name='TOTAL'
)

print("\n" + "="*80)
print("FREQUENCY × SIZE CROSS-TAB")
print("="*80)
print("\n", crosstab.to_string())

# Detailed breakdown for Annual and Bi-Weekly (marketed as "Weekly")
print("\n\n" + "="*80)
print("ANNUAL MEMBERSHIPS BREAKDOWN")
print("="*80)

annual = df_active[df_active['frequency'] == 'annual']
print(f"\nTotal Annual Memberships: {len(annual)}")

annual_by_size = annual.groupby('size').agg({
    'membership_id': 'count',
    'billing_amount': ['sum', 'mean']
})
annual_by_size.columns = ['Count', 'Total Revenue', 'Avg Price']
print("\n", annual_by_size.to_string())

# Show the specific types
print("\n\nAnnual Membership Types:")
annual_types = annual.groupby(['size', 'name']).size().reset_index(name='Count')
for size in ['solo', 'duo', 'family']:
    size_types = annual_types[annual_types['size'] == size]
    if len(size_types) > 0:
        print(f"\n  {size.upper()}:")
        for _, row in size_types.iterrows():
            print(f"    {row['name']:40s} {row['Count']:3d} memberships")

print("\n\n" + "="*80)
print("BI-WEEKLY (WEEKLY) MEMBERSHIPS BREAKDOWN")
print("="*80)

biweekly = df_active[df_active['frequency'] == 'bi_weekly']
print(f"\nTotal Bi-Weekly (Weekly) Memberships: {len(biweekly)}")

biweekly_by_size = biweekly.groupby('size').agg({
    'membership_id': 'count',
    'billing_amount': ['sum', 'mean']
})
biweekly_by_size.columns = ['Count', 'Total Revenue', 'Avg Price']
print("\n", biweekly_by_size.to_string())

# Show the specific types
print("\n\nBi-Weekly (Weekly) Membership Types:")
biweekly_types = biweekly.groupby(['size', 'name']).size().reset_index(name='Count')
for size in ['solo', 'duo', 'family']:
    size_types = biweekly_types[biweekly_types['size'] == size]
    if len(size_types) > 0:
        print(f"\n  {size.upper()}:")
        for _, row in size_types.iterrows():
            print(f"    {row['name']:40s} {row['Count']:3d} memberships")

print("\n\n" + "="*80)
print("MONTHLY MEMBERSHIPS BREAKDOWN")
print("="*80)

monthly = df_active[df_active['frequency'] == 'monthly']
print(f"\nTotal Monthly Memberships: {len(monthly)}")

monthly_by_size = monthly.groupby('size').agg({
    'membership_id': 'count',
    'billing_amount': ['sum', 'mean']
})
monthly_by_size.columns = ['Count', 'Total Revenue', 'Avg Price']
print("\n", monthly_by_size.to_string())

# Show the specific types
print("\n\nMonthly Membership Types:")
monthly_types = monthly.groupby(['size', 'name']).size().reset_index(name='Count')
for size in ['solo', 'duo', 'family', 'corporate']:
    size_types = monthly_types[monthly_types['size'] == size]
    if len(size_types) > 0:
        print(f"\n  {size.upper()}:")
        for _, row in size_types.iterrows():
            print(f"    {row['name']:40s} {row['Count']:3d} memberships")

# Summary table
print("\n\n" + "="*80)
print("SUMMARY: MEMBERSHIPS BY FREQUENCY AND SIZE")
print("="*80)

summary_data = []
for freq in ['annual', 'bi_weekly', 'monthly']:
    freq_df = df_active[df_active['frequency'] == freq]

    freq_label = {
        'annual': 'Annual',
        'bi_weekly': 'Bi-Weekly (Weekly)',
        'monthly': 'Monthly'
    }[freq]

    for size in ['solo', 'duo', 'family']:
        count = len(freq_df[freq_df['size'] == size])
        if count > 0:
            avg_price = freq_df[freq_df['size'] == size]['billing_amount'].mean()
            summary_data.append({
                'Frequency': freq_label,
                'Size': size.capitalize(),
                'Count': count,
                'Avg Price': f"${avg_price:.2f}"
            })

df_summary = pd.DataFrame(summary_data)
print("\n", df_summary.to_string(index=False))

# Totals by size
print("\n\n" + "="*80)
print("TOTALS BY SIZE (ALL FREQUENCIES)")
print("="*80)

size_totals = df_active.groupby('size').agg({
    'membership_id': 'count',
    'billing_amount': ['sum', 'mean']
})
size_totals.columns = ['Count', 'Total Monthly Revenue', 'Avg Price']
size_totals['% of Total'] = (size_totals['Count'] / len(df_active) * 100).round(1)

print("\n", size_totals.to_string())

# Totals by frequency
print("\n\n" + "="*80)
print("TOTALS BY FREQUENCY (ALL SIZES)")
print("="*80)

freq_totals = df_active.groupby('frequency').agg({
    'membership_id': 'count',
    'billing_amount': ['sum', 'mean']
})
freq_totals.columns = ['Count', 'Total Monthly Revenue', 'Avg Price']
freq_totals['% of Total'] = (freq_totals['Count'] / len(df_active) * 100).round(1)

print("\n", freq_totals.to_string())

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

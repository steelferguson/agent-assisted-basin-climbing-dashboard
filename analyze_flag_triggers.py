"""
Analyze Day Pass Flag Triggers

Counts how many customers have been triggered by day pass-related flags
in the AB test system.
"""

import sys
import os
import pandas as pd
import boto3
from io import StringIO

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

# AWS credentials
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
bucket_name = "basin-climbing-data-prod"

# S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

print("\n" + "="*80)
print("DAY PASS FLAG TRIGGER ANALYSIS")
print("="*80 + "\n")

# Load customer flags
print("1. Loading customer flags from S3...")
obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_flags.csv')
flags_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
print(f"   ✅ Loaded {len(flags_df):,} flag records")

# Day pass related flags
day_pass_flags = [
    'first_time_day_pass_2wk_offer',
    'second_visit_offer_eligible',
    'second_visit_2wk_offer',
    '2_week_pass_purchase'
]

print("\n2. Filtering to day pass flags...")
print(f"   Flags we're looking for:")
for flag in day_pass_flags:
    print(f"      - {flag}")

# Filter to day pass flags
day_pass_flag_records = flags_df[flags_df['flag_type'].isin(day_pass_flags)].copy()

if len(day_pass_flag_records) == 0:
    print("\n   ⚠️  No day pass flags found")
    sys.exit(0)

print(f"\n   ✅ Found {len(day_pass_flag_records):,} flag records")

# Count unique customers
total_customers = day_pass_flag_records['customer_id'].nunique()

print("\n" + "="*80)
print("RESULTS")
print("="*80)

print(f"\n📊 Total unique customers with day pass flags: {total_customers:,}")

# Breakdown by flag type
print("\n📋 Breakdown by flag type:")
flag_summary = (
    day_pass_flag_records
    .groupby('flag_type')
    .agg({
        'customer_id': 'nunique',
        'triggered_date': 'count'
    })
    .rename(columns={'customer_id': 'unique_customers', 'triggered_date': 'total_flags'})
    .sort_values('unique_customers', ascending=False)
)

for flag_type, row in flag_summary.iterrows():
    print(f"   {flag_type}:")
    print(f"      - Unique customers: {row['unique_customers']:,}")
    print(f"      - Total flag events: {row['total_flags']:,}")

# Customers with multiple flags
print("\n🔄 Customers with multiple day pass flags:")
customer_flag_counts = day_pass_flag_records.groupby('customer_id')['flag_type'].nunique()
multi_flag_customers = customer_flag_counts[customer_flag_counts > 1]

if len(multi_flag_customers) > 0:
    print(f"   {len(multi_flag_customers):,} customers have multiple flag types")

    # Show distribution
    flag_count_dist = multi_flag_customers.value_counts().sort_index()
    for num_flags, count in flag_count_dist.items():
        print(f"      - {count:,} customers with {num_flags} different flag types")

    # Show sample
    print("\n   Sample customers with multiple flags:")
    sample_customers = multi_flag_customers.head(5).index
    for customer_id in sample_customers:
        customer_flags = day_pass_flag_records[
            day_pass_flag_records['customer_id'] == customer_id
        ]['flag_type'].unique().tolist()
        print(f"      Customer {customer_id}: {', '.join(customer_flags)}")
else:
    print("   No customers have multiple flag types")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

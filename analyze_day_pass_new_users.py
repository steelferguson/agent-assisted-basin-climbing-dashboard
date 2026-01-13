"""
Analyze Day Pass Purchases: New vs Returning Customers

Determines what percentage of day pass purchases are from new customers
vs returning customers.
"""

import sys
import os
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime, timedelta

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
print("DAY PASS PURCHASE ANALYSIS: NEW vs RETURNING CUSTOMERS")
print("="*80 + "\n")

# Load customer events
print("1. Loading customer events from S3...")
obj = s3_client.get_object(Bucket=bucket_name, Key='customers/customer_events.csv')
events_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
print(f"   ✅ Loaded {len(events_df):,} customer events")

# Parse dates
events_df['event_date'] = pd.to_datetime(events_df['event_date'])

# Filter to day pass purchases only
day_pass_events = events_df[events_df['event_type'] == 'day_pass_purchase'].copy()

print(f"\n2. Found {len(day_pass_events):,} day pass purchases")

if len(day_pass_events) == 0:
    print("   No day pass purchases found")
    sys.exit(0)

# Show date range
print(f"   Date range: {day_pass_events['event_date'].min().date()} to {day_pass_events['event_date'].max().date()}")

# For each day pass purchase, determine if it's from a new customer
# A new customer is one who has no prior purchase/checkin events before this purchase
print("\n3. Analyzing customer history for each purchase...")

results = []

for idx, purchase in day_pass_events.iterrows():
    customer_id = purchase['customer_id']
    purchase_date = purchase['event_date']

    # Get all events for this customer BEFORE this purchase (excluding flag_set events)
    prior_events = events_df[
        (events_df['customer_id'] == customer_id) &
        (events_df['event_date'] < purchase_date) &
        (events_df['event_type'] != 'flag_set')  # Exclude flag events
    ]

    is_new_customer = len(prior_events) == 0

    # Get all events for this customer (including this one, excluding flags)
    all_customer_events = events_df[
        (events_df['customer_id'] == customer_id) &
        (events_df['event_type'] != 'flag_set')
    ]

    results.append({
        'customer_id': customer_id,
        'purchase_date': purchase_date,
        'is_new_customer': is_new_customer,
        'prior_events_count': len(prior_events),
        'total_events_count': len(all_customer_events)
    })

results_df = pd.DataFrame(results)

# Calculate statistics
new_customer_purchases = results_df[results_df['is_new_customer'] == True]
returning_customer_purchases = results_df[results_df['is_new_customer'] == False]

new_count = len(new_customer_purchases)
returning_count = len(returning_customer_purchases)
total_count = len(results_df)

new_pct = (new_count / total_count * 100) if total_count > 0 else 0
returning_pct = (returning_count / total_count * 100) if total_count > 0 else 0

# Print results
print("\n" + "="*80)
print("RESULTS")
print("="*80)
print(f"\n📊 Total day pass purchases: {total_count:,}")
print(f"\n   🆕 New customers: {new_count:,} ({new_pct:.1f}%)")
print(f"   🔄 Returning customers: {returning_count:,} ({returning_pct:.1f}%)")

# Note: All day passes are treated as a single category in this analysis

# Show recent trends (last 90 days)
print("\n📅 Recent trends (last 90 days):")
recent_cutoff = datetime.now() - timedelta(days=90)
recent_purchases = results_df[results_df['purchase_date'] >= recent_cutoff]

if len(recent_purchases) > 0:
    recent_new = len(recent_purchases[recent_purchases['is_new_customer'] == True])
    recent_total = len(recent_purchases)
    recent_new_pct = (recent_new / recent_total * 100) if recent_total > 0 else 0

    print(f"   Total purchases: {recent_total:,}")
    print(f"   New customers: {recent_new:,} ({recent_new_pct:.1f}%)")
    print(f"   Returning: {recent_total - recent_new:,} ({100-recent_new_pct:.1f}%)")
else:
    print("   No purchases in last 90 days")

# Sample of new customer purchases
print("\n🔍 Sample of new customer day pass purchases:")
if len(new_customer_purchases) > 0:
    sample = new_customer_purchases.sort_values('purchase_date', ascending=False).head(10)
    for _, row in sample.iterrows():
        print(f"   {row['purchase_date'].date()} - Customer {row['customer_id']}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

"""
Investigate why day passes appear to be all single purchases
Are groups making separate transactions? Or is data structured differently?
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
print("INVESTIGATING DAY PASS GROUP PURCHASE PATTERNS")
print("="*80)

df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")

df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()

# Filter to Day Pass category
day_pass = df_2025[df_2025['revenue_category'].str.contains('Day Pass', case=False, na=False)].copy()

# Exclude special passes
event_keywords = ['birthday', 'event', 'party', 'rental', 'private']
prepaid_keywords = ['7 day', '5 climb', 'punch pass', 'multi', 'pack']

day_pass['is_event'] = day_pass['Description'].str.contains('|'.join(event_keywords), case=False, na=False)
day_pass['is_prepaid'] = day_pass['Description'].str.contains('|'.join(prepaid_keywords), case=False, na=False)
day_pass['is_spectator'] = day_pass['Description'].str.contains('spectator', case=False, na=False)

exclude_mask = day_pass['is_event'] | day_pass['is_prepaid'] | day_pass['is_spectator']
regular_passes = day_pass[~exclude_mask].copy()

print(f"\nRegular day passes in 2025: {len(regular_passes)}")

# Look at all available columns
print("\n" + "="*80)
print("AVAILABLE DATA FIELDS")
print("="*80)
print("\nColumns in transaction data:")
for col in regular_passes.columns:
    print(f"  - {col}")

# Check if there's a Customer Name field
print("\n\n" + "="*80)
print("APPROACH: LOOK FOR SIMULTANEOUS PURCHASES")
print("Looking for passes bought within 1 minute by same customer/location")
print("="*80)

if 'Customer Name' in regular_passes.columns:
    # Create datetime with minute precision
    regular_passes['datetime'] = pd.to_datetime(regular_passes['Date'])
    regular_passes['datetime_minute'] = regular_passes['datetime'].dt.floor('1min')

    # Group by customer name + minute
    proximity_groups = regular_passes.groupby(['Customer Name', 'datetime_minute']).agg({
        'transaction_id': 'count',
        'Total Amount': 'sum',
        'Description': lambda x: list(x)
    }).reset_index()
    proximity_groups.columns = ['customer', 'datetime', 'pass_count', 'total_amount', 'descriptions']

    # Filter to potential groups (2+ passes)
    potential_groups = proximity_groups[proximity_groups['pass_count'] >= 2].copy()

    print(f"\nPotential group purchases (2+ passes within 1 minute):")
    print(f"  Total: {len(potential_groups)}")

    if len(potential_groups) > 0:
        print("\n\nDistribution of passes in near-simultaneous purchases:")
        print("-"*60)
        group_dist = potential_groups['pass_count'].value_counts().sort_index()
        for size, count in group_dist.items():
            pct = (count / len(potential_groups) * 100)
            print(f"  {size} passes: {count:3d} events ({pct:.1f}%)")

        print("\n\nSample group purchases:")
        for i, row in potential_groups.head(15).iterrows():
            print(f"\n{row['customer']} @ {row['datetime']}")
            print(f"  {row['pass_count']} passes for ${row['total_amount']:.2f}")
            for desc in row['descriptions'][:5]:  # Show first 5
                print(f"    - {desc[:70]}")

        # Overall statistics
        print("\n\n" + "="*80)
        print("GROUP PURCHASE STATISTICS")
        print("="*80)

        # Among ALL purchases, what % are in groups?
        total_purchase_events = len(proximity_groups)
        solo_purchases = len(proximity_groups[proximity_groups['pass_count'] == 1])
        group_purchases = len(potential_groups)

        print(f"\nTotal purchase events: {total_purchase_events}")
        print(f"  Solo purchases (1 pass): {solo_purchases} ({solo_purchases/total_purchase_events*100:.1f}%)")
        print(f"  Group purchases (2+ passes): {group_purchases} ({group_purchases/total_purchase_events*100:.1f}%)")

        # Total passes sold in groups vs solo
        passes_in_groups = potential_groups['pass_count'].sum()
        passes_total = len(regular_passes)

        print(f"\nTotal passes sold: {passes_total}")
        print(f"  Sold as solo: {passes_total - passes_in_groups} ({(passes_total-passes_in_groups)/passes_total*100:.1f}%)")
        print(f"  Sold in groups: {passes_in_groups} ({passes_in_groups/passes_total*100:.1f}%)")

        print(f"\nAverage group size (when buying in groups): {potential_groups['pass_count'].mean():.2f}")
        print(f"Overall average (all purchases): {len(regular_passes) / len(proximity_groups):.2f}")

else:
    print("\n✗ Customer Name field not available")

# Check for different time windows
print("\n\n" + "="*80)
print("TRYING 5-MINUTE WINDOW")
print("="*80)

if 'Customer Name' in regular_passes.columns:
    regular_passes['datetime_5min'] = regular_passes['datetime'].dt.floor('5min')

    proximity_5min = regular_passes.groupby(['Customer Name', 'datetime_5min']).agg({
        'transaction_id': 'count',
        'Total Amount': 'sum'
    }).reset_index()
    proximity_5min.columns = ['customer', 'datetime', 'pass_count', 'total_amount']

    potential_groups_5min = proximity_5min[proximity_5min['pass_count'] >= 2]

    print(f"\nPotential group purchases (2+ passes within 5 minutes): {len(potential_groups_5min)}")

    if len(potential_groups_5min) > 0:
        dist_5min = potential_groups_5min['pass_count'].value_counts().sort_index()
        print("\nDistribution:")
        for size, count in dist_5min.items():
            print(f"  {size} passes: {count:3d} events")

# Look at same-day purchases by same customer
print("\n\n" + "="*80)
print("SAME-DAY PURCHASES (DIFFERENT CUSTOMERS, SAME TIMESTAMP)")
print("Looking for group visits where each person pays separately")
print("="*80)

if 'datetime' in regular_passes.columns:
    # Group by exact datetime (second precision)
    regular_passes['datetime_exact'] = regular_passes['datetime'].dt.floor('1s')

    same_time = regular_passes.groupby('datetime_exact').agg({
        'transaction_id': 'count',
        'Customer Name': lambda x: list(set(x)) if 'Customer Name' in regular_passes.columns else [],
        'Total Amount': 'sum',
        'Description': lambda x: list(x)
    }).reset_index()
    same_time.columns = ['datetime', 'pass_count', 'customers', 'total_amount', 'descriptions']

    # Groups of 2+ at exact same time
    simultaneous_groups = same_time[same_time['pass_count'] >= 2]

    print(f"\nPurchases at exact same second (2+ transactions): {len(simultaneous_groups)}")

    if len(simultaneous_groups) > 0:
        print("\nDistribution:")
        sim_dist = simultaneous_groups['pass_count'].value_counts().sort_index()
        for size, count in sim_dist.items():
            print(f"  {size} passes: {count:3d} events")

        print("\n\nSample simultaneous purchases:")
        for i, row in simultaneous_groups.head(10).iterrows():
            print(f"\n{row['datetime']}: {row['pass_count']} passes for ${row['total_amount']:.2f}")
            if row['customers']:
                print(f"  Customers: {', '.join(str(c) for c in row['customers'][:5])}")
            for desc in row['descriptions'][:5]:
                print(f"    - {desc[:70]}")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

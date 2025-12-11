"""
Analyze day pass quantity field to understand group sizes
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
print("DAY PASS GROUP SIZE DISTRIBUTION")
print("Using the 'quantity' field from transaction data")
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

print(f"\nTotal regular day pass transactions in 2025: {len(regular_passes)}")

# Look at quantity field
print("\n" + "="*80)
print("QUANTITY FIELD ANALYSIS")
print("="*80)

# Check what's in quantity
print("\nQuantity field sample values:")
print(regular_passes['quantity'].value_counts().head(20).to_string())

# Clean and convert
regular_passes['quantity_clean'] = pd.to_numeric(regular_passes['quantity'], errors='coerce')

# Check for NaN
nan_count = regular_passes['quantity_clean'].isna().sum()
print(f"\nMissing/invalid quantity values: {nan_count}")

if nan_count > 0:
    print("\nSample rows with missing quantity:")
    print(regular_passes[regular_passes['quantity_clean'].isna()][['Description', 'quantity', 'Total Amount']].head(10).to_string(index=False))

# Fill NaN with 1 (assume single pass if not specified)
regular_passes['quantity_clean'] = regular_passes['quantity_clean'].fillna(1.0)

# Distribution
print("\n\n" + "="*80)
print("GROUP SIZE DISTRIBUTION")
print("="*80)

qty_dist = regular_passes['quantity_clean'].value_counts().sort_index()

print("\nNumber of passes per transaction:")
print("-"*60)
total_transactions = len(regular_passes)
total_passes = regular_passes['quantity_clean'].sum()

for qty, count in qty_dist.items():
    pct_of_transactions = (count / total_transactions * 100)
    passes_in_this_size = qty * count
    pct_of_passes = (passes_in_this_size / total_passes * 100)
    print(f"  {int(qty):2d} pass(es): {count:4d} transactions ({pct_of_transactions:5.1f}%) = {int(passes_in_this_size):4d} passes sold ({pct_of_passes:5.1f}%)")

# Summary statistics
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

print(f"\nTotal transactions: {total_transactions:,}")
print(f"Total passes sold: {int(total_passes):,}")
print(f"Average group size: {regular_passes['quantity_clean'].mean():.2f} passes per transaction")
print(f"Median group size: {regular_passes['quantity_clean'].median():.0f}")
print(f"Max group size: {int(regular_passes['quantity_clean'].max())}")

# Percentage breakdowns
solo = (regular_passes['quantity_clean'] == 1).sum()
pairs = (regular_passes['quantity_clean'] == 2).sum()
small_groups = ((regular_passes['quantity_clean'] >= 3) & (regular_passes['quantity_clean'] <= 4)).sum()
large_groups = (regular_passes['quantity_clean'] >= 5).sum()

print(f"\n\nPurchase Patterns:")
print(f"  Solo (1 pass):       {solo:4d} ({solo/total_transactions*100:5.1f}%)")
print(f"  Pairs (2 passes):    {pairs:4d} ({pairs/total_transactions*100:5.1f}%)")
print(f"  Small groups (3-4):  {small_groups:4d} ({small_groups/total_transactions*100:5.1f}%)")
print(f"  Large groups (5+):   {large_groups:4d} ({large_groups/total_transactions*100:5.1f}%)")

# By pass volume
solo_passes = (regular_passes[regular_passes['quantity_clean'] == 1]['quantity_clean'].sum())
pair_passes = (regular_passes[regular_passes['quantity_clean'] == 2]['quantity_clean'].sum())
small_group_passes = regular_passes[(regular_passes['quantity_clean'] >= 3) & (regular_passes['quantity_clean'] <= 4)]['quantity_clean'].sum()
large_group_passes = regular_passes[regular_passes['quantity_clean'] >= 5]['quantity_clean'].sum()

print(f"\n\nPasses Sold (by group type):")
print(f"  Solo purchases:      {int(solo_passes):4d} passes ({solo_passes/total_passes*100:5.1f}%)")
print(f"  Pair purchases:      {int(pair_passes):4d} passes ({pair_passes/total_passes*100:5.1f}%)")
print(f"  Small group (3-4):   {int(small_group_passes):4d} passes ({small_group_passes/total_passes*100:5.1f}%)")
print(f"  Large group (5+):    {int(large_group_passes):4d} passes ({large_group_passes/total_passes*100:5.1f}%)")

# Large groups detail
print("\n\n" + "="*80)
print("LARGE GROUPS (5+ PASSES)")
print("="*80)

large_group_txns = regular_passes[regular_passes['quantity_clean'] >= 5].copy()
print(f"\nTransactions with 5+ passes: {len(large_group_txns)}")

if len(large_group_txns) > 0:
    print(f"Total passes in these transactions: {int(large_group_txns['quantity_clean'].sum())}")
    print(f"Average large group size: {large_group_txns['quantity_clean'].mean():.1f}")

    print("\n\nSample large group transactions:")
    print("-"*80)
    for i, row in large_group_txns.head(15).iterrows():
        print(f"{int(row['quantity_clean']):2d} passes @ ${row['Total Amount']:>7,.2f} on {row['Date'].strftime('%Y-%m-%d')} - {row['Description'][:55]}")

# Monthly trend
print("\n\n" + "="*80)
print("MONTHLY TRENDS")
print("="*80)

regular_passes['Month'] = regular_passes['Date'].dt.to_period('M')

monthly_stats = regular_passes.groupby('Month').agg({
    'quantity_clean': ['count', 'sum', 'mean'],
    'Total Amount': 'sum'
}).round(2)
monthly_stats.columns = ['Transactions', 'Total Passes', 'Avg Group Size', 'Revenue']

print("\n", monthly_stats.to_string())

# Youth vs Adult group patterns
print("\n\n" + "="*80)
print("GROUP SIZE BY AGE CATEGORY")
print("="*80)

youth_mask = regular_passes['Description'].str.contains('youth|under 14|kid|child', case=False, na=False)
adult_mask = regular_passes['Description'].str.contains('adult|14 and up', case=False, na=False)

youth_passes = regular_passes[youth_mask]
adult_passes = regular_passes[adult_mask]

if len(youth_passes) > 0:
    print(f"\nYouth passes:")
    print(f"  Transactions: {len(youth_passes)}")
    print(f"  Average group size: {youth_passes['quantity_clean'].mean():.2f}")
    print(f"  Solo: {(youth_passes['quantity_clean'] == 1).sum()} ({(youth_passes['quantity_clean'] == 1).sum()/len(youth_passes)*100:.1f}%)")
    print(f"  Groups (2+): {(youth_passes['quantity_clean'] >= 2).sum()} ({(youth_passes['quantity_clean'] >= 2).sum()/len(youth_passes)*100:.1f}%)")

if len(adult_passes) > 0:
    print(f"\nAdult passes:")
    print(f"  Transactions: {len(adult_passes)}")
    print(f"  Average group size: {adult_passes['quantity_clean'].mean():.2f}")
    print(f"  Solo: {(adult_passes['quantity_clean'] == 1).sum()} ({(adult_passes['quantity_clean'] == 1).sum()/len(adult_passes)*100:.1f}%)")
    print(f"  Groups (2+): {(adult_passes['quantity_clean'] >= 2).sum()} ({(adult_passes['quantity_clean'] >= 2).sum()/len(adult_passes)*100:.1f}%)")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

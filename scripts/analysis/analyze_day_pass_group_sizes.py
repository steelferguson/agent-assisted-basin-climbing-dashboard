"""
Analyze group size distribution for day pass purchases
How many passes are purchased together in single transactions?
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
print("DAY PASS GROUP SIZE ANALYSIS")
print("="*80)

df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")

df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()

# Filter to Day Pass category
day_pass = df_2025[df_2025['revenue_category'].str.contains('Day Pass', case=False, na=False)].copy()

print(f"\nTotal Day Pass transactions in 2025: {len(day_pass)}")

# Exclude event-specific and prepaid passes (like before)
event_keywords = ['birthday', 'event', 'party', 'rental', 'private']
prepaid_keywords = ['7 day', '5 climb', 'punch pass', 'multi', 'pack']

day_pass['is_event'] = day_pass['Description'].str.contains('|'.join(event_keywords), case=False, na=False)
day_pass['is_prepaid'] = day_pass['Description'].str.contains('|'.join(prepaid_keywords), case=False, na=False)
day_pass['is_spectator'] = day_pass['Description'].str.contains('spectator', case=False, na=False)

exclude_mask = day_pass['is_event'] | day_pass['is_prepaid'] | day_pass['is_spectator']
regular_passes = day_pass[~exclude_mask].copy()

print(f"Regular single-day passes: {len(regular_passes)}")

# Approach 1: Look at Quantity column if it exists
print("\n" + "="*80)
print("APPROACH 1: ANALYZING QUANTITY FIELD")
print("="*80)

if 'Quantity' in regular_passes.columns:
    print("\n✓ Quantity field exists")

    # Clean quantity data
    regular_passes['Quantity'] = pd.to_numeric(regular_passes['Quantity'], errors='coerce')
    regular_passes['Quantity'] = regular_passes['Quantity'].fillna(1)

    # Distribution
    qty_dist = regular_passes['Quantity'].value_counts().sort_index()

    print("\nGroup Size Distribution (by Quantity field):")
    print("-"*60)
    for qty, count in qty_dist.items():
        pct = (count / len(regular_passes) * 100)
        print(f"  {int(qty)} pass(es): {count:4d} transactions ({pct:5.1f}%)")

    print(f"\nTotal passes sold: {regular_passes['Quantity'].sum():.0f}")
    print(f"Average group size: {regular_passes['Quantity'].mean():.2f}")
    print(f"Median group size: {regular_passes['Quantity'].median():.0f}")

    # Large groups
    large_groups = regular_passes[regular_passes['Quantity'] >= 4]
    print(f"\n\nTransactions with 4+ passes: {len(large_groups)}")
    if len(large_groups) > 0:
        print("\nSample large group transactions:")
        for i, row in large_groups.head(10).iterrows():
            print(f"  {int(row['Quantity'])} passes @ ${row['Total Amount']:.2f} - {row['Description'][:60]}")
else:
    print("\n✗ No Quantity field available")

# Approach 2: Group by transaction_id/timestamp/customer
print("\n\n" + "="*80)
print("APPROACH 2: GROUPING BY TRANSACTION ID")
print("="*80)

if 'transaction_id' in regular_passes.columns:
    print("\n✓ transaction_id field exists")

    # Group by transaction ID - count how many day pass line items per transaction
    txn_groups = regular_passes.groupby('transaction_id').agg({
        'Description': 'count',  # Number of line items
        'Total Amount': 'sum',
        'Date': 'first'
    }).reset_index()
    txn_groups.columns = ['transaction_id', 'pass_count', 'total_amount', 'date']

    print(f"\nUnique transactions: {len(txn_groups)}")

    # Distribution
    txn_dist = txn_groups['pass_count'].value_counts().sort_index()

    print("\nGroup Size Distribution (by transaction ID):")
    print("-"*60)
    for count, freq in txn_dist.items():
        pct = (freq / len(txn_groups) * 100)
        print(f"  {count} pass(es): {freq:4d} transactions ({pct:5.1f}%)")

    print(f"\nAverage passes per transaction: {txn_groups['pass_count'].mean():.2f}")
    print(f"Median passes per transaction: {txn_groups['pass_count'].median():.0f}")

    # Look at large groups
    large_txns = txn_groups[txn_groups['pass_count'] >= 4]
    print(f"\n\nTransactions with 4+ passes: {len(large_txns)}")
    if len(large_txns) > 0:
        print("\nSample large group transactions:")
        for i, row in large_txns.head(10).iterrows():
            txn_details = regular_passes[regular_passes['transaction_id'] == row['transaction_id']]
            print(f"\n  Transaction on {row['date'].strftime('%Y-%m-%d')}: {row['pass_count']} passes for ${row['total_amount']:.2f}")
            for _, detail in txn_details.iterrows():
                print(f"    - {detail['Description'][:70]}")
else:
    print("\n✗ No transaction_id field available")

    # Try grouping by timestamp + customer name
    if 'Date' in regular_passes.columns and 'Customer Name' in regular_passes.columns:
        print("\nTrying to group by Date + Customer Name...")

        regular_passes['datetime'] = pd.to_datetime(regular_passes['Date'])

        # Group by customer name + date
        grouped = regular_passes.groupby(['Customer Name', pd.Grouper(key='datetime', freq='1min')]).agg({
            'Description': 'count',
            'Total Amount': 'sum'
        }).reset_index()
        grouped.columns = ['customer', 'datetime', 'pass_count', 'total_amount']

        # Filter to actual groups
        grouped = grouped[grouped['pass_count'] > 0]

        print(f"\nPurchase events: {len(grouped)}")

        dist = grouped['pass_count'].value_counts().sort_index()
        print("\nGroup Size Distribution (by Customer + Timestamp):")
        print("-"*60)
        for count, freq in dist.items():
            pct = (freq / len(grouped) * 100)
            print(f"  {count} pass(es): {freq:4d} events ({pct:5.1f}%)")

# Approach 3: Look at description patterns for "adult + youth" combos
print("\n\n" + "="*80)
print("APPROACH 3: DETECTING ADULT + YOUTH COMBINATIONS")
print("="*80)

# Check if descriptions indicate age groups
youth_mask = regular_passes['Description'].str.contains('youth|under 14|kid|child', case=False, na=False)
adult_mask = regular_passes['Description'].str.contains('adult|14 and up', case=False, na=False)

youth_passes = regular_passes[youth_mask]
adult_passes = regular_passes[adult_mask]

print(f"\nYouth passes: {len(youth_passes)}")
print(f"Adult passes: {len(adult_passes)}")

if 'transaction_id' in regular_passes.columns and len(youth_passes) > 0 and len(adult_passes) > 0:
    # Find transactions with both adult and youth
    youth_txn_ids = set(youth_passes['transaction_id'].unique())
    adult_txn_ids = set(adult_passes['transaction_id'].unique())

    mixed_txn_ids = youth_txn_ids.intersection(adult_txn_ids)

    print(f"\nTransactions with BOTH adult and youth passes: {len(mixed_txn_ids)}")

    if len(mixed_txn_ids) > 0:
        print("\nSample mixed-age group transactions:")
        for txn_id in list(mixed_txn_ids)[:10]:
            txn_details = regular_passes[regular_passes['transaction_id'] == txn_id]
            total = txn_details['Total Amount'].sum()
            print(f"\n  Transaction {txn_id}: ${total:.2f}")
            for _, detail in txn_details.iterrows():
                print(f"    - {detail['Description'][:70]}")

# Summary insights
print("\n\n" + "="*80)
print("SUMMARY")
print("="*80)

if 'Quantity' in regular_passes.columns:
    avg_qty = regular_passes['Quantity'].mean()
    solo_pct = (regular_passes['Quantity'] == 1).sum() / len(regular_passes) * 100

    print(f"\n✓ Most common purchase pattern:")
    print(f"  - Average group size: {avg_qty:.2f} passes per transaction")
    print(f"  - Solo purchases: {solo_pct:.1f}%")
    print(f"  - Group purchases (2+): {100-solo_pct:.1f}%")

# Month-by-month trend
if 'Quantity' in regular_passes.columns:
    print("\n\n" + "="*80)
    print("GROUP SIZE BY MONTH")
    print("="*80)

    regular_passes['Month'] = regular_passes['Date'].dt.to_period('M')

    monthly_group = regular_passes.groupby('Month').agg({
        'Quantity': ['count', 'sum', 'mean'],
        'Total Amount': 'sum'
    })
    monthly_group.columns = ['Transactions', 'Total Passes', 'Avg Group Size', 'Revenue']

    print("\n", monthly_group.to_string())

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

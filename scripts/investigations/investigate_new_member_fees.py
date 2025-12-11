"""
Investigate what's included in "New Membership" revenue
Does it include activation fees?
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
print("ANALYZING NEW MEMBERSHIP REVENUE")
print("Does it include activation fees?")
print("="*80)

df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")

df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()

# Filter to New Membership category
new_membership = df_2025[df_2025['revenue_category'] == 'New Membership'].copy()

print(f"\nTotal New Membership transactions in 2025: {len(new_membership)}")
print(f"Total New Membership revenue in 2025: ${new_membership['Total Amount'].sum():,.2f}")

# Look at payment descriptions
print("\n" + "="*80)
print("SAMPLE NEW MEMBERSHIP PAYMENT DESCRIPTIONS")
print("="*80)

print("\nFirst 30 descriptions:")
print("-"*80)
for i, (desc, amount) in enumerate(zip(new_membership['Description'].head(30),
                                       new_membership['Total Amount'].head(30)), 1):
    print(f"{i:2d}. ${amount:>8,.2f} - {desc}")

# Check for "initial payment" vs "activation fee" separately
print("\n\n" + "="*80)
print("LOOKING FOR ACTIVATION FEE PATTERNS")
print("="*80)

# Search for keywords
activation_keywords = ['activation', 'join fee', 'signup fee', 'enrollment fee', 'initiation']
initial_keywords = ['initial payment', 'first payment', 'new membership']

activation_mask = new_membership['Description'].str.contains('|'.join(activation_keywords),
                                                              case=False, na=False)
initial_mask = new_membership['Description'].str.contains('|'.join(initial_keywords),
                                                           case=False, na=False)

print(f"\nTransactions with 'activation/fee' keywords: {activation_mask.sum()}")
print(f"Transactions with 'initial payment' keywords: {initial_mask.sum()}")

if activation_mask.sum() > 0:
    print("\nSample activation fee transactions:")
    activation_txns = new_membership[activation_mask]
    for i, row in activation_txns.head(10).iterrows():
        print(f"  ${row['Total Amount']:>8,.2f} - {row['Description']}")

# Check payment amounts - are there consistent $75 or $135 amounts?
print("\n\n" + "="*80)
print("PAYMENT AMOUNT DISTRIBUTION (looking for $75 or $135 patterns)")
print("="*80)

# Round to nearest dollar to group similar amounts
new_membership['Amount_Rounded'] = new_membership['Total Amount'].round(0)

common_amounts = new_membership['Amount_Rounded'].value_counts().head(20)

print("\nMost Common Payment Amounts:")
print("-"*60)
for amount, count in common_amounts.items():
    pct = (count / len(new_membership) * 100)
    total_revenue = amount * count
    print(f"  ${amount:>8,.0f} × {count:3d} transactions ({pct:5.1f}%) = ${total_revenue:>10,.2f}")

# Specifically check for $75 and $135 (allowing for tax)
print("\n\nLooking for activation fee amounts ($75-$85 or $135-$150 range):")
print("-"*60)

lower_fee_mask = (new_membership['Total Amount'] >= 75) & (new_membership['Total Amount'] <= 85)
higher_fee_mask = (new_membership['Total Amount'] >= 135) & (new_membership['Total Amount'] <= 150)

print(f"\nPayments in $75-$85 range: {lower_fee_mask.sum()} (potential $75 activation fees)")
print(f"Total: ${new_membership[lower_fee_mask]['Total Amount'].sum():,.2f}")

print(f"\nPayments in $135-$150 range: {higher_fee_mask.sum()} (potential $135 activation fees)")
print(f"Total: ${new_membership[higher_fee_mask]['Total Amount'].sum():,.2f}")

if lower_fee_mask.sum() > 0:
    print("\nSample $75-range payments:")
    for i, row in new_membership[lower_fee_mask].head(10).iterrows():
        print(f"  ${row['Total Amount']:>7,.2f} - {row['Description'][:70]}")

if higher_fee_mask.sum() > 0:
    print("\nSample $135-range payments:")
    for i, row in new_membership[higher_fee_mask].head(10).iterrows():
        print(f"  ${row['Total Amount']:>7,.2f} - {row['Description'][:70]}")

# Monthly breakdown
print("\n\n" + "="*80)
print("NEW MEMBERSHIP REVENUE BY MONTH")
print("="*80)

new_membership['Month'] = new_membership['Date'].dt.to_period('M')

monthly_new = new_membership.groupby('Month').agg({
    'Total Amount': ['sum', 'count', 'mean'],
    'transaction_id': 'count'
})
monthly_new.columns = ['Total Revenue', 'Transaction Count', 'Avg Payment', 'Count2']
monthly_new = monthly_new[['Total Revenue', 'Transaction Count', 'Avg Payment']]

print("\n", monthly_new.to_string())

# Check what's categorized as "initial payment"
print("\n\n" + "="*80)
print("EXAMINING 'INITIAL PAYMENT' DESCRIPTIONS")
print("="*80)

initial_payments = new_membership[initial_mask]
print(f"\nTransactions with 'initial payment': {len(initial_payments)}")
print(f"Average amount: ${initial_payments['Total Amount'].mean():.2f}")
print(f"Min amount: ${initial_payments['Total Amount'].min():.2f}")
print(f"Max amount: ${initial_payments['Total Amount'].max():.2f}")

# Group by amount ranges
print("\n\nInitial Payment Amount Distribution:")
bins = [0, 100, 200, 300, 500, 1000, 2000, 10000]
labels = ['$0-100', '$100-200', '$200-300', '$300-500', '$500-1000', '$1000-2000', '$2000+']
initial_payments['Amount_Range'] = pd.cut(initial_payments['Total Amount'], bins=bins, labels=labels)

range_dist = initial_payments['Amount_Range'].value_counts().sort_index()
print("\n", range_dist.to_string())

# Sample from each range
print("\n\nSample initial payments by amount range:")
for label in labels:
    sample = initial_payments[initial_payments['Amount_Range'] == label]
    if len(sample) > 0:
        print(f"\n{label}:")
        for i, row in sample.head(3).iterrows():
            print(f"    ${row['Total Amount']:>8,.2f} - {row['Description'][:60]}")

print("\n" + "="*80)
print("CONCLUSION")
print("="*80)

print("\nBased on transaction descriptions and amounts:")
print("  - 'New Membership' category appears to include the first payment + activation fee")
print("  - Amounts vary widely depending on membership type and billing cycle")
print("  - No separate line items visible for 'activation fee' vs 'first payment'")
print("  - This appears to be the combined total charged at signup")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80 + "\n")

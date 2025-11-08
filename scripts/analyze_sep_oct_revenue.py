"""
Analyze September and October 2025 revenue by category and payment source
"""
import pandas as pd
import boto3
import os
from datetime import datetime

# Load data from S3
s3 = boto3.client('s3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

print("Loading transaction data from S3...")
obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='data/outputs/stripe_and_square_combined_data.csv')
df = pd.read_csv(obj['Body'])

print(f"Loaded {len(df):,} transactions")
print(f"\nColumns: {df.columns.tolist()}")

# Use date_ column with mixed format
df['date'] = pd.to_datetime(df['date_'], format='mixed', utc=True)

# Filter to September and October 2024
df_sep = df[(df['date'].dt.year == 2024) & (df['date'].dt.month == 9)].copy()
df_oct = df[(df['date'].dt.year == 2024) & (df['date'].dt.month == 10)].copy()

print(f"\nSeptember transactions: {len(df_sep):,}")
print(f"October transactions: {len(df_oct):,}")

# Check if we have sales tax data
tax_cols = [col for col in df.columns if 'tax' in col.lower()]
print(f"\nTax-related columns found: {tax_cols}")

# Show sample record to see structure
print("\nSample transaction:")
print(df.iloc[0].to_dict())

# Define category groupings
def categorize(row):
    cat = row.get('revenue_category', '')

    # Day Passes
    if cat == 'Day Pass':
        return 'Day Passes'

    # Memberships (combine new and renewals)
    elif cat in ['Membership Renewal', 'New Membership']:
        return 'Memberships'

    # Retail
    elif cat == 'Retail':
        return 'Retail'

    # Programming (classes, camps, teams, fitness)
    elif cat in ['Programming', 'Team']:
        return 'Programming'

    # Event Booking might be rentals?
    elif cat == 'Event Booking':
        return 'Rentals'

    # Default
    else:
        return 'Other'

df_sep['Category'] = df_sep.apply(categorize, axis=1)
df_oct['Category'] = df_oct.apply(categorize, axis=1)

def analyze_month(df, month_name):
    print(f"\n{'='*80}")
    print(f"{month_name} 2025 Revenue Analysis")
    print(f"{'='*80}\n")

    # Total revenue
    total_revenue = df['Total Amount'].sum()
    print(f"TOTAL REVENUE: ${total_revenue:,.2f}\n")

    # By category
    print("Revenue by Category:")
    print("-" * 60)
    category_revenue = df.groupby('Category')['Total Amount'].sum().sort_values(ascending=False)

    for cat in ['Day Passes', 'Memberships', 'Rentals', 'Programming', 'Retail']:
        rev = category_revenue.get(cat, 0)
        pct = (rev / total_revenue * 100) if total_revenue > 0 else 0
        print(f"{cat:20s}  ${rev:>12,.2f}  ({pct:>5.1f}%)")

    # Other category
    other_rev = category_revenue.get('Other', 0)
    other_pct = (other_rev / total_revenue * 100) if total_revenue > 0 else 0
    print(f"{'Other':20s}  ${other_rev:>12,.2f}  ({other_pct:>5.1f}%)")

    print()

    # By payment source
    print("Revenue by Payment Source:")
    print("-" * 60)
    source_revenue = df.groupby('Data Source')['Total Amount'].sum().sort_values(ascending=False)
    for source, rev in source_revenue.items():
        pct = (rev / total_revenue * 100) if total_revenue > 0 else 0
        print(f"{source:20s}  ${rev:>12,.2f}  ({pct:>5.1f}%)")

# Analyze both months
analyze_month(df_sep, "September")
analyze_month(df_oct, "October")

# Combined Sep + Oct
print(f"\n{'='*80}")
print("COMBINED SEPTEMBER + OCTOBER 2025")
print(f"{'='*80}\n")

df_both = pd.concat([df_sep, df_oct])
total_both = df_both['Total Amount'].sum()
print(f"TOTAL REVENUE (Sep + Oct): ${total_both:,.2f}\n")

print("Revenue by Category (Sep + Oct Combined):")
print("-" * 60)
category_both = df_both.groupby('Category')['Total Amount'].sum().sort_values(ascending=False)

for cat in ['Day Passes', 'Memberships', 'Rentals', 'Programming', 'Retail']:
    rev = category_both.get(cat, 0)
    pct = (rev / total_both * 100) if total_both > 0 else 0
    print(f"{cat:20s}  ${rev:>12,.2f}  ({pct:>5.1f}%)")

other_rev = category_both.get('Other', 0)
other_pct = (other_rev / total_both * 100) if total_both > 0 else 0
print(f"{'Other':20s}  ${other_rev:>12,.2f}  ({other_pct:>5.1f}%)")

print()

print("Revenue by Payment Source (Sep + Oct Combined):")
print("-" * 60)
source_both = df_both.groupby('Data Source')['Total Amount'].sum().sort_values(ascending=False)
for source, rev in source_both.items():
    pct = (rev / total_both * 100) if total_both > 0 else 0
    print(f"{source:20s}  ${rev:>12,.2f}  ({pct:>5.1f}%)")

# Check for tax info
print(f"\n{'='*80}")
print("SALES TAX INFORMATION")
print(f"{'='*80}\n")

if tax_cols:
    print(f"Tax columns found in data: {tax_cols}")
    for col in tax_cols:
        total_tax = df_both[col].sum() if col in df_both.columns else 0
        print(f"{col}: ${total_tax:,.2f}")

    # Show sample Stripe transactions with tax info
    stripe_sample = df_both[df_both['Data Source'] == 'Stripe'].head(3)
    print("\nSample Stripe transactions:")
    for idx, row in stripe_sample.iterrows():
        print(f"\nDate: {row['Date']}, Amount: ${row['Total Amount']:.2f}")
        for col in tax_cols:
            if col in row:
                print(f"  {col}: {row[col]}")
else:
    print("⚠️  No tax-related columns found in the transaction data.")
    print("This suggests sales tax may not be tracked in the current data structure.")
    print("\nTo verify, we should check:")
    print("1. Stripe dashboard directly")
    print("2. Square dashboard for tax collection settings")
    print("3. Individual transaction details in Stripe/Square APIs")

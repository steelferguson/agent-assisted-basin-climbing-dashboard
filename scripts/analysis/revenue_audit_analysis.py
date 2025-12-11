"""
Revenue Audit Analysis
Investigating revenue drop from ~$70K/month to ~$50-55K/month starting September 2025
"""

import pandas as pd
import numpy as np
import boto3
from datetime import datetime, timedelta
import os
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def load_csv_from_s3(bucket_name, s3_key):
    """Load a CSV file from S3 into a pandas DataFrame"""
    try:
        print(f"Loading {s3_key} from S3...")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(f"  ✓ Loaded {len(df)} rows")
        return df
    except Exception as e:
        print(f"  ✗ Error loading {s3_key}: {e}")
        return None

def get_monthly_revenue_summary(df_transactions):
    """Calculate monthly revenue totals from combined transaction data"""
    print("\n" + "="*80)
    print("MONTHLY REVENUE SUMMARY")
    print("="*80)

    # Ensure Date column is datetime
    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])

    # Filter to 2025 data only
    df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()

    # Add month column
    df_2025['Month'] = df_2025['Date'].dt.to_period('M')

    # Calculate monthly totals
    monthly_summary = df_2025.groupby('Month').agg({
        'Total Amount': 'sum',
        'transaction_id': 'count',
        'Name': 'nunique'
    }).round(2)

    monthly_summary.columns = ['Total Revenue', 'Transaction Count', 'Unique Customers']
    monthly_summary['Avg Transaction'] = (monthly_summary['Total Revenue'] / monthly_summary['Transaction Count']).round(2)

    print("\nMonthly Revenue (2025):")
    print(monthly_summary.to_string())

    # Calculate month-over-month change
    monthly_summary['Revenue Change $'] = monthly_summary['Total Revenue'].diff()
    monthly_summary['Revenue Change %'] = (monthly_summary['Total Revenue'].pct_change() * 100).round(2)

    print("\n\nMonth-over-Month Changes:")
    print(monthly_summary[['Total Revenue', 'Revenue Change $', 'Revenue Change %']].to_string())

    return monthly_summary

def analyze_revenue_by_category(df_transactions):
    """Analyze revenue breakdown by category for each month"""
    print("\n" + "="*80)
    print("REVENUE BY CATEGORY - MONTHLY BREAKDOWN")
    print("="*80)

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
    df_2025['Month'] = df_2025['Date'].dt.to_period('M')

    # Create pivot table
    category_by_month = pd.pivot_table(
        df_2025,
        values='Total Amount',
        index='revenue_category',
        columns='Month',
        aggfunc='sum',
        fill_value=0
    ).round(2)

    print("\nRevenue by Category (each column is a month):")
    print(category_by_month.to_string())

    # Calculate category totals and percentages
    category_totals = df_2025.groupby('revenue_category')['Total Amount'].sum().sort_values(ascending=False)

    print("\n\nTotal Revenue by Category (2025 YTD):")
    for cat, amount in category_totals.items():
        pct = (amount / category_totals.sum() * 100)
        print(f"  {cat:30s} ${amount:>12,.2f}  ({pct:.1f}%)")

    return category_by_month

def analyze_revenue_by_source(df_transactions):
    """Analyze revenue by payment source (Stripe vs Square)"""
    print("\n" + "="*80)
    print("REVENUE BY PAYMENT SOURCE")
    print("="*80)

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
    df_2025['Month'] = df_2025['Date'].dt.to_period('M')

    # Revenue by source and month
    source_by_month = pd.pivot_table(
        df_2025,
        values='Total Amount',
        index='Data Source',
        columns='Month',
        aggfunc='sum',
        fill_value=0
    ).round(2)

    print("\nRevenue by Source (Stripe vs Square):")
    print(source_by_month.to_string())

    # Add totals
    print("\n\nSource Totals (2025 YTD):")
    source_totals = df_2025.groupby('Data Source')['Total Amount'].sum().sort_values(ascending=False)
    for source, amount in source_totals.items():
        pct = (amount / source_totals.sum() * 100)
        print(f"  {source:20s} ${amount:>12,.2f}  ({pct:.1f}%)")

    return source_by_month

def analyze_membership_revenue_detail(df_transactions):
    """Deep dive into membership revenue"""
    print("\n" + "="*80)
    print("MEMBERSHIP REVENUE DEEP DIVE")
    print("="*80)

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
    df_2025['Month'] = df_2025['Date'].dt.to_period('M')

    # Filter to membership transactions
    membership_mask = df_2025['revenue_category'].str.contains('Membership', case=False, na=False)
    df_memberships = df_2025[membership_mask].copy()

    print(f"\nTotal Membership Transactions in 2025: {len(df_memberships)}")

    # Monthly membership revenue
    monthly_membership = df_memberships.groupby('Month').agg({
        'Total Amount': 'sum',
        'transaction_id': 'count',
        'Name': 'nunique'
    }).round(2)
    monthly_membership.columns = ['Membership Revenue', 'Payment Count', 'Unique Payers']

    print("\nMonthly Membership Revenue:")
    print(monthly_membership.to_string())

    # Membership by sub-category if available
    if 'sub_category' in df_memberships.columns:
        print("\n\nMembership Revenue by Sub-Category:")
        subcat_by_month = pd.pivot_table(
            df_memberships,
            values='Total Amount',
            index='sub_category',
            columns='Month',
            aggfunc='sum',
            fill_value=0
        ).round(2)
        print(subcat_by_month.to_string())

    return monthly_membership

def analyze_customer_counts(df_transactions):
    """Analyze unique customer counts and transaction patterns"""
    print("\n" + "="*80)
    print("CUSTOMER ACTIVITY ANALYSIS")
    print("="*80)

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
    df_2025['Month'] = df_2025['Date'].dt.to_period('M')

    # Unique customers per month
    customers_per_month = df_2025.groupby('Month')['Name'].nunique()

    print("\nUnique Customers per Month:")
    for month, count in customers_per_month.items():
        print(f"  {month}: {count} customers")

    # Average spend per customer
    avg_spend_per_customer = df_2025.groupby('Month').apply(
        lambda x: x['Total Amount'].sum() / x['Name'].nunique()
    ).round(2)

    print("\nAverage Spend per Customer per Month:")
    for month, avg in avg_spend_per_customer.items():
        print(f"  {month}: ${avg:.2f}")

    return customers_per_month

def identify_missing_payers(df_transactions):
    """Identify customers who paid regularly but stopped"""
    print("\n" + "="*80)
    print("IDENTIFYING CUSTOMERS WITH PAYMENT PATTERN CHANGES")
    print("="*80)

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()
    df_2025['Month'] = df_2025['Date'].dt.to_period('M')

    # Get customers who paid in June-August but not in Sept-Nov
    early_months = df_2025[df_2025['Month'].isin([pd.Period('2025-06'), pd.Period('2025-07'), pd.Period('2025-08')])]
    late_months = df_2025[df_2025['Month'].isin([pd.Period('2025-09'), pd.Period('2025-10'), pd.Period('2025-11')])]

    early_payers = set(early_months['Name'].unique())
    late_payers = set(late_months['Name'].unique())

    # Customers who stopped paying
    stopped_paying = early_payers - late_payers

    print(f"\nCustomers who paid in Jun-Aug but NOT in Sep-Nov: {len(stopped_paying)}")

    if len(stopped_paying) > 0:
        # Get their payment history
        stopped_paying_df = df_2025[df_2025['Name'].isin(stopped_paying)].copy()

        # Calculate total lost revenue from these customers
        avg_payment_early = early_months[early_months['Name'].isin(stopped_paying)].groupby('Name')['Total Amount'].mean()
        estimated_monthly_loss = avg_payment_early.sum()

        print(f"Estimated Monthly Revenue Loss from these customers: ${estimated_monthly_loss:,.2f}")

        # Show top customers by average payment amount
        print("\nTop 20 Customers Who Stopped Paying (by avg payment amount):")
        top_stopped = avg_payment_early.sort_values(ascending=False).head(20)
        for i, (name, avg_amt) in enumerate(top_stopped.items(), 1):
            payment_count = len(early_months[early_months['Name'] == name])
            print(f"  {i:2d}. {name:40s} Avg: ${avg_amt:>8,.2f}  (paid {payment_count} times)")

    return stopped_paying

def main():
    """Main analysis function"""
    print("\n" + "="*80)
    print("BASIN CLIMBING REVENUE AUDIT")
    print("Hypothesis: Revenue dropped from ~$70K/month to ~$50-55K starting Sept 2025")
    print("="*80)

    # Load combined transaction data
    print("\n\nStep 1: Loading Data from S3...")
    df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")

    if df_transactions is None:
        print("ERROR: Could not load transaction data. Exiting.")
        return

    print(f"\nData loaded successfully!")
    print(f"  Total transactions: {len(df_transactions):,}")
    print(f"  Date range: {df_transactions['Date'].min()} to {df_transactions['Date'].max()}")
    print(f"  Columns: {', '.join(df_transactions.columns.tolist())}")

    # Run all analyses
    print("\n\n")
    monthly_summary = get_monthly_revenue_summary(df_transactions)

    category_breakdown = analyze_revenue_by_category(df_transactions)

    source_breakdown = analyze_revenue_by_source(df_transactions)

    membership_detail = analyze_membership_revenue_detail(df_transactions)

    customer_counts = analyze_customer_counts(df_transactions)

    stopped_paying = identify_missing_payers(df_transactions)

    # Final Summary
    print("\n" + "="*80)
    print("SUMMARY OF FINDINGS")
    print("="*80)
    print("\nThis analysis examined revenue patterns from June-November 2025.")
    print("Review the detailed breakdowns above to identify where revenue declined.")
    print("\nKey areas to investigate:")
    print("  1. Month-over-month revenue changes")
    print("  2. Category-level revenue shifts")
    print("  3. Payment source changes (Stripe vs Square)")
    print("  4. Membership payment patterns")
    print("  5. Customers who stopped paying")

    print("\n" + "="*80)
    print("Analysis Complete!")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

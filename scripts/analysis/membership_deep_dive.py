"""
Deep Dive into Membership Data
Analyzing which specific memberships stopped being charged
"""

import pandas as pd
import numpy as np
import boto3
from datetime import datetime
import os
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def load_csv_from_s3(bucket_name, s3_key):
    """Load CSV from S3"""
    try:
        print(f"Loading {s3_key}...")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        print(f"  ✓ Loaded {len(df)} rows")
        return df
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None

def analyze_membership_counts_over_time(df_memberships):
    """Analyze how many active memberships existed each month"""
    print("\n" + "="*80)
    print("ACTIVE MEMBERSHIP COUNTS BY MONTH")
    print("="*80)

    # Ensure date columns are datetime
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    # Define months to analyze
    months = pd.date_range('2025-06-01', '2025-11-30', freq='MS')

    monthly_counts = []

    for month_start in months:
        month_end = month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)

        # Count memberships active during this month
        active_mask = (
            (df_memberships['start_date'] <= month_end) &
            ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= month_start))
        )

        active_count = active_mask.sum()
        monthly_counts.append({
            'Month': month_start.strftime('%Y-%m'),
            'Active Memberships': active_count
        })

    df_monthly = pd.DataFrame(monthly_counts)
    print("\n", df_monthly.to_string(index=False))

    # Calculate change
    df_monthly['Change'] = df_monthly['Active Memberships'].diff()
    print("\n\nMonth-over-Month Change:")
    print(df_monthly[['Month', 'Active Memberships', 'Change']].to_string(index=False))

    return df_monthly

def analyze_membership_by_type(df_memberships):
    """Break down memberships by type/size"""
    print("\n" + "="*80)
    print("MEMBERSHIP BREAKDOWN BY TYPE")
    print("="*80)

    # Ensure dates
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    months = pd.date_range('2025-06-01', '2025-11-30', freq='MS')

    # Get membership type column (might be 'type' or 'membership_type' or similar)
    type_col = None
    for col in ['type', 'membership_type', 'plan_name', 'name']:
        if col in df_memberships.columns:
            type_col = col
            break

    if type_col is None:
        print("Could not find membership type column")
        print(f"Available columns: {df_memberships.columns.tolist()}")
        return None

    print(f"\nUsing column '{type_col}' for membership types")

    # For each month, count memberships by type
    results = []

    for month_start in months:
        month_end = month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)

        active_mask = (
            (df_memberships['start_date'] <= month_end) &
            ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= month_start))
        )

        active_this_month = df_memberships[active_mask]

        type_counts = active_this_month[type_col].value_counts()

        for membership_type, count in type_counts.items():
            results.append({
                'Month': month_start.strftime('%Y-%m'),
                'Membership Type': membership_type,
                'Count': count
            })

    df_results = pd.DataFrame(results)

    # Pivot for easier viewing
    pivot = df_results.pivot(index='Membership Type', columns='Month', values='Count').fillna(0).astype(int)

    print("\nMembership Counts by Type and Month:")
    print(pivot.to_string())

    # Calculate changes from June to November
    if '2025-06' in pivot.columns and '2025-11' in pivot.columns:
        pivot['Change (Jun→Nov)'] = pivot['2025-11'] - pivot['2025-06']
        print("\n\nChange from June to November by Type:")
        changes = pivot[['2025-06', '2025-11', 'Change (Jun→Nov)']].sort_values('Change (Jun→Nov)')
        print(changes.to_string())

    return pivot

def identify_memberships_that_ended(df_memberships):
    """Identify specific memberships that ended between June and November"""
    print("\n" + "="*80)
    print("MEMBERSHIPS THAT ENDED (June - November 2025)")
    print("="*80)

    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    # Memberships that ended between June 1 and Nov 30, 2025
    ended_mask = (
        df_memberships['end_date'].notna() &
        (df_memberships['end_date'] >= '2025-06-01') &
        (df_memberships['end_date'] <= '2025-11-30')
    )

    ended_memberships = df_memberships[ended_mask].copy()

    print(f"\nTotal memberships that ended in this period: {len(ended_memberships)}")

    if len(ended_memberships) > 0:
        # Group by month
        ended_memberships['end_month'] = ended_memberships['end_date'].dt.to_period('M')

        ended_by_month = ended_memberships.groupby('end_month').size()

        print("\nMemberships Ended by Month:")
        for month, count in ended_by_month.items():
            print(f"  {month}: {count} memberships")

        # Get membership details
        print("\n\nTop Ended Memberships (by monthly_rate):")

        # Find rate column
        rate_col = None
        for col in ['monthly_rate', 'rate', 'amount', 'price']:
            if col in ended_memberships.columns:
                rate_col = col
                break

        if rate_col:
            ended_sorted = ended_memberships.sort_values(rate_col, ascending=False)

            cols_to_show = ['end_date']
            for col in ['member_name', 'name', 'first_name', 'last_name']:
                if col in ended_sorted.columns:
                    cols_to_show.append(col)
                    break

            for col in ['type', 'membership_type', 'plan_name']:
                if col in ended_sorted.columns:
                    cols_to_show.append(col)
                    break

            cols_to_show.append(rate_col)

            print(ended_sorted[cols_to_show].head(30).to_string(index=False))
        else:
            print("Could not find rate column")

    return ended_memberships

def analyze_failed_payments(df_failed):
    """Analyze failed payment data if available"""
    print("\n" + "="*80)
    print("FAILED MEMBERSHIP PAYMENTS")
    print("="*80)

    if df_failed is None or len(df_failed) == 0:
        print("No failed payment data available")
        return None

    print(f"Total failed payments: {len(df_failed)}")

    # Ensure date column
    df_failed['created'] = pd.to_datetime(df_failed['created'], errors='coerce')

    # Filter to 2025
    df_failed_2025 = df_failed[df_failed['created'].dt.year == 2025].copy()

    print(f"Failed payments in 2025: {len(df_failed_2025)}")

    # Group by month
    df_failed_2025['month'] = df_failed_2025['created'].dt.to_period('M')

    monthly_failures = df_failed_2025.groupby('month').agg({
        'payment_intent_id': 'count',
        'amount': 'sum'
    })

    monthly_failures.columns = ['Failed Count', 'Failed Amount']

    print("\nFailed Payments by Month:")
    print(monthly_failures.to_string())

    # Breakdown by decline code if available
    if 'decline_code' in df_failed_2025.columns:
        print("\n\nFailure Reasons (Top 10):")
        decline_counts = df_failed_2025['decline_code'].value_counts().head(10)
        for reason, count in decline_counts.items():
            print(f"  {reason}: {count}")

    return df_failed_2025

def cross_reference_payments_and_memberships(df_transactions, df_memberships):
    """Cross-reference actual payments with expected memberships"""
    print("\n" + "="*80)
    print("EXPECTED vs ACTUAL MEMBERSHIP PAYMENTS")
    print("="*80)

    # This is complex - need to match membership IDs to payments
    # Look for membership IDs in payment descriptions

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')

    # Get September memberships that should have been active
    sept_start = pd.Timestamp('2025-09-01')
    sept_end = pd.Timestamp('2025-09-30')

    active_sept_mask = (
        (df_memberships['start_date'] <= sept_end) &
        ((df_memberships['end_date'].isna()) | (pd.to_datetime(df_memberships['end_date']) >= sept_start))
    )

    active_sept = df_memberships[active_sept_mask].copy()
    print(f"\nExpected active memberships in September 2025: {len(active_sept)}")

    # Get actual membership payments in September
    sept_payments = df_transactions[
        (df_transactions['Date'] >= sept_start) &
        (df_transactions['Date'] <= sept_end) &
        (df_transactions['revenue_category'].str.contains('Membership', case=False, na=False))
    ]

    print(f"Actual membership payments in September: {len(sept_payments)}")
    print(f"Unique payers in September: {sept_payments['Name'].nunique()}")

    # Calculate expected revenue
    rate_col = None
    for col in ['monthly_rate', 'rate', 'amount']:
        if col in active_sept.columns:
            rate_col = col
            break

    if rate_col:
        expected_revenue = active_sept[rate_col].sum()
        actual_revenue = sept_payments['Total Amount'].sum()

        print(f"\nExpected membership revenue (Sept): ${expected_revenue:,.2f}")
        print(f"Actual membership revenue (Sept): ${actual_revenue:,.2f}")
        print(f"Shortfall: ${expected_revenue - actual_revenue:,.2f}")

    return active_sept, sept_payments

def main():
    print("\n" + "="*80)
    print("MEMBERSHIP DEEP DIVE ANALYSIS")
    print("="*80)

    # Load data
    print("\nLoading data from S3...")

    df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")
    df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
    df_failed = load_csv_from_s3(AWS_BUCKET_NAME, "stripe/failed_membership_payments.csv")

    if df_memberships is not None:
        print(f"\nMembership data columns: {df_memberships.columns.tolist()}")

        monthly_counts = analyze_membership_counts_over_time(df_memberships)

        type_breakdown = analyze_membership_by_type(df_memberships)

        ended_memberships = identify_memberships_that_ended(df_memberships)

    if df_failed is not None:
        failed_analysis = analyze_failed_payments(df_failed)

    if df_transactions is not None and df_memberships is not None:
        active_sept, sept_payments = cross_reference_payments_and_memberships(df_transactions, df_memberships)

    print("\n" + "="*80)
    print("MEMBERSHIP ANALYSIS COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

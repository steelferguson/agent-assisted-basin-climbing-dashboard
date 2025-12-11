"""
Analyze the $90 for 90 Program specifically
This appears to be a major source of the revenue drop
"""

import pandas as pd
import boto3
from datetime import datetime
import os
from io import StringIO
import warnings
warnings.filterwarnings('ignore')

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "basin-climbing-data-prod"

s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def load_csv_from_s3(bucket_name, s3_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Error loading {s3_key}: {e}")
        return None

def main():
    print("\n" + "="*80)
    print("$90 FOR 90 PROGRAM ANALYSIS")
    print("="*80)

    df_memberships = load_csv_from_s3(AWS_BUCKET_NAME, "capitan/memberships.csv")
    df_transactions = load_csv_from_s3(AWS_BUCKET_NAME, "transactions/combined_transaction_data.csv")

    if df_memberships is None or df_transactions is None:
        print("Could not load data")
        return

    # Filter to 90 for 90 memberships
    ninety_mask = df_memberships['name'].str.contains('$90 for 90', case=False, na=False)
    df_90 = df_memberships[ninety_mask].copy()

    print(f"\nTotal $90 for 90 memberships ever created: {len(df_90)}")

    # Convert dates
    df_90['start_date'] = pd.to_datetime(df_90['start_date'])
    df_90['end_date'] = pd.to_datetime(df_90['end_date'])

    # Analyze by month
    print("\n" + "="*80)
    print("$90 FOR 90 MEMBERSHIP COUNTS BY MONTH")
    print("="*80)

    months = pd.date_range('2025-06-01', '2025-11-30', freq='MS')

    for month_start in months:
        month_end = month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)

        active = df_90[
            (df_90['start_date'] <= month_end) &
            ((df_90['end_date'].isna()) | (df_90['end_date'] >= month_start))
        ]

        solo_count = len(active[active['name'].str.contains('Solo', na=False)])
        family_count = len(active[active['name'].str.contains('Family', na=False)])

        print(f"\n{month_start.strftime('%B %Y')}:")
        print(f"  Solo: {solo_count}")
        print(f"  Family: {family_count}")
        print(f"  Total: {len(active)}")

    # When did these memberships end?
    print("\n" + "="*80)
    print("WHEN DID $90 FOR 90 MEMBERSHIPS END?")
    print("="*80)

    ended_90 = df_90[df_90['end_date'].notna()].copy()
    ended_90['end_month'] = ended_90['end_date'].dt.to_period('M')

    end_counts = ended_90.groupby('end_month').size().sort_index()

    print("\nEnded $90 for 90 Memberships by Month:")
    for month, count in end_counts.items():
        if month >= pd.Period('2025-06'):
            print(f"  {month}: {count} memberships")

    # Breakdown by type
    print("\n\nEnded by Type:")
    for month in [pd.Period('2025-08'), pd.Period('2025-09'), pd.Period('2025-11')]:
        month_ended = ended_90[ended_90['end_month'] == month]
        solo = len(month_ended[month_ended['name'].str.contains('Solo', na=False)])
        family = len(month_ended[month_ended['name'].str.contains('Family', na=False)])
        print(f"  {month}: {solo} Solo, {family} Family")

    # Calculate revenue impact
    print("\n" + "="*80)
    print("REVENUE IMPACT OF $90 FOR 90 PROGRAM ENDING")
    print("="*80)

    # Estimate revenue loss
    # Assuming $90 for 90 Solo was ~$90/month and Family was higher
    solo_rate = 90
    family_rate = 180  # Estimate

    print("\nEstimated Monthly Revenue Loss:")
    print(f"  Lost Solo memberships: 143 × ${solo_rate} = ${143 * solo_rate:,.2f}")
    print(f"  Lost Family memberships: 51 × ${family_rate} = ${51 * family_rate:,.2f}")
    print(f"  Total estimated loss: ${(143 * solo_rate) + (51 * family_rate):,.2f}")

    # Check if billing_amount column exists
    if 'billing_amount' in df_90.columns:
        # Get June-August active memberships
        june_active = df_90[
            (df_90['start_date'] <= pd.Timestamp('2025-08-31')) &
            ((df_90['end_date'].isna()) | (df_90['end_date'] >= pd.Timestamp('2025-06-01')))
        ]

        # Get Nov active
        nov_active = df_90[
            (df_90['start_date'] <= pd.Timestamp('2025-11-30')) &
            ((df_90['end_date'].isna()) | (df_90['end_date'] >= pd.Timestamp('2025-11-01')))
        ]

        june_revenue = june_active['billing_amount'].sum()
        nov_revenue = nov_active['billing_amount'].sum()

        print(f"\n\nActual Billing Amounts:")
        print(f"  June-August avg monthly: ${june_revenue/3:,.2f}")
        print(f"  November: ${nov_revenue:,.2f}")
        print(f"  Loss: ${(june_revenue/3) - nov_revenue:,.2f}")

    # Check transactions for $90 for 90 payments
    print("\n" + "="*80)
    print("$90 FOR 90 PAYMENTS IN TRANSACTION DATA")
    print("="*80)

    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'])
    df_trans_2025 = df_transactions[df_transactions['Date'].dt.year == 2025].copy()

    # Look for $90 for 90 in descriptions
    ninety_payments = df_trans_2025[
        df_trans_2025['Description'].str.contains('90 for 90', case=False, na=False) |
        df_trans_2025['Description'].str.contains('$90 for 90', case=False, na=False)
    ].copy()

    if len(ninety_payments) > 0:
        ninety_payments['Month'] = ninety_payments['Date'].dt.to_period('M')

        monthly_revenue = ninety_payments.groupby('Month').agg({
            'Total Amount': 'sum',
            'transaction_id': 'count'
        })

        print("\nActual $90 for 90 Payments by Month:")
        print(monthly_revenue.to_string())
    else:
        print("\nNo direct $90 for 90 references found in transaction descriptions")
        print("(These may be bundled under general 'Membership Renewal' category)")

    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()

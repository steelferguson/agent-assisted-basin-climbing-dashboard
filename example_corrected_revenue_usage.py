#!/usr/bin/env python3
"""
Example: How to use the corrected Stripe revenue calculation

This shows how to use the updated StripeFetcher to get accurate net revenue
that properly accounts for refunds.
"""

import os
import sys
import datetime

sys.path.append('./data_pipeline')
from fetch_stripe_data import StripeFetcher

def example_usage():
    """Example of how to use the corrected revenue calculation"""
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found in environment")
        return
    
    # Initialize the updated StripeFetcher
    stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
    
    # Define your date range (example: August 2025)
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    
    print("EXAMPLE: Corrected Stripe Revenue Calculation")
    print("=" * 50)
    
    try:
        # Option 1: Get comprehensive revenue breakdown (RECOMMENDED)
        print("Method 1: Using get_net_revenue_with_refunds() - RECOMMENDED")
        print("-" * 50)
        
        revenue_breakdown = stripe_fetcher.get_net_revenue_with_refunds(
            stripe_key, start_date, end_date
        )
        
        print(f"Period: {revenue_breakdown['period_start']} to {revenue_breakdown['period_end']}")
        print(f"Gross Revenue: ${revenue_breakdown['gross_revenue']:,.2f} ({revenue_breakdown['transaction_count']} transactions)")
        print(f"Total Refunds: ${revenue_breakdown['total_refunds']:,.2f} ({revenue_breakdown['refund_count']} refunds)")
        print(f"Net Revenue: ${revenue_breakdown['net_revenue']:,.2f}")
        print()
        
        if revenue_breakdown['refund_details']:
            print("Refund breakdown:")
            for refund in revenue_breakdown['refund_details']:
                print(f"  {refund['date']}: ${refund['amount']:,.2f} - {refund['reason']}")
        print()
        
        # Option 2: Manual calculation using separate methods
        print("Method 2: Manual calculation using separate methods")
        print("-" * 50)
        
        # Get gross revenue
        df_payments = stripe_fetcher.pull_and_transform_stripe_payment_intents_data(
            stripe_key, start_date, end_date, save_json=False, save_csv=False
        )
        gross_revenue = df_payments['Total Amount'].sum()
        
        # Get refunds separately
        total_refunds, refund_details = stripe_fetcher.get_refunds_for_period(
            stripe_key, start_date, end_date
        )
        
        # Calculate net revenue
        net_revenue = gross_revenue - total_refunds
        
        print(f"Gross Revenue: ${gross_revenue:,.2f}")
        print(f"Refunds: ${total_refunds:,.2f}")
        print(f"Net Revenue: ${net_revenue:,.2f}")
        print()
        
        # Comparison with old method (for validation)
        print("Comparison with old method (gross revenue only):")
        print("-" * 50)
        
        old_df = stripe_fetcher.pull_and_transform_stripe_payment_data(
            stripe_key, start_date, end_date, save_json=False, save_csv=False
        )
        old_revenue = old_df['Total Amount'].sum()
        
        print(f"Old method (gross only): ${old_revenue:,.2f}")
        print(f"New method (net): ${net_revenue:,.2f}")
        print(f"Difference: ${old_revenue - net_revenue:,.2f}")
        print(f"This difference should match your discrepancy!")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

def monthly_revenue_report_example():
    """Example: Generate a monthly revenue report with proper refund handling"""
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found")
        return
    
    stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
    
    print("\nEXAMPLE: Monthly Revenue Report (with refunds)")
    print("=" * 50)
    
    # Generate report for multiple months
    months = [
        (2025, 6, "June 2025"),
        (2025, 7, "July 2025"), 
        (2025, 8, "August 2025")
    ]
    
    total_net_revenue = 0
    
    for year, month, month_name in months:
        # Calculate days in month
        if month == 2:
            days_in_month = 28
        elif month in [4, 6, 9, 11]:
            days_in_month = 30
        else:
            days_in_month = 31
            
        start_date = datetime.datetime(year, month, 1, 0, 0, 0)
        end_date = datetime.datetime(year, month, days_in_month, 23, 59, 59)
        
        try:
            breakdown = stripe_fetcher.get_net_revenue_with_refunds(
                stripe_key, start_date, end_date
            )
            
            print(f"{month_name}:")
            print(f"  Gross: ${breakdown['gross_revenue']:,.2f}")
            print(f"  Refunds: ${breakdown['total_refunds']:,.2f}")
            print(f"  Net: ${breakdown['net_revenue']:,.2f}")
            
            total_net_revenue += breakdown['net_revenue']
            
        except Exception as e:
            print(f"{month_name}: ERROR - {e}")
    
    print(f"\nTotal Net Revenue (3 months): ${total_net_revenue:,.2f}")

if __name__ == "__main__":
    example_usage()
    monthly_revenue_report_example()
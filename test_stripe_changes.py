#!/usr/bin/env python3
"""
Test script to compare old Charges API vs new Payment Intents API for Stripe
"""
import os
import sys
import datetime
sys.path.append('./data_pipeline')

from fetch_stripe_data import StripeFetcher

def test_stripe_methods():
    """Compare old vs new Stripe methods on a small date range"""
    # Test with a recent week to see differences
    end_date = datetime.datetime(2025, 5, 7)  # Based on our sample data
    start_date = datetime.datetime(2025, 5, 1) 
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found in environment")
        return
    
    print("=== TESTING STRIPE API METHODS ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print()
    
    stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
    
    try:
        # Test OLD method (Charges API)
        print("1. Testing OLD method (Charges API)...")
        df_old = stripe_fetcher.pull_and_transform_stripe_payment_data(
            stripe_key, start_date, end_date, save_json=False, save_csv=False
        )
        print(f"OLD method results:")
        print(f"  Transactions: {len(df_old)}")
        print(f"  Total revenue: ${df_old['Total Amount'].sum():,.2f}")
        print()
        
        # Test NEW method (Payment Intents API)
        print("2. Testing NEW method (Payment Intents API)...")
        df_new = stripe_fetcher.pull_and_transform_stripe_payment_intents_data(
            stripe_key, start_date, end_date, save_json=False, save_csv=False
        )
        print(f"NEW method results:")
        print(f"  Transactions: {len(df_new)}")
        print(f"  Total revenue: ${df_new['Total Amount'].sum():,.2f}")
        print()
        
        # Compare results
        print("3. COMPARISON:")
        diff_count = len(df_old) - len(df_new)
        diff_revenue = df_old['Total Amount'].sum() - df_new['Total Amount'].sum()
        print(f"  Transaction difference: {diff_count} ({diff_count/len(df_old)*100:.1f}% reduction)")
        print(f"  Revenue difference: ${diff_revenue:,.2f}")
        
        if diff_count > 0:
            print(f"  → New method filtered out {diff_count} incomplete transactions")
        elif diff_count < 0:
            print(f"  → WARNING: New method found {abs(diff_count)} more transactions")
        else:
            print(f"  → Same transaction count (possibly no incomplete transactions in this period)")
            
        # Save comparison data for further analysis
        df_old.to_csv("data/outputs/stripe_comparison_old_method.csv", index=False)
        df_new.to_csv("data/outputs/stripe_comparison_new_method.csv", index=False)
        print(f"\nComparison data saved to data/outputs/stripe_comparison_*.csv")
        
    except Exception as e:
        print(f"ERROR during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_stripe_methods()
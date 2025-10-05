#!/usr/bin/env python3
"""
Test script to compare current Square method vs strict validation method
"""
import os
import sys
import datetime
sys.path.append('./data_pipeline')

from fetch_square_data import SquareFetcher

def test_square_methods():
    """Compare current vs strict Square methods on August 2025"""
    # Test with full month of August 2025  
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0) 
    
    square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
    if not square_token:
        print("ERROR: SQUARE_PRODUCTION_API_TOKEN not found in environment")
        return
    
    print("=== TESTING SQUARE API METHODS ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print()
    
    square_fetcher = SquareFetcher(square_token, location_id="L37KDMNNG84EA")
    
    try:
        # Test CURRENT method (includes OPEN orders)
        print("1. Testing CURRENT method (includes OPEN orders)...")
        df_current = square_fetcher.pull_and_transform_square_payment_data(
            start_date, end_date, save_json=False, save_csv=False
        )
        print(f"CURRENT method results:")
        print(f"  Transactions: {len(df_current)}")
        print(f"  Total revenue: ${df_current['Total Amount'].sum():,.2f}")
        print()
        
        # Test STRICT method (only COMPLETED payments + COMPLETED orders)
        print("2. Testing STRICT method (payment AND order both COMPLETED)...")
        df_strict = square_fetcher.pull_and_transform_square_payment_data_strict(
            start_date, end_date, save_json=False, save_csv=False
        )
        print(f"STRICT method results:")
        print(f"  Transactions: {len(df_strict)}")
        print(f"  Total revenue: ${df_strict['Total Amount'].sum():,.2f}")
        print()
        
        # Compare results
        print("3. COMPARISON:")
        diff_count = len(df_current) - len(df_strict)
        diff_revenue = df_current['Total Amount'].sum() - df_strict['Total Amount'].sum()
        print(f"  Transaction difference: {diff_count} ({diff_count/len(df_current)*100:.1f}% reduction)")
        print(f"  Revenue difference: ${diff_revenue:,.2f}")
        
        if diff_count > 0:
            print(f"  → Strict method filtered out {diff_count} incomplete transactions")
            print(f"  → This suggests {diff_count} transactions had incomplete payment/order status")
        elif diff_count < 0:
            print(f"  → WARNING: Strict method found {abs(diff_count)} more transactions")
        else:
            print(f"  → Same transaction count (all transactions were properly completed)")
            
        # Save comparison data for further analysis
        df_current.to_csv("data/outputs/square_comparison_current_method.csv", index=False)
        df_strict.to_csv("data/outputs/square_comparison_strict_method.csv", index=False)
        print(f"\nComparison data saved to data/outputs/square_comparison_*.csv")
        
    except Exception as e:
        print(f"ERROR during testing: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_square_methods()
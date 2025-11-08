#!/usr/bin/env python3
"""
Test corrected revenue calculation including refunds for August 2025
"""
import os
import sys
import datetime
sys.path.append('./data_pipeline')

from fetch_stripe_data import StripeFetcher
from fetch_square_data import SquareFetcher

def test_corrected_revenue():
    """Test corrected revenue calculation with refunds for August 2025"""
    
    # August 2025 date range
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    
    print("=== CORRECTED REVENUE CALCULATION TEST ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print()
    
    # Test Stripe with refunds
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if stripe_key:
        print("STRIPE CORRECTED CALCULATION:")
        stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
        
        try:
            # Get gross revenue (Payment Intents)
            df_stripe = stripe_fetcher.pull_and_transform_stripe_payment_intents_data(
                stripe_key, start_date, end_date, save_json=False, save_csv=False
            )
            
            # Get refunds for the same period
            refunds = stripe_fetcher.get_refunds_for_period(stripe_key, start_date, end_date)
            
            # Calculate net revenue
            revenue_breakdown = stripe_fetcher.calculate_net_revenue_with_refunds(df_stripe, refunds)
            
            print(f"  Gross revenue: ${revenue_breakdown['gross_revenue']:,.2f}")
            print(f"  Refunds ({revenue_breakdown['refund_count']}): ${revenue_breakdown['total_refunds']:,.2f}")
            print(f"  NET REVENUE: ${revenue_breakdown['net_revenue']:,.2f}")
            print(f"  Expected: $50,650.00")
            difference = revenue_breakdown['net_revenue'] - 50650.00
            print(f"  Difference: ${difference:,.2f} ({abs(difference)/50650*100:.2f}%)")
            print()
            
        except Exception as e:
            print(f"  ERROR: {e}")
            print()
    
    # Test Square (already fixed)
    square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
    if square_token:
        print("SQUARE CORRECTED CALCULATION:")
        square_fetcher = SquareFetcher(square_token, location_id="L37KDMNNG84EA")
        
        try:
            # Use strict method (no double counting)
            df_square = square_fetcher.pull_and_transform_square_payment_data_strict(
                start_date, end_date, save_json=False, save_csv=False
            )
            
            square_revenue = df_square['Total Amount'].sum()
            print(f"  Square revenue: ${square_revenue:,.2f}")
            print(f"  Expected: $18,763.00")
            difference = square_revenue - 18763.00
            print(f"  Difference: ${difference:,.2f} ({abs(difference)/18763*100:.2f}%)")
            print()
            
        except Exception as e:
            print(f"  ERROR: {e}")
            print()
    
    # Combined totals
    if stripe_key and square_token:
        try:
            combined_net = revenue_breakdown['net_revenue'] + square_revenue
            expected_combined = 50650.00 + 18763.00
            print("COMBINED CORRECTED TOTALS:")
            print(f"  Stripe (net): ${revenue_breakdown['net_revenue']:,.2f}")
            print(f"  Square: ${square_revenue:,.2f}")
            print(f"  TOTAL: ${combined_net:,.2f}")
            print(f"  Expected: ${expected_combined:,.2f}")
            final_difference = combined_net - expected_combined
            print(f"  Final difference: ${final_difference:,.2f} ({abs(final_difference)/expected_combined*100:.2f}%)")
            
        except Exception as e:
            print(f"  ERROR calculating combined: {e}")

if __name__ == "__main__":
    test_corrected_revenue()
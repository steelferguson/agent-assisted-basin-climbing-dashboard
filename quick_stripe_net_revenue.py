#!/usr/bin/env python3
"""
Quick calculation of Stripe net revenue with refunds
"""
import os
import sys
import datetime
sys.path.append('./data_pipeline')

from fetch_stripe_data import StripeFetcher

def quick_stripe_net():
    """Quick Stripe net revenue calculation"""
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found")
        return
    
    start_date = datetime.datetime(2025, 9, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 9, 30, 23, 59, 59)
    
    print("=== QUICK STRIPE NET REVENUE TEST ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    stripe_fetcher = StripeFetcher(stripe_key=stripe_key)
    
    # We know from previous test: Gross = $53,562.81
    # We found refunds = $2,752.47
    # So net should be = $50,810.34
    
    try:
        refunds = stripe_fetcher.get_refunds_for_period(stripe_key, start_date, end_date)
        
        gross_revenue = 53562.81  # From our previous test
        total_refunds = sum(refund.amount / 100 for refund in refunds if refund.status == 'succeeded')
        net_revenue = gross_revenue - total_refunds
        
        print(f"Gross revenue: ${gross_revenue:,.2f}")
        print(f"Refunds: ${total_refunds:,.2f}")
        print(f"NET REVENUE: ${net_revenue:,.2f}")
        print(f"Expected: $50,650.00")
        print(f"Difference: ${net_revenue - 50650:.2f}")
        print(f"Accuracy: {(1 - abs(net_revenue - 50650)/50650)*100:.1f}%")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    quick_stripe_net()
#!/usr/bin/env python3
"""
Quick test to check Stripe refunds for August 2025 and calculate net revenue
"""
import os
import sys
import datetime
import stripe

def check_stripe_refunds():
    """Check refunds for August 2025 to explain revenue discrepancy"""
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found in environment")
        return
    
    stripe.api_key = stripe_key
    
    # August 2025 date range
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    
    print("=== STRIPE REFUNDS INVESTIGATION ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print()
    
    try:
        # Get refunds for August 2025
        refunds = stripe.Refund.list(
            created={
                'gte': int(start_date.timestamp()),
                'lte': int(end_date.timestamp())
            },
            limit=100  # Adjust if needed
        )
        
        total_refunds = 0
        refund_count = 0
        
        print("Refunds found:")
        for refund in refunds:
            amount = refund.amount / 100  # Convert from cents
            total_refunds += amount
            refund_count += 1
            created = datetime.datetime.fromtimestamp(refund.created)
            print(f"  {created.date()}: ${amount:.2f} (Status: {refund.status})")
        
        print()
        print(f"REFUND SUMMARY:")
        print(f"  Total refunds: {refund_count}")
        print(f"  Total refund amount: ${total_refunds:.2f}")
        print()
        
        # Calculate corrected revenue
        gross_revenue = 53562.81  # Our current calculation
        net_revenue = gross_revenue - total_refunds
        expected_revenue = 50650.00
        
        print(f"REVENUE CALCULATION:")
        print(f"  Gross revenue (our current): ${gross_revenue:.2f}")
        print(f"  Less refunds: ${total_refunds:.2f}")
        print(f"  Net revenue (corrected): ${net_revenue:.2f}")
        print(f"  Expected revenue: ${expected_revenue:.2f}")
        print(f"  Remaining difference: ${net_revenue - expected_revenue:.2f}")
        
        discrepancy_explained = (total_refunds / (gross_revenue - expected_revenue)) * 100
        print(f"  Refunds explain {discrepancy_explained:.1f}% of discrepancy")
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_stripe_refunds()
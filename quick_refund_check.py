#!/usr/bin/env python3
"""
Quick Refund Check for August 2025
This is a streamlined script to quickly check if refunds explain the $2,913 discrepancy
"""

import os
import stripe
import datetime

def quick_refund_investigation():
    """Quick check for refunds in August 2025"""
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found")
        return
    
    stripe.api_key = stripe_key
    
    # August 2025 date range
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    
    print("=== QUICK REFUND CHECK FOR AUGUST 2025 ===")
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print()
    
    try:
        # Check refunds directly via Refunds API
        refunds = stripe.Refund.list(
            created={
                "gte": int(start_date.timestamp()),
                "lte": int(end_date.timestamp()),
            },
            limit=1000000,
        )
        
        total_refunds = 0
        refund_count = 0
        refund_details = []
        
        for refund in refunds.auto_paging_iter():
            refund_count += 1
            refund_amount = refund.amount / 100
            total_refunds += refund_amount
            
            refund_details.append({
                'date': datetime.datetime.fromtimestamp(refund.created).strftime('%Y-%m-%d'),
                'amount': refund_amount,
                'reason': refund.reason or 'No reason',
                'status': refund.status,
                'charge_id': refund.charge
            })
        
        print(f"RESULTS:")
        print(f"  Refunds found: {refund_count}")
        print(f"  Total refund amount: ${total_refunds:,.2f}")
        print()
        
        if total_refunds > 0:
            print(f"ANALYSIS:")
            difference_from_discrepancy = abs(total_refunds - 2913)
            print(f"  Expected discrepancy: $2,913")
            print(f"  Actual refunds: ${total_refunds:,.2f}")
            print(f"  Difference: ${difference_from_discrepancy:,.2f}")
            
            if difference_from_discrepancy < 100:
                print(f"  🎯 LIKELY CAUSE: Refunds closely match the discrepancy!")
                print(f"     → Your revenue calculation likely doesn't subtract refunds")
            elif difference_from_discrepancy < 500:
                print(f"  ⚠️  POSSIBLE CAUSE: Refunds are close to the discrepancy")
            else:
                print(f"  ❌ Refunds don't explain the discrepancy")
            
            print(f"\nREFUND DETAILS:")
            for detail in refund_details:
                print(f"  {detail['date']}: ${detail['amount']:,.2f} ({detail['reason']}, {detail['status']})")
        else:
            print(f"  ✅ No refunds found - discrepancy is not due to refunds")
            print(f"  → Look into other causes like:")
            print(f"     - Date boundary issues (July 31st vs Aug 1st)")
            print(f"     - Uncaptured charges being included")
            print(f"     - Currency conversion issues")
            print(f"     - Different calculation methodologies")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    quick_refund_investigation()
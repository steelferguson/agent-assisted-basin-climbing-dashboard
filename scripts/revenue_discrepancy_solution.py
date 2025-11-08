#!/usr/bin/env python3
"""
SOLUTION: Stripe Revenue Discrepancy for August 2025

Problem: Getting $53,563 but expecting $50,650 (difference: $2,913)
Root Cause: Refunds not being deducted from gross revenue
Solution: Subtract refunds from gross revenue to get net revenue
"""

import os
import stripe
import datetime
import pandas as pd

def solve_revenue_discrepancy():
    """Show the solution to the revenue discrepancy"""
    
    print("=" * 60)
    print("STRIPE REVENUE DISCREPANCY SOLUTION")
    print("=" * 60)
    print("Problem: $53,563 actual vs $50,650 expected = $2,913 discrepancy")
    print("Period: August 2025")
    print()
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found")
        return
    
    stripe.api_key = stripe_key
    
    # August 2025 date range
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    
    try:
        # Step 1: Get current gross revenue (your existing calculation)
        print("STEP 1: Current Calculation (Gross Revenue Only)")
        print("-" * 50)
        
        # Read existing data
        old_df = pd.read_csv('data/outputs/stripe_comparison_old_method.csv')
        gross_revenue = old_df['Total Amount'].sum()
        print(f"Current calculation result: ${gross_revenue:,.2f}")
        print(f"Transaction count: {len(old_df)}")
        print()
        
        # Step 2: Get refunds for the period
        print("STEP 2: Find Refunds for August 2025")
        print("-" * 50)
        
        refunds = stripe.Refund.list(
            created={
                "gte": int(start_date.timestamp()),
                "lte": int(end_date.timestamp()),
            },
            limit=1000000,
        )
        
        total_refunds = 0
        refund_count = 0
        refund_breakdown = []
        
        for refund in refunds.auto_paging_iter():
            refund_count += 1
            refund_amount = refund.amount / 100
            total_refunds += refund_amount
            refund_breakdown.append({
                'date': datetime.datetime.fromtimestamp(refund.created).strftime('%Y-%m-%d'),
                'amount': refund_amount,
                'reason': refund.reason or 'No reason provided'
            })
        
        print(f"Refunds found: {refund_count}")
        print(f"Total refund amount: ${total_refunds:,.2f}")
        print()
        print("Refund breakdown:")
        for refund in refund_breakdown:
            print(f"  {refund['date']}: ${refund['amount']:,.2f} ({refund['reason']})")
        print()
        
        # Step 3: Calculate corrected net revenue
        print("STEP 3: Corrected Revenue Calculation")
        print("-" * 50)
        
        net_revenue = gross_revenue - total_refunds
        original_discrepancy = gross_revenue - 50650
        corrected_discrepancy = net_revenue - 50650
        
        print(f"Gross Revenue: ${gross_revenue:,.2f}")
        print(f"Less: Refunds: ${total_refunds:,.2f}")
        print(f"Net Revenue: ${net_revenue:,.2f}")
        print()
        print(f"Expected Revenue: $50,650.00")
        print(f"Original Discrepancy: ${original_discrepancy:,.2f}")
        print(f"Corrected Discrepancy: ${corrected_discrepancy:,.2f}")
        print()
        
        # Analysis
        improvement = original_discrepancy - corrected_discrepancy
        print("ANALYSIS:")
        print("-" * 50)
        print(f"Improvement: ${improvement:,.2f} ({improvement/original_discrepancy*100:.1f}% of original discrepancy)")
        
        if abs(corrected_discrepancy) < 200:
            print("✅ PROBLEM SOLVED: Remaining discrepancy is minimal")
        elif abs(corrected_discrepancy) < 500:
            print("🎯 MOSTLY SOLVED: Small remaining discrepancy likely due to:")
            print("   - Rounding differences")
            print("   - Different date boundary handling")
            print("   - Minor calculation methodology differences")
        else:
            print("⚠️  PARTIALLY SOLVED: Additional investigation needed")
        
        print()
        print("ROOT CAUSE: Your current revenue calculation includes gross payments")
        print("but doesn't subtract refunds, leading to inflated revenue figures.")
        print()
        
        # Solution implementation
        print("=" * 60)
        print("IMPLEMENTATION SOLUTION")
        print("=" * 60)
        
        print("1. ADD REFUND FETCHING FUNCTION:")
        print("""
def get_stripe_refunds(stripe_key, start_date, end_date):
    stripe.api_key = stripe_key
    refunds = stripe.Refund.list(
        created={
            "gte": int(start_date.timestamp()),
            "lte": int(end_date.timestamp()),
        },
        limit=1000000,
    )
    
    total_refunds = 0
    for refund in refunds.auto_paging_iter():
        total_refunds += refund.amount / 100
    
    return total_refunds
""")
        
        print("2. UPDATE YOUR REVENUE CALCULATION:")
        print("""
# OLD (incorrect):
revenue = df['Total Amount'].sum()

# NEW (correct):
gross_revenue = df['Total Amount'].sum()
refunds = get_stripe_refunds(stripe_key, start_date, end_date)
net_revenue = gross_revenue - refunds

print(f"Gross Revenue: ${gross_revenue:,.2f}")
print(f"Refunds: ${refunds:,.2f}")
print(f"Net Revenue: ${net_revenue:,.2f}")
""")
        
        print("3. APPLY TO YOUR EXISTING CODE:")
        print("   - Modify data_pipeline/fetch_stripe_data.py")
        print("   - Add refund handling to StripeFetcher class")
        print("   - Update all revenue reports to use net revenue")
        
        return {
            'gross_revenue': gross_revenue,
            'total_refunds': total_refunds,
            'net_revenue': net_revenue,
            'original_discrepancy': original_discrepancy,
            'corrected_discrepancy': corrected_discrepancy
        }
        
    except Exception as e:
        print(f"ERROR: {e}")
        return None

if __name__ == "__main__":
    solve_revenue_discrepancy()
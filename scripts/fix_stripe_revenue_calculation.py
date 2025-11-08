#!/usr/bin/env python3
"""
Fix Stripe Revenue Calculation to Handle Refunds Properly

This script provides the corrected revenue calculation that:
1. Properly deducts refunds from gross revenue
2. Provides detailed breakdown of revenue components
3. Addresses the $2,913 discrepancy in August 2025
"""

import os
import sys
import datetime
import stripe
import pandas as pd

sys.path.append('./data_pipeline')
from fetch_stripe_data import StripeFetcher

class EnhancedStripeFetcher(StripeFetcher):
    """Enhanced Stripe fetcher that properly handles refunds"""
    
    def get_refunds_for_period(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime):
        """
        Fetch all refunds for a given period
        
        Returns:
            tuple: (total_refunds_amount, refund_details_list)
        """
        stripe.api_key = stripe_key
        
        refunds = stripe.Refund.list(
            created={
                "gte": int(start_date.timestamp()),
                "lte": int(end_date.timestamp()),
            },
            limit=1000000,
        )
        
        total_refunds = 0
        refund_details = []
        
        for refund in refunds.auto_paging_iter():
            refund_amount = refund.amount / 100  # Convert from cents
            total_refunds += refund_amount
            
            refund_details.append({
                'refund_id': refund.id,
                'charge_id': refund.charge,
                'amount': refund_amount,
                'date': datetime.datetime.fromtimestamp(refund.created).date(),
                'reason': refund.reason or 'No reason provided',
                'status': refund.status,
                'currency': refund.currency
            })
        
        return total_refunds, refund_details
    
    def get_net_revenue_with_refunds(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime, save_breakdown: bool = True):
        """
        Calculate net revenue properly accounting for refunds
        
        Returns:
            dict: Complete revenue breakdown
        """
        print(f"Calculating net revenue for {start_date.date()} to {end_date.date()}")
        
        # Get gross revenue (existing method)
        df_payments = self.pull_and_transform_stripe_payment_intents_data(
            stripe_key, start_date, end_date, save_json=False, save_csv=False
        )
        
        gross_revenue = df_payments['Total Amount'].sum()
        transaction_count = len(df_payments)
        
        # Get refunds
        total_refunds, refund_details = self.get_refunds_for_period(stripe_key, start_date, end_date)
        
        # Calculate net revenue
        net_revenue = gross_revenue - total_refunds
        
        # Create breakdown
        revenue_breakdown = {
            'period': f"{start_date.date()} to {end_date.date()}",
            'gross_revenue': gross_revenue,
            'total_refunds': total_refunds,
            'net_revenue': net_revenue,
            'transaction_count': transaction_count,
            'refund_count': len(refund_details),
            'refund_details': refund_details
        }
        
        # Save detailed breakdown if requested
        if save_breakdown:
            # Save refund details
            if refund_details:
                refund_df = pd.DataFrame(refund_details)
                refund_df.to_csv('data/outputs/stripe_refunds_breakdown.csv', index=False)
                print(f"Refund details saved to: data/outputs/stripe_refunds_breakdown.csv")
            
            # Save revenue summary
            summary_data = [{
                'metric': 'Gross Revenue',
                'amount': gross_revenue,
                'description': f'Total from {transaction_count} successful payment intents'
            }, {
                'metric': 'Total Refunds',
                'amount': total_refunds,
                'description': f'Total from {len(refund_details)} refunds'
            }, {
                'metric': 'Net Revenue',
                'amount': net_revenue,
                'description': 'Gross revenue minus refunds'
            }]
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv('data/outputs/stripe_revenue_summary.csv', index=False)
            print(f"Revenue summary saved to: data/outputs/stripe_revenue_summary.csv")
        
        return revenue_breakdown
    
    def analyze_revenue_discrepancy(self, expected_revenue: float, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime):
        """
        Analyze discrepancy between expected and actual revenue
        """
        print("=" * 60)
        print("STRIPE REVENUE DISCREPANCY ANALYSIS")
        print("=" * 60)
        
        breakdown = self.get_net_revenue_with_refunds(stripe_key, start_date, end_date)
        
        print(f"\nREVENUE BREAKDOWN:")
        print(f"  Period: {breakdown['period']}")
        print(f"  Gross Revenue: ${breakdown['gross_revenue']:,.2f} ({breakdown['transaction_count']} transactions)")
        print(f"  Total Refunds: ${breakdown['total_refunds']:,.2f} ({breakdown['refund_count']} refunds)")
        print(f"  Net Revenue: ${breakdown['net_revenue']:,.2f}")
        print()
        
        print(f"DISCREPANCY ANALYSIS:")
        print(f"  Expected Revenue: ${expected_revenue:,.2f}")
        print(f"  Actual Net Revenue: ${breakdown['net_revenue']:,.2f}")
        
        discrepancy = breakdown['net_revenue'] - expected_revenue
        print(f"  Remaining Discrepancy: ${discrepancy:,.2f}")
        
        if abs(discrepancy) < 100:
            print(f"  ✅ RESOLVED: Discrepancy is now minimal (<$100)")
        elif abs(discrepancy) < 500:
            print(f"  🎯 MOSTLY RESOLVED: Small remaining discrepancy (<$500)")
        else:
            print(f"  ⚠️  PARTIALLY RESOLVED: Significant discrepancy remains")
            print(f"     → Additional investigation needed for remaining ${abs(discrepancy):,.2f}")
        
        print(f"\nREFUND DETAILS:")
        if breakdown['refund_details']:
            for refund in breakdown['refund_details']:
                print(f"  {refund['date']}: ${refund['amount']:,.2f} - {refund['reason']} ({refund['status']})")
        else:
            print(f"  No refunds found")
        
        return breakdown

def demonstrate_corrected_calculation():
    """Demonstrate the corrected revenue calculation for August 2025"""
    
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        print("ERROR: STRIPE_PRODUCTION_API_KEY not found in environment")
        return
    
    # August 2025 - the month with the discrepancy
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    expected_revenue = 50650.00  # The expected amount
    
    # Use enhanced fetcher
    enhanced_fetcher = EnhancedStripeFetcher(stripe_key=stripe_key)
    
    try:
        # Analyze the discrepancy with corrected calculation
        breakdown = enhanced_fetcher.analyze_revenue_discrepancy(
            expected_revenue, stripe_key, start_date, end_date
        )
        
        print("\n" + "=" * 60)
        print("IMPLEMENTATION RECOMMENDATIONS")
        print("=" * 60)
        
        print(f"\n1. UPDATE YOUR REVENUE CALCULATION CODE:")
        print(f"   Replace your current calculation with:")
        print(f"""
   # Instead of just:
   total_revenue = df['Total Amount'].sum()
   
   # Use this corrected approach:
   from fix_stripe_revenue_calculation import EnhancedStripeFetcher
   
   enhanced_fetcher = EnhancedStripeFetcher(stripe_key=stripe_key)
   revenue_breakdown = enhanced_fetcher.get_net_revenue_with_refunds(
       stripe_key, start_date, end_date
   )
   
   net_revenue = revenue_breakdown['net_revenue']
   print(f"Net Revenue (after refunds): ${{net_revenue:,.2f}}")
""")
        
        print(f"\n2. UPDATE EXISTING STRIPE FETCHER:")
        print(f"   Add the following method to your StripeFetcher class:")
        print(f"""
   def get_refunds_for_period(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime):
       stripe.api_key = stripe_key
       refunds = stripe.Refund.list(
           created={{
               "gte": int(start_date.timestamp()),
               "lte": int(end_date.timestamp()),
           }},
           limit=1000000,
       )
       
       total_refunds = 0
       for refund in refunds.auto_paging_iter():
           total_refunds += refund.amount / 100
       
       return total_refunds
""")
        
        print(f"\n3. INTEGRATE REFUND HANDLING:")
        print(f"   Modify your main revenue calculation to always account for refunds:")
        print(f"""
   # In your main revenue reporting:
   gross_revenue = df['Total Amount'].sum()
   total_refunds = stripe_fetcher.get_refunds_for_period(stripe_key, start_date, end_date)
   net_revenue = gross_revenue - total_refunds
   
   print(f"Gross Revenue: ${{gross_revenue:,.2f}}")
   print(f"Refunds: ${{total_refunds:,.2f}}")
   print(f"Net Revenue: ${{net_revenue:,.2f}}")
""")
        
        return breakdown
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    demonstrate_corrected_calculation()
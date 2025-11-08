#!/usr/bin/env python3
"""
Comprehensive Stripe Revenue Discrepancy Investigation for August 2025
Expected: $50,650 | Actual: $53,563 | Difference: $2,913

This script investigates potential causes:
1. Refunds not properly deducted
2. Payment Intent data validation issues
3. Date range/timezone alignment problems
4. Other Stripe API fields affecting revenue
"""

import os
import sys
import datetime
import stripe
import pandas as pd
from collections import Counter

sys.path.append('./data_pipeline')
from fetch_stripe_data import StripeFetcher

def setup_stripe():
    """Initialize Stripe with production key"""
    stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
    if not stripe_key:
        raise ValueError("STRIPE_PRODUCTION_API_KEY not found in environment")
    stripe.api_key = stripe_key
    return stripe_key

def investigate_refunds(start_date, end_date):
    """
    Investigation 1: Refunds Analysis
    Checks if refunds are properly accounted for in revenue calculations
    """
    print("=" * 60)
    print("INVESTIGATION 1: REFUNDS ANALYSIS")
    print("=" * 60)
    
    print(f"Date range: {start_date.date()} to {end_date.date()}")
    
    # Method A: Get refunds via Charges API (refunds show as negative amounts)
    print("\n1A. Checking Charges API for refunds...")
    charges = stripe.Charge.list(
        created={
            "gte": int(start_date.timestamp()),
            "lte": int(end_date.timestamp()),
        },
        limit=1000000,
    )
    
    refund_data = []
    total_refunds = 0
    charge_count = 0
    
    for charge in charges.auto_paging_iter():
        charge_count += 1
        if charge.refunded and charge.amount_refunded > 0:
            refund_amount = charge.amount_refunded / 100  # Convert from cents
            total_refunds += refund_amount
            refund_data.append({
                'charge_id': charge.id,
                'original_amount': charge.amount / 100,
                'refund_amount': refund_amount,
                'description': charge.get('description', 'No Description'),
                'refund_date': datetime.datetime.fromtimestamp(charge.created).date(),
                'currency': charge.currency
            })
    
    print(f"   Total charges examined: {charge_count}")
    print(f"   Charges with refunds: {len(refund_data)}")
    print(f"   Total refund amount: ${total_refunds:,.2f}")
    
    # Method B: Direct Refunds API call
    print("\n1B. Checking Refunds API directly...")
    refunds = stripe.Refund.list(
        created={
            "gte": int(start_date.timestamp()),
            "lte": int(end_date.timestamp()),
        },
        limit=1000000,
    )
    
    direct_refund_data = []
    direct_total_refunds = 0
    
    for refund in refunds.auto_paging_iter():
        refund_amount = refund.amount / 100
        direct_total_refunds += refund_amount
        direct_refund_data.append({
            'refund_id': refund.id,
            'charge_id': refund.charge,
            'amount': refund_amount,
            'reason': refund.reason,
            'status': refund.status,
            'created': datetime.datetime.fromtimestamp(refund.created).date(),
            'currency': refund.currency
        })
    
    print(f"   Direct refunds found: {len(direct_refund_data)}")
    print(f"   Direct refunds total: ${direct_total_refunds:,.2f}")
    
    # Analysis
    print(f"\n1C. Refunds Analysis:")
    if total_refunds > 0 or direct_total_refunds > 0:
        print(f"   ⚠️  REFUNDS DETECTED!")
        print(f"   Via Charges API: ${total_refunds:,.2f}")
        print(f"   Via Refunds API: ${direct_total_refunds:,.2f}")
        print(f"   → These refunds may not be deducted from your revenue calculation")
        if abs(total_refunds - 2913) < 100:
            print(f"   🎯 POTENTIAL MATCH: Refund amount (~${total_refunds:,.2f}) is close to discrepancy ($2,913)")
    else:
        print(f"   ✅ No refunds found in this period")
    
    return refund_data, direct_refund_data

def investigate_payment_intent_issues(start_date, end_date):
    """
    Investigation 2: Payment Intent Data Validation
    Checks for partial captures, disputes, currency issues, etc.
    """
    print("\n" + "=" * 60)
    print("INVESTIGATION 2: PAYMENT INTENT DATA VALIDATION")
    print("=" * 60)
    
    # Get all Payment Intents for the period (not just succeeded ones)
    payment_intents = stripe.PaymentIntent.list(
        created={
            "gte": int(start_date.timestamp()),
            "lte": int(end_date.timestamp()),
        },
        limit=1000000,
    )
    
    # Analyze all Payment Intent statuses and amounts
    status_summary = Counter()
    currency_summary = Counter()
    capture_issues = []
    amount_discrepancies = []
    
    total_amount_intended = 0
    total_amount_received = 0
    
    for pi in payment_intents.auto_paging_iter():
        status_summary[pi.status] += 1
        currency_summary[pi.currency] += 1
        
        amount_intended = pi.amount / 100
        amount_received = pi.amount_received / 100
        
        total_amount_intended += amount_intended
        if pi.status == 'succeeded':
            total_amount_received += amount_received
        
        # Check for partial captures
        if pi.status == 'succeeded' and amount_received != amount_intended:
            capture_issues.append({
                'payment_intent_id': pi.id,
                'intended': amount_intended,
                'received': amount_received,
                'difference': amount_intended - amount_received,
                'description': pi.get('description', 'No Description')
            })
        
        # Check for amount discrepancies
        if amount_intended != amount_received:
            amount_discrepancies.append({
                'payment_intent_id': pi.id,
                'status': pi.status,
                'intended': amount_intended,
                'received': amount_received,
                'difference': amount_intended - amount_received
            })
    
    print(f"\n2A. Payment Intent Status Breakdown:")
    for status, count in status_summary.items():
        print(f"   {status}: {count}")
    
    print(f"\n2B. Currency Breakdown:")
    for currency, count in currency_summary.items():
        print(f"   {currency}: {count}")
    
    print(f"\n2C. Amount Analysis:")
    print(f"   Total intended amount: ${total_amount_intended:,.2f}")
    print(f"   Total received amount (succeeded only): ${total_amount_received:,.2f}")
    print(f"   Difference: ${total_amount_intended - total_amount_received:,.2f}")
    
    print(f"\n2D. Partial Capture Issues:")
    if capture_issues:
        print(f"   ⚠️  {len(capture_issues)} partial captures detected!")
        total_partial_loss = sum(issue['difference'] for issue in capture_issues)
        print(f"   Total amount lost to partial captures: ${total_partial_loss:,.2f}")
        for issue in capture_issues[:5]:  # Show first 5
            print(f"     PI {issue['payment_intent_id']}: intended ${issue['intended']:.2f}, received ${issue['received']:.2f}")
    else:
        print(f"   ✅ No partial capture issues")
    
    print(f"\n2E. Amount Discrepancies:")
    if amount_discrepancies:
        print(f"   ⚠️  {len(amount_discrepancies)} amount discrepancies found!")
        for disc in amount_discrepancies[:5]:  # Show first 5
            print(f"     {disc['payment_intent_id']} ({disc['status']}): intended ${disc['intended']:.2f}, received ${disc['received']:.2f}")
    else:
        print(f"   ✅ No amount discrepancies")
    
    return capture_issues, amount_discrepancies

def investigate_date_timezone_issues(start_date, end_date):
    """
    Investigation 3: Date Range and Timezone Alignment
    Checks for timezone-related date boundary issues
    """
    print("\n" + "=" * 60)
    print("INVESTIGATION 3: DATE RANGE & TIMEZONE ALIGNMENT")
    print("=" * 60)
    
    # Test different timezone interpretations
    print(f"\n3A. Testing different date range interpretations:")
    print(f"   Input range: {start_date} to {end_date}")
    print(f"   Input timestamps: {int(start_date.timestamp())} to {int(end_date.timestamp())}")
    
    # Test UTC vs local time boundaries
    import pytz
    
    # Assuming US timezone (adjust as needed for your business)
    us_tz = pytz.timezone('America/Los_Angeles')  # Adjust to your timezone
    
    # Convert to UTC boundaries
    start_local = us_tz.localize(datetime.datetime(2025, 8, 1, 0, 0, 0))
    end_local = us_tz.localize(datetime.datetime(2025, 8, 31, 23, 59, 59))
    
    start_utc = start_local.astimezone(pytz.UTC)
    end_utc = end_local.astimezone(pytz.UTC)
    
    print(f"   Local timezone range: {start_local} to {end_local}")
    print(f"   UTC timezone range: {start_utc} to {end_utc}")
    
    # Get transactions for boundary dates
    print(f"\n3B. Checking transactions on boundary dates:")
    
    # Last day of July
    july_31_start = datetime.datetime(2025, 7, 31, 0, 0, 0)
    july_31_end = datetime.datetime(2025, 7, 31, 23, 59, 59)
    
    # First day of September
    sept_1_start = datetime.datetime(2025, 9, 1, 0, 0, 0)
    sept_1_end = datetime.datetime(2025, 9, 1, 23, 59, 59)
    
    def get_day_transactions(day_start, day_end, day_name):
        charges = stripe.Charge.list(
            created={
                "gte": int(day_start.timestamp()),
                "lte": int(day_end.timestamp()),
            },
            limit=1000000,
        )
        total = sum(charge.amount / 100 for charge in charges.auto_paging_iter() if charge.captured)
        count = sum(1 for charge in charges.auto_paging_iter() if charge.captured)
        print(f"   {day_name}: {count} transactions, ${total:,.2f}")
        return total, count
    
    july_31_total, july_31_count = get_day_transactions(july_31_start, july_31_end, "July 31, 2025")
    sept_1_total, sept_1_count = get_day_transactions(sept_1_start, sept_1_end, "Sept 1, 2025")
    
    print(f"\n3C. Boundary Analysis:")
    if july_31_total > 0:
        print(f"   ⚠️  July 31st had ${july_31_total:,.2f} in transactions")
        print(f"   → These might be included due to timezone differences")
    
    if sept_1_total > 0:
        print(f"   ⚠️  Sept 1st had ${sept_1_total:,.2f} in transactions")
        print(f"   → These might be missing due to timezone differences")
    
    boundary_difference = july_31_total - sept_1_total
    if abs(boundary_difference - 2913) < 100:
        print(f"   🎯 POTENTIAL MATCH: Boundary difference (${boundary_difference:,.2f}) close to discrepancy")
    
    return july_31_total, sept_1_total

def investigate_additional_stripe_fields():
    """
    Investigation 4: Additional Stripe API Fields
    Examines other fields that might affect revenue counting
    """
    print("\n" + "=" * 60)
    print("INVESTIGATION 4: ADDITIONAL STRIPE FIELDS ANALYSIS")
    print("=" * 60)
    
    start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
    end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
    
    print(f"\n4A. Examining Charge fields that might affect revenue:")
    
    charges = stripe.Charge.list(
        created={
            "gte": int(start_date.timestamp()),
            "lte": int(end_date.timestamp()),
        },
        limit=1000000,
    )
    
    uncaptured_count = 0
    uncaptured_amount = 0
    disputed_count = 0
    disputed_amount = 0
    fee_total = 0
    
    for charge in charges.auto_paging_iter():
        # Check captured status
        if not charge.captured:
            uncaptured_count += 1
            uncaptured_amount += charge.amount / 100
        
        # Check disputes
        if charge.disputed:
            disputed_count += 1
            disputed_amount += charge.amount / 100
        
        # Check fees (if available)
        if hasattr(charge, 'balance_transaction'):
            try:
                bt = stripe.BalanceTransaction.retrieve(charge.balance_transaction)
                fee_total += bt.fee / 100
            except:
                pass  # Skip if can't retrieve balance transaction
    
    print(f"   Uncaptured charges: {uncaptured_count} (${uncaptured_amount:,.2f})")
    print(f"   Disputed charges: {disputed_count} (${disputed_amount:,.2f})")
    print(f"   Total fees: ${fee_total:,.2f}")
    
    print(f"\n4B. Analysis:")
    if uncaptured_amount > 0:
        print(f"   ⚠️  Uncaptured charges total ${uncaptured_amount:,.2f}")
        print(f"   → Your current code might be including these in revenue")
        if abs(uncaptured_amount - 2913) < 100:
            print(f"   🎯 POTENTIAL MATCH: Uncaptured amount close to discrepancy")
    
    if disputed_amount > 0:
        print(f"   ⚠️  Disputed charges total ${disputed_amount:,.2f}")
        print(f"   → These should likely be excluded from revenue")
    
    return uncaptured_amount, disputed_amount

def generate_recommendations(refund_data, direct_refund_data, capture_issues, july_31_total, uncaptured_amount):
    """
    Generate specific code recommendations based on investigation results
    """
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS & CODE SOLUTIONS")
    print("=" * 60)
    
    total_refunds = sum(r['refund_amount'] for r in refund_data)
    total_direct_refunds = sum(r['amount'] for r in direct_refund_data)
    total_capture_loss = sum(issue['difference'] for issue in capture_issues)
    
    print(f"\n📊 Summary of Potential Issues:")
    print(f"   Refunds (via Charges): ${total_refunds:,.2f}")
    print(f"   Refunds (via Refunds API): ${total_direct_refunds:,.2f}")
    print(f"   Partial capture losses: ${total_capture_loss:,.2f}")
    print(f"   July 31st boundary: ${july_31_total:,.2f}")
    print(f"   Uncaptured charges: ${uncaptured_amount:,.2f}")
    
    # Determine most likely cause
    issues = [
        ("Refunds not deducted", max(total_refunds, total_direct_refunds)),
        ("Uncaptured charges included", uncaptured_amount),
        ("Date boundary issues", july_31_total),
        ("Partial capture issues", total_capture_loss),
    ]
    
    issues.sort(key=lambda x: abs(x[1] - 2913))
    
    print(f"\n🎯 Most Likely Causes (sorted by proximity to $2,913 discrepancy):")
    for i, (issue, amount) in enumerate(issues, 1):
        diff = abs(amount - 2913)
        print(f"   {i}. {issue}: ${amount:,.2f} (diff: ${diff:,.2f})")
    
    print(f"\n🛠️  RECOMMENDED CODE CHANGES:")
    
    # Recommendation 1: Handle refunds properly
    if total_refunds > 0 or total_direct_refunds > 0:
        print(f"\n1. REFUNDS HANDLING:")
        print(f"   Problem: Refunds may not be properly deducted from revenue")
        print(f"   Solution: Add refund deduction to your revenue calculation")
        print(f"   
   # Add this function to fetch_stripe_data.py:
   def get_refunds_for_period(self, stripe_key: str, start_date: datetime.datetime, end_date: datetime.datetime):
       '''Fetch all refunds for a given period'''
       stripe.api_key = stripe_key
       refunds = stripe.Refund.list(
           created={{
               \"gte\": int(start_date.timestamp()),
               \"lte\": int(end_date.timestamp()),
           }},
           limit=1000000,
       )
       
       total_refunds = 0
       refund_details = []
       for refund in refunds.auto_paging_iter():
           refund_amount = refund.amount / 100
           total_refunds += refund_amount
           refund_details.append({{
               'refund_id': refund.id,
               'charge_id': refund.charge,
               'amount': refund_amount,
               'date': datetime.datetime.fromtimestamp(refund.created).date()
           }})
       
       return total_refunds, refund_details
   
   # Modify your revenue calculation to subtract refunds:
   gross_revenue = df['Total Amount'].sum()
   total_refunds, refund_details = stripe_fetcher.get_refunds_for_period(stripe_key, start_date, end_date)
   net_revenue = gross_revenue - total_refunds
   print(f\"Gross Revenue: ${{gross_revenue:,.2f}}\")
   print(f\"Total Refunds: ${{total_refunds:,.2f}}\")
   print(f\"Net Revenue: ${{net_revenue:,.2f}}\")
")
    
    # Recommendation 2: Fix uncaptured charges
    if uncaptured_amount > 0:
        print(f"\n2. UNCAPTURED CHARGES:")
        print(f"   Problem: Including uncaptured charges in revenue")
        print(f"   Solution: Filter out uncaptured charges")
        print(f"   
   # Modify create_stripe_payments_df() in fetch_stripe_data.py:
   # Change line 129-130 from:
   if charge.get(\"captured\") is False:
       continue
   
   # To be more explicit:
   if not charge.get(\"captured\", False):
       print(f\"Skipping uncaptured charge: {{charge.id}} - ${{charge.amount/100:.2f}}\")
       continue
")
    
    # Recommendation 3: Handle timezone properly
    if july_31_total > 0:
        print(f"\n3. TIMEZONE ALIGNMENT:")
        print(f"   Problem: Date boundaries might include wrong dates due to timezone")
        print(f"   Solution: Use explicit timezone handling")
        print(f"   
   # Add timezone-aware date handling:
   import pytz
   
   def create_timezone_aware_range(year, month, day_start, day_end, timezone='America/Los_Angeles'):
       tz = pytz.timezone(timezone)
       start_local = tz.localize(datetime.datetime(year, month, day_start, 0, 0, 0))
       end_local = tz.localize(datetime.datetime(year, month, day_end, 23, 59, 59))
       return start_local.astimezone(pytz.UTC), end_local.astimezone(pytz.UTC)
   
   # Use in your API calls:
   start_utc, end_utc = create_timezone_aware_range(2025, 8, 1, 31)
   # Then use start_utc and end_utc for Stripe API calls
")
    
    print(f"\n🔍 IMMEDIATE DEBUGGING STEPS:")
    print(f"1. Run this investigation script to identify the exact cause")
    print(f"2. Check if refunds exist and are being deducted")
    print(f"3. Verify all charges are captured before including in revenue")
    print(f"4. Confirm date range boundaries align with your business timezone")
    print(f"5. Cross-reference with your expected revenue calculation method")

def main():
    """Run complete investigation"""
    print("STRIPE REVENUE DISCREPANCY INVESTIGATION")
    print("Expected: $50,650 | Actual: $53,563 | Difference: $2,913")
    print("Date: August 2025")
    
    try:
        stripe_key = setup_stripe()
        
        # Define August 2025 date range
        start_date = datetime.datetime(2025, 8, 1, 0, 0, 0)
        end_date = datetime.datetime(2025, 8, 31, 23, 59, 59)
        
        # Run all investigations
        refund_data, direct_refund_data = investigate_refunds(start_date, end_date)
        capture_issues, amount_discrepancies = investigate_payment_intent_issues(start_date, end_date)
        july_31_total, sept_1_total = investigate_date_timezone_issues(start_date, end_date)
        uncaptured_amount, disputed_amount = investigate_additional_stripe_fields()
        
        # Generate recommendations
        generate_recommendations(refund_data, direct_refund_data, capture_issues, july_31_total, uncaptured_amount)
        
        # Save detailed data for further analysis
        if refund_data:
            pd.DataFrame(refund_data).to_csv('data/outputs/stripe_refunds_analysis.csv', index=False)
            print(f"\n📁 Detailed refund data saved to: data/outputs/stripe_refunds_analysis.csv")
        
        if direct_refund_data:
            pd.DataFrame(direct_refund_data).to_csv('data/outputs/stripe_direct_refunds_analysis.csv', index=False)
            print(f"📁 Direct refund data saved to: data/outputs/stripe_direct_refunds_analysis.csv")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
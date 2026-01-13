"""
Analyze flagged customers and their contact information.
Creates a detailed markdown report.
"""

import pandas as pd
import boto3
import os
from io import StringIO
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('.env')

def analyze_flags():
    """Analyze all flagged customers and their contact info."""

    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )

    # Load flags
    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='customers/customer_flags.csv')
    flags_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    # Load customers
    obj = s3.get_object(Bucket='basin-climbing-data-prod', Key='capitan/customers.csv')
    customers_df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

    # Merge
    merged = flags_df.merge(
        customers_df[['customer_id', 'email', 'phone', 'first_name', 'last_name']],
        on='customer_id',
        how='left'
    )

    # Clean up contact info
    def is_valid(val):
        if pd.isna(val):
            return False
        val_str = str(val).strip().lower()
        if val_str in ['', 'nan', 'none']:
            return False
        return True

    merged['has_email'] = merged['email'].apply(is_valid)
    merged['has_phone'] = merged['phone'].apply(is_valid)
    merged['has_either'] = merged['has_email'] | merged['has_phone']
    merged['has_both'] = merged['has_email'] & merged['has_phone']

    # Parse dates
    merged['flag_added_date'] = pd.to_datetime(merged['flag_added_date'])

    # Generate markdown report
    lines = []
    lines.append("# Flagged Customers Contact Information Report")
    lines.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"**Total Flags:** {len(flags_df)}")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Summary statistics
    lines.append("## Summary Statistics")
    lines.append("")
    lines.append("### All Flags")
    lines.append(f"- **Total flags:** {len(merged)}")
    lines.append(f"- **With email:** {merged['has_email'].sum()} ({merged['has_email'].sum()/len(merged)*100:.1f}%)")
    lines.append(f"- **With phone:** {merged['has_phone'].sum()} ({merged['has_phone'].sum()/len(merged)*100:.1f}%)")
    lines.append(f"- **With both:** {merged['has_both'].sum()} ({merged['has_both'].sum()/len(merged)*100:.1f}%)")
    lines.append(f"- **With email OR phone:** {merged['has_either'].sum()} ({merged['has_either'].sum()/len(merged)*100:.1f}%)")
    lines.append(f"- **Missing both:** {(~merged['has_either']).sum()} ({(~merged['has_either']).sum()/len(merged)*100:.1f}%)")
    lines.append("")

    # AB test flags
    ab_flags = merged[merged['flag_type'].isin(['first_time_day_pass_2wk_offer', 'second_visit_offer_eligible', 'second_visit_2wk_offer'])]
    lines.append("### AB Test Flags Only")
    lines.append(f"- **Total AB test flags:** {len(ab_flags)}")
    lines.append(f"- **With email:** {ab_flags['has_email'].sum()} ({ab_flags['has_email'].sum()/len(ab_flags)*100:.1f}%)")
    lines.append(f"- **With phone:** {ab_flags['has_phone'].sum()} ({ab_flags['has_phone'].sum()/len(ab_flags)*100:.1f}%)")
    lines.append(f"- **With both:** {ab_flags['has_both'].sum()} ({ab_flags['has_both'].sum()/len(ab_flags)*100:.1f}%)")
    lines.append(f"- **With email OR phone:** {ab_flags['has_either'].sum()} ({ab_flags['has_either'].sum()/len(ab_flags)*100:.1f}%)")
    lines.append(f"- **Missing both:** {(~ab_flags['has_either']).sum()} ({(~ab_flags['has_either']).sum()/len(ab_flags)*100:.1f}%)")
    lines.append("")
    lines.append("---")
    lines.append("")

    # Breakdown by flag type
    lines.append("## Breakdown by Flag Type")
    lines.append("")

    for flag_type in sorted(merged['flag_type'].unique()):
        subset = merged[merged['flag_type'] == flag_type]

        lines.append(f"### {flag_type}")
        lines.append(f"**Total:** {len(subset)} customers")
        lines.append("")
        lines.append(f"- With email: {subset['has_email'].sum()} ({subset['has_email'].sum()/len(subset)*100:.1f}%)")
        lines.append(f"- With phone: {subset['has_phone'].sum()} ({subset['has_phone'].sum()/len(subset)*100:.1f}%)")
        lines.append(f"- With either: {subset['has_either'].sum()} ({subset['has_either'].sum()/len(subset)*100:.1f}%)")
        lines.append(f"- Missing both: {(~subset['has_either']).sum()} ({(~subset['has_either']).sum()/len(subset)*100:.1f}%)")
        lines.append("")

    lines.append("---")
    lines.append("")

    # Detailed customer list
    lines.append("## Detailed Customer List")
    lines.append("")

    for flag_type in sorted(merged['flag_type'].unique()):
        subset = merged[merged['flag_type'] == flag_type].sort_values('flag_added_date', ascending=False)

        lines.append(f"### {flag_type} ({len(subset)} customers)")
        lines.append("")
        lines.append("| Customer ID | Name | Email | Phone | Added |")
        lines.append("|-------------|------|-------|-------|-------|")

        for _, row in subset.iterrows():
            cust_id = row['customer_id']
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip() or "(no name)"

            # Format email
            if row['has_email']:
                email = row['email']
            else:
                email = "❌ Missing"

            # Format phone
            if row['has_phone']:
                phone = str(row['phone'])
            else:
                phone = "❌ Missing"

            # Format date
            date = row['flag_added_date'].strftime('%Y-%m-%d') if pd.notna(row['flag_added_date']) else "Unknown"

            lines.append(f"| {cust_id} | {name} | {email} | {phone} | {date} |")

        lines.append("")

    # Problem customers
    lines.append("---")
    lines.append("")
    lines.append("## 🚨 Problem Customers (Missing Both Email AND Phone)")
    lines.append("")

    problems = merged[~merged['has_either']].sort_values('flag_type')

    if len(problems) == 0:
        lines.append("✅ No problem customers! All flagged customers have at least email or phone.")
    else:
        lines.append(f"Found **{len(problems)}** customers missing both email and phone:")
        lines.append("")
        lines.append("| Customer ID | Name | Flag Type | Added |")
        lines.append("|-------------|------|-----------|-------|")

        for _, row in problems.iterrows():
            cust_id = row['customer_id']
            name = f"{row.get('first_name', '')} {row.get('last_name', '')}".strip() or "(no name)"
            flag_type = row['flag_type']
            date = row['flag_added_date'].strftime('%Y-%m-%d') if pd.notna(row['flag_added_date']) else "Unknown"

            lines.append(f"| {cust_id} | {name} | {flag_type} | {date} |")

        lines.append("")
        lines.append("**Action:** These customers cannot be reached via automated email or SMS. Consider:")
        lines.append("- Manual outreach at front desk")
        lines.append("- Ask for contact info during next check-in")
        lines.append("- Check if they're kids (contact parents instead)")

    # Write report
    report_path = 'FLAGGED_CUSTOMERS_CONTACT_REPORT.md'
    with open(report_path, 'w') as f:
        f.write('\n'.join(lines))

    print(f"✅ Report saved to: {report_path}")
    print(f"\nQuick Summary:")
    print(f"  Total flags: {len(merged)}")
    print(f"  Reachable (email or phone): {merged['has_either'].sum()} ({merged['has_either'].sum()/len(merged)*100:.1f}%)")
    print(f"  Unreachable (no contact): {(~merged['has_either']).sum()} ({(~merged['has_either']).sum()/len(merged)*100:.1f}%)")
    print(f"\nAB Test flags:")
    print(f"  Total: {len(ab_flags)}")
    print(f"  Reachable: {ab_flags['has_either'].sum()} ({ab_flags['has_either'].sum()/len(ab_flags)*100:.1f}%)")

    return merged


if __name__ == "__main__":
    analyze_flags()

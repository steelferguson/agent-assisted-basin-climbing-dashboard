"""
AB Test System Audit Script

This script provides a comprehensive audit of the day pass conversion AB test system.
It checks:
1. Customer flagging (are the right people getting flagged?)
2. Shopify sync (are flags making it to Shopify as tags?)
3. Experiment tracking (are customers being logged in experiments?)
4. Communication delivery (are emails/SMS being sent?)

Usage:
    python audit_ab_test_system.py

Environment Variables Required:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    SHOPIFY_STORE_DOMAIN, SHOPIFY_ADMIN_TOKEN (optional, for Shopify checks)
"""

import os
import sys
import pandas as pd
import boto3
import json
from io import StringIO
from datetime import datetime, timedelta
from collections import defaultdict

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv('.env')
except ImportError:
    pass

# Try importing Shopify sync (optional)
try:
    from data_pipeline.sync_flags_to_shopify import ShopifyFlagSyncer
    SHOPIFY_AVAILABLE = True
except:
    SHOPIFY_AVAILABLE = False

# Colors for terminal output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(text):
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text.center(80)}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*80}{Colors.END}\n")


def print_section(text):
    print(f"\n{Colors.BOLD}{text}{Colors.END}")
    print("-" * 60)


def print_success(text):
    print(f"{Colors.GREEN}✓{Colors.END} {text}")


def print_error(text):
    print(f"{Colors.RED}✗{Colors.END} {text}")


def print_warning(text):
    print(f"{Colors.YELLOW}⚠{Colors.END} {text}")


def print_info(text):
    print(f"  {text}")


def load_from_s3(s3_client, bucket, key):
    """Load CSV from S3 into DataFrame."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        return df
    except s3_client.exceptions.NoSuchKey:
        return pd.DataFrame()
    except Exception as e:
        print_error(f"Error loading {key}: {e}")
        return pd.DataFrame()


def audit_customer_flags(s3_client, bucket):
    """Audit customer flags in S3."""
    print_section("1. CUSTOMER FLAGS AUDIT")

    # Load flags
    df_flags = load_from_s3(s3_client, bucket, 'customers/customer_flags.csv')

    if df_flags.empty:
        print_error("No customer flags found in S3!")
        print_info("Location: s3://basin-climbing-data-prod/customers/customer_flags.csv")
        print_info("This could mean:")
        print_info("  1. Flag engine hasn't run yet")
        print_info("  2. No customers meet flag criteria")
        print_info("  3. S3 path is wrong")
        return None

    print_success(f"Found {len(df_flags)} customer flags in S3")

    # Parse dates
    df_flags['triggered_date'] = pd.to_datetime(df_flags['triggered_date'])
    df_flags['flag_added_date'] = pd.to_datetime(df_flags['flag_added_date'])

    # Breakdown by flag type
    print_info("\nFlag breakdown by type:")
    for flag_type, count in df_flags['flag_type'].value_counts().items():
        print_info(f"  • {flag_type}: {count} customers")

    # AB test specific flags
    ab_test_flags = [
        'first_time_day_pass_2wk_offer',
        'second_visit_offer_eligible',
        'second_visit_2wk_offer'
    ]

    df_ab_flags = df_flags[df_flags['flag_type'].isin(ab_test_flags)]

    if df_ab_flags.empty:
        print_warning("\nNo AB test flags found!")
        print_info("Expected flag types:")
        for flag in ab_test_flags:
            print_info(f"  • {flag}")
        return df_flags

    print_success(f"\nAB test flags: {len(df_ab_flags)} customers")

    # Analyze AB groups
    print_info("\nAB group distribution:")
    for flag_type in ab_test_flags:
        flag_subset = df_ab_flags[df_ab_flags['flag_type'] == flag_type]
        if not flag_subset.empty:
            print_info(f"\n  {flag_type}:")

            # Parse flag_data to get ab_group
            groups = {'A': 0, 'B': 0, 'Unknown': 0}
            for _, row in flag_subset.iterrows():
                try:
                    flag_data = json.loads(row['flag_data'])
                    group = flag_data.get('ab_group', 'Unknown')
                    groups[group] = groups.get(group, 0) + 1
                except:
                    groups['Unknown'] += 1

            for group, count in groups.items():
                if count > 0:
                    print_info(f"    Group {group}: {count} customers")

    # Recent flags (last 7 days)
    today = pd.Timestamp.now().normalize()
    recent_flags = df_ab_flags[df_ab_flags['flag_added_date'] >= (today - pd.Timedelta(days=7))]

    if recent_flags.empty:
        print_warning(f"\nNo AB test flags added in last 7 days")
        print_info(f"Most recent flag: {df_ab_flags['flag_added_date'].max()}")
    else:
        print_success(f"\nFlags added in last 7 days: {len(recent_flags)}")
        print_info("Daily breakdown:")
        for date, count in recent_flags.groupby(recent_flags['flag_added_date'].dt.date).size().items():
            print_info(f"  {date}: {count} flags")

    return df_flags


def audit_experiment_tracking(s3_client, bucket):
    """Audit experiment tracking entries."""
    print_section("2. EXPERIMENT TRACKING AUDIT")

    # Load experiment entries
    df_entries = load_from_s3(s3_client, bucket, 'experiments/customer_experiment_entries.csv')

    if df_entries.empty:
        print_error("No experiment tracking entries found!")
        print_info("Location: s3://basin-climbing-data-prod/experiments/customer_experiment_entries.csv")
        print_info("This means:")
        print_info("  • Experiment tracking is not being logged")
        print_info("  • Check customer_flags_engine.py lines 179-196")
        return None

    print_success(f"Found {len(df_entries)} experiment entries")

    # Filter to day_pass_conversion experiment
    df_day_pass_exp = df_entries[df_entries['experiment_id'] == 'day_pass_conversion_2026_01']

    if df_day_pass_exp.empty:
        print_warning("No entries for 'day_pass_conversion_2026_01' experiment!")
        print_info("\nExperiments found:")
        for exp_id in df_entries['experiment_id'].unique():
            print_info(f"  • {exp_id}")
        return df_entries

    print_success(f"\nday_pass_conversion_2026_01 experiment: {len(df_day_pass_exp)} customers")

    # Group breakdown
    print_info("\nGroup distribution:")
    for group, count in df_day_pass_exp['group'].value_counts().items():
        pct = (count / len(df_day_pass_exp)) * 100
        print_info(f"  Group {group}: {count} customers ({pct:.1f}%)")

    # Entry flag breakdown
    print_info("\nEntry points (which flag triggered entry):")
    for flag, count in df_day_pass_exp['entry_flag'].value_counts().items():
        print_info(f"  • {flag}: {count} customers")

    # Recent entries
    df_day_pass_exp['entry_date'] = pd.to_datetime(df_day_pass_exp['entry_date'])
    recent_entries = df_day_pass_exp[df_day_pass_exp['entry_date'] >= (pd.Timestamp.now() - pd.Timedelta(days=7))]

    if recent_entries.empty:
        print_warning(f"\nNo experiment entries in last 7 days")
        print_info(f"Most recent entry: {df_day_pass_exp['entry_date'].max()}")
    else:
        print_success(f"\nExperiment entries in last 7 days: {len(recent_entries)}")

    return df_entries


def audit_shopify_sync():
    """Audit Shopify sync status."""
    print_section("3. SHOPIFY SYNC AUDIT")

    if not SHOPIFY_AVAILABLE:
        print_error("Shopify sync module not available")
        print_info("Cannot import sync_flags_to_shopify module")
        return

    # Check credentials
    if not os.getenv("SHOPIFY_STORE_DOMAIN") or not os.getenv("SHOPIFY_ADMIN_TOKEN"):
        print_error("Shopify credentials not configured")
        print_info("Missing environment variables:")
        if not os.getenv("SHOPIFY_STORE_DOMAIN"):
            print_info("  • SHOPIFY_STORE_DOMAIN")
        if not os.getenv("SHOPIFY_ADMIN_TOKEN"):
            print_info("  • SHOPIFY_ADMIN_TOKEN")
        return

    print_success("Shopify credentials configured")

    try:
        syncer = ShopifyFlagSyncer()
        print_success("Shopify sync module initialized")
        print_info(f"Store: {syncer.store_domain}")

        # Load flags to see what should be synced
        flags_df = syncer.load_flags_from_s3()

        if flags_df.empty:
            print_warning("No flags to sync to Shopify")
            return

        print_success(f"Found {len(flags_df)} flags ready to sync")

        # Count by flag type
        print_info("\nFlags by type:")
        for flag_type, count in flags_df['flag_name'].value_counts().items():
            tag_name = flag_type.replace('_', '-')
            print_info(f"  • {flag_type} → tag '{tag_name}': {count} customers")

        # Note: We can't easily check which customers are already synced without
        # making lots of Shopify API calls, which would hit rate limits.
        print_info("\n⚠ Cannot verify which customers have tags without API calls")
        print_info("To check sync status, run: python data_pipeline/sync_flags_to_shopify.py")

    except Exception as e:
        print_error(f"Error initializing Shopify sync: {e}")
        import traceback
        traceback.print_exc()


def audit_communications(s3_client, bucket):
    """Audit email and SMS communications."""
    print_section("4. COMMUNICATION DELIVERY AUDIT")

    # Check customer events for email_sent and sms_sent
    df_events = load_from_s3(s3_client, bucket, 'customers/customer_events.csv')

    if df_events.empty:
        print_error("No customer events found!")
        print_info("Location: s3://basin-climbing-data-prod/customers/customer_events.csv")
        return

    print_success(f"Found {len(df_events)} customer events")

    # Parse dates
    df_events['event_date'] = pd.to_datetime(df_events['event_date'])

    # Filter to last 30 days
    last_30_days = pd.Timestamp.now() - pd.Timedelta(days=30)
    df_recent = df_events[df_events['event_date'] >= last_30_days]

    print_info(f"\nEvents in last 30 days: {len(df_recent)}")

    # Email events
    df_emails = df_recent[df_recent['event_type'] == 'email_sent']

    if df_emails.empty:
        print_warning("\nNo email_sent events in last 30 days")
        print_info("This could mean:")
        print_info("  • Mailchimp integration not running")
        print_info("  • No campaigns sent in last 30 days")
        print_info("  • Event builder not including email events")
    else:
        print_success(f"\nEmail events: {len(df_emails)} emails sent to {df_emails['customer_id'].nunique()} customers")

        # Check for offers in emails
        emails_with_offers = 0
        for _, row in df_emails.iterrows():
            try:
                if pd.notna(row.get('event_data')):
                    event_data = json.loads(row['event_data'])
                    if event_data.get('has_offer'):
                        emails_with_offers += 1
            except:
                pass

        if emails_with_offers > 0:
            print_info(f"  • {emails_with_offers} emails contained offers")
        else:
            print_warning("  • No emails contained offers (or offer tracking not working)")

    # SMS events
    df_sms = df_recent[df_recent['event_type'] == 'sms_sent']

    if df_sms.empty:
        print_warning("\nNo sms_sent events in last 30 days")
        print_info("This could mean:")
        print_info("  • Twilio SMS tracking not implemented yet")
        print_info("  • No SMS campaigns sent")
        print_info("  • SMS events not being logged")
    else:
        print_success(f"\nSMS events: {len(df_sms)} messages sent to {df_sms['customer_id'].nunique()} customers")

    # Flag set events (customers getting flagged)
    df_flags_set = df_recent[df_recent['event_type'] == 'flag_set']

    if df_flags_set.empty:
        print_warning("\nNo flag_set events in last 30 days")
        print_info("Flags may have been set, but not logged as events")
    else:
        print_success(f"\nFlag events: {len(df_flags_set)} flags set")

        # Breakdown by flag type
        print_info("\nFlags set by type:")
        for _, row in df_flags_set.iterrows():
            try:
                if pd.notna(row.get('event_data')):
                    event_data = json.loads(row['event_data'])
                    flag_type = event_data.get('flag_type', 'unknown')
            except:
                flag_type = 'unknown'

        flag_counts = defaultdict(int)
        for _, row in df_flags_set.iterrows():
            try:
                if pd.notna(row.get('event_data')):
                    event_data = json.loads(row['event_data'])
                    flag_type = event_data.get('flag_type', 'unknown')
                    flag_counts[flag_type] += 1
            except:
                flag_counts['unknown'] += 1

        for flag_type, count in flag_counts.items():
            print_info(f"  • {flag_type}: {count}")


def audit_ab_group_assignment(s3_client, bucket):
    """Verify AB group assignment is working correctly."""
    print_section("5. AB GROUP ASSIGNMENT VERIFICATION")

    # Load customer data
    df_customers = load_from_s3(s3_client, bucket, 'capitan/customers.csv')

    if df_customers.empty:
        print_error("Cannot load customer data")
        return

    print_success(f"Loaded {len(df_customers)} customers")

    # Load flags
    df_flags = load_from_s3(s3_client, bucket, 'customers/customer_flags.csv')

    if df_flags.empty:
        print_warning("No flags to analyze")
        return

    # Get AB test flags
    ab_test_flags = df_flags[df_flags['flag_type'].isin([
        'first_time_day_pass_2wk_offer',
        'second_visit_offer_eligible',
        'second_visit_2wk_offer'
    ])]

    if ab_test_flags.empty:
        print_warning("No AB test flags to analyze")
        return

    print_info(f"\nAnalyzing {len(ab_test_flags)} AB test flags...")

    # Check group assignment logic
    group_a_should_be = ['first_time_day_pass_2wk_offer']
    group_b_should_be = ['second_visit_offer_eligible', 'second_visit_2wk_offer']

    correct_assignments = 0
    incorrect_assignments = 0

    for _, flag in ab_test_flags.iterrows():
        try:
            flag_data = json.loads(flag['flag_data'])
            ab_group = flag_data.get('ab_group')
            flag_type = flag['flag_type']

            if flag_type in group_a_should_be and ab_group == 'A':
                correct_assignments += 1
            elif flag_type in group_b_should_be and ab_group == 'B':
                correct_assignments += 1
            else:
                incorrect_assignments += 1
                print_error(f"  Wrong group: customer {flag['customer_id']} has flag {flag_type} but group {ab_group}")
        except:
            pass

    if incorrect_assignments == 0:
        print_success(f"\nAll {correct_assignments} AB group assignments are correct!")
    else:
        print_error(f"\nFound {incorrect_assignments} incorrect group assignments!")
        print_info(f"Correct: {correct_assignments}")


def generate_action_plan(s3_client, bucket):
    """Generate action plan based on audit results."""
    print_section("6. ACTION PLAN")

    # Load all data sources
    df_flags = load_from_s3(s3_client, bucket, 'customers/customer_flags.csv')
    df_entries = load_from_s3(s3_client, bucket, 'experiments/customer_experiment_entries.csv')
    df_events = load_from_s3(s3_client, bucket, 'customers/customer_events.csv')

    issues = []

    # Check 1: Are flags being created?
    if df_flags.empty:
        issues.append(("CRITICAL", "No customer flags in S3",
                      "Run customer_flags_engine.py to evaluate rules"))
    else:
        ab_flags = df_flags[df_flags['flag_type'].isin([
            'first_time_day_pass_2wk_offer',
            'second_visit_offer_eligible',
            'second_visit_2wk_offer'
        ])]
        if ab_flags.empty:
            issues.append(("HIGH", "No AB test flags created",
                          "Check flag rules in customer_flags_config.py"))

    # Check 2: Is experiment tracking working?
    if df_entries.empty:
        issues.append(("HIGH", "No experiment tracking entries",
                      "Check customer_flags_engine.py lines 179-196"))

    # Check 3: Are communications being sent?
    if not df_events.empty:
        df_events['event_date'] = pd.to_datetime(df_events['event_date'])
        last_30_days = pd.Timestamp.now() - pd.Timedelta(days=30)
        recent_emails = df_events[(df_events['event_type'] == 'email_sent') &
                                  (df_events['event_date'] >= last_30_days)]
        if recent_emails.empty:
            issues.append(("MEDIUM", "No emails sent in last 30 days",
                          "Check Mailchimp integration in customer_events_builder.py"))

        recent_sms = df_events[(df_events['event_type'] == 'sms_sent') &
                               (df_events['event_date'] >= last_30_days)]
        if recent_sms.empty:
            issues.append(("MEDIUM", "No SMS sent in last 30 days",
                          "Twilio SMS tracking may not be implemented"))

    # Check 4: Shopify sync
    if not SHOPIFY_AVAILABLE or not os.getenv("SHOPIFY_STORE_DOMAIN"):
        issues.append(("MEDIUM", "Shopify sync not configured",
                      "Configure SHOPIFY_STORE_DOMAIN and SHOPIFY_ADMIN_TOKEN"))

    if not issues:
        print_success("No critical issues found!")
        print_info("\nSystem appears to be working correctly.")
        return

    # Print issues by priority
    for priority in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        priority_issues = [i for i in issues if i[0] == priority]
        if priority_issues:
            print(f"\n{priority} Issues:")
            for _, issue, action in priority_issues:
                print_error(f"  • {issue}")
                print_info(f"    Action: {action}")


def main():
    """Run complete AB test system audit."""
    print_header("AB TEST SYSTEM AUDIT")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Check AWS credentials
    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        print_error("AWS credentials not configured!")
        print_info("Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables")
        sys.exit(1)

    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = 'basin-climbing-data-prod'

    # Run audits
    audit_customer_flags(s3_client, bucket)
    audit_experiment_tracking(s3_client, bucket)
    audit_shopify_sync()
    audit_communications(s3_client, bucket)
    audit_ab_group_assignment(s3_client, bucket)
    generate_action_plan(s3_client, bucket)

    print_header("AUDIT COMPLETE")
    print(f"\nFor detailed investigation, check S3 files at:")
    print(f"  s3://{bucket}/customers/customer_flags.csv")
    print(f"  s3://{bucket}/experiments/customer_experiment_entries.csv")
    print(f"  s3://{bucket}/customers/customer_events.csv\n")


if __name__ == "__main__":
    main()

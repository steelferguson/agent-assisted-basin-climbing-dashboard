"""
Generates and emails a CSV of customers who should be added to Mailchimp
for the 2-week pass journey automation.

Includes two customer paths:
- Path A: 2-week pass purchasers (immediate entry to journey)
- Path B: Day pass purchasers who returned for a 2nd visit

Emails the CSV daily to vicky@basinclimbing.com (CC: steel@basinclimbing.com)
"""

import pandas as pd
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Attachment, FileContent, FileName,
    FileType, Disposition, ContentId
)
import base64
import io
from datetime import datetime
from data_pipeline import config, upload_data


def identify_journey_customers(df_master, df_flags, df_events, df_identifiers):
    """
    Identifies customers who should enter the 2-week pass journey.

    Args:
        df_master: Customer master data with UUIDs
        df_flags: Customer flags (may have Capitan IDs as customer_id)
        df_events: Customer events
        df_identifiers: Customer identifiers for ID mapping

    Returns:
        pd.DataFrame with columns: customer_id, email, first_name, last_name,
                                   phone, journey_path, reason
    """
    journey_customers = []

    # Build mapping from Capitan ID to UUID
    capitan_to_uuid = {}
    for _, row in df_identifiers[df_identifiers['source'] == 'capitan'].iterrows():
        source_id = row.get('source_id', '')
        if source_id and str(source_id).startswith('customer:'):
            capitan_id = str(source_id).replace('customer:', '').strip()
            if capitan_id:
                capitan_to_uuid[capitan_id] = row['customer_id']

    # PATH A: Customers with 2_week_pass_purchase flag
    two_week_flags = df_flags[df_flags['flag_type'] == '2_week_pass_purchase']

    # Convert Capitan IDs to UUIDs
    path_a_customer_ids = []
    for flag_customer_id in two_week_flags['customer_id'].unique():
        uuid = capitan_to_uuid.get(str(flag_customer_id))
        if uuid:
            path_a_customer_ids.append(uuid)
        else:
            # Try as-is in case it's already a UUID
            path_a_customer_ids.append(flag_customer_id)

    path_a_customers = df_master[df_master['customer_id'].isin(path_a_customer_ids)]

    for _, customer in path_a_customers.iterrows():
        email = customer.get('primary_email', '')
        if pd.notna(email) and email:
            # Parse name from primary_name
            name = customer.get('primary_name', '')
            name_parts = str(name).split() if pd.notna(name) else []
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

            # Format phone number
            phone = customer.get('primary_phone', '')
            if pd.notna(phone):
                # Convert float to string and clean
                phone = str(phone).replace('.0', '').strip()
            else:
                phone = ''

            journey_customers.append({
                'customer_id': customer['customer_id'],
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'phone': phone,
                'journey_path': 'A',
                'reason': '2-week pass purchaser'
            })

    # PATH B: Day pass purchasers who returned for a second visit
    # Use the second_visit_offer_eligible flag which already identifies these customers
    second_visit_flags = df_flags[df_flags['flag_type'] == 'second_visit_offer_eligible']

    # Convert Capitan IDs to UUIDs
    path_b_customer_ids = []
    for flag_customer_id in second_visit_flags['customer_id'].unique():
        uuid = capitan_to_uuid.get(str(flag_customer_id))
        if uuid:
            path_b_customer_ids.append(uuid)
        else:
            # Try as-is in case it's already a UUID
            path_b_customer_ids.append(flag_customer_id)

    path_b_customers = df_master[df_master['customer_id'].isin(path_b_customer_ids)]

    for _, customer in path_b_customers.iterrows():
        # Skip if already in path A
        if customer['customer_id'] in path_a_customer_ids:
            continue

        email = customer.get('primary_email', '')
        if pd.notna(email) and email:
            # Parse name from primary_name
            name = customer.get('primary_name', '')
            name_parts = str(name).split() if pd.notna(name) else []
            first_name = name_parts[0] if len(name_parts) > 0 else ''
            last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

            # Format phone number
            phone = customer.get('primary_phone', '')
            if pd.notna(phone):
                # Convert float to string and clean
                phone = str(phone).replace('.0', '').strip()
            else:
                phone = ''

            journey_customers.append({
                'customer_id': customer['customer_id'],
                'email': email,
                'first_name': first_name,
                'last_name': last_name,
                'phone': phone,
                'journey_path': 'B',
                'reason': 'Day pass + 2nd visit'
            })

    return pd.DataFrame(journey_customers)


def generate_mailchimp_csv(df_journey_customers):
    """
    Converts journey customers DataFrame to Mailchimp import CSV format.

    Mailchimp CSV format:
    - Email Address (required)
    - First Name
    - Last Name
    - Phone Number
    - Tags (comma-separated)

    Returns:
        CSV string ready for Mailchimp import
    """
    # Create the CSV with Mailchimp column headers
    mailchimp_df = pd.DataFrame({
        'Email Address': df_journey_customers['email'],
        'First Name': df_journey_customers['first_name'].fillna(''),
        'Last Name': df_journey_customers['last_name'].fillna(''),
        'Phone Number': df_journey_customers['phone'].fillna(''),
        'Tags': '2-week-pass-purchase'  # All get the same tag for the journey
    })

    # Convert to CSV string
    csv_buffer = io.StringIO()
    mailchimp_df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def send_csv_email(csv_content, customer_count, path_a_count, path_b_count):
    """
    Sends the Mailchimp import CSV via SendGrid email.

    To: vicky@basinclimbing.com
    CC: steel@basinclimbing.com
    Subject: Daily Mailchimp Import - 2-Week Pass Journey
    Attachment: mailchimp_import_YYYYMMDD.csv
    """
    sendgrid_api_key = config.sendgrid_api_key

    if not sendgrid_api_key:
        raise ValueError("SENDGRID_API_KEY not configured in environment")

    today = datetime.now().strftime('%Y%m%d')
    filename = f'mailchimp_import_{today}.csv'

    # Create email body
    email_body = f"""
Hi Vicky,

Here is today's Mailchimp import CSV for the 2-week pass journey automation.

Summary:
- Total customers: {customer_count}
- Path A (2-week pass purchasers): {path_a_count}
- Path B (day pass + 2nd visit): {path_b_count}

Please import this CSV into the Mailchimp audience to trigger the automated journey emails.

The CSV includes:
- Email Address
- First Name
- Last Name
- Phone Number
- Tags (2-week-pass-purchase)

Best,
Basin Climbing Data Pipeline
"""

    message = Mail(
        from_email='steel@basinclimbing.com',
        to_emails='vicky@basinclimbing.com',
        subject=f'Daily Mailchimp Import - 2-Week Pass Journey ({today})',
        plain_text_content=email_body
    )

    # Add CC
    message.add_cc('steel@basinclimbing.com')

    # Attach CSV
    encoded_csv = base64.b64encode(csv_content.encode()).decode()
    attachment = Attachment(
        FileContent(encoded_csv),
        FileName(filename),
        FileType('text/csv'),
        Disposition('attachment')
    )
    message.attachment = attachment

    try:
        sg = SendGridAPIClient(sendgrid_api_key)
        response = sg.send(message)
        print(f"✅ Email sent successfully!")
        print(f"   Status code: {response.status_code}")
        print(f"   To: vicky@basinclimbing.com")
        print(f"   CC: steel@basinclimbing.com")
        print(f"   Attachment: {filename}")
        print(f"   Customers: {customer_count} ({path_a_count} path A, {path_b_count} path B)")
        return True
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False


def run_mailchimp_csv_email():
    """
    Main function to generate and email the Mailchimp import CSV.
    Loads data from S3, identifies journey customers, generates CSV, and emails it.
    """
    print("\n" + "=" * 70)
    print("Mailchimp Journey Import CSV Generator")
    print("=" * 70)

    uploader = upload_data.DataUploader()

    # Load customer_master
    print("\n📥 Loading customer data from S3...")
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customers_master)
        df_master = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_master)} customers")
    except Exception as e:
        print(f"   ❌ Error loading customer_master: {e}")
        return False

    # Load customer_flags
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_flags)
        df_flags = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_flags)} flags")
    except Exception as e:
        print(f"   ❌ Error loading customer_flags: {e}")
        return False

    # Load customer_events (for path B check-in counting)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_events)
        df_events = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_events)} events")
    except Exception as e:
        print(f"   ⚠️  Could not load customer_events: {e}")
        df_events = pd.DataFrame()

    # Load customer_identifiers (for ID mapping)
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_customer_identifiers)
        df_identifiers = uploader.convert_csv_to_df(csv_content)
        print(f"   ✅ Loaded {len(df_identifiers)} identifiers")
    except Exception as e:
        print(f"   ❌ Error loading customer_identifiers: {e}")
        return False

    # Identify customers for journey
    print("\n🔍 Identifying customers for 2-week pass journey...")
    df_journey = identify_journey_customers(df_master, df_flags, df_events, df_identifiers)

    if df_journey.empty:
        print("   ℹ️  No customers found for journey")
        return True

    path_a_count = len(df_journey[df_journey['journey_path'] == 'A'])
    path_b_count = len(df_journey[df_journey['journey_path'] == 'B'])

    print(f"   ✅ Found {len(df_journey)} customers")
    print(f"      Path A (2-week pass): {path_a_count}")
    print(f"      Path B (day pass + 2nd visit): {path_b_count}")

    # Generate Mailchimp CSV
    print("\n📄 Generating Mailchimp CSV...")
    csv_content = generate_mailchimp_csv(df_journey)
    print(f"   ✅ Generated CSV with {len(df_journey)} rows")

    # Preview the CSV
    print("\n📋 CSV Preview (first 10 rows):")
    preview_df = pd.read_csv(io.StringIO(csv_content))
    print(preview_df.head(10).to_string(index=False))

    # Save locally for testing
    today = datetime.now().strftime('%Y%m%d')
    local_file = f'mailchimp_import_{today}.csv'
    with open(local_file, 'w') as f:
        f.write(csv_content)
    print(f"\n💾 Saved locally: {local_file}")

    # Send email
    print("\n📧 Sending email...")
    if not config.sendgrid_api_key:
        print("   ⚠️  SENDGRID_API_KEY not configured - skipping email")
        print("   (Email will be sent when running in production pipeline)")
        return True

    success = send_csv_email(csv_content, len(df_journey), path_a_count, path_b_count)

    if success:
        print("\n✅ Mailchimp CSV email sent successfully!")
    else:
        print("\n❌ Failed to send email")

    return success


if __name__ == "__main__":
    run_mailchimp_csv_email()

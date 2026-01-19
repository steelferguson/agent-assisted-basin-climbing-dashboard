"""
Fetch Mailchimp Recipient Activity

Fetches individual recipient-level email activity from Mailchimp campaigns.
This data is used to track which customers received which emails, enabling:
1. Preventing duplicate offers
2. Adding email events to customer timelines
3. Analyzing email engagement per customer

Storage:
- S3: mailchimp/recipient_activity.csv
- Local: data/outputs/mailchimp_recipient_activity.csv
"""

import os
import pandas as pd
import boto3
from io import StringIO
from typing import Optional
from data_pipeline.fetch_mailchimp_data import MailchimpDataFetcher


class MailchimpRecipientActivityFetcher:
    """Fetch and manage Mailchimp recipient-level email activity."""

    def __init__(self):
        # Mailchimp credentials
        self.api_key = os.getenv("MAILCHIMP_API_KEY")
        self.server_prefix = os.getenv("MAILCHIMP_SERVER_PREFIX")

        if not self.api_key or not self.server_prefix:
            raise ValueError("MAILCHIMP_API_KEY and MAILCHIMP_SERVER_PREFIX must be set")

        # AWS credentials
        self.aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.bucket_name = "basin-climbing-data-prod"
        self.s3_key = "mailchimp/recipient_activity.csv"

        # S3 client
        if self.aws_access_key_id and self.aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key
            )
        else:
            self.s3_client = None

        # Mailchimp fetcher
        self.fetcher = MailchimpDataFetcher(
            api_key=self.api_key,
            server_prefix=self.server_prefix,
            anthropic_api_key=None  # Don't need AI analysis for recipient data
        )

        print("✅ Mailchimp Recipient Activity Fetcher initialized")

    def load_existing_from_s3(self) -> pd.DataFrame:
        """
        Load existing recipient activity from S3.

        Returns:
            DataFrame with existing activity
        """
        if not self.s3_client:
            print("⚠️  No S3 client available")
            return pd.DataFrame()

        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.s3_key)
            df = pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))

            # Convert date columns to datetime to avoid comparison errors
            if 'sent_date' in df.columns:
                df['sent_date'] = pd.to_datetime(df['sent_date'], errors='coerce')

            print(f"✅ Loaded {len(df)} existing records from S3")
            return df
        except self.s3_client.exceptions.NoSuchKey:
            print("ℹ️  No existing recipient activity found in S3 (first run)")
            return pd.DataFrame()
        except Exception as e:
            print(f"⚠️  Error loading from S3: {e}")
            return pd.DataFrame()

    def fetch_and_save(self, days_back: int = 30, save_local: bool = True):
        """
        Fetch recipient activity and save to S3 and optionally local.

        Args:
            days_back: Number of days back to fetch campaigns
            save_local: Whether to save a local copy
        """
        print("\n" + "="*80)
        print("MAILCHIMP RECIPIENT ACTIVITY SYNC")
        print("="*80)

        # Fetch new recipient activity
        new_df = self.fetcher.fetch_recipient_activity(days_back=days_back)

        if new_df.empty:
            print("\n⚠️  No new recipient activity found")
            return

        # Load existing data
        print("\n💾 Merging with existing data...")
        existing_df = self.load_existing_from_s3()

        # Merge and deduplicate
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            # Deduplicate by email + campaign_id
            combined_df = combined_df.drop_duplicates(subset=['email_address', 'campaign_id'], keep='last')
            combined_df = combined_df.sort_values('sent_date', ascending=False)
            df = combined_df
        else:
            df = new_df

        # Save to S3
        if self.s3_client:
            try:
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)

                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=self.s3_key,
                    Body=csv_buffer.getvalue()
                )
                print(f"✅ Saved {len(df)} total records to S3")
                print(f"   Location: s3://{self.bucket_name}/{self.s3_key}")
            except Exception as e:
                print(f"⚠️  Error saving to S3: {e}")

        # Save local copy
        if save_local:
            output_path = "data/outputs/mailchimp_recipient_activity.csv"
            os.makedirs("data/outputs", exist_ok=True)
            df.to_csv(output_path, index=False)
            print(f"✅ Saved local copy to {output_path}")

        # Print summary
        print(f"\n📊 Recipient Activity Summary:")
        print(f"   Total records: {len(df)}")
        print(f"   Unique recipients: {df['email_address'].nunique()}")
        print(f"   Unique campaigns: {df['campaign_id'].nunique()}")
        print(f"   Date range: {df['sent_date'].min()} to {df['sent_date'].max()}")
        print(f"   Opens: {df['opened'].sum()} ({df['opened'].mean()*100:.1f}%)")
        print(f"   Clicks: {df['clicked'].sum()} ({df['clicked'].mean()*100:.1f}%)")

        print("\n" + "="*80)
        print("✅ SYNC COMPLETE")
        print("="*80)


def main():
    """Run Mailchimp recipient activity fetch."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    fetcher = MailchimpRecipientActivityFetcher()
    fetcher.fetch_and_save(days_back=30, save_local=True)


if __name__ == "__main__":
    main()

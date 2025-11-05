"""
One-time Backfill Script for Check-in History

Fetches the full year of historical check-in data from Capitan.
Run this once to get historical data, then the daily pipeline will keep it updated.

Usage:
    python backfill_checkin_history.py

This will:
1. Fetch 365 days of check-in history
2. Upload to S3 at capitan/checkins.csv
3. Create a snapshot
"""

from data_pipeline.pipeline_handler import upload_new_capitan_checkins
import datetime

def backfill_checkin_history():
    """Fetch full year of check-in history."""
    print(f"\n{'='*80}")
    print(f"CHECK-IN HISTORY BACKFILL - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")

    print("Fetching 365 days of check-in history...")
    print("This may take a few minutes...\n")

    try:
        upload_new_capitan_checkins(
            save_local=True,  # Save locally for review
            days_back=365  # Full year
        )
        print("\n✅ Historical check-in data backfill complete!")
        print("   - Data uploaded to S3: capitan/checkins.csv")
        print("   - Local copy saved to: data/outputs/capitan_checkins.csv")
        print("   - Snapshot created")
        print("\nThe daily pipeline will now keep this data updated with the last 7 days.")

    except Exception as e:
        print(f"\n❌ Error during backfill: {e}")
        print("Please check your Capitan API credentials and try again.")

    print(f"\n{'='*80}")
    print(f"BACKFILL COMPLETE - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    backfill_checkin_history()

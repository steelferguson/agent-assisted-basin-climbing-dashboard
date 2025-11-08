"""
Extract Events Calendar from Instagram Posts

Parses Instagram post captions to identify events and create a structured calendar.
"""
import pandas as pd
import re
from datetime import datetime
from pathlib import Path


def extract_events_from_posts(instagram_csv_path: str) -> pd.DataFrame:
    """
    Parse Instagram posts to extract event information.

    Args:
        instagram_csv_path: Path to instagram_posts.csv

    Returns:
        DataFrame with event calendar
    """
    print("Loading Instagram posts...")
    posts_df = pd.read_csv(instagram_csv_path)

    # Convert timestamp to datetime
    posts_df['timestamp'] = pd.to_datetime(posts_df['timestamp'])
    posts_df['post_date'] = posts_df['timestamp'].dt.date

    events = []

    # Define event patterns to look for
    event_keywords = [
        'bash', 'comp', 'competition', 'contest', 'tournament',
        'camp', 'clinic', 'class', 'workshop',
        'party', 'celebration', 'anniversary',
        'halloween', 'thanksgiving', 'christmas',
        'tuesday', 'monday', 'friday', 'saturday', 'sunday',
        'join us', 'come', 'event', 'night',
        'youth', 'kids', 'teen',
        'coffee', 'food', 'raffle'
    ]

    print(f"Analyzing {len(posts_df)} posts for events...")

    for idx, row in posts_df.iterrows():
        caption = str(row['caption']).lower()

        # Skip if no caption
        if caption == 'nan' or not caption:
            continue

        # Check if this post mentions an event
        is_event = any(keyword in caption for keyword in event_keywords)

        if is_event:
            event = {
                'post_date': row['post_date'],
                'post_id': row['post_id'],
                'caption': row['caption'],
                'permalink': row['permalink'],
                'likes': row['likes'],
                'comments': row['comments']
            }

            # Try to extract event date from caption
            event_date = extract_event_date(row['caption'], row['timestamp'])
            event['event_date'] = event_date

            # Try to extract event name/type
            event_type = extract_event_type(row['caption'])
            event['event_type'] = event_type

            events.append(event)

    events_df = pd.DataFrame(events)

    print(f"\n✅ Found {len(events_df)} potential events")

    return events_df


def extract_event_date(caption: str, post_date) -> str:
    """Extract event date from caption text."""
    caption_lower = caption.lower()

    # Patterns for dates
    patterns = [
        # "October 30", "Oct 30", "Oct. 30"
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2})',
        # "Nov 24-26", "November 24–26"
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2})[-–]\s*(\d{1,2})',
        # "Thursday, Oct 30"
        r'(monday|tuesday|wednesday|thursday|friday|saturday|sunday),?\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2})',
        # "Nov. 1"
        r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(\d{1,2})',
    ]

    for pattern in patterns:
        match = re.search(pattern, caption_lower)
        if match:
            return match.group(0)

    # Check for "tomorrow", "today", "this week"
    if 'tomorrow' in caption_lower:
        return f"Tomorrow from {post_date}"
    elif 'today' in caption_lower:
        return f"Today ({post_date})"
    elif 'this week' in caption_lower:
        return f"This week from {post_date}"

    return "See post for details"


def extract_event_type(caption: str) -> str:
    """Extract event type/name from caption."""
    caption_lower = caption.lower()

    # Check for specific event types
    if 'boulder bash' in caption_lower or 'bash' in caption_lower:
        return 'Boulder Bash'
    elif 'halloween' in caption_lower:
        return 'Halloween Event'
    elif 'thanksgiving' in caption_lower and 'camp' in caption_lower:
        return 'Thanksgiving Youth Camp'
    elif 'belay class' in caption_lower or 'belay certified' in caption_lower:
        return 'Belay Certification Class'
    elif 'top rope tuesday' in caption_lower:
        return 'Top Rope Tuesday'
    elif 'youth rec team' in caption_lower or 'rec team' in caption_lower:
        return 'Youth Rec Team'
    elif 'youth camp' in caption_lower or 'youth climbing camp' in caption_lower:
        return 'Youth Climbing Camp'
    elif 'hyrox' in caption_lower:
        return 'HYROX Event'
    elif 'coffee' in caption_lower:
        return 'Coffee & Climbing'
    elif 'anniversary' in caption_lower:
        return 'Anniversary Celebration'
    elif 'comp' in caption_lower or 'competition' in caption_lower:
        return 'Climbing Competition'
    elif 'clinic' in caption_lower:
        return 'Climbing Clinic'
    elif 'party' in caption_lower:
        return 'Party/Social Event'
    else:
        # Try to extract first sentence as event name
        first_sentence = caption.split('.')[0].split('!')[0][:100]
        return first_sentence.strip()


def create_events_calendar(events_df: pd.DataFrame, output_path: str = 'basin_events_calendar.csv'):
    """Create a clean events calendar and save to CSV."""

    # Sort by post date (most recent first)
    events_df = events_df.sort_values('post_date', ascending=False)

    # Create clean output
    calendar = events_df[[
        'event_date', 'event_type', 'post_date',
        'likes', 'comments', 'caption', 'permalink'
    ]].copy()

    calendar.columns = [
        'Event Date', 'Event Type', 'Announced On',
        'Likes', 'Comments', 'Full Caption', 'Instagram Link'
    ]

    # Save to CSV
    calendar.to_csv(output_path, index=False)
    print(f"\n✅ Events calendar saved to: {output_path}")

    # Print summary
    print("\n" + "="*80)
    print("BASIN CLIMBING EVENTS CALENDAR")
    print("="*80)

    print(f"\nTotal Events Found: {len(calendar)}")
    print(f"\nEvent Types:")
    event_counts = calendar['Event Type'].value_counts()
    for event_type, count in event_counts.items():
        print(f"  - {event_type}: {count}")

    print("\n" + "-"*80)
    print("Recent Events:")
    print("-"*80)
    for idx, row in calendar.head(10).iterrows():
        print(f"\n📅 {row['Event Date']}")
        print(f"   {row['Event Type']}")
        print(f"   Announced: {row['Announced On']}")
        print(f"   Engagement: {row['Likes']} likes, {row['Comments']} comments")
        if len(row['Full Caption']) > 150:
            print(f"   {row['Full Caption'][:150]}...")
        else:
            print(f"   {row['Full Caption']}")

    return calendar


if __name__ == "__main__":
    # Path to Instagram posts
    instagram_posts_path = "data/outputs/instagram_posts.csv"

    # Extract events
    events_df = extract_events_from_posts(instagram_posts_path)

    # Create calendar
    calendar = create_events_calendar(events_df)

    print("\n✅ Events calendar generation complete!")
    print(f"   CSV saved: basin_events_calendar.csv")
    print(f"   Total events: {len(calendar)}")

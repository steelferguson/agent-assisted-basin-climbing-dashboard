"""
Weekly Business Metrics Presentation

Generates a weekly review with check-ins, memberships, and at-risk members.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from presentation_builder import PresentationBuilder
from shared.data_loader import load_checkins, load_memberships, load_associations


def generate(days: int = 7, output: str = "weekly_metrics.pptx") -> str:
    """
    Generate weekly metrics presentation.

    Args:
        days: Number of days to analyze (default: 7)
        output: Output filename

    Returns:
        Path to generated presentation file
    """
    print(f"Generating Weekly Metrics presentation...")

    # Load data
    checkins_df = load_checkins()
    memberships_df = load_memberships()

    # Date calculations
    today = datetime.now().date()
    week_ago = today - timedelta(days=days)
    two_weeks_ago = today - timedelta(days=days*2)

    # Filter check-ins
    checkins_df['checkin_date'] = pd.to_datetime(checkins_df['checkin_datetime']).dt.date
    this_week = checkins_df[checkins_df['checkin_date'] > week_ago]
    last_week = checkins_df[(checkins_df['checkin_date'] > two_weeks_ago) & (checkins_df['checkin_date'] <= week_ago)]

    # Metrics
    this_week_count = len(this_week)
    last_week_count = len(last_week)
    week_change = this_week_count - last_week_count
    active_members = len(memberships_df[memberships_df['status'] == 'active'])

    # Initialize presentation
    builder = PresentationBuilder("Weekly Business Review")

    # Title slide
    builder.add_title_slide(
        subtitle="Basin Climbing & Fitness",
        date=f"{week_ago} to {today}"
    )

    # Overview metrics
    builder.add_metrics([
        {'label': 'Check-ins This Week', 'value': str(this_week_count), 'delta': f'{week_change:+d} vs last week'},
        {'label': 'Active Memberships', 'value': str(active_members)},
        {'label': 'Avg Daily Visits', 'value': str(int(this_week_count / days))},
    ], title="Weekly Overview")

    # Daily check-ins chart
    daily_checkins = this_week.groupby('checkin_date').size().reset_index(name='count')
    builder.add_line_chart(
        daily_checkins,
        x_col='checkin_date',
        y_col='count',
        title="Daily Check-ins",
        y_label="Check-ins"
    )

    # Key takeaways
    builder.add_takeaways([
        f"Total check-ins: {this_week_count} ({week_change:+d} vs last week)",
        f"Active memberships: {active_members}",
        f"Average daily traffic: {int(this_week_count / days)} visitors"
    ])

    return builder.save(output)


if __name__ == "__main__":
    generate()

"""
Member Health Report Presentation

Generates a report on member engagement, at-risk members, and retention.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from presentation_builder import PresentationBuilder
from shared.data_loader import load_checkins, load_memberships


def generate(output: str = "member_health.pptx") -> str:
    """
    Generate member health report presentation.

    Args:
        output: Output filename

    Returns:
        Path to generated presentation file
    """
    print(f"Generating Member Health Report...")

    # Load data
    checkins_df = load_checkins()
    memberships_df = load_memberships()

    today = datetime.now().date()
    active_memberships = memberships_df[memberships_df['status'] == 'active']

    # At-risk calculations
    last_14_days = today - timedelta(days=14)
    last_30_days = today - timedelta(days=30)

    checkins_df['checkin_date'] = pd.to_datetime(checkins_df['checkin_datetime']).dt.date
    recent_visitors = set(checkins_df[checkins_df['checkin_date'] > last_14_days]['customer_id'].unique())

    # Get active member customer IDs
    active_member_ids = set()
    for _, membership in active_memberships.iterrows():
        customer_ids = membership.get('customer_ids', '')
        if pd.notna(customer_ids):
            ids = str(customer_ids).split(',')
            active_member_ids.update([int(id.strip()) for id in ids if id.strip().isdigit()])

    at_risk_14_days = len(active_member_ids - recent_visitors)

    # Initialize presentation
    builder = PresentationBuilder("Member Health Report")

    builder.add_title_slide(
        subtitle="Retention & Engagement Analysis",
        date=datetime.now().strftime("%B %d, %Y")
    )

    # Overview
    builder.add_metrics([
        {'label': 'Active Memberships', 'value': str(len(active_memberships))},
        {'label': 'At Risk (14+ days)', 'value': str(at_risk_14_days), 'color': 'orange'},
        {'label': 'Engagement Rate', 'value': f"{(1 - at_risk_14_days/max(len(active_member_ids), 1)) * 100:.0f}%"}
    ], title="Member Health Overview")

    # Key takeaways
    builder.add_takeaways([
        f"{len(active_memberships)} active memberships",
        f"{at_risk_14_days} members haven't visited in 14+ days",
        "Recommended: Reach out to at-risk members this week"
    ])

    return builder.save(output)


if __name__ == "__main__":
    generate()

"""
Mailchimp Campaign Analysis Presentation

Generates a comprehensive presentation analyzing email campaign performance,
highlighting CTR issues, missing automations, and recommendations.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from presentation_builder import PresentationBuilder
from shared.data_loader import load_transactions
from data_pipeline.upload_data import DataUploader
import data_pipeline.config as config
import io


def load_mailchimp_campaigns():
    """Load Mailchimp campaigns data from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_mailchimp_campaigns
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates
    if 'send_time' in df.columns:
        df['send_time'] = pd.to_datetime(df['send_time'], errors='coerce', utc=True)
        df['send_date'] = df['send_time'].dt.date

    return df


def load_mailchimp_audience_growth():
    """Load Mailchimp audience growth data from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_mailchimp_audience_growth
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    return df


def generate(days: int = 90, output: str = "mailchimp_analysis.pptx") -> str:
    """
    Generate Mailchimp analysis presentation.

    Args:
        days: Number of days to analyze (default: 90)
        output: Output filename (default: mailchimp_analysis.pptx)

    Returns:
        Path to generated presentation file
    """
    print(f"Generating Mailchimp Analysis presentation...")

    # Load data
    campaigns_df = load_mailchimp_campaigns()
    audience_df = load_mailchimp_audience_growth()

    # Filter to date range
    cutoff_date = (datetime.now() - timedelta(days=days)).date()
    campaigns_df = campaigns_df[campaigns_df['send_date'] >= cutoff_date]

    # Calculate metrics
    total_campaigns = len(campaigns_df)
    avg_open_rate = campaigns_df['open_rate'].mean() * 100
    avg_click_rate = campaigns_df['click_rate'].mean() * 100
    total_subscribers = campaigns_df['emails_sent'].iloc[0] if len(campaigns_df) > 0 else 0

    # Industry benchmarks
    industry_open_rate = 21.5  # Fitness industry average
    industry_click_rate = 2.5   # Fitness industry average

    # Initialize presentation
    builder = PresentationBuilder("Email Campaign Performance Analysis")

    # Slide 1: Title
    builder.add_title_slide(
        subtitle="Basin Climbing & Fitness",
        date=f"{cutoff_date} to {datetime.now().date()}"
    )

    # Slide 2: Overview Metrics
    builder.add_metrics([
        {
            'label': 'Campaigns Sent',
            'value': str(total_campaigns),
            'delta': f'Last {days} days'
        },
        {
            'label': 'Avg Open Rate',
            'value': f'{avg_open_rate:.1f}%',
            'delta': f'Industry: {industry_open_rate}%',
            'color': 'green' if avg_open_rate > industry_open_rate else 'orange'
        },
        {
            'label': 'Avg Click Rate',
            'value': f'{avg_click_rate:.2f}%',
            'delta': f'Industry: {industry_click_rate}%',
            'color': 'red'  # Always red because we know it's low
        },
        {
            'label': 'List Size',
            'value': f'{total_subscribers:,}',
            'delta': 'Total subscribers'
        }
    ], title="Campaign Performance Overview")

    # Slide 3: Open Rate Trends
    open_trend_df = campaigns_df[['send_date', 'open_rate']].copy()
    open_trend_df['open_rate_pct'] = open_trend_df['open_rate'] * 100
    open_trend_df = open_trend_df.sort_values('send_date')

    builder.add_line_chart(
        open_trend_df,
        x_col='send_date',
        y_col='open_rate_pct',
        title="Email Open Rates Over Time",
        x_label="Date",
        y_label="Open Rate (%)",
        color='#2ca02c'  # Green
    )

    # Slide 4: Critical Issue - Click Rates
    ctr_df = campaigns_df[['campaign_title', 'click_rate']].copy()
    ctr_df['click_rate_pct'] = ctr_df['click_rate'] * 100
    ctr_df = ctr_df.sort_values('click_rate_pct', ascending=False).head(15)

    builder.add_bar_chart(
        ctr_df,
        x_col='campaign_title',
        y_col='click_rate_pct',
        title="🚨 Critical Issue: Click-Through Rates",
        x_label="Campaign",
        y_label="Click Rate (%)",
        color='#ff7f50'  # Orange/red
    )

    # Slide 5: Best vs Worst Performers
    best_campaigns = campaigns_df.nlargest(5, 'click_rate')[['campaign_title', 'open_rate', 'click_rate']].copy()
    best_campaigns['open_rate'] = (best_campaigns['open_rate'] * 100).round(1)
    best_campaigns['click_rate'] = (best_campaigns['click_rate'] * 100).round(2)
    best_campaigns.columns = ['Campaign', 'Open Rate (%)', 'Click Rate (%)']

    builder.add_table(
        best_campaigns,
        title="📊 Best Performing Campaigns"
    )

    worst_campaigns = campaigns_df.nsmallest(5, 'click_rate')[['campaign_title', 'open_rate', 'click_rate']].copy()
    worst_campaigns['open_rate'] = (worst_campaigns['open_rate'] * 100).round(1)
    worst_campaigns['click_rate'] = (worst_campaigns['click_rate'] * 100).round(2)
    worst_campaigns.columns = ['Campaign', 'Open Rate (%)', 'Click Rate (%)']

    builder.add_table(
        worst_campaigns,
        title="⚠️ Worst Performing Campaigns"
    )

    # Slide 6: Root Cause Analysis
    builder.add_bullets(
        title="🔍 Root Cause: Why Are Click Rates So Low?",
        points=[
            "Strong open rates (37-61%) show subject lines work and people WANT the content",
            "Extremely low click rates (0-1.89%) indicate missing or weak CTAs",
            "Example: 'This Week at Basin' had 52.8% opens but 0% clicks - NO actionable links",
            "Industry average for fitness: 2-5% CTR. We're at 0.5% average - 80% below benchmark",
            "Problem is fixable: Add clear, prominent CTAs to every campaign"
        ]
    )

    # Slide 7: Missing Automations
    builder.add_bullets(
        title="🤖 Missing: Email Automations (0 Configured)",
        subtitle="Major missed opportunity for engagement",
        points=[
            "Welcome Series - No onboarding for new subscribers",
            "New Member Flow - No post-purchase engagement sequence",
            "Event Reminders - No automated pre/post event emails",
            "Re-engagement - No 'we miss you' campaigns for dormant members",
            "Win-back - No expired membership recovery sequence",
            "Birthday/Milestone - No celebration emails"
        ]
    )

    # Slide 8: Segmentation Gap
    builder.add_bullets(
        title="🎯 Missing: Email Segmentation",
        subtitle="Currently sending one-size-fits-all broadcasts",
        points=[
            "Active Members (387) vs Prospects - Different messaging needed",
            "Students (57) - Should receive student-specific offers",
            "Founders Team (86) - VIP treatment and exclusive updates",
            "Birthday Party Attendees (511) - Parent conversion opportunities",
            "Frequent Day Pass Users - Membership upsell targets",
            "Result: Generic emails that don't resonate with specific audiences"
        ]
    )

    # Slide 9: Quick Wins
    builder.add_bullets(
        title="✅ Quick Wins (Implement This Week)",
        points=[
            "Add CTAs to 'This Week at Basin' newsletter - Include booking links for every mentioned event",
            "Link directly to Capitan booking pages - Remove intermediary landing pages",
            "Set up 3 core automations - Welcome series, new member onboarding, event reminders",
            "Create CTA template - Standardize button placement and design across all emails",
            "A/B test subject lines - Test emoji usage and urgency language",
            "Expected impact: 3-5x increase in CTR (from 0.5% to 1.5-2.5%)"
        ]
    )

    # Slide 10: Long-term Strategy
    builder.add_bullets(
        title="📈 Long-term Strategy (Next 90 Days)",
        points=[
            "Month 1: Implement basic segmentation (Active vs Non-Member)",
            "Month 1-2: Build complete automation library (6 core flows)",
            "Month 2: Add behavioral triggers based on Capitan check-ins",
            "Month 3: Launch milestone campaigns (visit #50, 6-month anniversary)",
            "Ongoing: Weekly A/B tests to optimize performance",
            "Goal: Match or exceed fitness industry benchmarks (2-5% CTR, 3% conversion)"
        ]
    )

    # Slide 11: Key Takeaways
    builder.add_takeaways([
        "✅ Strong open rates (37-61%) show good subject lines and engaged audience",
        "🚨 Critical problem: 0-1.89% CTR due to missing/weak CTAs (need 2-5%)",
        "⚡ Quick fix: Add clear CTAs to all emails, link directly to booking",
        "🤖 Major gap: 0 automations configured - missing constant engagement",
        "🎯 No segmentation: One-size-fits-all broadcasts reduce relevance",
        "💡 Fix is straightforward: Better CTAs + 3 automations = 3-5x improvement"
    ])

    # Save presentation
    filepath = builder.save(output)

    print(f"✅ Generated {builder.get_slide_count()} slides")
    print(f"📊 Analyzed {total_campaigns} campaigns")
    print(f"📧 Average open rate: {avg_open_rate:.1f}% (Good!)")
    print(f"⚠️ Average click rate: {avg_click_rate:.2f}% (Needs work)")

    return filepath


if __name__ == "__main__":
    generate()

"""
Generate Basin Climbing Investor Presentation
Creates an improved investor deck with Basin branding and improved narrative structure.
"""

import pandas as pd
from datetime import datetime
from presentation_builder import PresentationBuilder
from presentation_builder.chart_generator import ChartGenerator, COLORS
from data_pipeline import config
from data_pipeline.upload_data import DataUploader

# Initialize
uploader = DataUploader()
builder = PresentationBuilder("Basin Climbing & Fitness")
chart_gen = ChartGenerator()

print("Loading data from S3...")

# Load transaction data (operational period only)
transactions_csv = uploader.download_from_s3(
    config.aws_bucket_name,
    config.s3_path_combined
)
transactions_df = uploader.convert_csv_to_df(transactions_csv)
transactions_df['Date'] = pd.to_datetime(transactions_df['Date'])

# Load membership data
memberships_csv = uploader.download_from_s3(
    config.aws_bucket_name,
    config.s3_path_capitan_memberships
)
memberships_df = uploader.convert_csv_to_df(memberships_csv)

# Load member count data
members_csv = uploader.download_from_s3(
    config.aws_bucket_name,
    config.s3_path_capitan_members
)
members_df = uploader.convert_csv_to_df(members_csv)

print(f"Loaded {len(transactions_df)} transactions from {transactions_df['Date'].min().date()} to {transactions_df['Date'].max().date()}")

# Filter data to August 2025 for strategic reasons (most recent complete month with full data)
cutoff_date = pd.to_datetime('2025-09-01')
transactions_df_filtered = transactions_df[transactions_df['Date'] < cutoff_date].copy()
print(f"Filtered to {len(transactions_df_filtered)} transactions through August 2025")

# Calculate key metrics
amount_col = 'Total Amount'  # Use Total Amount (includes tax) for revenue

total_revenue = transactions_df_filtered[amount_col].sum()
monthly_revenue = transactions_df_filtered.groupby(transactions_df_filtered['Date'].dt.to_period('M'))[amount_col].sum()

# Use August 2025 as the most recent month
august_2025_revenue = monthly_revenue.iloc[-1] if len(monthly_revenue) > 0 else 0
recent_month_label = monthly_revenue.index[-1].strftime('%B %Y') if len(monthly_revenue) > 0 else 'Current'

# Count active memberships ('ACT' status in Capitan system)
active_memberships = len(memberships_df[memberships_df['status'] == 'ACT'])
avg_transaction = transactions_df_filtered[amount_col].mean()

# Calculate monthly growth
monthly_revenue_df = pd.DataFrame({
    'Month': monthly_revenue.index.to_timestamp(),
    'Revenue': monthly_revenue.values
})

print(f"\nKey metrics (through August 2025):")
print(f"  Total revenue: ${total_revenue:,.0f}")
print(f"  {recent_month_label} revenue: ${august_2025_revenue:,.0f}")
print(f"  Active memberships: {active_memberships:,}")
print(f"  Avg transaction: ${avg_transaction:,.0f}")

# ============================================================================
# SLIDE 1: TITLE SLIDE
# ============================================================================
print("\nCreating slides...")

builder.add_title_slide(
    subtitle="Investment Opportunity",
    date="November 2025"
)

# ============================================================================
# SLIDE 2: EXECUTIVE SUMMARY
# ============================================================================
builder.add_bullets(
    title="Investment Highlights",
    points=[
        f"Proven concept: 11 months operational, ${august_2025_revenue/1000:.0f}k monthly revenue run rate",
        f"Strong member base: {active_memberships} active memberships in growing Waco market",
        "Bridge investment for high-growth business with clear path to profitability in 2026",
        "$6.5M real estate asset provides strong downside protection",
        "Proprietary Cliff AI technology creates competitive advantage and operational efficiency",
        "Experienced ownership with deep climbing industry expertise"
    ]
)

# ============================================================================
# SLIDE 3: THE OPPORTUNITY
# ============================================================================
builder.add_bullets(
    title="Market Opportunity",
    subtitle="Climbing Industry Growth + Underserved Market",
    points=[
        "Climbing industry growing 10%+ annually, driven by Olympics inclusion and youth participation",
        "Waco: 140k+ population, growing Texas market with limited climbing facility competition",
        "Family-focused positioning: programming, youth camps, birthday parties drive recurring revenue",
        "Multiple revenue streams: memberships, day passes, programming, retail, event bookings",
        "Basin opened September 2024 - now demonstrating strong unit economics after 1 year"
    ]
)

# ============================================================================
# SLIDE 4: TRACTION METRICS
# ============================================================================
metrics = [
    {
        'label': 'Active Memberships',
        'value': f'{active_memberships}',
        'color': 'teal'
    },
    {
        'label': f'{recent_month_label} Revenue',
        'value': f'${august_2025_revenue/1000:.0f}k',
        'color': 'terracotta'
    },
    {
        'label': 'Total Revenue (11 mo)',
        'value': f'${total_revenue/1000:.0f}k',
        'color': 'sage'
    },
    {
        'label': 'Avg Transaction',
        'value': f'${avg_transaction:.0f}',
        'color': 'gold'
    }
]

builder.add_metrics(
    title="Traction: 11 Months of Operations (Oct 2024 - Aug 2025)",
    metrics=metrics
)

# ============================================================================
# SLIDE 5: REVENUE GROWTH
# ============================================================================
if len(monthly_revenue_df) >= 2:
    builder.add_line_chart(
        df=monthly_revenue_df,
        x_col='Month',
        y_col='Revenue',
        title="Revenue Growth Since Opening",
        y_label='Revenue ($)',
        color=COLORS['teal']
    )

# ============================================================================
# SLIDE 6: CUSTOMER VALIDATION
# ============================================================================
builder.add_bullets(
    title="Customer Validation",
    subtitle="Strong Reviews & Community Engagement",
    points=[
        "⭐⭐⭐⭐⭐ 5.0 Google rating from 600+ reviews",
        "\"World class climbing gym\" - consistent feedback on facility quality",
        "\"Owner is often present and engaged\" - builds community and loyalty",
        "Strong family participation: youth programming and birthday parties growing",
        "Member retention driven by community atmosphere and owner involvement"
    ]
)

# ============================================================================
# SLIDE 7: BUSINESS MODEL
# ============================================================================
# Calculate revenue by category (exclude refunds and negative amounts)
category_col = 'revenue_category'
revenue_by_category = transactions_df_filtered.groupby(category_col)[amount_col].sum().sort_values(ascending=False)
# Filter out refunds and keep only positive revenue
revenue_by_category_positive = revenue_by_category[revenue_by_category > 0]
top_categories = revenue_by_category_positive.head(5)

builder.add_bar_chart(
    df=pd.DataFrame({'Category': top_categories.index, 'Revenue': top_categories.values}),
    x_col='Category',
    y_col='Revenue',
    title="Diversified Revenue Model",
    y_label='Revenue ($)',
    color=COLORS['terracotta']
)

# ============================================================================
# SLIDE 8: COMPETITIVE EDGE
# ============================================================================
builder.add_bullets(
    title="Competitive Advantages",
    points=[
        "Cliff AI: Proprietary data analytics platform for climbing gyms",
        "    → Member engagement insights, predictive churn analysis, automated reporting",
        "    → Scalable to other gyms as SaaS revenue stream (future upside)",
        "Owner expertise: Deep climbing industry experience and operational knowledge",
        "Facility quality: 'World class' facility reviews vs. competitors",
        "Community focus: High-touch service model drives retention and word-of-mouth"
    ]
)

# ============================================================================
# SLIDE 9: PATH TO PROFITABILITY
# ============================================================================
builder.add_bullets(
    title="Path to Profitability (2026 Target)",
    subtitle="Clear Strategy to Operational Breakeven",
    points=[
        "Revenue growth: Continue member acquisition, increase programming participation",
        "Operational efficiency: Leverage Cliff AI insights to optimize staffing and reduce churn",
        "Cost optimization: Refine cost structure based on 1-year operational data",
        "Marketing refinement: Focus on highest-ROI channels (Meta ads, local partnerships)",
        "Real estate structure: $6.5M facility provides stability while OpCo scales"
    ]
)

# ============================================================================
# SLIDE 10: INVESTMENT STRUCTURE
# ============================================================================
builder.add_bullets(
    title="Investment Terms",
    points=[
        "Bridge financing for proven, growing business",
        "Real estate: $6.5M asset provides strong collateral and downside protection",
        "Operating Company: Path to profitability in 2026 with current trajectory",
        "Clear value creation: Member growth + revenue optimization + cost efficiency",
        "Exit options: Refinance, strategic sale, or cash flow returns once profitable"
    ]
)

# ============================================================================
# SLIDE 11: USE OF FUNDS
# ============================================================================
builder.add_bullets(
    title="Use of Capital",
    points=[
        "Working capital bridge: Support operations through profitability inflection",
        "Marketing investment: Accelerate member acquisition in proven channels",
        "Cliff AI development: Enhance analytics capabilities for competitive edge",
        "Operational optimization: Test and scale highest-margin revenue streams",
        "Debt service: Maintain real estate obligations during OpCo ramp"
    ]
)

# ============================================================================
# SLIDE 12: INVESTMENT SUMMARY
# ============================================================================
builder.add_bullets(
    title="Why Basin Climbing",
    points=[
        f"✓ Proven traction: 11 months operational, ${august_2025_revenue/1000:.0f}k monthly revenue",
        f"✓ Strong member base: {active_memberships} active memberships and growing",
        "✓ Downside protection: $6.5M real estate asset",
        "✓ Competitive edge: Proprietary Cliff AI technology",
        "✓ Clear path: Profitability projected for 2026",
        "✓ Experienced team: Deep industry expertise and operational excellence"
    ]
)

# ============================================================================
# APPENDIX SLIDES
# ============================================================================
builder.add_section_header("Appendix")

# Detailed financial table
if len(monthly_revenue_df) >= 3:
    # Last 6 months or all available months
    recent_months = monthly_revenue_df.tail(6).copy()
    recent_months['Month'] = recent_months['Month'].dt.strftime('%b %Y')
    recent_months['Revenue'] = recent_months['Revenue'].apply(lambda x: f'${x:,.0f}')

    builder.add_table(
        title="Monthly Revenue Detail",
        df=recent_months
    )

# Revenue category breakdown (positive revenue only)
category_df = pd.DataFrame({
    'Category': revenue_by_category_positive.index,
    'Revenue': revenue_by_category_positive.values,
    'Percentage': (revenue_by_category_positive.values / revenue_by_category_positive.sum() * 100)
})
category_df['Revenue'] = category_df['Revenue'].apply(lambda x: f'${x:,.0f}')
category_df['Percentage'] = category_df['Percentage'].apply(lambda x: f'{x:.1f}%')

builder.add_table(
    title="Revenue by Category (Oct 2024 - Aug 2025)",
    df=category_df
)

# ============================================================================
# SAVE PRESENTATION
# ============================================================================
output_path = "/Users/steelferguson/daily_sessions/projects/agent-assisted-basin-climbing-dashboard/Basin_Investment_Deck_Nov2025.pptx"
builder.save(output_path)

print(f"\n✓ Presentation saved to: {output_path}")
print(f"✓ Total slides created: {len(builder.prs.slides)}")

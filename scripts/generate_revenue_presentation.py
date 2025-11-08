"""
Generate Revenue Month-over-Month Presentation
"""
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from presentation_builder import PresentationBuilder
from shared.data_loader import load_transactions


def generate_revenue_presentation(output: str = "basin_revenue_mom.pptx"):
    """Generate month-over-month revenue presentation."""
    print("Generating Basin Revenue Month-over-Month presentation...")

    # Load transaction data
    transactions_df = load_transactions()

    # Convert to datetime and extract month
    transactions_df['transaction_date'] = pd.to_datetime(transactions_df['Date'])
    transactions_df['year_month'] = transactions_df['transaction_date'].dt.to_period('M')

    # Calculate monthly revenue
    monthly_revenue = transactions_df.groupby('year_month')['Total Amount'].sum().reset_index()
    monthly_revenue.columns = ['month', 'revenue']
    monthly_revenue['month'] = monthly_revenue['month'].astype(str)
    monthly_revenue['revenue_formatted'] = monthly_revenue['revenue'].apply(lambda x: f"${x:,.0f}")

    # Calculate month-over-month change
    monthly_revenue['mom_change'] = monthly_revenue['revenue'].diff()
    monthly_revenue['mom_change_pct'] = monthly_revenue['revenue'].pct_change() * 100

    # Get latest month stats
    latest_month = monthly_revenue.iloc[-1]
    prev_month = monthly_revenue.iloc[-2] if len(monthly_revenue) > 1 else None
    total_revenue = monthly_revenue['revenue'].sum()
    avg_revenue = monthly_revenue['revenue'].mean()

    # Initialize presentation
    builder = PresentationBuilder("Basin Climbing Revenue Analysis")

    # Title slide
    builder.add_title_slide(
        subtitle="Month-over-Month Revenue Trends",
        date=datetime.now().strftime("%B %d, %Y")
    )

    # Overview metrics
    metrics = [
        {
            'label': 'Latest Month Revenue',
            'value': f"${latest_month['revenue']:,.0f}",
            'delta': f"{latest_month['mom_change_pct']:+.1f}% vs prior month" if pd.notna(latest_month['mom_change_pct']) else "First month"
        },
        {
            'label': 'Total Revenue (All Time)',
            'value': f"${total_revenue:,.0f}"
        },
        {
            'label': 'Average Monthly Revenue',
            'value': f"${avg_revenue:,.0f}"
        }
    ]

    builder.add_metrics(metrics, title="Revenue Overview")

    # Revenue trend line chart
    builder.add_line_chart(
        monthly_revenue,
        x_col='month',
        y_col='revenue',
        title="Monthly Revenue Trend",
        x_label="Month",
        y_label="Revenue ($)",
        color='#2c7fb8'  # Basin teal
    )

    # Revenue table with MoM changes
    table_df = monthly_revenue[['month', 'revenue_formatted', 'mom_change', 'mom_change_pct']].copy()
    table_df['mom_change'] = table_df['mom_change'].apply(lambda x: f"${x:+,.0f}" if pd.notna(x) else "—")
    table_df['mom_change_pct'] = table_df['mom_change_pct'].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "—")
    table_df.columns = ['Month', 'Revenue', 'MoM Change ($)', 'MoM Change (%)']

    builder.add_table(table_df, title="Monthly Revenue Details")

    # Key takeaways
    takeaways = [
        f"Latest month ({latest_month['month']}): ${latest_month['revenue']:,.0f}",
        f"Total revenue across all months: ${total_revenue:,.0f}",
        f"Average monthly revenue: ${avg_revenue:,.0f}"
    ]

    if prev_month is not None and pd.notna(latest_month['mom_change_pct']):
        if latest_month['mom_change_pct'] > 0:
            takeaways.append(f"Revenue up {latest_month['mom_change_pct']:.1f}% from previous month")
        else:
            takeaways.append(f"Revenue down {abs(latest_month['mom_change_pct']):.1f}% from previous month")

    # Find best and worst months
    best_month = monthly_revenue.loc[monthly_revenue['revenue'].idxmax()]
    worst_month = monthly_revenue.loc[monthly_revenue['revenue'].idxmin()]
    takeaways.append(f"Best month: {best_month['month']} (${best_month['revenue']:,.0f})")
    takeaways.append(f"Lowest month: {worst_month['month']} (${worst_month['revenue']:,.0f})")

    builder.add_takeaways(takeaways)

    # Save presentation
    filepath = builder.save(output)

    print(f"\n✅ Generated {builder.get_slide_count()} slides")
    print(f"📊 Analyzed {len(monthly_revenue)} months of revenue data")
    print(f"💰 Total revenue: ${total_revenue:,.0f}")
    print(f"📈 Latest month: ${latest_month['revenue']:,.0f}")

    return filepath


if __name__ == "__main__":
    generate_revenue_presentation()

"""
Basin Climbing & Fitness Dashboard - Streamlit Version

A comprehensive analytics dashboard for Basin Climbing & Fitness.
Organized into logical tabs for better navigation and analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_pipeline import upload_data
from data_pipeline import config
import os

# Page config
st.set_page_config(
    page_title="Basin Climbing Dashboard",
    page_icon="🧗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Basin brand colors
COLORS = {
    'primary': '#8B4229',      # Rust/terracotta
    'secondary': '#BAA052',    # Gold
    'tertiary': '#96A682',     # Sage green
    'quaternary': '#1A2E31',   # Dark teal
    'background': '#FFFFFF',
    'text': '#213B3F',
    'dark_grey': '#4A4A4A'
}

# Revenue category colors
REVENUE_CATEGORY_COLORS = {
    'Day Pass': COLORS['quaternary'],        # Dark teal
    'New Membership': COLORS['primary'],      # Rust
    'Membership Renewal': '#D4AF6A',          # Lighter gold (different from secondary)
    'Programming': COLORS['tertiary'],        # Sage green
    'Team Dues': '#C85A3E',                   # Lighter rust (different from primary)
    'Retail': COLORS['dark_grey'],            # Dark grey
    'Event Booking': '#B8C9A8',               # Lighter sage (different from tertiary)
}


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load all data from S3 with caching."""
    uploader = upload_data.DataUploader()

    def load_df(bucket, key):
        csv_content = uploader.download_from_s3(bucket, key)
        return uploader.convert_csv_to_df(csv_content)

    df_transactions = load_df(config.aws_bucket_name, config.s3_path_combined)
    df_memberships = load_df(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_members = load_df(config.aws_bucket_name, config.s3_path_capitan_members)
    df_projection = load_df(config.aws_bucket_name, config.s3_path_capitan_membership_revenue_projection)
    df_at_risk = load_df(config.aws_bucket_name, config.s3_path_at_risk_members)
    df_facebook_ads = load_df(config.aws_bucket_name, config.s3_path_facebook_ads)
    df_events = load_df(config.aws_bucket_name, config.s3_path_capitan_events)
    df_checkins = load_df(config.aws_bucket_name, config.s3_path_capitan_checkins)
    df_instagram = load_df(config.aws_bucket_name, config.s3_path_instagram_posts)
    df_mailchimp = load_df(config.aws_bucket_name, config.s3_path_mailchimp_campaigns)
    df_failed_payments = load_df(config.aws_bucket_name, config.s3_path_failed_payments)

    return df_transactions, df_memberships, df_members, df_projection, df_at_risk, df_facebook_ads, df_events, df_checkins, df_instagram, df_mailchimp, df_failed_payments


# Load data
with st.spinner('Loading data from S3...'):
    df_transactions, df_memberships, df_members, df_projection, df_at_risk, df_facebook_ads, df_events, df_checkins, df_instagram, df_mailchimp, df_failed_payments = load_data()

# Prepare at-risk members data
if not df_at_risk.empty:
    df_at_risk['full_name'] = df_at_risk['first_name'] + ' ' + df_at_risk['last_name']
    df_at_risk_display = df_at_risk[[
        'full_name', 'age', 'membership_type', 'last_checkin_date',
        'risk_category', 'risk_description', 'capitan_link'
    ]].copy()
    df_at_risk_display.columns = [
        'Name', 'Age', 'Membership Type', 'Last Check-in',
        'Risk Category', 'Description', 'Capitan Link'
    ]

# App title
st.title('🧗 Basin Climbing & Fitness Dashboard')
st.markdown('---')

# Create tabs
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Revenue",
    "👥 Membership",
    "🎟️ Day Passes & Check-ins",
    "🎉 Rentals",
    "💪 Programming",
    "📱 Marketing"
])

# ============================================================================
# TAB 1: REVENUE
# ============================================================================
with tab1:
    st.header('Revenue Analysis')

    # Timeframe selector
    timeframe = st.selectbox(
        'Select Timeframe',
        options=['D', 'W', 'M'],
        format_func=lambda x: {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly'}[x],
        index=2  # Default to Monthly
    )

    # Data source selector
    data_sources = st.multiselect(
        'Data Sources',
        options=['Stripe', 'Square'],
        default=['Stripe', 'Square']
    )

    # Filter data
    df_filtered = df_transactions[df_transactions['Data Source'].isin(data_sources)].copy()
    df_filtered['Date'] = pd.to_datetime(df_filtered['Date'], errors='coerce')
    df_filtered = df_filtered[df_filtered['Date'].notna()]
    df_filtered['date'] = df_filtered['Date'].dt.to_period(timeframe).dt.start_time

    # Revenue by category
    revenue_by_category = (
        df_filtered.groupby(['date', 'revenue_category'])['Total Amount']
        .sum()
        .reset_index()
    )

    category_order = [
        'Day Pass', 'New Membership', 'Membership Renewal',
        'Programming', 'Team Dues', 'Retail', 'Event Booking'
    ]

    # Line chart - Total Revenue Over Time
    st.subheader('Total Revenue Over Time')
    total_revenue = df_filtered.groupby('date')['Total Amount'].sum().reset_index()

    fig_line = px.line(
        total_revenue,
        x='date',
        y='Total Amount',
        title='Total Revenue Over Time',
        text=total_revenue['Total Amount'].apply(lambda x: f'${x/1000:.1f}K')
    )
    fig_line.update_traces(
        line_color=COLORS['primary'],
        textposition='top center',
        textfont=dict(size=10)
    )
    fig_line.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Revenue ($)',
        xaxis_title='Date'
    )
    st.plotly_chart(fig_line, use_container_width=True)

    # Stacked bar chart - Revenue by Category
    st.subheader('Revenue by Category (Stacked)')
    fig_stacked = px.bar(
        revenue_by_category,
        x='date',
        y='Total Amount',
        color='revenue_category',
        title='Revenue by Category',
        barmode='stack',
        category_orders={'revenue_category': category_order},
        color_discrete_map=REVENUE_CATEGORY_COLORS
    )
    fig_stacked.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Revenue ($)',
        xaxis_title='Date',
        legend_title='Category'
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

    # Percentage chart - Revenue by Category
    st.subheader('Percentage of Revenue by Category')
    total_revenue_per_date = (
        revenue_by_category.groupby('date')['Total Amount'].sum().reset_index()
    )
    total_revenue_per_date.columns = ['date', 'total_revenue']
    revenue_with_total = pd.merge(revenue_by_category, total_revenue_per_date, on='date')
    revenue_with_total['percentage'] = (
        revenue_with_total['Total Amount'] / revenue_with_total['total_revenue']
    ) * 100

    fig_percentage = px.bar(
        revenue_with_total,
        x='date',
        y='percentage',
        color='revenue_category',
        title='Percentage of Revenue by Category',
        barmode='stack',
        category_orders={'revenue_category': category_order},
        text=revenue_with_total['percentage'].apply(lambda x: f'{x:.1f}%'),
        color_discrete_map=REVENUE_CATEGORY_COLORS
    )
    fig_percentage.update_traces(
        textposition='outside',  # Always position labels outside for consistent size/visibility
        textfont=dict(size=11),
        cliponaxis=False
    )
    fig_percentage.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Percentage (%)',
        xaxis_title='Date',
        legend_title='Category'
    )
    st.plotly_chart(fig_percentage, use_container_width=True)

    # Refund rate chart
    st.subheader('Refund Rate by Category')
    df_filtered_copy = df_filtered.copy()
    df_filtered_copy['is_refund'] = df_filtered_copy['Total Amount'] < 0

    refund_stats = df_filtered_copy.groupby('revenue_category').agg({
        'Total Amount': lambda x: {
            'gross': x[x > 0].sum(),
            'refunds': abs(x[x < 0].sum()),
            'net': x.sum()
        }
    }).reset_index()

    refund_stats['gross_revenue'] = refund_stats['Total Amount'].apply(lambda x: x['gross'])
    refund_stats['refunds'] = refund_stats['Total Amount'].apply(lambda x: x['refunds'])
    refund_stats['net_revenue'] = refund_stats['Total Amount'].apply(lambda x: x['net'])
    refund_stats.drop('Total Amount', axis=1, inplace=True)

    refund_stats['refund_rate'] = (refund_stats['refunds'] / refund_stats['gross_revenue'] * 100).fillna(0)
    refund_stats = refund_stats[refund_stats['gross_revenue'] > 0]
    refund_stats = refund_stats.sort_values('refund_rate', ascending=False)

    fig_refund = px.bar(
        refund_stats,
        y='revenue_category',
        x='refund_rate',
        title='Refund Rate by Category (%)',
        orientation='h',
        text='refund_rate'
    )
    fig_refund.update_traces(
        texttemplate='%{text:.1f}%',
        textposition='outside',
        marker_color=COLORS['primary']
    )
    fig_refund.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        xaxis_title='Refund Rate (%)',
        yaxis_title='Category',
        height=400
    )
    st.plotly_chart(fig_refund, use_container_width=True)

    # Accounting groups chart
    st.subheader('Revenue by Accounting Groups')
    accounting_groups = revenue_by_category.copy()

    def map_to_accounting_group(category):
        if category in ['New Membership', 'Membership Renewal']:
            return 'Memberships'
        elif category in ['Team Dues', 'Programming']:
            return 'Team & Programming'
        else:
            return category

    accounting_groups['accounting_group'] = accounting_groups['revenue_category'].apply(map_to_accounting_group)
    accounting_revenue = accounting_groups.groupby(['date', 'accounting_group'])['Total Amount'].sum().reset_index()

    accounting_total = accounting_revenue.groupby('date')['Total Amount'].sum().reset_index()
    accounting_total.columns = ['date', 'total_revenue']
    accounting_with_total = pd.merge(accounting_revenue, accounting_total, on='date')
    accounting_with_total['percentage'] = (accounting_with_total['Total Amount'] / accounting_with_total['total_revenue']) * 100

    accounting_colors = {
        'Memberships': COLORS['primary'],         # Rust
        'Team & Programming': COLORS['tertiary'], # Sage green
        'Day Pass': COLORS['quaternary'],         # Dark teal
        'Retail': '#8B7355',                      # Brown (distinct from memberships)
        'Event Booking': '#B8C9A8',               # Light sage
    }

    fig_accounting = px.bar(
        accounting_with_total,
        x='date',
        y='percentage',
        color='accounting_group',
        title='Revenue by Accounting Groups (Memberships, Team & Programming, etc.)',
        barmode='stack',
        text=accounting_with_total['percentage'].apply(lambda x: f'{x:.1f}%'),
        color_discrete_map=accounting_colors
    )
    fig_accounting.update_traces(
        textposition='auto',  # Auto positions text inside when fits, outside when too small
        textfont=dict(size=11, color='white'),
        insidetextanchor='middle'
    )
    fig_accounting.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Percentage (%)',
        xaxis_title='Date',
        legend_title='Group'
    )
    st.plotly_chart(fig_accounting, use_container_width=True)

    # Membership Revenue Projection
    st.subheader('Membership Revenue: Historical & Projected')

    # Get historical membership revenue (last 3 months)
    df_membership_revenue = df_transactions[
        df_transactions['revenue_category'].isin(['New Membership', 'Membership Renewal'])
    ].copy()
    df_membership_revenue['date'] = pd.to_datetime(df_membership_revenue['Date'])
    df_membership_revenue['month'] = df_membership_revenue['date'].dt.to_period('M')

    historical_revenue = (
        df_membership_revenue.groupby('month')['Total Amount']
        .sum()
        .reset_index()
    )
    historical_revenue.columns = ['month', 'amount']
    historical_revenue['month_str'] = historical_revenue['month'].astype(str)
    historical_revenue['type'] = 'Realized'

    # Get current month
    current_month = pd.Period.now('M')

    # Filter to last 3 months
    historical_revenue = historical_revenue[
        historical_revenue['month'] >= (current_month - 2)
    ]

    # Get projected revenue
    df_proj = df_projection.copy()
    df_proj['date'] = pd.to_datetime(df_proj['date'])
    df_proj['month'] = df_proj['date'].dt.to_period('M')

    proj_summary = df_proj.groupby('month')['projected_total'].sum().reset_index()
    proj_summary.columns = ['month', 'amount']
    proj_summary['month_str'] = proj_summary['month'].astype(str)
    proj_summary['type'] = 'Projected'

    # Filter to next 4 months (including current month)
    proj_summary = proj_summary[
        (proj_summary['month'] >= current_month) &
        (proj_summary['month'] <= current_month + 3)
    ]

    # For current month, we want both realized (so far) and projected (scheduled)
    # Get realized revenue for current month
    current_month_realized = historical_revenue[
        historical_revenue['month'] == current_month
    ].copy()

    # Remove current month from projected (we'll add it back with both components)
    proj_summary_future = proj_summary[proj_summary['month'] > current_month].copy()
    current_month_projected = proj_summary[proj_summary['month'] == current_month].copy()

    # Combine data for chart
    # Past months: realized only
    past_months = historical_revenue[historical_revenue['month'] < current_month].copy()

    # Current month: both realized and projected stacked
    # Future months: projected only

    # Create chart data
    chart_data = []

    # Add past months (realized)
    for _, row in past_months.iterrows():
        chart_data.append({
            'month': row['month_str'],
            'Realized': row['amount'],
            'Projected': 0
        })

    # Add current month (both)
    if not current_month_realized.empty and not current_month_projected.empty:
        chart_data.append({
            'month': current_month.strftime('%Y-%m'),
            'Realized': current_month_realized['amount'].iloc[0],
            'Projected': current_month_projected['amount'].iloc[0]
        })
    elif not current_month_projected.empty:
        # No realized revenue yet this month
        chart_data.append({
            'month': current_month.strftime('%Y-%m'),
            'Realized': 0,
            'Projected': current_month_projected['amount'].iloc[0]
        })

    # Add future months (projected only)
    for _, row in proj_summary_future.iterrows():
        chart_data.append({
            'month': row['month_str'],
            'Realized': 0,
            'Projected': row['amount']
        })

    df_chart = pd.DataFrame(chart_data)

    # Create stacked bar chart
    fig_projection = go.Figure()

    # Add realized revenue bars
    fig_projection.add_trace(go.Bar(
        name='Realized',
        x=df_chart['month'],
        y=df_chart['Realized'],
        marker_color=COLORS['primary'],
        text=df_chart['Realized'].apply(lambda x: f'${x/1000:.1f}K' if x > 0 else ''),
        textposition='inside',
        textfont=dict(size=11, color='white')
    ))

    # Add projected revenue bars
    fig_projection.add_trace(go.Bar(
        name='Projected',
        x=df_chart['month'],
        y=df_chart['Projected'],
        marker_color=COLORS['secondary'],
        text=df_chart['Projected'].apply(lambda x: f'${x/1000:.1f}K' if x > 0 else ''),
        textposition='inside',
        textfont=dict(size=11, color='white')
    ))

    fig_projection.update_layout(
        barmode='stack',
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Revenue ($)',
        xaxis_title='Month',
        legend_title='Type',
        showlegend=True
    )

    st.plotly_chart(fig_projection, use_container_width=True)

    # Payment Failure Rates
    st.subheader('Payment Failure Rates by Membership Type')
    st.markdown('Analysis of failed membership payments over the last 180 days')

    if not df_failed_payments.empty and not df_memberships.empty:
        from data_pipeline.process_failed_payments import calculate_failure_rates_by_type

        # Calculate failure rates
        df_failure_rates = calculate_failure_rates_by_type(df_failed_payments, df_memberships)

        # Filter to show only categories with >0% failure rate
        df_failure_rates_display = df_failure_rates[df_failure_rates['failure_rate_pct'] > 0].copy()

        if not df_failure_rates_display.empty:
            # Create two columns for metrics
            col1, col2 = st.columns(2)

            with col1:
                st.metric(
                    "Total Failed Payments",
                    len(df_failed_payments),
                    help="Failed membership payment attempts in last 180 days"
                )

            with col2:
                insufficient_funds_count = len(df_failed_payments[df_failed_payments['decline_code'] == 'insufficient_funds'])
                insufficient_funds_pct = (insufficient_funds_count / len(df_failed_payments) * 100) if len(df_failed_payments) > 0 else 0
                st.metric(
                    "Due to Insufficient Funds",
                    f"{insufficient_funds_count} ({insufficient_funds_pct:.1f}%)",
                    help="Failures specifically due to insufficient funds"
                )

            # Create bar chart showing failure rates
            fig_failures = go.Figure()

            # Sort by insufficient funds rate descending
            df_failure_rates_display = df_failure_rates_display.sort_values('insufficient_funds_rate_pct', ascending=True)

            # Create hover text with detailed info
            df_failure_rates_display['hover_text'] = df_failure_rates_display.apply(
                lambda row: (
                    f"<b>{row['membership_type']}</b><br>" +
                    f"Active Members: {row['active_memberships']}<br>" +
                    f"Failed Payments: {row['total_failures']}<br>" +
                    f"Insufficient Funds: {row['insufficient_funds_failures']}<br>" +
                    f"Failure Rate: {row['failure_rate_pct']:.1f}%"
                ),
                axis=1
            )

            # Add insufficient funds rate
            fig_failures.add_trace(go.Bar(
                name='Insufficient Funds',
                y=df_failure_rates_display['membership_type'],
                x=df_failure_rates_display['insufficient_funds_rate_pct'],
                orientation='h',
                marker_color=COLORS['primary'],
                text=df_failure_rates_display.apply(
                    lambda row: f'{row["insufficient_funds_rate_pct"]:.1f}% ({row["insufficient_funds_failures"]} fails)',
                    axis=1
                ),
                textposition='auto',
                hovertext=df_failure_rates_display['hover_text'],
                hoverinfo='text',
            ))

            # Add other failures rate
            df_failure_rates_display['other_failure_rate'] = (
                df_failure_rates_display['failure_rate_pct'] - df_failure_rates_display['insufficient_funds_rate_pct']
            )
            df_failure_rates_display['other_failures_count'] = (
                df_failure_rates_display['total_failures'] - df_failure_rates_display['insufficient_funds_failures']
            )

            fig_failures.add_trace(go.Bar(
                name='Other Failures',
                y=df_failure_rates_display['membership_type'],
                x=df_failure_rates_display['other_failure_rate'],
                orientation='h',
                marker_color=COLORS['secondary'],
                text=df_failure_rates_display.apply(
                    lambda row: f'{row["other_failure_rate"]:.1f}% ({row["other_failures_count"]} fails)' if row['other_failure_rate'] > 0 else '',
                    axis=1
                ),
                textposition='auto',
                hovertext=df_failure_rates_display['hover_text'],
                hoverinfo='text',
            ))

            fig_failures.update_layout(
                barmode='stack',
                plot_bgcolor=COLORS['background'],
                paper_bgcolor=COLORS['background'],
                font_color=COLORS['text'],
                xaxis_title='Failure Rate (%)',
                yaxis_title='Membership Type',
                legend_title='Failure Reason',
                showlegend=True,
                height=400
            )

            st.plotly_chart(fig_failures, use_container_width=True)

            # Show detailed table
            with st.expander("📊 View Detailed Failure Rates"):
                df_failure_rates_table = df_failure_rates_display[[
                    'membership_type', 'active_memberships', 'unique_with_failures',
                    'insufficient_funds_failures', 'failure_rate_pct', 'insufficient_funds_rate_pct'
                ]].copy()

                df_failure_rates_table.columns = [
                    'Membership Type', 'Active Members', 'Members with Failures',
                    'Insufficient Funds Count', 'Total Failure Rate (%)', 'Insufficient Funds Rate (%)'
                ]

                st.dataframe(df_failure_rates_table, use_container_width=True, hide_index=True)

                # Key insights
                st.markdown("**Key Insights:**")
                highest_insuff = df_failure_rates_display.iloc[-1]  # Last row (highest after sorting)
                st.markdown(f"- **{highest_insuff['membership_type']}** has the highest insufficient funds rate at **{highest_insuff['insufficient_funds_rate_pct']:.1f}%**")

                total_unique_failures = df_failure_rates_display['unique_with_failures'].sum()
                st.markdown(f"- **{total_unique_failures}** unique memberships have experienced payment failures")

                if insufficient_funds_count > 0:
                    st.markdown(f"- **{insufficient_funds_pct:.1f}%** of all payment failures are due to insufficient funds")

        else:
            st.info("No payment failures in the last 180 days!")

    else:
        st.info("Payment failure data not available")

# ============================================================================
# TAB 2: MEMBERSHIP
# ============================================================================
with tab2:
    st.header('Membership Analysis')

    # Membership Timeline
    st.subheader('Active Memberships Over Time')

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        status_filter = st.multiselect(
            'Status',
            options=['ACT', 'END', 'FRZ'],
            default=['ACT', 'END'],
            format_func=lambda x: {'ACT': 'Active', 'END': 'Ended', 'FRZ': 'Frozen'}[x]
        )

    with col2:
        frequency_filter = st.multiselect(
            'Frequency',
            options=['bi_weekly', 'monthly', 'annual', 'prepaid_3mo', 'prepaid_6mo', 'prepaid_12mo'],
            default=['bi_weekly', 'monthly', 'annual', 'prepaid_3mo', 'prepaid_6mo', 'prepaid_12mo'],
            format_func=lambda x: x.replace('_', ' ').title()
        )

    with col3:
        size_filter = st.multiselect(
            'Size',
            options=['solo', 'duo', 'family', 'corporate'],
            default=['solo', 'duo', 'family', 'corporate'],
            format_func=lambda x: x.title()
        )

    # Category filters
    category_options = {
        'founder': 'Founder',
        'college': 'College',
        'corporate': 'Corporate',
        'mid_day': 'Mid-Day',
        'fitness_only': 'Fitness Only',
        'has_fitness_addon': 'Has Fitness Addon',
        'team_dues': 'Team Dues',
        '90_for_90': '90 for 90',
        'not_special': 'Not in Special Category'
    }

    category_filter = st.multiselect(
        'Special Categories',
        options=list(category_options.keys()),
        default=['founder', 'college', 'corporate', 'mid_day', 'fitness_only', 'has_fitness_addon', 'team_dues', '90_for_90', 'not_special'],
        format_func=lambda x: category_options[x]
    )

    # Filter memberships
    df_memberships_filtered = df_memberships[df_memberships['status'].isin(status_filter)].copy()
    df_memberships_filtered = df_memberships_filtered[df_memberships_filtered['frequency'].isin(frequency_filter)]
    df_memberships_filtered = df_memberships_filtered[df_memberships_filtered['size'].isin(size_filter)]

    # Apply category filters
    if 'include_bcf' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_bcf']]
    if 'founder' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_founder']]
    if 'college' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_college']]
    if 'corporate' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_corporate']]
    if 'mid_day' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_mid_day']]
    if 'fitness_only' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_fitness_only']]
    if 'has_fitness_addon' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['has_fitness_addon']]
    if 'team_dues' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_team_dues']]
    if '90_for_90' not in category_filter:
        df_memberships_filtered = df_memberships_filtered[~df_memberships_filtered['is_90_for_90']]
    if 'not_special' not in category_filter:
        # Exclude members NOT in any special category (i.e., only show special category members)
        special_mask = (
            df_memberships_filtered['is_bcf'] |
            df_memberships_filtered['is_founder'] |
            df_memberships_filtered['is_college'] |
            df_memberships_filtered['is_corporate'] |
            df_memberships_filtered['is_mid_day'] |
            df_memberships_filtered['is_fitness_only'] |
            df_memberships_filtered['has_fitness_addon'] |
            df_memberships_filtered['is_team_dues'] |
            df_memberships_filtered['is_90_for_90']
        )
        df_memberships_filtered = df_memberships_filtered[special_mask]

    # Process dates
    df_memberships_filtered['start_date'] = pd.to_datetime(df_memberships_filtered['start_date'], errors='coerce')
    df_memberships_filtered['end_date'] = pd.to_datetime(df_memberships_filtered['end_date'], errors='coerce')

    if not df_memberships_filtered.empty:
        min_date = df_memberships_filtered['start_date'].min()
        max_date = pd.Timestamp.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')

        daily_counts = []
        for date in date_range:
            active = df_memberships_filtered[
                (df_memberships_filtered['start_date'] <= date) &
                (df_memberships_filtered['end_date'] >= date)
            ]
            counts = active['frequency'].value_counts().to_dict()
            daily_counts.append({
                'date': date,
                **{freq: counts.get(freq, 0) for freq in frequency_filter}
            })

        daily_counts_df = pd.DataFrame(daily_counts)

        # Create stacked area chart
        fig_timeline = go.Figure()

        frequency_colors = {
            'bi_weekly': '#1f77b4',
            'monthly': '#ff7f0e',
            'annual': '#2ca02c',
            'prepaid_3mo': '#8B4229',
            'prepaid_6mo': '#BAA052',
            'prepaid_12mo': '#96A682',
        }

        for freq in frequency_filter:
            if freq in daily_counts_df.columns:
                fig_timeline.add_trace(go.Scatter(
                    x=daily_counts_df['date'],
                    y=daily_counts_df[freq],
                    mode='lines',
                    name=freq.replace('_', ' ').title(),
                    stackgroup='one',
                    line=dict(color=frequency_colors.get(freq, COLORS['primary']))
                ))

        # Add total line
        total = daily_counts_df[frequency_filter].sum(axis=1)
        fig_timeline.add_trace(go.Scatter(
            x=daily_counts_df['date'],
            y=total,
            mode='lines',
            name='Total',
            line=dict(color='#222222', width=2, dash='dash')
        ))

        fig_timeline.update_layout(
            title='Active Memberships Over Time by Payment Frequency',
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            height=600,
            xaxis_title='Date',
            yaxis_title='Number of Active Memberships',
            hovermode='x unified'
        )

        st.plotly_chart(fig_timeline, use_container_width=True)

    # Active Members Over Time
    st.subheader('Active Members Over Time')

    # Use same filters as above but work with df_members
    df_members_filtered = df_members[df_members['status'].isin(status_filter)].copy()
    df_members_filtered = df_members_filtered[df_members_filtered['frequency'].isin(frequency_filter)]
    df_members_filtered = df_members_filtered[df_members_filtered['size'].isin(size_filter)]

    # Apply category filters
    if 'include_bcf' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_bcf']]
    if 'founder' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_founder']]
    if 'college' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_college']]
    if 'corporate' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_corporate']]
    if 'mid_day' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_mid_day']]
    if 'fitness_only' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_fitness_only']]
    if 'has_fitness_addon' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['has_fitness_addon']]
    if 'team_dues' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_team_dues']]
    if '90_for_90' not in category_filter:
        df_members_filtered = df_members_filtered[~df_members_filtered['is_90_for_90']]
    if 'not_special' not in category_filter:
        # Exclude members NOT in any special category (i.e., only show special category members)
        special_mask = (
            df_members_filtered['is_bcf'] |
            df_members_filtered['is_founder'] |
            df_members_filtered['is_college'] |
            df_members_filtered['is_corporate'] |
            df_members_filtered['is_mid_day'] |
            df_members_filtered['is_fitness_only'] |
            df_members_filtered['has_fitness_addon'] |
            df_members_filtered['is_team_dues'] |
            df_members_filtered['is_90_for_90']
        )
        df_members_filtered = df_members_filtered[special_mask]

    # Process dates
    df_members_filtered['start_date'] = pd.to_datetime(df_members_filtered['start_date'], errors='coerce')
    df_members_filtered['end_date'] = pd.to_datetime(df_members_filtered['end_date'], errors='coerce')

    if not df_members_filtered.empty:
        min_date = df_members_filtered['start_date'].min()
        max_date = pd.Timestamp.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')

        daily_member_counts = []
        for date in date_range:
            active_members = df_members_filtered[
                (df_members_filtered['start_date'] <= date) &
                (df_members_filtered['end_date'] >= date)
            ]
            count = len(active_members)
            daily_member_counts.append({
                'date': date,
                'count': count
            })

        daily_members_df = pd.DataFrame(daily_member_counts)

        # Create line chart
        fig_members_timeline = px.line(
            daily_members_df,
            x='date',
            y='count',
            title='Active Individual Members Over Time',
            line_shape='linear'
        )
        fig_members_timeline.update_traces(line_color=COLORS['primary'], line_width=2)
        fig_members_timeline.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            height=500,
            xaxis_title='Date',
            yaxis_title='Number of Active Members',
            hovermode='x unified'
        )

        st.plotly_chart(fig_members_timeline, use_container_width=True)
    else:
        st.info('No members match the selected filters')

    # Memberships by Size
    st.subheader('Active Memberships by Group Size')

    if not df_memberships_filtered.empty:
        daily_size_counts = []
        for date in date_range:
            active = df_memberships_filtered[
                (df_memberships_filtered['start_date'] <= date) &
                (df_memberships_filtered['end_date'] >= date)
            ]
            counts = active['size'].value_counts().to_dict()
            daily_size_counts.append({
                'date': date,
                **{size: counts.get(size, 0) for size in size_filter}
            })

        daily_size_df = pd.DataFrame(daily_size_counts)

        # Create stacked area chart
        fig_size = go.Figure()

        size_colors = {
            'solo': COLORS['primary'],
            'duo': COLORS['secondary'],
            'family': COLORS['tertiary'],
            'corporate': COLORS['quaternary']
        }

        for size in size_filter:
            if size in daily_size_df.columns:
                fig_size.add_trace(go.Scatter(
                    x=daily_size_df['date'],
                    y=daily_size_df[size],
                    mode='lines',
                    name=size.title(),
                    stackgroup='one',
                    line=dict(color=size_colors.get(size, COLORS['primary']))
                ))

        # Add total line
        total_size = daily_size_df[size_filter].sum(axis=1)
        fig_size.add_trace(go.Scatter(
            x=daily_size_df['date'],
            y=total_size,
            mode='lines',
            name='Total',
            line=dict(color='#222222', width=2, dash='dash')
        ))

        fig_size.update_layout(
            title='Active Memberships Over Time by Group Size',
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            height=600,
            xaxis_title='Date',
            yaxis_title='Number of Active Memberships',
            hovermode='x unified'
        )

        st.plotly_chart(fig_size, use_container_width=True)
    else:
        st.info('No memberships match the selected filters')

    # Memberships by Special Category
    st.subheader('Active Memberships by Special Category')

    if not df_memberships_filtered.empty:
        # Define categories to track
        special_categories = {
            'Founder': 'is_founder',
            'College': 'is_college',
            'Corporate': 'is_corporate',
            'Mid-Day': 'is_mid_day',
            'Fitness Only': 'is_fitness_only',
            'Has Fitness Addon': 'has_fitness_addon',
            'Team Dues': 'is_team_dues',
            '90 for 90': 'is_90_for_90',
            'Regular': 'regular'  # Not in any special category
        }

        daily_category_counts = []
        for date in date_range:
            active = df_memberships_filtered[
                (df_memberships_filtered['start_date'] <= date) &
                (df_memberships_filtered['end_date'] >= date)
            ]

            counts = {}
            for category_name, column_name in special_categories.items():
                if column_name == 'regular':
                    # Count memberships NOT in any special category
                    regular_mask = ~(
                        active['is_founder'] |
                        active['is_college'] |
                        active['is_corporate'] |
                        active['is_mid_day'] |
                        active['is_fitness_only'] |
                        active['has_fitness_addon'] |
                        active['is_team_dues'] |
                        active['is_90_for_90']
                    )
                    counts[category_name] = regular_mask.sum()
                else:
                    counts[category_name] = active[column_name].sum()

            daily_category_counts.append({
                'date': date,
                **counts
            })

        daily_category_df = pd.DataFrame(daily_category_counts)

        # Create stacked area chart
        fig_category = go.Figure()

        category_colors = {
            'Founder': '#8B4229',
            'College': '#BAA052',
            'Corporate': '#96A682',
            'Mid-Day': '#1A2E31',
            'Fitness Only': '#C85A3E',
            'Has Fitness Addon': '#D4AF6A',
            'Team Dues': '#B8C9A8',
            '90 for 90': '#4A4A4A',
            'Regular': '#E0E0E0'
        }

        for category in special_categories.keys():
            if category in daily_category_df.columns:
                fig_category.add_trace(go.Scatter(
                    x=daily_category_df['date'],
                    y=daily_category_df[category],
                    mode='lines',
                    name=category,
                    stackgroup='one',
                    line=dict(color=category_colors.get(category, COLORS['primary']))
                ))

        # Add total line
        total_category = daily_category_df[list(special_categories.keys())].sum(axis=1)
        fig_category.add_trace(go.Scatter(
            x=daily_category_df['date'],
            y=total_category,
            mode='lines',
            name='Total',
            line=dict(color='#222222', width=2, dash='dash')
        ))

        fig_category.update_layout(
            title='Active Memberships Over Time by Special Category',
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            height=600,
            xaxis_title='Date',
            yaxis_title='Number of Active Memberships',
            hovermode='x unified'
        )

        st.plotly_chart(fig_category, use_container_width=True)
    else:
        st.info('No memberships match the selected filters')

    # 90 for 90 Conversion
    st.subheader('90 for 90 Conversion Summary')

    ninety_members = df_members[df_members['is_90_for_90'] == True].copy()

    if not ninety_members.empty:
        ninety_members['person_id'] = ninety_members['member_first_name'] + ' ' + ninety_members['member_last_name']
        df_members_copy = df_members.copy()
        df_members_copy['person_id'] = df_members_copy['member_first_name'] + ' ' + df_members_copy['member_last_name']

        unique_person_ids = ninety_members['person_id'].unique()

        converted_count = 0
        not_converted_count = 0

        for person_id in unique_person_ids:
            person_ninety = ninety_members[ninety_members['person_id'] == person_id]
            ninety_start_date = pd.to_datetime(person_ninety['start_date'].min(), errors='coerce')

            if pd.notna(ninety_start_date):
                regular_memberships = df_members_copy[
                    (df_members_copy['person_id'] == person_id) &
                    (df_members_copy['is_90_for_90'] == False) &
                    (pd.to_datetime(df_members_copy['start_date'], errors='coerce') > ninety_start_date)
                ]
                if len(regular_memberships) > 0:
                    converted_count += 1
                else:
                    not_converted_count += 1

        total = converted_count + not_converted_count
        conversion_rate = (converted_count / total * 100) if total > 0 else 0

        summary_data = pd.DataFrame({
            'Status': ['Converted', 'Not Converted'],
            'Count': [converted_count, not_converted_count]
        })

        fig_90 = px.bar(
            summary_data,
            x='Status',
            y='Count',
            title=f'90 for 90 Conversion Summary (Conversion Rate: {conversion_rate:.1f}%)',
            color='Status',
            color_discrete_map={
                'Converted': COLORS['secondary'],
                'Not Converted': COLORS['primary']
            }
        )
        fig_90.update_traces(texttemplate='%{y}', textposition='outside')
        fig_90.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            height=400,
            showlegend=False,
            yaxis_title='Number of Members'
        )
        st.plotly_chart(fig_90, use_container_width=True)
    else:
        st.info('No 90 for 90 memberships found')

    # New Members & Attrition
    st.subheader('New Memberships & Attrition Over Time')

    # Calculate new memberships and attrition by month
    df_memberships_dates = df_memberships.copy()
    df_memberships_dates['start_date'] = pd.to_datetime(df_memberships_dates['start_date'], errors='coerce')
    df_memberships_dates['end_date'] = pd.to_datetime(df_memberships_dates['end_date'], errors='coerce')

    # Remove rows with invalid dates
    df_memberships_dates = df_memberships_dates[df_memberships_dates['start_date'].notna()]

    # Get date range
    min_date = df_memberships_dates['start_date'].min()
    max_date = pd.Timestamp.now()

    # Create monthly periods
    month_range = pd.period_range(start=min_date.to_period('M'), end=max_date.to_period('M'), freq='M')

    monthly_data = []
    for month in month_range:
        month_start = month.to_timestamp()
        month_end = (month + 1).to_timestamp()

        # Count new memberships that started in this month
        new_members = len(df_memberships_dates[
            (df_memberships_dates['start_date'] >= month_start) &
            (df_memberships_dates['start_date'] < month_end)
        ])

        # Count memberships that ended in this month (attrition)
        # ONLY count memberships with status='END' to avoid counting active memberships' billing dates
        attrited = len(df_memberships_dates[
            (df_memberships_dates['status'] == 'END') &
            (df_memberships_dates['end_date'] >= month_start) &
            (df_memberships_dates['end_date'] < month_end)
        ])

        # Net change
        net_change = new_members - attrited

        monthly_data.append({
            'month': month.to_timestamp(),
            'New Memberships': new_members,
            'Attrition': attrited,
            'Net Change': net_change
        })

    df_monthly = pd.DataFrame(monthly_data)

    if not df_monthly.empty:
        # Create figure with secondary y-axis
        fig_attrition = go.Figure()

        # Add new memberships bars
        fig_attrition.add_trace(go.Bar(
            x=df_monthly['month'],
            y=df_monthly['New Memberships'],
            name='New Memberships',
            marker_color=COLORS['secondary'],
            text=df_monthly['New Memberships'],
            textposition='outside',
            textfont=dict(size=10)
        ))

        # Add attrition bars (negative values for visual effect)
        fig_attrition.add_trace(go.Bar(
            x=df_monthly['month'],
            y=-df_monthly['Attrition'],  # Negative to show below axis
            name='Attrition',
            marker_color=COLORS['primary'],
            text=df_monthly['Attrition'],
            textposition='outside',
            textfont=dict(size=10)
        ))

        # Add net change line
        fig_attrition.add_trace(go.Scatter(
            x=df_monthly['month'],
            y=df_monthly['Net Change'],
            name='Net Change',
            mode='lines+markers',
            line=dict(color=COLORS['quaternary'], width=3),
            marker=dict(size=8),
            yaxis='y2'
        ))

        fig_attrition.update_layout(
            title='Monthly New Memberships & Attrition',
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            height=500,
            xaxis_title='Month',
            yaxis_title='Count',
            yaxis2=dict(
                title='Net Change',
                overlaying='y',
                side='right',
                showgrid=False
            ),
            hovermode='x unified',
            barmode='relative',
            legend=dict(
                orientation='h',
                yanchor='bottom',
                y=1.02,
                xanchor='right',
                x=1
            )
        )

        # Add horizontal line at y=0
        fig_attrition.add_hline(y=0, line_dash='dash', line_color='gray', opacity=0.5)

        st.plotly_chart(fig_attrition, use_container_width=True)

        # Summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric('Total New Memberships', df_monthly['New Memberships'].sum())
        with col2:
            st.metric('Total Attrition', df_monthly['Attrition'].sum())
        with col3:
            net_total = df_monthly['Net Change'].sum()
            st.metric('Net Growth', net_total, delta=None)
    else:
        st.info('No membership data available for attrition analysis')

    # At-Risk Members Table
    st.subheader('At-Risk Members')

    if not df_at_risk.empty:
        risk_category_filter = st.multiselect(
            'Filter by Risk Category',
            options=df_at_risk['risk_category'].unique(),
            default=df_at_risk['risk_category'].unique()
        )

        df_at_risk_filtered = df_at_risk_display[
            df_at_risk['risk_category'].isin(risk_category_filter)
        ]

        st.dataframe(
            df_at_risk_filtered,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Capitan Link': st.column_config.LinkColumn('Capitan Link')
            }
        )

        st.caption(f'Total at-risk members: {len(df_at_risk_filtered)}')

    # Recently Attrited Members
    st.subheader('Recently Attrited Members (Last 60 Days)')

    # Find members whose membership ended in last 60 days and no longer have active membership
    from datetime import datetime, timedelta
    sixty_days_ago = datetime.now() - timedelta(days=60)

    # Get members whose membership ended recently
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')
    recently_ended = df_memberships[
        (df_memberships['end_date'] >= sixty_days_ago) &
        (df_memberships['end_date'] <= datetime.now())
    ].copy()

    # Check if they have any active memberships
    active_member_ids = df_memberships[
        df_memberships['status'] == 'ACT'
    ]['owner_id'].unique()

    # Filter to only those without active memberships
    attrited = recently_ended[
        ~recently_ended['owner_id'].isin(active_member_ids)
    ].copy()

    if not attrited.empty:
        # Get unique members (they might have multiple ended memberships)
        attrited_unique = attrited.sort_values('end_date', ascending=False).drop_duplicates('owner_id')

        # Join with members table to get names
        # Match owner_id from memberships to customer_id in members
        attrited_with_names = attrited_unique.merge(
            df_members[['customer_id', 'member_first_name', 'member_last_name']],
            left_on='owner_id',
            right_on='customer_id',
            how='left'
        )

        # Prepare display data
        attrited_display = attrited_with_names[[
            'owner_id', 'member_first_name', 'member_last_name', 'membership_owner_age', 'name', 'end_date'
        ]].copy()
        attrited_display.columns = ['Customer ID', 'First Name', 'Last Name', 'Age', 'Membership Type', 'End Date']

        # Add Capitan link
        attrited_display['Capitan Link'] = attrited_display['Customer ID'].apply(
            lambda x: f"https://app.hellocapitan.com/customers/{x}/check-ins" if pd.notna(x) else ''
        )

        # Format end date
        attrited_display['End Date'] = pd.to_datetime(attrited_display['End Date']).dt.strftime('%Y-%m-%d')

        st.dataframe(
            attrited_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                'Capitan Link': st.column_config.LinkColumn('Capitan Link')
            }
        )

        st.caption(f'Total recently attrited members: {len(attrited_unique)}')
    else:
        st.info('No recently attrited members found')

# ============================================================================
# TAB 3: DAY PASSES & CHECK-INS
# ============================================================================
with tab3:
    st.header('Day Passes & Check-ins')

    # Timeframe selector for day pass charts
    timeframe_daypass = st.selectbox(
        'Select Timeframe',
        options=['D', 'W', 'M', 'Y'],
        format_func=lambda x: {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly', 'Y': 'Yearly'}[x],
        index=2,  # Default to Monthly
        key='daypass_timeframe'
    )

    # Day Pass Count
    st.subheader('Total Day Passes Purchased')

    df_day_pass = df_transactions[df_transactions['revenue_category'] == 'Day Pass'].copy()
    df_day_pass['Date'] = pd.to_datetime(df_day_pass['Date'], errors='coerce')
    df_day_pass = df_day_pass[df_day_pass['Date'].notna()]
    df_day_pass['date'] = df_day_pass['Date'].dt.to_period(timeframe_daypass).dt.start_time

    day_pass_sum = (
        df_day_pass.groupby('date')['Day Pass Count']
        .sum()
        .reset_index(name='total_day_passes')
    )

    # Calculate total for caption
    total_day_passes = day_pass_sum['total_day_passes'].sum()

    fig_day_pass_count = px.bar(
        day_pass_sum,
        x='date',
        y='total_day_passes',
        title='Total Day Passes Purchased',
        text=day_pass_sum['total_day_passes']
    )
    fig_day_pass_count.update_traces(
        marker_color=COLORS['quaternary'],
        textposition='outside',
        textfont=dict(size=11)
    )
    fig_day_pass_count.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Number of Day Passes',
        xaxis_title='Date'
    )
    st.plotly_chart(fig_day_pass_count, use_container_width=True)
    st.caption(f'Total day passes: {int(total_day_passes):,}')

    # Day Pass Revenue
    st.subheader('Day Pass Revenue')

    day_pass_revenue = (
        df_day_pass.groupby('date')['Total Amount']
        .sum()
        .reset_index(name='revenue')
    )

    fig_day_pass_revenue = px.bar(
        day_pass_revenue,
        x='date',
        y='revenue',
        title='Day Pass Revenue Over Time',
        text=day_pass_revenue['revenue'].apply(lambda x: f'${x/1000:.1f}K')
    )
    fig_day_pass_revenue.update_traces(
        marker_color=COLORS['tertiary'],
        textposition='outside',
        textfont=dict(size=11)
    )
    fig_day_pass_revenue.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Revenue ($)',
        xaxis_title='Date'
    )
    st.plotly_chart(fig_day_pass_revenue, use_container_width=True)

    # Check-ins by Member vs Non-Member
    st.subheader('Check-ins: Members vs Non-Members')

    if not df_checkins.empty:
        # Prepare check-in data
        df_checkins_chart = df_checkins.copy()
        df_checkins_chart['checkin_datetime'] = pd.to_datetime(df_checkins_chart['checkin_datetime'], errors='coerce', utc=True)
        df_checkins_chart = df_checkins_chart[df_checkins_chart['checkin_datetime'].notna()].copy()
        df_checkins_chart['checkin_datetime'] = df_checkins_chart['checkin_datetime'].dt.tz_localize(None)
        df_checkins_chart['date'] = df_checkins_chart['checkin_datetime'].dt.to_period(timeframe_daypass).dt.start_time

        # Determine if check-in is from member or non-member
        # Check if customer has a membership (is in memberships df with active status)
        active_member_customer_ids = set()
        if 'owner_id' in df_memberships.columns:
            active_member_customer_ids = set(df_memberships[df_memberships['status'] == 'ACT']['owner_id'].dropna())

        # If we have customer_id in members df, also use that
        if 'customer_id' in df_members.columns:
            member_customer_ids = set(df_members['customer_id'].dropna())
            active_member_customer_ids = active_member_customer_ids.union(member_customer_ids)
        elif 'member_id' in df_members.columns:
            member_customer_ids = set(df_members['member_id'].dropna())
            active_member_customer_ids = active_member_customer_ids.union(member_customer_ids)

        df_checkins_chart['type'] = df_checkins_chart['customer_id'].apply(
            lambda x: 'Member' if x in active_member_customer_ids else 'Non-Member'
        )

        checkins_by_type = df_checkins_chart.groupby(['date', 'type']).size().reset_index(name='count')

        fig_checkins_type = px.bar(
            checkins_by_type,
            x='date',
            y='count',
            color='type',
            title='Check-ins by Member vs Non-Member',
            barmode='stack',
            color_discrete_map={'Member': COLORS['primary'], 'Non-Member': COLORS['quaternary']}
        )
        fig_checkins_type.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Number of Check-ins',
            xaxis_title='Date',
            legend_title='Type'
        )
        st.plotly_chart(fig_checkins_type, use_container_width=True)
    else:
        st.info('No check-in data available')

    # Check-ins by Day of Week and Month
    st.subheader('Check-ins by Day of Week and Month')

    if not df_checkins.empty:
        # Group by month and day of week
        df_checkins_dow = df_checkins.copy()
        df_checkins_dow['checkin_datetime'] = pd.to_datetime(df_checkins_dow['checkin_datetime'], errors='coerce', utc=True)
        df_checkins_dow = df_checkins_dow[df_checkins_dow['checkin_datetime'].notna()].copy()
        df_checkins_dow['checkin_datetime'] = df_checkins_dow['checkin_datetime'].dt.tz_localize(None)
        df_checkins_dow['month'] = df_checkins_dow['checkin_datetime'].dt.to_period('M').astype(str)
        df_checkins_dow['day_of_week'] = df_checkins_dow['checkin_datetime'].dt.day_name()

        # Order days of week
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

        # Group by day of week and month
        checkins_dow_summary = df_checkins_dow.groupby(['day_of_week', 'month']).size().reset_index(name='count')

        # Chart 1: Grouped by Day of Week first (x-axis = Day, color = Month)
        fig_checkins_by_day = px.bar(
            checkins_dow_summary,
            x='day_of_week',
            y='count',
            color='month',
            title='Check-ins by Day of Week (Grouped by Day, Colored by Month)',
            barmode='group',
            category_orders={'day_of_week': day_order}
        )
        fig_checkins_by_day.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Number of Check-ins',
            xaxis_title='Day of Week',
            legend_title='Month'
        )
        st.plotly_chart(fig_checkins_by_day, use_container_width=True)

        # Chart 2: Grouped by Month first (x-axis = Month, color = Day of Week)
        st.subheader('Check-ins by Month and Day of Week')

        # Regroup for month-first view
        checkins_month_summary = df_checkins_dow.groupby(['month', 'day_of_week']).size().reset_index(name='count')

        fig_checkins_by_month = px.bar(
            checkins_month_summary,
            x='month',
            y='count',
            color='day_of_week',
            title='Check-ins by Month (Grouped by Month, Colored by Day of Week)',
            barmode='group',
            category_orders={'day_of_week': day_order}
        )
        fig_checkins_by_month.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Number of Check-ins',
            xaxis_title='Month',
            legend_title='Day of Week'
        )
        st.plotly_chart(fig_checkins_by_month, use_container_width=True)
    else:
        st.info('No check-in data available')

# ============================================================================
# TAB 4: RENTALS
# ============================================================================
with tab4:
    st.header('Rentals')

    # Birthday Parties Booked
    st.subheader('Birthday Parties Booked')

    df_birthday = df_transactions[df_transactions['sub_category'] == 'birthday'].copy()
    # Only count Calendly payments (first payment)
    df_birthday = df_birthday[df_birthday['Description'].str.contains('Calendly', case=False, na=False)]
    df_birthday['Date'] = pd.to_datetime(df_birthday['Date'], errors='coerce')
    df_birthday = df_birthday[df_birthday['Date'].notna()]
    df_birthday['date'] = df_birthday['Date'].dt.to_period(timeframe).dt.start_time

    if not df_birthday.empty:
        # Count number of parties booked
        birthday_count = (
            df_birthday.groupby('date')
            .size()
            .reset_index(name='num_parties')
        )

        fig_birthday_count = px.bar(
            birthday_count,
            x='date',
            y='num_parties',
            title='Number of Birthday Parties Booked',
            text='num_parties'
        )
        fig_birthday_count.update_traces(
            marker_color=COLORS['secondary'],
            textposition='outside',
            textfont=dict(size=11)
        )
        fig_birthday_count.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Number of Parties',
            xaxis_title='Date'
        )
        st.plotly_chart(fig_birthday_count, use_container_width=True)
        st.caption(f'Total parties booked: {birthday_count["num_parties"].sum()}')
    else:
        st.info('No birthday party data available')

    # Birthday Party Revenue
    st.subheader('Birthday Party Revenue')

    birthday_revenue = (
        df_birthday.groupby('date')['Total Amount']
        .sum()
        .reset_index()
    )

    fig_birthday_revenue = px.line(
        birthday_revenue,
        x='date',
        y='Total Amount',
        title='Birthday Party Revenue'
    )
    fig_birthday_revenue.update_traces(line_color=COLORS['quaternary'])
    fig_birthday_revenue.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Revenue ($)',
        xaxis_title='Date'
    )
    st.plotly_chart(fig_birthday_revenue, use_container_width=True)

    # All Rental Revenue (Event Booking category)
    st.subheader('All Rental Revenue (Event Bookings)')

    df_rentals = df_transactions[df_transactions['revenue_category'] == 'Event Booking'].copy()
    df_rentals['Date'] = pd.to_datetime(df_rentals['Date'], errors='coerce')
    df_rentals = df_rentals[df_rentals['Date'].notna()]
    df_rentals['date'] = df_rentals['Date'].dt.to_period(timeframe).dt.start_time

    # Group by sub_category
    rental_by_type = (
        df_rentals.groupby(['date', 'sub_category'])['Total Amount']
        .sum()
        .reset_index()
    )

    fig_all_rentals = px.bar(
        rental_by_type,
        x='date',
        y='Total Amount',
        color='sub_category',
        title='All Rental Revenue by Type (Birthday Parties, Events, etc.)',
        barmode='stack'
    )
    fig_all_rentals.update_layout(
        plot_bgcolor=COLORS['background'],
        paper_bgcolor=COLORS['background'],
        font_color=COLORS['text'],
        yaxis_title='Revenue ($)',
        xaxis_title='Date',
        legend_title='Rental Type'
    )
    st.plotly_chart(fig_all_rentals, use_container_width=True)

# ============================================================================
# TAB 5: PROGRAMMING
# ============================================================================
with tab5:
    st.header('Programming')

    # Youth Team Members
    st.subheader('Youth Team Members Over Time')

    # Extract team type from membership name in df_memberships
    youth_memberships = []
    for _, membership in df_memberships.iterrows():
        name = str(membership.get('name', '')).lower()
        status = membership.get('status')

        # Only include active memberships
        if status != 'ACT':
            continue

        # Determine team type
        team_type = None
        if 'recreation' in name or 'rec team' in name:
            team_type = 'Recreation'
        elif 'development' in name or 'dev team' in name:
            team_type = 'Development'
        elif 'competitive' in name or 'comp team' in name:
            team_type = 'Competitive'

        if team_type:
            start_date = pd.to_datetime(membership.get('start_date'), errors='coerce')
            end_date = pd.to_datetime(membership.get('end_date'), errors='coerce')

            if pd.notna(start_date) and pd.notna(end_date):
                youth_memberships.append({
                    'team_type': team_type,
                    'start_date': start_date,
                    'end_date': end_date
                })

    if youth_memberships:
        df_youth = pd.DataFrame(youth_memberships)

        min_date = df_youth['start_date'].min()
        max_date = pd.Timestamp.now()
        date_range = pd.date_range(start=min_date, end=max_date, freq='M')

        youth_counts = []
        for date in date_range:
            active_youth = df_youth[
                (df_youth['start_date'] <= date) &
                (df_youth['end_date'] >= date)
            ]
            # Count by team type
            counts_by_type = active_youth['team_type'].value_counts().to_dict()
            youth_counts.append({
                'date': date,
                'Recreation': counts_by_type.get('Recreation', 0),
                'Development': counts_by_type.get('Development', 0),
                'Competitive': counts_by_type.get('Competitive', 0)
            })

        youth_df = pd.DataFrame(youth_counts)

        # Reshape for stacked area chart
        youth_melted = youth_df.melt(id_vars='date',
                                      value_vars=['Recreation', 'Development', 'Competitive'],
                                      var_name='Team Type',
                                      value_name='Members')

        fig_youth = px.area(
            youth_melted,
            x='date',
            y='Members',
            color='Team Type',
            title='Active Youth Team Members by Team Type',
            color_discrete_map={
                'Recreation': COLORS['primary'],
                'Development': COLORS['secondary'],
                'Competitive': COLORS['tertiary']
            }
        )
        fig_youth.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Number of Team Members',
            xaxis_title='Date',
            hovermode='x unified'
        )
        st.plotly_chart(fig_youth, use_container_width=True)
    else:
        st.info('No youth team data available')

    # Timeframe selector for Programming tab
    timeframe_prog = st.selectbox(
        'Select Timeframe',
        options=['D', 'W', 'M'],
        format_func=lambda x: {'D': 'Daily', 'W': 'Weekly', 'M': 'Monthly'}[x],
        index=2,  # Default to Monthly
        key='programming_timeframe'
    )

    # Youth Team Revenue
    st.subheader('Youth Team Revenue')

    df_team_revenue = df_transactions[df_transactions['revenue_category'] == 'Team'].copy()
    df_team_revenue['Date'] = pd.to_datetime(df_team_revenue['Date'], errors='coerce')
    df_team_revenue = df_team_revenue[df_team_revenue['Date'].notna()]
    df_team_revenue['date'] = df_team_revenue['Date'].dt.to_period(timeframe_prog).dt.start_time

    if not df_team_revenue.empty:
        team_revenue = (
            df_team_revenue.groupby('date')['Total Amount']
            .sum()
            .reset_index()
        )

        fig_team_revenue = px.bar(
            team_revenue,
            x='date',
            y='Total Amount',
            title='Youth Team Revenue',
            text=team_revenue['Total Amount'].apply(lambda x: f'${x/1000:.1f}K')
        )
        fig_team_revenue.update_traces(
            marker_color=COLORS['primary'],
            textposition='outside',
            textfont=dict(size=11)
        )
        fig_team_revenue.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Revenue ($)',
            xaxis_title='Date'
        )
        st.plotly_chart(fig_team_revenue, use_container_width=True)
    else:
        st.info('No youth team revenue data available')

    # Fitness Revenue
    st.subheader('Fitness Revenue')

    if 'fitness_amount' in df_transactions.columns:
        df_fitness = df_transactions[df_transactions['fitness_amount'] > 0].copy()
        df_fitness['Date'] = pd.to_datetime(df_fitness['Date'], errors='coerce')
        df_fitness = df_fitness[df_fitness['Date'].notna()]
        df_fitness['date'] = df_fitness['Date'].dt.to_period(timeframe_prog).dt.start_time

        fitness_revenue = (
            df_fitness.groupby('date')['fitness_amount']
            .sum()
            .reset_index()
        )

        fig_fitness = px.bar(
            fitness_revenue,
            x='date',
            y='fitness_amount',
            title='Fitness Revenue (Classes, Fitness-Only Memberships, Add-ons)',
            text=fitness_revenue['fitness_amount'].apply(lambda x: f'${x/1000:.1f}K')
        )
        fig_fitness.update_traces(
            marker_color=COLORS['secondary'],
            textposition='outside',
            textfont=dict(size=11)
        )
        fig_fitness.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Fitness Revenue ($)',
            xaxis_title='Date'
        )
        st.plotly_chart(fig_fitness, use_container_width=True)
    else:
        st.info('Fitness revenue data is being calculated. Please wait for the next data pipeline run.')

    # Fitness Class Attendance
    st.subheader('Fitness Class Attendance')

    fitness_event_keywords = ['HYROX', 'transformation', 'strength', 'fitness', 'yoga', 'workout']

    df_events_filtered = df_events.copy()
    df_events_filtered['event_type_name_lower'] = df_events_filtered['event_type_name'].str.lower()

    fitness_mask = df_events_filtered['event_type_name_lower'].apply(
        lambda x: any(keyword.lower() in str(x) for keyword in fitness_event_keywords) if pd.notna(x) else False
    )
    df_events_filtered = df_events_filtered[fitness_mask]

    if not df_events_filtered.empty:
        # Ensure we have a copy to avoid SettingWithCopyWarning
        df_events_filtered = df_events_filtered.copy()
        df_events_filtered['start_datetime'] = pd.to_datetime(df_events_filtered['start_datetime'], errors='coerce', utc=True)
        df_events_filtered = df_events_filtered[df_events_filtered['start_datetime'].notna()].copy()

        if not df_events_filtered.empty and len(df_events_filtered) > 0:
            # Convert timezone-aware datetime to timezone-naive for period conversion
            df_events_filtered['start_datetime'] = df_events_filtered['start_datetime'].dt.tz_localize(None)
            df_events_filtered['date'] = df_events_filtered['start_datetime'].dt.to_period(timeframe_prog).dt.start_time

            attendance = (
                df_events_filtered.groupby('date')['num_reservations']
                .sum()
                .reset_index()
            )

            fig_attendance = px.bar(
                attendance,
                x='date',
                y='num_reservations',
                title='Fitness Class Attendance (Total Reservations)',
                text=attendance['num_reservations'].apply(lambda x: f'{x/1000:.1f}K' if x >= 1000 else str(int(x)))
            )
            fig_attendance.update_traces(
                marker_color=COLORS['tertiary'],
                textposition='outside',
                textfont=dict(size=11)
            )
            fig_attendance.update_layout(
                plot_bgcolor=COLORS['background'],
                paper_bgcolor=COLORS['background'],
                font_color=COLORS['text'],
                yaxis_title='Total Attendance',
                xaxis_title='Date'
            )
            st.plotly_chart(fig_attendance, use_container_width=True)

            # Fitness Check-ins by Class Type
            st.subheader('Fitness Check-ins by Class Type')

            # Group by class type (event_type_name or truncated name)
            df_events_by_type = df_events_filtered.copy()

            # Truncate long names for better display
            def truncate_name(name, max_length=30):
                if pd.isna(name):
                    return 'Unknown'
                name_str = str(name)
                if len(name_str) > max_length:
                    return name_str[:max_length-3] + '...'
                return name_str

            df_events_by_type['class_type'] = df_events_by_type['event_type_name'].apply(truncate_name)

            class_type_attendance = (
                df_events_by_type.groupby(['date', 'class_type'])['num_reservations']
                .sum()
                .reset_index()
            )

            fig_class_types = px.bar(
                class_type_attendance,
                x='date',
                y='num_reservations',
                color='class_type',
                title='Fitness Check-ins by Class Type',
                barmode='stack'
            )
            fig_class_types.update_layout(
                plot_bgcolor=COLORS['background'],
                paper_bgcolor=COLORS['background'],
                font_color=COLORS['text'],
                yaxis_title='Check-ins',
                xaxis_title='Date',
                legend_title='Class Type'
            )
            st.plotly_chart(fig_class_types, use_container_width=True)
        else:
            st.info('No fitness class data available with valid dates')
    else:
        st.info('No fitness class data available')

    # Camp Signups
    st.subheader('Camp Signups')

    # Filter for camp events
    df_camps = df_events[
        df_events['event_type_name'].str.contains('camp', case=False, na=False)
    ].copy()

    if not df_camps.empty:
        # Parse dates
        df_camps['start_datetime'] = pd.to_datetime(df_camps['start_datetime'], errors='coerce', utc=True)
        df_camps = df_camps[df_camps['start_datetime'].notna()].copy()

        # Convert to timezone-naive for comparisons
        df_camps['start_datetime'] = df_camps['start_datetime'].dt.tz_localize(None)

        # Separate upcoming and past camps
        now = pd.Timestamp.now()
        df_upcoming = df_camps[df_camps['start_datetime'] >= now].copy()
        df_past = df_camps[df_camps['start_datetime'] < now].copy()

        # Display upcoming camps
        st.markdown('#### 🔜 Upcoming Camps')
        if not df_upcoming.empty:
            df_upcoming_display = df_upcoming.sort_values('start_datetime')[[
                'start_datetime', 'event_type_name', 'num_reservations', 'capacity'
            ]].copy()

            # Calculate fill rate
            df_upcoming_display['fill_rate'] = (
                df_upcoming_display['num_reservations'] / df_upcoming_display['capacity'] * 100
            ).round(1)

            # Format for display
            df_upcoming_display['start_datetime'] = df_upcoming_display['start_datetime'].dt.strftime('%Y-%m-%d')
            df_upcoming_display['Signups'] = df_upcoming_display['num_reservations'].astype(int).astype(str) + ' / ' + df_upcoming_display['capacity'].astype(int).astype(str)
            df_upcoming_display['Fill Rate'] = df_upcoming_display['fill_rate'].astype(str) + '%'

            # Select and rename columns for display
            df_upcoming_display = df_upcoming_display[[
                'start_datetime', 'event_type_name', 'Signups', 'Fill Rate'
            ]].copy()
            df_upcoming_display.columns = ['Date', 'Camp Name', 'Signups', 'Fill Rate']

            st.dataframe(df_upcoming_display, use_container_width=True, hide_index=True)

            # Summary stats for upcoming
            total_upcoming_signups = df_upcoming['num_reservations'].sum()
            total_upcoming_capacity = df_upcoming['capacity'].sum()
            avg_fill_rate = (total_upcoming_signups / total_upcoming_capacity * 100) if total_upcoming_capacity > 0 else 0

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric('Total Signups', f'{int(total_upcoming_signups)}')
            with col2:
                st.metric('Total Capacity', f'{int(total_upcoming_capacity)}')
            with col3:
                st.metric('Avg Fill Rate', f'{avg_fill_rate:.1f}%')
        else:
            st.info('No upcoming camps scheduled')

        st.markdown('---')

        # Display past camps
        st.markdown('#### 📅 Past Camps')
        if not df_past.empty:
            df_past_display = df_past.sort_values('start_datetime', ascending=False).head(15)[[
                'start_datetime', 'event_type_name', 'num_reservations', 'capacity'
            ]].copy()

            # Calculate fill rate
            df_past_display['fill_rate'] = (
                df_past_display['num_reservations'] / df_past_display['capacity'] * 100
            ).round(1)

            # Format for display
            df_past_display['start_datetime'] = df_past_display['start_datetime'].dt.strftime('%Y-%m-%d')
            df_past_display['Attendance'] = df_past_display['num_reservations'].astype(int).astype(str) + ' / ' + df_past_display['capacity'].astype(int).astype(str)
            df_past_display['Fill Rate'] = df_past_display['fill_rate'].astype(str) + '%'

            # Select and rename columns for display
            df_past_display = df_past_display[[
                'start_datetime', 'event_type_name', 'Attendance', 'Fill Rate'
            ]].copy()
            df_past_display.columns = ['Date', 'Camp Name', 'Attendance', 'Fill Rate']

            st.dataframe(df_past_display, use_container_width=True, hide_index=True)

            # Chart: Past camp attendance over time
            df_past_chart = df_past.copy()
            df_past_chart['date'] = df_past_chart['start_datetime'].dt.to_period('M').dt.start_time

            past_attendance = df_past_chart.groupby('date').agg({
                'num_reservations': 'sum',
                'capacity': 'sum'
            }).reset_index()

            fig_past_camps = go.Figure()
            fig_past_camps.add_trace(go.Bar(
                x=past_attendance['date'],
                y=past_attendance['num_reservations'],
                name='Attendance',
                marker_color=COLORS['primary']
            ))
            fig_past_camps.add_trace(go.Scatter(
                x=past_attendance['date'],
                y=past_attendance['capacity'],
                name='Capacity',
                mode='lines+markers',
                line=dict(color=COLORS['quaternary'], width=2),
                marker=dict(size=8)
            ))

            fig_past_camps.update_layout(
                title='Past Camp Attendance vs Capacity',
                plot_bgcolor=COLORS['background'],
                paper_bgcolor=COLORS['background'],
                font_color=COLORS['text'],
                yaxis_title='Count',
                xaxis_title='Month',
                hovermode='x unified',
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                )
            )
            st.plotly_chart(fig_past_camps, use_container_width=True)
        else:
            st.info('No past camp data available')
    else:
        st.info('No camp events found')

# ============================================================================
# TAB 6: MARKETING
# ============================================================================
with tab6:
    st.header('Marketing Performance')

    # Timeframe selector
    marketing_timeframe = st.radio(
        'Select Timeframe',
        options=['Day', 'Week', 'Month'],
        index=2,
        key='marketing_timeframe',
        horizontal=True
    )

    timeframe_map = {'Day': 'D', 'Week': 'W', 'Month': 'M'}
    marketing_period = timeframe_map[marketing_timeframe]

    # ========== FACEBOOK ADS SECTION ==========
    st.subheader('Facebook/Instagram Ads Performance')

    if not df_facebook_ads.empty:
        df_ads = df_facebook_ads.copy()
        df_ads['date'] = pd.to_datetime(df_ads['date'], errors='coerce')
        df_ads = df_ads[df_ads['date'].notna()]

        # Aggregate by campaign for lifetime performance
        st.markdown('**Ad Campaigns - Lifetime Performance**')

        campaign_lifetime = df_ads.groupby(['campaign_name', 'campaign_id']).agg({
            'spend': 'sum',
            'impressions': 'sum',
            'clicks': 'sum',
            'purchases': 'sum',
            'add_to_carts': 'sum',
            'link_clicks': 'sum',
            'leads': 'sum',
            'registrations': 'sum',
            'date': ['min', 'max']  # Get start and end dates
        }).reset_index()

        # Flatten column names
        campaign_lifetime.columns = ['campaign_name', 'campaign_id', 'spend', 'impressions', 'clicks',
                                      'purchases', 'add_to_carts', 'link_clicks', 'leads',
                                      'registrations', 'start_date', 'end_date']

        # Infer campaign objective based on which metric has the most activity
        def infer_objective(row):
            """Infer campaign objective from metrics."""
            if row['purchases'] > 0:
                return 'Purchases', row['purchases'], row['spend'] / row['purchases'] if row['purchases'] > 0 else 0
            elif row['add_to_carts'] > 0:
                return 'Add to Cart', row['add_to_carts'], row['spend'] / row['add_to_carts'] if row['add_to_carts'] > 0 else 0
            elif row['registrations'] > 0:
                return 'Registrations', row['registrations'], row['spend'] / row['registrations'] if row['registrations'] > 0 else 0
            elif row['leads'] > 0:
                return 'Leads', row['leads'], row['spend'] / row['leads'] if row['leads'] > 0 else 0
            elif row['link_clicks'] > 0:
                return 'Link Clicks', row['link_clicks'], row['spend'] / row['link_clicks'] if row['link_clicks'] > 0 else 0
            elif row['clicks'] > 0:
                return 'Clicks', row['clicks'], row['spend'] / row['clicks'] if row['clicks'] > 0 else 0
            else:
                return 'Impressions', row['impressions'], row['spend'] / row['impressions'] if row['impressions'] > 0 else 0

        campaign_lifetime[['objective', 'result_count', 'cost_per_result']] = campaign_lifetime.apply(
            lambda row: pd.Series(infer_objective(row)), axis=1
        )

        # Prepare display
        from datetime import datetime, timedelta
        today = pd.Timestamp.now().normalize()

        ads_display = campaign_lifetime[[
            'campaign_name', 'start_date', 'end_date', 'objective', 'spend', 'result_count', 'cost_per_result',
            'impressions', 'clicks'
        ]].copy()

        # Determine if campaign is active (has activity within last 2 days)
        ads_display['is_active'] = (today - pd.to_datetime(ads_display['end_date'])).dt.days <= 2

        # Format columns
        ads_display['start_date'] = pd.to_datetime(ads_display['start_date']).dt.strftime('%Y-%m-%d')
        ads_display['end_date_formatted'] = ads_display.apply(
            lambda row: 'Active' if row['is_active'] else pd.to_datetime(row['end_date']).strftime('%Y-%m-%d'),
            axis=1
        )
        ads_display['spend'] = ads_display['spend'].apply(lambda x: f'${x:.2f}')
        ads_display['cost_per_result'] = ads_display['cost_per_result'].apply(lambda x: f'${x:.2f}')
        ads_display['result_count'] = ads_display['result_count'].astype(int)

        # Select and rename columns
        ads_display = ads_display[[
            'campaign_name', 'start_date', 'end_date_formatted', 'objective', 'spend', 'result_count',
            'cost_per_result', 'impressions', 'clicks'
        ]]
        ads_display.columns = ['Campaign', 'Start Date', 'Status', 'Objective', 'Total Spend', 'Results',
                                'Cost per Result', 'Impressions', 'Clicks']

        # Sort by start date descending (most recent first)
        ads_display = ads_display.sort_values('Start Date', ascending=False)

        st.dataframe(ads_display, use_container_width=True, hide_index=True)

        # Campaign comparison chart
        st.markdown('**Campaign Performance Comparison**')

        # Sort campaigns by spend for chart
        campaign_lifetime_sorted = campaign_lifetime.sort_values('spend', ascending=False)

        fig_ads = go.Figure()

        # Add spend bars
        fig_ads.add_trace(go.Bar(
            x=campaign_lifetime_sorted['campaign_name'],
            y=campaign_lifetime_sorted['spend'],
            name='Total Spend',
            marker_color=COLORS['primary'],
            yaxis='y',
            text=campaign_lifetime_sorted['spend'].apply(lambda x: f'${x:.0f}'),
            textposition='outside'
        ))

        # Add cost per result line
        fig_ads.add_trace(go.Scatter(
            x=campaign_lifetime_sorted['campaign_name'],
            y=campaign_lifetime_sorted['cost_per_result'],
            name='Cost per Result',
            line=dict(color=COLORS['secondary'], width=3),
            yaxis='y2',
            mode='lines+markers'
        ))

        fig_ads.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            title='Campaign Spend vs Cost per Result',
            xaxis_title='Campaign',
            yaxis=dict(title='Total Spend ($)', side='left'),
            yaxis2=dict(title='Cost per Result ($)', side='right', overlaying='y'),
            hovermode='x unified',
            showlegend=True
        )

        st.plotly_chart(fig_ads, use_container_width=True)
    else:
        st.info('No Facebook Ads data available')

    # ========== INSTAGRAM POSTS SECTION ==========
    st.subheader('Instagram Posts Performance')

    if not df_instagram.empty:
        df_ig = df_instagram.copy()
        df_ig['timestamp'] = pd.to_datetime(df_ig['timestamp'], errors='coerce')
        df_ig = df_ig[df_ig['timestamp'].notna()]

        # Posts metrics over time
        df_ig['period'] = df_ig['timestamp'].dt.to_period(marketing_period).dt.start_time

        ig_by_period = df_ig.groupby('period').agg({
            'post_id': 'count',
            'likes': ['mean', 'min', 'max'],
            'comments': ['mean', 'min', 'max'],
            'reach': ['mean', 'min', 'max'],
            'saved': ['mean', 'min', 'max'],
            'engagement_rate': ['mean', 'min', 'max']
        }).reset_index()

        # Flatten column names
        ig_by_period.columns = ['period', 'num_posts',
                                'likes_avg', 'likes_min', 'likes_max',
                                'comments_avg', 'comments_min', 'comments_max',
                                'reach_avg', 'reach_min', 'reach_max',
                                'saved_avg', 'saved_min', 'saved_max',
                                'engagement_avg', 'engagement_min', 'engagement_max']

        # Chart: Number of posts and average engagement
        fig_ig = make_subplots(specs=[[{"secondary_y": True}]])

        fig_ig.add_trace(
            go.Bar(x=ig_by_period['period'], y=ig_by_period['num_posts'],
                   name='Number of Posts', marker_color=COLORS['primary']),
            secondary_y=False
        )

        fig_ig.add_trace(
            go.Scatter(x=ig_by_period['period'], y=ig_by_period['likes_avg'],
                      name='Avg Likes', line=dict(color=COLORS['secondary'], width=3)),
            secondary_y=True
        )

        fig_ig.add_trace(
            go.Scatter(x=ig_by_period['period'], y=ig_by_period['comments_avg'],
                      name='Avg Comments', line=dict(color=COLORS['tertiary'], width=2)),
            secondary_y=True
        )

        fig_ig.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            title=f'Instagram Posts & Engagement by {marketing_timeframe}',
            hovermode='x unified'
        )
        fig_ig.update_yaxes(title_text='Number of Posts', secondary_y=False)
        fig_ig.update_yaxes(title_text='Engagement', secondary_y=True)

        st.plotly_chart(fig_ig, use_container_width=True)

        # Stats summary
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric('Total Posts', len(df_ig))
        with col2:
            st.metric('Avg Likes/Post', f"{df_ig['likes'].mean():.0f}")
        with col3:
            st.metric('Avg Comments/Post', f"{df_ig['comments'].mean():.1f}")
        with col4:
            st.metric('Avg Engagement Rate', f"{df_ig['engagement_rate'].mean():.2f}%")

    else:
        st.info('No Instagram data available')

    # ========== EMAIL CAMPAIGNS SECTION ==========
    st.subheader('Email Campaign Performance')

    if not df_mailchimp.empty:
        df_email = df_mailchimp.copy()
        df_email['send_time'] = pd.to_datetime(df_email['send_time'], errors='coerce')
        df_email = df_email[df_email['send_time'].notna()]

        # Recent campaigns table
        st.markdown('**Recent Email Campaigns**')

        recent_emails = df_email.sort_values('send_time', ascending=False).head(10)

        emails_display = recent_emails[[
            'campaign_title', 'send_time', 'emails_sent', 'open_rate',
            'click_rate', 'unsubscribed'
        ]].copy()

        emails_display['send_time'] = pd.to_datetime(emails_display['send_time']).dt.strftime('%Y-%m-%d')
        emails_display['open_rate'] = emails_display['open_rate'].apply(lambda x: f'{x:.1f}%' if pd.notna(x) else '0%')
        emails_display['click_rate'] = emails_display['click_rate'].apply(lambda x: f'{x:.2f}%' if pd.notna(x) else '0%')
        emails_display.columns = ['Campaign', 'Send Date', 'Emails Sent', 'Open Rate', 'Click Rate', 'Unsubscribed']

        st.dataframe(emails_display, use_container_width=True, hide_index=True)

        # Email performance over time
        df_email['period'] = df_email['send_time'].dt.to_period(marketing_period).dt.start_time

        email_by_period = df_email.groupby('period').agg({
            'campaign_id': 'count',
            'emails_sent': 'sum',
            'open_rate': 'mean',
            'click_rate': 'mean',
            'unsubscribed': 'sum'
        }).reset_index()
        email_by_period.columns = ['period', 'num_campaigns', 'emails_sent', 'avg_open_rate', 'avg_click_rate', 'unsubscribed']

        # Chart: Campaigns and engagement rates
        fig_email = make_subplots(specs=[[{"secondary_y": True}]])

        fig_email.add_trace(
            go.Bar(x=email_by_period['period'], y=email_by_period['num_campaigns'],
                   name='Number of Campaigns', marker_color=COLORS['primary']),
            secondary_y=False
        )

        fig_email.add_trace(
            go.Scatter(x=email_by_period['period'], y=email_by_period['avg_open_rate'],
                      name='Avg Open Rate (%)', line=dict(color=COLORS['secondary'], width=3)),
            secondary_y=True
        )

        fig_email.add_trace(
            go.Scatter(x=email_by_period['period'], y=email_by_period['avg_click_rate'],
                      name='Avg Click Rate (%)', line=dict(color=COLORS['tertiary'], width=2)),
            secondary_y=True
        )

        fig_email.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            title=f'Email Campaigns & Engagement by {marketing_timeframe}',
            hovermode='x unified'
        )
        fig_email.update_yaxes(title_text='Number of Campaigns', secondary_y=False)
        fig_email.update_yaxes(title_text='Rate (%)', secondary_y=True)

        st.plotly_chart(fig_email, use_container_width=True)

        # Stats summary
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric('Total Campaigns', len(df_email))
        with col2:
            st.metric('Avg Open Rate', f"{df_email['open_rate'].mean():.1f}%")
        with col3:
            st.metric('Avg Click Rate', f"{df_email['click_rate'].mean():.2f}%")
        with col4:
            st.metric('Total Unsubscribes', int(df_email['unsubscribed'].sum()))

    else:
        st.info('No email campaign data available')

# Footer
st.markdown('---')
st.caption('Basin Climbing & Fitness Analytics Dashboard | Data updated every 5 minutes')

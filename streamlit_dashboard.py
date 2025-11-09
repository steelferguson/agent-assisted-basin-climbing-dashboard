"""
Basin Climbing & Fitness Dashboard - Streamlit Version

A comprehensive analytics dashboard for Basin Climbing & Fitness.
Organized into logical tabs for better navigation and analysis.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

    return df_transactions, df_memberships, df_members, df_projection, df_at_risk, df_facebook_ads, df_events, df_checkins


# Load data
with st.spinner('Loading data from S3...'):
    df_transactions, df_memberships, df_members, df_projection, df_at_risk, df_facebook_ads, df_events, df_checkins = load_data()

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
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Revenue",
    "👥 Membership",
    "🎟️ Day Passes & Check-ins",
    "🎉 Rentals",
    "💪 Programming"
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

        # Prepare display data
        attrited_display = attrited_unique[[
            'owner_id', 'membership_owner_age', 'name', 'end_date'
        ]].copy()
        attrited_display.columns = ['Customer ID', 'Age', 'Membership Type', 'End Date']

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

# Footer
st.markdown('---')
st.caption('Basin Climbing & Fitness Analytics Dashboard | Data updated every 5 minutes')

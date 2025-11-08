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
    'Day Pass': COLORS['quaternary'],
    'New Membership': COLORS['primary'],
    'Membership Renewal': COLORS['secondary'],
    'Programming': COLORS['tertiary'],
    'Team Dues': '#8B4229',
    'Retail': COLORS['secondary'],
    'Event Booking': '#96A682',
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

    return df_transactions, df_memberships, df_members, df_projection, df_at_risk, df_facebook_ads, df_events


# Load data
with st.spinner('Loading data from S3...'):
    df_transactions, df_memberships, df_members, df_projection, df_at_risk, df_facebook_ads, df_events = load_data()

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
        title='Total Revenue Over Time'
    )
    fig_line.update_traces(line_color=COLORS['primary'])
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
        textposition='inside',
        textfont=dict(size=12, color='white')
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
        'Memberships': REVENUE_CATEGORY_COLORS.get('Membership Renewal', COLORS['primary']),
        'Team & Programming': REVENUE_CATEGORY_COLORS.get('Programming', COLORS['secondary']),
        'Day Pass': REVENUE_CATEGORY_COLORS.get('Day Pass', COLORS['quaternary']),
        'Retail': REVENUE_CATEGORY_COLORS.get('Retail', COLORS['tertiary']),
        'Event Booking': REVENUE_CATEGORY_COLORS.get('Event Booking', '#8B4229'),
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
        textposition='inside',
        textfont=dict(size=12, color='white')
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

# ============================================================================
# TAB 2: MEMBERSHIP
# ============================================================================
with tab2:
    st.header('Membership Analysis')

    # Membership Revenue Projection
    st.subheader('Membership Revenue Projections (Current Month + 3 Months)')

    # Frequency filter for projections
    projection_frequencies = st.multiselect(
        'Membership Frequency',
        options=['bi_weekly', 'monthly', 'annual', 'prepaid_3mo', 'prepaid_6mo', 'prepaid_12mo'],
        default=['bi_weekly', 'monthly'],
        format_func=lambda x: x.replace('_', ' ').title()
    )

    if projection_frequencies:
        df_proj_filtered = df_projection[df_projection['frequency'].isin(projection_frequencies)]

        proj_summary = df_proj_filtered.groupby('month')['projected_revenue'].sum().reset_index()

        fig_projection = px.bar(
            proj_summary,
            x='month',
            y='projected_revenue',
            title='Projected Membership Revenue'
        )
        fig_projection.update_traces(marker_color=COLORS['secondary'])
        fig_projection.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            yaxis_title='Projected Revenue ($)',
            xaxis_title='Month'
        )
        st.plotly_chart(fig_projection, use_container_width=True)

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
            default=['bi_weekly', 'monthly'],
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
    if 'not_special' in category_filter:
        df_memberships_filtered = df_memberships_filtered[df_memberships_filtered['is_not_in_special']]

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

# ============================================================================
# TAB 3: DAY PASSES & CHECK-INS (Placeholder)
# ============================================================================
with tab3:
    st.header('Day Passes & Check-ins')
    st.info('Day pass and check-in charts will be added in the next iteration.')

# ============================================================================
# TAB 4: RENTALS (Placeholder)
# ============================================================================
with tab4:
    st.header('Rentals')
    st.info('Rental charts will be added in the next iteration.')

# ============================================================================
# TAB 5: PROGRAMMING (Placeholder)
# ============================================================================
with tab5:
    st.header('Programming')
    st.info('Programming charts will be added in the next iteration.')

# Footer
st.markdown('---')
st.caption('Basin Climbing & Fitness Analytics Dashboard | Data updated every 5 minutes')

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
# TAB 2: MEMBERSHIP (Placeholder)
# ============================================================================
with tab2:
    st.header('Membership Analysis')
    st.info('Membership charts will be added in the next iteration.')

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

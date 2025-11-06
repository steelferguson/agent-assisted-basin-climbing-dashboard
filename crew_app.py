"""
Basin Climbing - Crew Dashboard

A simplified dashboard for crew members without financial data.
Shows membership stats, check-ins, events, and operational metrics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from shared.data_loader import (
    load_memberships,
    load_members,
    load_checkins,
    load_associations,
    load_association_members,
    load_events,
    refresh_all_data
)

# Page config
st.set_page_config(
    page_title="Basin Crew Dashboard",
    page_icon="🧗",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 {
        color: #1f77b4;
    }
    h2 {
        color: #2c3e50;
        margin-top: 30px;
    }
    .at-risk {
        background-color: #fff3cd;
        padding: 10px;
        border-left: 4px solid #ffc107;
        margin: 5px 0;
    }
</style>
""", unsafe_allow_html=True)

# Title and header
st.title("🧗 Basin Climbing - Crew Dashboard")
st.markdown("*Real-time operational metrics and member insights*")

# Sidebar
with st.sidebar:
    st.header("🔄 Data Refresh")
    if st.button("Refresh Data", type="primary"):
        refresh_all_data()
        st.success("Data refreshed!")
        st.rerun()

    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%I:%M %p')}")

# Load data
try:
    with st.spinner("Loading data..."):
        memberships_df = load_memberships()
        members_df = load_members()
        checkins_df = load_checkins()
        associations_df = load_associations()
        association_members_df = load_association_members()
        events_df = load_events()

    # Calculate metrics
    today = datetime.now().date()
    active_memberships = memberships_df[memberships_df['status'] == 'active']
    active_member_count = len(active_memberships)

    # Today's check-ins
    checkins_df['checkin_date'] = pd.to_datetime(checkins_df['checkin_datetime']).dt.date
    todays_checkins = len(checkins_df[checkins_df['checkin_date'] == today])

    # This week's check-ins
    week_ago = today - timedelta(days=7)
    this_week_checkins = len(checkins_df[checkins_df['checkin_date'] > week_ago])

    # Upcoming events (next 7 days)
    events_df['event_date'] = pd.to_datetime(events_df['start_datetime']).dt.date
    next_week = today + timedelta(days=7)
    upcoming_events = events_df[
        (events_df['event_date'] >= today) &
        (events_df['event_date'] <= next_week) &
        (events_df['is_cancelled'] == False)  # Filter out cancelled events
    ]
    upcoming_event_count = len(upcoming_events)

    # ===========================
    # OVERVIEW METRICS
    # ===========================
    st.header("📊 Today's Overview")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="🏃 Today's Check-ins",
            value=todays_checkins,
            delta=f"{todays_checkins} visitors"
        )

    with col2:
        st.metric(
            label="👥 Active Members",
            value=active_member_count,
            delta="Total memberships"
        )

    with col3:
        st.metric(
            label="📅 This Week's Visits",
            value=this_week_checkins,
            delta="Last 7 days"
        )

    with col4:
        st.metric(
            label="🎯 Upcoming Events",
            value=upcoming_event_count,
            delta="Next 7 days"
        )

    # ===========================
    # CHECK-IN TRENDS
    # ===========================
    st.header("📈 Check-in Trends")

    # Last 30 days check-ins
    thirty_days_ago = today - timedelta(days=30)
    recent_checkins = checkins_df[checkins_df['checkin_date'] > thirty_days_ago].copy()

    # Daily check-ins chart
    daily_checkins = recent_checkins.groupby('checkin_date').size().reset_index(name='count')
    daily_checkins = daily_checkins.sort_values('checkin_date')

    fig_checkins = px.line(
        daily_checkins,
        x='checkin_date',
        y='count',
        title="Daily Check-ins (Last 30 Days)",
        labels={'checkin_date': 'Date', 'count': 'Check-ins'},
        markers=True
    )
    fig_checkins.update_traces(line_color='#1f77b4', line_width=3)
    fig_checkins.update_layout(hovermode='x unified')
    st.plotly_chart(fig_checkins, use_container_width=True)

    # Peak hours analysis
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("⏰ Peak Hours")
        recent_checkins['hour'] = pd.to_datetime(recent_checkins['checkin_datetime']).dt.hour
        hourly_checkins = recent_checkins.groupby('hour').size().reset_index(name='count')

        fig_hours = px.bar(
            hourly_checkins,
            x='hour',
            y='count',
            title="Check-ins by Hour of Day",
            labels={'hour': 'Hour', 'count': 'Total Check-ins'},
            color='count',
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig_hours, use_container_width=True)

    with col2:
        st.subheader("📆 Day of Week")
        recent_checkins['day_of_week'] = pd.to_datetime(recent_checkins['checkin_datetime']).dt.day_name()
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dow_checkins = recent_checkins.groupby('day_of_week').size().reset_index(name='count')
        dow_checkins['day_of_week'] = pd.Categorical(dow_checkins['day_of_week'], categories=day_order, ordered=True)
        dow_checkins = dow_checkins.sort_values('day_of_week')

        fig_dow = px.bar(
            dow_checkins,
            x='day_of_week',
            y='count',
            title="Check-ins by Day of Week",
            labels={'day_of_week': 'Day', 'count': 'Total Check-ins'},
            color='count',
            color_continuous_scale='Teal'
        )
        st.plotly_chart(fig_dow, use_container_width=True)

    # ===========================
    # MEMBERSHIP BREAKDOWN
    # ===========================
    st.header("🎫 Membership Overview")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("By Type")
        # Count memberships by interval (frequency)
        membership_types = active_memberships['interval'].value_counts().reset_index()
        membership_types.columns = ['Type', 'Count']

        fig_types = px.pie(
            membership_types,
            names='Type',
            values='Count',
            title="Active Memberships by Type",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_types, use_container_width=True)

    with col2:
        st.subheader("By Size")
        # Count by size (solo, duo, family)
        size_counts = active_memberships['size'].value_counts().reset_index()
        size_counts.columns = ['Size', 'Count']

        fig_sizes = px.bar(
            size_counts,
            x='Size',
            y='Count',
            title="Active Memberships by Size",
            color='Count',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(fig_sizes, use_container_width=True)

    # ===========================
    # AT-RISK MEMBERS
    # ===========================
    st.header("⚠️ At-Risk Members")

    # Members who haven't checked in recently
    last_14_days = today - timedelta(days=14)
    recent_visitor_ids = set(checkins_df[
        checkins_df['checkin_date'] > last_14_days
    ]['customer_id'].unique())

    # Active members who haven't visited
    active_member_ids = set()
    for _, membership in active_memberships.iterrows():
        customer_ids = membership.get('customer_ids', '')
        if pd.notna(customer_ids):
            ids = str(customer_ids).split(',')
            active_member_ids.update([int(id.strip()) for id in ids if id.strip().isdigit()])

    at_risk_ids = active_member_ids - recent_visitor_ids

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="🚨 Haven't Visited (14+ days)",
            value=len(at_risk_ids),
            delta="Active members",
            delta_color="inverse"
        )

    with col2:
        # Memberships expiring soon (next 30 days)
        thirty_days_from_now = today + timedelta(days=30)
        expiring_soon = active_memberships[
            (pd.to_datetime(active_memberships['end_date']).dt.date >= today) &
            (pd.to_datetime(active_memberships['end_date']).dt.date <= thirty_days_from_now)
        ]
        st.metric(
            label="📅 Expiring Soon (30 days)",
            value=len(expiring_soon),
            delta="Need renewal",
            delta_color="inverse"
        )

    with col3:
        # Members who haven't visited in 30+ days
        last_30_days = today - timedelta(days=30)
        very_dormant_ids = set(checkins_df[
            checkins_df['checkin_date'] <= last_30_days
        ]['customer_id'].unique()) & active_member_ids

        st.metric(
            label="😴 Very Dormant (30+ days)",
            value=len(very_dormant_ids),
            delta="Critical",
            delta_color="inverse"
        )

    # Show expiring memberships detail
    if len(expiring_soon) > 0:
        st.subheader("📋 Memberships Expiring Soon")
        expiring_display = expiring_soon[['name', 'interval', 'size', 'end_date']].copy()
        expiring_display['end_date'] = pd.to_datetime(expiring_display['end_date']).dt.strftime('%Y-%m-%d')
        expiring_display['days_remaining'] = (pd.to_datetime(expiring_display['end_date']).dt.date - today).dt.days

        expiring_display = expiring_display.sort_values('days_remaining')
        st.dataframe(
            expiring_display,
            hide_index=True,
            use_container_width=True,
            column_config={
                "name": "Membership Name",
                "interval": "Type",
                "size": "Size",
                "end_date": "End Date",
                "days_remaining": st.column_config.NumberColumn(
                    "Days Left",
                    format="%d days"
                )
            }
        )

    # ===========================
    # DAY PASS USAGE
    # ===========================
    st.header("🎟️ Day Pass Usage")

    # Filter check-ins that are day passes
    day_pass_keywords = ['day pass', 'Day Pass', 'DAY PASS']
    day_pass_checkins = checkins_df[
        checkins_df['entry_method_description'].str.contains('|'.join(day_pass_keywords), case=False, na=False)
    ]

    # Last 30 days
    recent_day_passes = day_pass_checkins[day_pass_checkins['checkin_date'] > thirty_days_ago]

    col1, col2 = st.columns(2)

    with col1:
        st.metric(
            label="🎫 Day Passes (Last 30 Days)",
            value=len(recent_day_passes),
            delta=f"{len(recent_day_passes[recent_day_passes['checkin_date'] > week_ago])} this week"
        )

        # Day pass trend
        daily_day_passes = recent_day_passes.groupby('checkin_date').size().reset_index(name='count')
        daily_day_passes = daily_day_passes.sort_values('checkin_date')

        fig_day_passes = px.area(
            daily_day_passes,
            x='checkin_date',
            y='count',
            title="Daily Day Pass Usage",
            labels={'checkin_date': 'Date', 'count': 'Day Passes'}
        )
        fig_day_passes.update_traces(fill='tozeroy', fillcolor='rgba(255, 127, 80, 0.3)', line_color='#ff7f50')
        st.plotly_chart(fig_day_passes, use_container_width=True)

    with col2:
        # Frequent day pass users (potential membership leads)
        day_pass_users = recent_day_passes.groupby('customer_email').size().reset_index(name='visit_count')
        frequent_users = day_pass_users[day_pass_users['visit_count'] >= 4].sort_values('visit_count', ascending=False)

        st.metric(
            label="🎯 Frequent Day Pass Users",
            value=len(frequent_users),
            delta="4+ visits (potential members)"
        )

        if len(frequent_users) > 0:
            st.subheader("Top Day Pass Users")
            st.dataframe(
                frequent_users.head(10),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "customer_email": "Email",
                    "visit_count": st.column_config.NumberColumn(
                        "Visits",
                        format="%d visits"
                    )
                }
            )
        else:
            st.info("No frequent day pass users this month")

    # ===========================
    # UPCOMING EVENTS
    # ===========================
    st.header("🎯 Upcoming Events")

    if len(upcoming_events) > 0:
        # Display upcoming events
        events_display = upcoming_events[['event_type_name', 'start_datetime', 'capacity']].copy()
        events_display['start_datetime'] = pd.to_datetime(events_display['start_datetime']).dt.strftime('%b %d, %I:%M %p')

        st.dataframe(
            events_display.sort_values('start_datetime'),
            hide_index=True,
            use_container_width=True,
            column_config={
                "event_type_name": "Event Name",
                "start_datetime": "Date & Time",
                "capacity": st.column_config.NumberColumn(
                    "Capacity",
                    format="%d people"
                )
            }
        )
    else:
        st.info("No upcoming events in the next 7 days")

    # ===========================
    # MEMBER ASSOCIATIONS
    # ===========================
    st.header("👥 Member Groups")

    # Top associations by member count
    top_associations = associations_df.nlargest(10, 'num_members')[['name', 'num_members']]

    fig_associations = px.bar(
        top_associations,
        x='num_members',
        y='name',
        orientation='h',
        title="Top 10 Member Groups",
        labels={'num_members': 'Members', 'name': 'Group'},
        color='num_members',
        color_continuous_scale='Cividis'
    )
    fig_associations.update_layout(showlegend=False)
    st.plotly_chart(fig_associations, use_container_width=True)

    # Footer
    st.divider()
    st.caption("🧗 Basin Climbing & Fitness - Crew Dashboard")

except Exception as e:
    st.error(f"Error loading dashboard: {str(e)}")
    st.info("Please check that all data sources are available in S3.")
    with st.expander("Error Details"):
        st.exception(e)

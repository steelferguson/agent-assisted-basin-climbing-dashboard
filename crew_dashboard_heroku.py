"""
Basin Climbing - Crew Dashboard (Heroku)

Operational dashboard for crew members showing key metrics:
- Memberships (chart, new members, attrition)
- Check-ins (overall and day pass)
- Team members (kids teams)
- Birthday party bookings
- Link to birthday party admin

No financial data - operational metrics only.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import boto3
from io import StringIO
import os

# Page config
st.set_page_config(
    page_title="Basin Crew Dashboard",
    page_icon="🧗",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Styling
st.markdown("""
<style>
    .main > div {
        padding-top: 2rem;
    }
    h1 {
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .birthday-link {
        background-color: #ff6b6b;
        color: white;
        padding: 15px 30px;
        border-radius: 8px;
        text-align: center;
        text-decoration: none;
        font-size: 18px;
        font-weight: bold;
        display: inline-block;
        margin: 20px 0;
    }
    .birthday-link:hover {
        background-color: #ff5252;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# Color scheme
COLORS = {
    'primary': '#1f77b4',
    'secondary': '#ff7f0e',
    'tertiary': '#2ca02c',
    'quaternary': '#d62728',
    'background': '#ffffff',
    'text': '#2c3e50',
    'gridline': '#e0e0e0',
    'axis_text': '#7f7f7f'
}


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data():
    """Load operational data from S3."""
    # AWS credentials
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = "basin-climbing-data-prod"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )

    def load_csv(key):
        """Helper to load CSV from S3."""
        try:
            obj = s3_client.get_object(Bucket=bucket_name, Key=key)
            return pd.read_csv(StringIO(obj['Body'].read().decode('utf-8')))
        except Exception as e:
            st.warning(f"Could not load {key}: {str(e)}")
            return pd.DataFrame()

    # Load data
    df_memberships = load_csv('capitan/memberships.csv')
    df_members = load_csv('capitan/members.csv')
    df_checkins = load_csv('capitan/checkins.csv')
    df_transactions = load_csv('transactions/combined_transactions.csv')

    return df_memberships, df_members, df_checkins, df_transactions


# Header
st.title("🧗 Basin Climbing - Crew Dashboard")
st.markdown("*Operational metrics and key insights*")

# Quick link to birthday party admin
st.markdown("""
<a href="https://basin-birthday-rsvp.web.app/admin.html" target="_blank" class="birthday-link">
    🎂 Birthday Party Admin →
</a>
""", unsafe_allow_html=True)

st.markdown("---")

# Load data
try:
    with st.spinner("Loading data..."):
        df_memberships, df_members, df_checkins, df_transactions = load_data()

    # Calculate date ranges
    today = datetime.now().date()
    week_ago = today - timedelta(days=7)
    month_ago = today - timedelta(days=30)

    # ========== MEMBERSHIP OVERVIEW ==========
    st.header("👥 Membership Overview")

    # Prepare membership data
    if not df_memberships.empty and 'start_date' in df_memberships.columns:
        df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
        df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

        # Active memberships
        active_memberships = df_memberships[df_memberships['status'] == 'ACT']
        active_count = len(active_memberships)

        # New members this week (first-time memberships)
        if 'owner_id' in df_memberships.columns:
            first_membership_dates = df_memberships.groupby('owner_id')['start_date'].min().reset_index()
            first_membership_dates.columns = ['owner_id', 'first_membership_date']
            df_memberships_calc = df_memberships.merge(first_membership_dates, on='owner_id', how='left')
            df_memberships_calc['is_first_membership'] = (
                df_memberships_calc['start_date'] == df_memberships_calc['first_membership_date']
            )

            new_members_week = len(df_memberships_calc[
                (df_memberships_calc['start_date'] >= pd.Timestamp(week_ago)) &
                (df_memberships_calc['is_first_membership'] == True)
            ])

            new_members_month = len(df_memberships_calc[
                (df_memberships_calc['start_date'] >= pd.Timestamp(month_ago)) &
                (df_memberships_calc['is_first_membership'] == True)
            ])
        else:
            new_members_week = 0
            new_members_month = 0

        # Attrited members this week/month
        attrited_week = len(df_memberships[
            (df_memberships['status'] == 'END') &
            (df_memberships['end_date'] >= pd.Timestamp(week_ago))
        ])

        attrited_month = len(df_memberships[
            (df_memberships['status'] == 'END') &
            (df_memberships['end_date'] >= pd.Timestamp(month_ago))
        ])

        # Display metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Active Memberships", f"{active_count:,}")

        with col2:
            st.metric("New Members (Week)", f"+{new_members_week}", delta=f"Month: {new_members_month}")

        with col3:
            st.metric("Attrited (Week)", f"{attrited_week}", delta=f"Month: {attrited_month}", delta_color="inverse")

        with col4:
            net_week = new_members_week - attrited_week
            net_month = new_members_month - attrited_month
            st.metric("Net Growth (Week)", f"{net_week:+d}", delta=f"Month: {net_month:+d}")

        # Membership trend chart (last 90 days)
        st.subheader("Membership Trend (Last 90 Days)")
        ninety_days_ago = today - timedelta(days=90)

        # Create daily active membership count
        date_range = pd.date_range(start=ninety_days_ago, end=today, freq='D')
        daily_active = []

        for date in date_range:
            active_on_date = len(df_memberships[
                (df_memberships['start_date'] <= pd.Timestamp(date)) &
                ((df_memberships['end_date'].isna()) | (df_memberships['end_date'] >= pd.Timestamp(date))) &
                (df_memberships['status'].isin(['ACT', 'FRZ']))  # Include frozen as they're still members
            ])
            daily_active.append({'date': date, 'active_memberships': active_on_date})

        df_trend = pd.DataFrame(daily_active)

        fig_trend = px.line(
            df_trend,
            x='date',
            y='active_memberships',
            title='Active Memberships Over Time',
            labels={'date': 'Date', 'active_memberships': 'Active Memberships'}
        )
        fig_trend.update_traces(line_color=COLORS['primary'], line_width=3)
        fig_trend.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            hovermode='x unified'
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    else:
        st.info("Membership data not available")

    st.markdown("---")

    # ========== CHECK-INS ==========
    st.header("📊 Check-ins")

    if not df_checkins.empty and 'checkin_datetime' in df_checkins.columns:
        df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
        df_checkins['checkin_date'] = df_checkins['checkin_datetime'].dt.date

        # Overall check-ins
        today_checkins = len(df_checkins[df_checkins['checkin_date'] == today])
        week_checkins = len(df_checkins[df_checkins['checkin_date'] >= week_ago])
        month_checkins = len(df_checkins[df_checkins['checkin_date'] >= month_ago])

        # Day pass check-ins
        if 'entry_method_description' in df_checkins.columns:
            day_pass_checkins = df_checkins[
                df_checkins['entry_method_description'].str.contains('day pass', case=False, na=False)
            ]

            today_day_pass = len(day_pass_checkins[day_pass_checkins['checkin_date'] == today])
            week_day_pass = len(day_pass_checkins[day_pass_checkins['checkin_date'] >= week_ago])
            month_day_pass = len(day_pass_checkins[day_pass_checkins['checkin_date'] >= month_ago])
        else:
            today_day_pass = 0
            week_day_pass = 0
            month_day_pass = 0

        # Display metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.subheader("Overall Check-ins")
            st.metric("Today", f"{today_checkins:,}")
            st.metric("This Week", f"{week_checkins:,}")
            st.metric("This Month", f"{month_checkins:,}")

        with col2:
            st.subheader("Day Pass Check-ins")
            st.metric("Today", f"{today_day_pass:,}")
            st.metric("This Week", f"{week_day_pass:,}")
            st.metric("This Month", f"{month_day_pass:,}")

        with col3:
            st.subheader("Day Pass %")
            today_pct = (today_day_pass / today_checkins * 100) if today_checkins > 0 else 0
            week_pct = (week_day_pass / week_checkins * 100) if week_checkins > 0 else 0
            month_pct = (month_day_pass / month_checkins * 100) if month_checkins > 0 else 0

            st.metric("Today", f"{today_pct:.1f}%")
            st.metric("This Week", f"{week_pct:.1f}%")
            st.metric("This Month", f"{month_pct:.1f}%")

        # Check-in trend chart (last 30 days)
        st.subheader("Daily Check-ins (Last 30 Days)")
        recent_checkins = df_checkins[df_checkins['checkin_date'] >= month_ago]
        daily_checkins = recent_checkins.groupby('checkin_date').size().reset_index(name='count')
        daily_checkins = daily_checkins.sort_values('checkin_date')

        fig_checkins = px.bar(
            daily_checkins,
            x='checkin_date',
            y='count',
            title='Daily Check-ins',
            labels={'checkin_date': 'Date', 'count': 'Check-ins'}
        )
        fig_checkins.update_traces(marker_color=COLORS['secondary'])
        fig_checkins.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            showlegend=False
        )
        st.plotly_chart(fig_checkins, use_container_width=True)

    else:
        st.info("Check-in data not available")

    st.markdown("---")

    # ========== TEAM MEMBERS ==========
    st.header("🏃 Team Members (Kids Teams)")

    if not df_memberships.empty and 'membership_type' in df_memberships.columns:
        # Team memberships
        team_memberships = df_memberships[
            (df_memberships['status'] == 'ACT') &
            (df_memberships['membership_type'].str.contains('team', case=False, na=False))
        ]

        team_count = len(team_memberships)

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Active Team Members", f"{team_count:,}")

        with col2:
            # Show breakdown by team type if available
            if not team_memberships.empty:
                team_breakdown = team_memberships['membership_type'].value_counts()
                st.markdown("**Team Breakdown:**")
                for team_type, count in team_breakdown.items():
                    st.write(f"- {team_type}: {count}")

    else:
        st.info("Team membership data not available")

    st.markdown("---")

    # ========== BIRTHDAY PARTIES ==========
    st.header("🎂 Birthday Party Bookings")

    if not df_transactions.empty and 'revenue_category' in df_transactions.columns:
        df_transactions['Date'] = pd.to_datetime(df_transactions['Date'], errors='coerce')
        df_transactions['date'] = df_transactions['Date'].dt.date

        # Birthday party bookings
        birthday_bookings = df_transactions[
            df_transactions['revenue_category'] == 'Event Booking'
        ]

        week_bookings = len(birthday_bookings[birthday_bookings['date'] >= week_ago])
        month_bookings = len(birthday_bookings[birthday_bookings['date'] >= month_ago])

        if 'Total Amount' in birthday_bookings.columns:
            week_revenue = birthday_bookings[birthday_bookings['date'] >= week_ago]['Total Amount'].sum()
            month_revenue = birthday_bookings[birthday_bookings['date'] >= month_ago]['Total Amount'].sum()
        else:
            week_revenue = 0
            month_revenue = 0

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Bookings (Week)", f"{week_bookings:,}")

        with col2:
            st.metric("Bookings (Month)", f"{month_bookings:,}")

        with col3:
            st.metric("Revenue (Week)", f"${week_revenue:,.0f}")

        with col4:
            st.metric("Revenue (Month)", f"${month_revenue:,.0f}")

        # Booking trend (last 30 days)
        st.subheader("Daily Birthday Bookings (Last 30 Days)")
        recent_bookings = birthday_bookings[birthday_bookings['date'] >= month_ago]
        daily_bookings = recent_bookings.groupby('date').size().reset_index(name='count')
        daily_bookings = daily_bookings.sort_values('date')

        fig_birthdays = px.bar(
            daily_bookings,
            x='date',
            y='count',
            title='Daily Birthday Bookings',
            labels={'date': 'Date', 'count': 'Bookings'}
        )
        fig_birthdays.update_traces(marker_color=COLORS['quaternary'])
        fig_birthdays.update_layout(
            plot_bgcolor=COLORS['background'],
            paper_bgcolor=COLORS['background'],
            font_color=COLORS['text'],
            showlegend=False
        )
        st.plotly_chart(fig_birthdays, use_container_width=True)

    else:
        st.info("Birthday party booking data not available")

    # Footer
    st.markdown("---")
    st.caption("🧗 Basin Climbing & Fitness - Crew Dashboard")
    st.caption(f"Last updated: {datetime.now().strftime('%I:%M %p on %B %d, %Y')}")

except Exception as e:
    st.error("Error loading dashboard")
    st.error(f"Details: {str(e)}")
    with st.expander("Full Error Details"):
        st.exception(e)

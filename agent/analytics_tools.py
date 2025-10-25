"""
Analytical tools for querying business data.

These tools provide pre-built functions for common analytical questions about:
- Revenue (total, breakdowns, comparisons)
- Memberships (counts, breakdowns, conversions)
- Day passes (counts, revenue)
- Generic pandas queries for ad-hoc analysis
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Optional, Literal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from langchain_core.tools import StructuredTool
import json
import os


# ============================================================================
# DATA LOADING
# ============================================================================

def load_data_frames():
    """Load all dataframes from S3."""
    from data_pipeline import upload_data, config

    uploader = upload_data.DataUploader()

    # Transactions
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_combined)
    df_transactions = uploader.convert_csv_to_df(csv_content)
    df_transactions['Date'] = pd.to_datetime(df_transactions['Date'], errors='coerce')

    # Memberships
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_memberships)
    df_memberships = uploader.convert_csv_to_df(csv_content)
    df_memberships['start_date'] = pd.to_datetime(df_memberships['start_date'], errors='coerce')
    df_memberships['end_date'] = pd.to_datetime(df_memberships['end_date'], errors='coerce')

    # Members
    csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_members)
    df_members = uploader.convert_csv_to_df(csv_content)
    df_members['start_date'] = pd.to_datetime(df_members['start_date'], errors='coerce')
    df_members['end_date'] = pd.to_datetime(df_members['end_date'], errors='coerce')

    return df_transactions, df_memberships, df_members


# ============================================================================
# REVENUE TOOLS
# ============================================================================

class RevenueInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    category: Optional[str] = Field(None, description="Filter by revenue category (e.g., 'Day Pass', 'Membership Renewal')")
    sub_category: Optional[str] = Field(None, description="Filter by sub-category")


def create_get_total_revenue_tool(df_transactions: pd.DataFrame):
    """Get total revenue for a time period."""

    def get_total_revenue(
        start_date: str,
        end_date: str,
        category: Optional[str] = None,
        sub_category: Optional[str] = None
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Filter by category if provided
        if category:
            df = df[df['revenue_category'] == category]
        if sub_category:
            df = df[df['sub_category'] == sub_category]

        total = df['Total Amount'].sum()
        count = len(df)

        filters = []
        if category:
            filters.append(f"category: {category}")
        if sub_category:
            filters.append(f"sub-category: {sub_category}")
        filter_str = f" ({', '.join(filters)})" if filters else ""

        return f"Total revenue{filter_str} from {start_date} to {end_date}: ${total:,.2f} ({count} transactions)"

    return StructuredTool.from_function(
        name="get_total_revenue",
        func=get_total_revenue,
        description="Get total revenue for a time period, optionally filtered by category or sub-category",
        args_schema=RevenueInput
    )


class RevenueBreakdownInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    group_by: Literal['category', 'sub_category', 'source'] = Field(
        description="Group revenue by: 'category' (revenue category), 'sub_category', or 'source' (Square/Stripe)"
    )


def create_get_revenue_breakdown_tool(df_transactions: pd.DataFrame):
    """Get revenue breakdown by category, sub-category, or source."""

    def get_revenue_breakdown(
        start_date: str,
        end_date: str,
        group_by: Literal['category', 'sub_category', 'source'] = 'category'
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Group by the specified column
        column_map = {
            'category': 'revenue_category',
            'sub_category': 'sub_category',
            'source': 'Data Source'
        }
        group_col = column_map[group_by]

        breakdown = df.groupby(group_col).agg({
            'Total Amount': ['sum', 'count']
        }).round(2)

        breakdown.columns = ['Total Revenue', 'Transaction Count']
        breakdown = breakdown.sort_values('Total Revenue', ascending=False)

        result = f"Revenue breakdown by {group_by} from {start_date} to {end_date}:\n\n"
        result += breakdown.to_string()
        result += f"\n\nGrand Total: ${breakdown['Total Revenue'].sum():,.2f}"

        return result

    return StructuredTool.from_function(
        name="get_revenue_breakdown",
        func=get_revenue_breakdown,
        description="Get revenue breakdown grouped by category, sub-category, or payment source (Square/Stripe)",
        args_schema=RevenueBreakdownInput
    )


# ============================================================================
# MEMBERSHIP TOOLS
# ============================================================================

class MemberCountInput(BaseModel):
    status: Optional[str] = Field('ACT', description="Membership status: 'ACT' (active), 'END' (ended), 'FRZ' (frozen), or None for all")
    membership_type: Optional[str] = Field(None, description="Filter by membership type (e.g., 'solo', 'family', 'duo')")
    as_of_date: Optional[str] = Field(None, description="Date to check membership status (YYYY-MM-DD). Defaults to today.")


def create_get_member_count_tool(df_memberships: pd.DataFrame, df_members: pd.DataFrame):
    """Get count of members or memberships."""

    def get_member_count(
        status: Optional[str] = 'ACT',
        membership_type: Optional[str] = None,
        as_of_date: Optional[str] = None
    ) -> str:
        df = df_memberships.copy()

        # Filter by date
        if as_of_date:
            as_of = pd.to_datetime(as_of_date)
        else:
            as_of = pd.Timestamp.now()

        # Filter to memberships active on the specified date
        df = df[(df['start_date'] <= as_of) & (df['end_date'] >= as_of)]

        # Filter by status
        if status:
            df = df[df['status'] == status]

        # Filter by membership type
        if membership_type:
            df = df[df['size'] == membership_type]

        membership_count = len(df)

        # Also count individual members
        df_mem = df_members.copy()
        df_mem = df_mem[(df_mem['start_date'] <= as_of) & (df_mem['end_date'] >= as_of)]
        if status:
            df_mem = df_mem[df_mem['status'] == status]
        if membership_type:
            df_mem = df_mem[df_mem['size'] == membership_type]
        member_count = len(df_mem)

        filters = []
        if status:
            filters.append(f"status: {status}")
        if membership_type:
            filters.append(f"type: {membership_type}")
        filter_str = f" ({', '.join(filters)})" if filters else ""

        date_str = as_of_date if as_of_date else "today"

        return f"As of {date_str}{filter_str}:\n- {membership_count} memberships\n- {member_count} individual members"

    return StructuredTool.from_function(
        name="get_member_count",
        func=get_member_count,
        description="Get count of active memberships and individual members, optionally filtered by status and type",
        args_schema=MemberCountInput
    )


class MembershipBreakdownInput(BaseModel):
    group_by: Literal['type', 'frequency', 'size', 'status'] = Field(
        description="Group memberships by: 'type' (special categories), 'frequency' (billing frequency), 'size' (solo/family/duo), or 'status' (ACT/END/FRZ)"
    )


def create_get_membership_breakdown_tool(df_memberships: pd.DataFrame):
    """Get membership breakdown by type, frequency, size, or status."""

    def get_membership_breakdown(
        group_by: Literal['type', 'frequency', 'size', 'status'] = 'size'
    ) -> str:
        df = df_memberships.copy()

        # Current active memberships
        now = pd.Timestamp.now()
        df = df[(df['start_date'] <= now) & (df['end_date'] >= now)]

        column_map = {
            'type': 'name',  # Full membership name
            'frequency': 'frequency',
            'size': 'size',
            'status': 'status'
        }
        group_col = column_map[group_by]

        breakdown = df[group_col].value_counts().sort_values(ascending=False)

        result = f"Current membership breakdown by {group_by}:\n\n"
        result += breakdown.to_string()
        result += f"\n\nTotal: {breakdown.sum()} memberships"

        return result

    return StructuredTool.from_function(
        name="get_membership_breakdown",
        func=get_membership_breakdown,
        description="Get breakdown of current active memberships by type, frequency, size, or status",
        args_schema=MembershipBreakdownInput
    )


class NewMembershipsInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")


def create_get_new_memberships_tool(df_memberships: pd.DataFrame):
    """Get count of new memberships in a time period."""

    def get_new_memberships(start_date: str, end_date: str) -> str:
        df = df_memberships.copy()

        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        new_members = df[(df['start_date'] >= start) & (df['start_date'] <= end)]
        count = len(new_members)

        # Breakdown by type
        by_size = new_members['size'].value_counts()

        result = f"New memberships from {start_date} to {end_date}: {count}\n\nBreakdown by size:\n"
        result += by_size.to_string()

        return result

    return StructuredTool.from_function(
        name="get_new_memberships",
        func=get_new_memberships,
        description="Get count of new memberships that started in a time period",
        args_schema=NewMembershipsInput
    )


def create_get_attrition_tool(df_memberships: pd.DataFrame):
    """Get count of memberships that ended in a time period."""

    def get_attrition(start_date: str, end_date: str) -> str:
        df = df_memberships.copy()

        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        ended_members = df[(df['end_date'] >= start) & (df['end_date'] <= end)]
        count = len(ended_members)

        # Breakdown by type
        by_size = ended_members['size'].value_counts()

        result = f"Memberships that ended from {start_date} to {end_date}: {count}\n\nBreakdown by size:\n"
        result += by_size.to_string()

        return result

    return StructuredTool.from_function(
        name="get_attrition",
        func=get_attrition,
        description="Get count of memberships that ended in a time period",
        args_schema=NewMembershipsInput
    )


# ============================================================================
# DAY PASS TOOLS
# ============================================================================

class DayPassInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    pass_type: Optional[str] = Field(None, description="Filter by pass type (e.g., 'adult', 'youth', 'adult with gear')")


def create_get_day_pass_count_tool(df_transactions: pd.DataFrame):
    """Get day pass count."""

    def get_day_pass_count(
        start_date: str,
        end_date: str,
        pass_type: Optional[str] = None
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Filter to day passes
        df = df[df['revenue_category'] == 'Day Pass']

        # Filter by pass type if provided
        if pass_type:
            df = df[df['sub_category'] == pass_type]

        total_passes = df['Day Pass Count'].sum()

        # Breakdown by type
        by_type = df.groupby('sub_category')['Day Pass Count'].sum().sort_values(ascending=False)

        filter_str = f" ({pass_type})" if pass_type else ""
        result = f"Day passes{filter_str} from {start_date} to {end_date}: {int(total_passes)}\n\n"

        if not pass_type:
            result += "Breakdown by type:\n"
            result += by_type.to_string()

        return result

    return StructuredTool.from_function(
        name="get_day_pass_count",
        func=get_day_pass_count,
        description="Get count of day passes sold in a time period, optionally filtered by pass type",
        args_schema=DayPassInput
    )


def create_get_day_pass_revenue_tool(df_transactions: pd.DataFrame):
    """Get day pass revenue."""

    def get_day_pass_revenue(
        start_date: str,
        end_date: str,
        pass_type: Optional[str] = None
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Filter to day passes
        df = df[df['revenue_category'] == 'Day Pass']

        # Filter by pass type if provided
        if pass_type:
            df = df[df['sub_category'] == pass_type]

        total_revenue = df['Total Amount'].sum()
        total_passes = df['Day Pass Count'].sum()

        filter_str = f" ({pass_type})" if pass_type else ""
        result = f"Day pass revenue{filter_str} from {start_date} to {end_date}:\n"
        result += f"- Total revenue: ${total_revenue:,.2f}\n"
        result += f"- Total passes: {int(total_passes)}\n"
        result += f"- Average per pass: ${total_revenue/total_passes:.2f}" if total_passes > 0 else ""

        return result

    return StructuredTool.from_function(
        name="get_day_pass_revenue",
        func=get_day_pass_revenue,
        description="Get day pass revenue for a time period, optionally filtered by pass type",
        args_schema=DayPassInput
    )


# ============================================================================
# VISUALIZATION TOOLS
# ============================================================================

class RevenueTimeseriesInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    grouping: Literal['day', 'week', 'month'] = Field('day', description="Time grouping: 'day', 'week', or 'month'")
    category: Optional[str] = Field(None, description="Filter by revenue category")


def create_revenue_timeseries_chart_tool(df_transactions: pd.DataFrame):
    """Create a line chart showing revenue over time."""

    def create_revenue_timeseries(
        start_date: str,
        end_date: str,
        grouping: Literal['day', 'week', 'month'] = 'day',
        category: Optional[str] = None
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Filter by category if provided
        if category:
            df = df[df['revenue_category'] == category]

        # Convert Date to datetime if it's not already
        df['Date'] = pd.to_datetime(df['Date'])

        # Group by time period
        if grouping == 'day':
            df_grouped = df.groupby(df['Date'].dt.date)['Total Amount'].sum().reset_index()
            df_grouped.columns = ['Date', 'Revenue']
        elif grouping == 'week':
            df['Week'] = df['Date'].dt.to_period('W').dt.start_time
            df_grouped = df.groupby('Week')['Total Amount'].sum().reset_index()
            df_grouped.columns = ['Date', 'Revenue']
        elif grouping == 'month':
            df['Month'] = df['Date'].dt.to_period('M').dt.start_time
            df_grouped = df.groupby('Month')['Total Amount'].sum().reset_index()
            df_grouped.columns = ['Date', 'Revenue']

        # Create chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_grouped['Date'],
            y=df_grouped['Revenue'],
            mode='lines+markers',
            name='Revenue',
            line=dict(color='#2ecc71', width=2),
            marker=dict(size=6)
        ))

        title = f"Revenue Over Time ({grouping.capitalize()})"
        if category:
            title += f" - {category}"

        fig.update_layout(
            title=title,
            xaxis_title='Date',
            yaxis_title='Revenue ($)',
            hovermode='x unified',
            template='plotly_white'
        )

        # Save chart
        os.makedirs('agent/charts', exist_ok=True)
        filename = f"agent/charts/revenue_timeseries_{grouping}_{start_date}_{end_date}.html"
        fig.write_html(filename)

        total = df_grouped['Revenue'].sum()
        avg = df_grouped['Revenue'].mean()

        return f"Revenue timeseries chart created and saved to {filename}\n\nSummary:\n- Total revenue: ${total:,.2f}\n- Average per {grouping}: ${avg:,.2f}\n- Data points: {len(df_grouped)}"

    return StructuredTool.from_function(
        name="create_revenue_timeseries_chart",
        func=create_revenue_timeseries,
        description="Create a line chart showing revenue over time, grouped by day/week/month",
        args_schema=RevenueTimeseriesInput
    )


class RevenueCategoryChartInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    chart_type: Literal['bar', 'pie'] = Field('bar', description="Chart type: 'bar' or 'pie'")


def create_revenue_category_chart_tool(df_transactions: pd.DataFrame):
    """Create a chart showing revenue breakdown by category."""

    def create_revenue_category_chart(
        start_date: str,
        end_date: str,
        chart_type: Literal['bar', 'pie'] = 'bar'
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Group by category
        category_revenue = df.groupby('revenue_category')['Total Amount'].sum().sort_values(ascending=False)

        # Create chart
        if chart_type == 'bar':
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=category_revenue.index,
                y=category_revenue.values,
                marker_color='#3498db'
            ))
            fig.update_layout(
                title=f"Revenue by Category ({start_date} to {end_date})",
                xaxis_title='Category',
                yaxis_title='Revenue ($)',
                template='plotly_white'
            )
        else:  # pie
            fig = go.Figure()
            fig.add_trace(go.Pie(
                labels=category_revenue.index,
                values=category_revenue.values,
                hole=0.3
            ))
            fig.update_layout(
                title=f"Revenue by Category ({start_date} to {end_date})",
                template='plotly_white'
            )

        # Save chart
        os.makedirs('agent/charts', exist_ok=True)
        filename = f"agent/charts/revenue_category_{chart_type}_{start_date}_{end_date}.html"
        fig.write_html(filename)

        total = category_revenue.sum()
        top_category = category_revenue.index[0]
        top_amount = category_revenue.values[0]

        return f"Revenue category {chart_type} chart created and saved to {filename}\n\nSummary:\n- Total revenue: ${total:,.2f}\n- Top category: {top_category} (${top_amount:,.2f}, {top_amount/total*100:.1f}%)\n- Categories: {len(category_revenue)}"

    return StructuredTool.from_function(
        name="create_revenue_category_chart",
        func=create_revenue_category_chart,
        description="Create a bar or pie chart showing revenue breakdown by category",
        args_schema=RevenueCategoryChartInput
    )


class MembershipTrendChartInput(BaseModel):
    months_back: int = Field(12, description="Number of months to look back (default 12)")


def create_membership_trend_chart_tool(df_memberships: pd.DataFrame):
    """Create a chart showing membership trends (new vs attrition)."""

    def create_membership_trend_chart(months_back: int = 12) -> str:
        df = df_memberships.copy()

        # Calculate date range
        end_date = pd.Timestamp.now()
        start_date = end_date - pd.DateOffset(months=months_back)

        # Create monthly buckets
        months = pd.date_range(start=start_date, end=end_date, freq='MS')

        new_counts = []
        attrition_counts = []

        for month in months:
            month_end = month + pd.offsets.MonthEnd(0)

            # New memberships
            new = df[(df['start_date'] >= month) & (df['start_date'] <= month_end)]
            new_counts.append(len(new))

            # Attrition
            ended = df[(df['end_date'] >= month) & (df['end_date'] <= month_end)]
            attrition_counts.append(len(ended))

        # Create chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=months,
            y=new_counts,
            mode='lines+markers',
            name='New Memberships',
            line=dict(color='#2ecc71', width=2),
            marker=dict(size=8)
        ))
        fig.add_trace(go.Scatter(
            x=months,
            y=attrition_counts,
            mode='lines+markers',
            name='Attrition',
            line=dict(color='#e74c3c', width=2),
            marker=dict(size=8)
        ))

        fig.update_layout(
            title=f"Membership Trends (Last {months_back} Months)",
            xaxis_title='Month',
            yaxis_title='Count',
            hovermode='x unified',
            template='plotly_white',
            legend=dict(x=0.01, y=0.99)
        )

        # Save chart
        os.makedirs('agent/charts', exist_ok=True)
        filename = f"agent/charts/membership_trends_{months_back}mo.html"
        fig.write_html(filename)

        total_new = sum(new_counts)
        total_attrition = sum(attrition_counts)
        net_growth = total_new - total_attrition

        return f"Membership trend chart created and saved to {filename}\n\nSummary ({months_back} months):\n- Total new memberships: {total_new}\n- Total attrition: {total_attrition}\n- Net growth: {net_growth:+d}\n- Average new per month: {total_new/months_back:.1f}\n- Average attrition per month: {total_attrition/months_back:.1f}"

    return StructuredTool.from_function(
        name="create_membership_trend_chart",
        func=create_membership_trend_chart,
        description="Create a line chart showing new memberships vs attrition over time",
        args_schema=MembershipTrendChartInput
    )


class DayPassChartInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")


def create_day_pass_breakdown_chart_tool(df_transactions: pd.DataFrame):
    """Create a chart showing day pass breakdown by type."""

    def create_day_pass_breakdown_chart(
        start_date: str,
        end_date: str
    ) -> str:
        df = df_transactions.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['Date'] >= start) & (df['Date'] <= end)]

        # Filter to day passes
        df = df[df['revenue_category'] == 'Day Pass']

        # Group by type
        pass_counts = df.groupby('sub_category')['Day Pass Count'].sum().sort_values(ascending=False)

        # Create chart
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=pass_counts.index,
            y=pass_counts.values,
            marker_color='#9b59b6'
        ))

        fig.update_layout(
            title=f"Day Pass Breakdown ({start_date} to {end_date})",
            xaxis_title='Pass Type',
            yaxis_title='Count',
            template='plotly_white'
        )

        # Save chart
        os.makedirs('agent/charts', exist_ok=True)
        filename = f"agent/charts/day_pass_breakdown_{start_date}_{end_date}.html"
        fig.write_html(filename)

        total = pass_counts.sum()
        top_type = pass_counts.index[0]
        top_count = pass_counts.values[0]

        return f"Day pass breakdown chart created and saved to {filename}\n\nSummary:\n- Total day passes: {int(total)}\n- Most popular: {top_type} ({int(top_count)} passes, {top_count/total*100:.1f}%)\n- Pass types: {len(pass_counts)}"

    return StructuredTool.from_function(
        name="create_day_pass_breakdown_chart",
        func=create_day_pass_breakdown_chart,
        description="Create a bar chart showing day pass counts by type",
        args_schema=DayPassChartInput
    )


# ============================================================================
# CREATE ALL TOOLS
# ============================================================================

def create_all_tools():
    """Create all analytical tools with loaded data."""
    print("Loading data from S3...")
    df_transactions, df_memberships, df_members = load_data_frames()
    print(f"Loaded {len(df_transactions)} transactions, {len(df_memberships)} memberships, {len(df_members)} members")

    tools = [
        # Revenue tools
        create_get_total_revenue_tool(df_transactions),
        create_get_revenue_breakdown_tool(df_transactions),

        # Membership tools
        create_get_member_count_tool(df_memberships, df_members),
        create_get_membership_breakdown_tool(df_memberships),
        create_get_new_memberships_tool(df_memberships),
        create_get_attrition_tool(df_memberships),

        # Day pass tools
        create_get_day_pass_count_tool(df_transactions),
        create_get_day_pass_revenue_tool(df_transactions),

        # Visualization tools
        create_revenue_timeseries_chart_tool(df_transactions),
        create_revenue_category_chart_tool(df_transactions),
        create_membership_trend_chart_tool(df_memberships),
        create_day_pass_breakdown_chart_tool(df_transactions),
    ]

    return tools

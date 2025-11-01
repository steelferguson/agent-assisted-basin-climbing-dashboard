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

# Chart output directory
CHART_OUTPUT_DIR = "outputs/charts"


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

    # Instagram data
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_instagram_posts)
        df_instagram_posts = uploader.convert_csv_to_df(csv_content)
        df_instagram_posts['timestamp'] = pd.to_datetime(df_instagram_posts['timestamp'], errors='coerce')

        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_instagram_comments)
        df_instagram_comments = uploader.convert_csv_to_df(csv_content)
        df_instagram_comments['timestamp'] = pd.to_datetime(df_instagram_comments['timestamp'], errors='coerce')
    except Exception as e:
        print(f"Warning: Could not load Instagram data: {e}")
        df_instagram_posts = pd.DataFrame()
        df_instagram_comments = pd.DataFrame()

    return df_transactions, df_memberships, df_members, df_instagram_posts, df_instagram_comments


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


class RevenueByTimePeriodInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    period: Literal['day', 'week', 'month'] = Field(
        'month',
        description="Time period to group by: 'day', 'week', or 'month'"
    )
    category: Optional[str] = Field(None, description="Optional: filter by revenue category")


def create_get_revenue_by_time_period_tool(df_transactions: pd.DataFrame):
    """Get revenue grouped by time period (day/week/month)."""

    def get_revenue_by_time_period(
        start_date: str,
        end_date: str,
        period: Literal['day', 'week', 'month'] = 'month',
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

        # Group by time period
        if period == 'day':
            df['Period'] = df['Date'].dt.date
        elif period == 'week':
            df['Period'] = df['Date'].dt.to_period('W').dt.start_time
        elif period == 'month':
            df['Period'] = df['Date'].dt.to_period('M').astype(str)

        breakdown = df.groupby('Period').agg({
            'Total Amount': ['sum', 'count']
        }).round(2)

        breakdown.columns = ['Revenue', 'Transactions']
        breakdown = breakdown.sort_index()

        # Find highest revenue period
        max_period = breakdown['Revenue'].idxmax()
        max_revenue = breakdown['Revenue'].max()

        filter_str = f" (category: {category})" if category else ""
        result = f"Revenue by {period}{filter_str} from {start_date} to {end_date}:\n\n"
        result += breakdown.to_string()
        result += f"\n\nTotal Revenue: ${breakdown['Revenue'].sum():,.2f}"
        result += f"\nTotal Transactions: {int(breakdown['Transactions'].sum()):,}"
        result += f"\nHighest {period.capitalize()}: {max_period} (${max_revenue:,.2f})"

        return result

    return StructuredTool.from_function(
        name="get_revenue_by_time_period",
        func=get_revenue_by_time_period,
        description="Get revenue grouped by time period (day/week/month). Perfect for finding which month/week/day had highest revenue.",
        args_schema=RevenueByTimePeriodInput
    )


class RevenueChangesInput(BaseModel):
    periods: int = Field(2, description="Number of periods to compare (e.g., 2 for last 2 months, 3 for last 3 months)")
    period_type: Literal['month', 'week', 'day'] = Field('month', description="Type of period: 'month', 'week', or 'day'")
    category: Optional[str] = Field(None, description="Optional: filter to a specific revenue category (e.g., 'Day Pass')")
    end_date: Optional[str] = Field(None, description="Optional: end date for comparison in YYYY-MM-DD format (defaults to today)")


def create_get_revenue_changes_tool(df_transactions: pd.DataFrame):
    """Compare revenue changes across N periods to identify growth and declines."""

    def get_revenue_changes(
        periods: int = 2,
        period_type: Literal['month', 'week', 'day'] = 'month',
        category: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> str:
        df = df_transactions.copy()

        # Determine end date
        if end_date:
            end = pd.to_datetime(end_date)
        else:
            end = pd.Timestamp.now()

        # Calculate period ranges
        period_ranges = []
        for i in range(periods):
            if period_type == 'month':
                period_end = end - pd.DateOffset(months=i)
                period_start = period_end - pd.DateOffset(months=1) + pd.Timedelta(days=1)
                period_label = period_end.strftime('%Y-%m')
            elif period_type == 'week':
                period_end = end - pd.DateOffset(weeks=i)
                period_start = period_end - pd.Timedelta(days=7)
                period_label = f"Week of {period_start.strftime('%Y-%m-%d')}"
            else:  # day
                period_end = end - pd.Timedelta(days=i)
                period_start = period_end
                period_label = period_end.strftime('%Y-%m-%d')

            period_ranges.append({
                'label': period_label,
                'start': period_start,
                'end': period_end
            })

        # Reverse so we go from oldest to newest
        period_ranges = list(reversed(period_ranges))

        # Filter by category if provided
        if category:
            df = df[df['revenue_category'] == category]
            if df.empty:
                return f"No data found for category '{category}'"

        # Calculate revenue by subcategory for each period
        subcategory_data = {}
        for period in period_ranges:
            period_df = df[(df['Date'] >= period['start']) & (df['Date'] <= period['end'])]
            revenue_by_sub = period_df.groupby('sub_category')['Total Amount'].sum()

            for sub, rev in revenue_by_sub.items():
                if sub not in subcategory_data:
                    subcategory_data[sub] = []
                subcategory_data[sub].append(rev)

        # Build result
        category_str = f" ({category})" if category else ""
        result = f"Revenue Changes by Subcategory{category_str} (Last {periods} {period_type}s):\n\n"

        # Calculate overall changes and sort by biggest absolute change
        changes = []
        for sub, revenues in subcategory_data.items():
            # Pad with zeros if subcategory didn't exist in all periods
            while len(revenues) < periods:
                revenues.insert(0, 0)

            first_rev = revenues[0]
            last_rev = revenues[-1]
            change_abs = last_rev - first_rev
            change_pct = ((change_abs / first_rev) * 100) if first_rev > 0 else 0

            changes.append({
                'subcategory': sub,
                'revenues': revenues,
                'change_abs': change_abs,
                'change_pct': change_pct
            })

        # Sort by absolute change (largest first, whether positive or negative)
        changes.sort(key=lambda x: abs(x['change_abs']), reverse=True)

        # Display each subcategory
        for item in changes:
            sub = item['subcategory']
            revenues = item['revenues']

            result += f"📊 {sub}:\n"
            for i, (period, rev) in enumerate(zip(period_ranges, revenues)):
                result += f"   {period['label']}: ${rev:,.2f}"

                # Show change from previous period
                if i > 0:
                    prev_rev = revenues[i-1]
                    change = rev - prev_rev
                    pct = ((change / prev_rev) * 100) if prev_rev > 0 else 0
                    symbol = "+" if change >= 0 else ""
                    result += f" ({symbol}${change:,.2f}, {symbol}{pct:.1f}%)"

                result += "\n"

            # Overall change
            symbol = "+" if item['change_abs'] >= 0 else ""
            result += f"   Overall: {symbol}${item['change_abs']:,.2f} ({symbol}{item['change_pct']:.1f}%)\n\n"

        # Top 3 growers
        growers = sorted([c for c in changes if c['change_abs'] > 0],
                        key=lambda x: x['change_abs'], reverse=True)[:3]
        if growers:
            result += "🚀 Top Growers:\n"
            for i, item in enumerate(growers, 1):
                result += f"{i}. {item['subcategory']}: +${item['change_abs']:,.2f} (+{item['change_pct']:.1f}%)\n"
            result += "\n"

        # Top 3 decliners
        decliners = sorted([c for c in changes if c['change_abs'] < 0],
                          key=lambda x: x['change_abs'])[:3]
        if decliners:
            result += "📉 Top Decliners:\n"
            for i, item in enumerate(decliners, 1):
                result += f"{i}. {item['subcategory']}: ${item['change_abs']:,.2f} ({item['change_pct']:.1f}%)\n"

        return result.strip()

    return StructuredTool.from_function(
        name="get_revenue_changes",
        func=get_revenue_changes,
        description="Compare revenue changes across multiple periods (months/weeks/days) to identify which subcategories are growing or declining. Perfect for 'which subcategories grew the most' questions.",
        args_schema=RevenueChangesInput
    )


class AvailableCategoriesInput(BaseModel):
    category: Optional[str] = Field(None, description="Optional: filter to show subcategories for a specific category only")


def create_get_available_categories_tool(df_transactions: pd.DataFrame):
    """List all available revenue categories and subcategories."""

    def get_available_categories(category: Optional[str] = None) -> str:
        df = df_transactions.copy()

        if category:
            # Show subcategories for a specific category
            category_df = df[df['revenue_category'] == category]
            if category_df.empty:
                available = df['revenue_category'].unique().tolist()
                return f"Category '{category}' not found. Available categories: {', '.join(available)}"

            subcategories = category_df['sub_category'].dropna().unique().tolist()
            result = f"Subcategories for '{category}':\n"
            for sub in sorted(subcategories):
                count = len(category_df[category_df['sub_category'] == sub])
                revenue = category_df[category_df['sub_category'] == sub]['Total Amount'].sum()
                result += f"  - {sub}: {count} transactions (${revenue:,.2f})\n"
            return result.strip()
        else:
            # Show all categories with their subcategories
            result = "Available Revenue Categories and Subcategories:\n\n"

            categories = df['revenue_category'].dropna().unique()
            for cat in sorted(categories):
                cat_df = df[df['revenue_category'] == cat]
                total_revenue = cat_df['Total Amount'].sum()
                result += f"📊 {cat} (${total_revenue:,.2f} total)\n"

                subcategories = cat_df['sub_category'].dropna().unique()
                if len(subcategories) > 0:
                    for sub in sorted(subcategories):
                        sub_df = cat_df[cat_df['sub_category'] == sub]
                        sub_revenue = sub_df['Total Amount'].sum()
                        result += f"   └─ {sub}: ${sub_revenue:,.2f}\n"
                else:
                    result += "   └─ (no subcategories)\n"
                result += "\n"

            return result.strip()

    return StructuredTool.from_function(
        name="get_available_categories",
        func=get_available_categories,
        description="Discover all available revenue categories and subcategories in the data. Use this to know what categories/subcategories exist before filtering.",
        args_schema=AvailableCategoriesInput
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

        # Save chart with timestamp
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{CHART_OUTPUT_DIR}/{timestamp}_revenue_timeseries_{grouping}_{start_date}_{end_date}.html"
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

        # Save chart with timestamp
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{CHART_OUTPUT_DIR}/{timestamp}_revenue_category_{chart_type}_{start_date}_{end_date}.html"
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

        # Save chart with timestamp
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{CHART_OUTPUT_DIR}/{timestamp}_membership_trends_{months_back}mo.html"
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

        # Save chart with timestamp
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{CHART_OUTPUT_DIR}/{timestamp}_day_pass_breakdown_{start_date}_{end_date}.html"
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
# INSTAGRAM TOOLS
# ============================================================================

class InstagramPostsInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    sort_by: Optional[Literal['likes', 'comments', 'reach', 'engagement_rate']] = Field(
        'engagement_rate',
        description="Sort posts by metric (default: engagement_rate)"
    )
    limit: Optional[int] = Field(10, description="Number of top posts to return (default: 10)")


def create_get_top_instagram_posts_tool(df_posts: pd.DataFrame):
    """Get top performing Instagram posts by engagement."""

    def get_top_instagram_posts(
        start_date: str,
        end_date: str,
        sort_by: Literal['likes', 'comments', 'reach', 'engagement_rate'] = 'engagement_rate',
        limit: int = 10
    ) -> str:
        if df_posts.empty:
            return "No Instagram data available. Please upload Instagram data first."

        df = df_posts.copy()

        # Filter by date (make timezone-aware for comparison)
        start = pd.to_datetime(start_date).tz_localize('UTC')
        end = pd.to_datetime(end_date).tz_localize('UTC')
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

        if df.empty:
            return f"No Instagram posts found between {start_date} and {end_date}"

        # Sort by metric
        df = df.sort_values(sort_by, ascending=False).head(limit)

        result = f"Top {limit} Instagram posts by {sort_by} ({start_date} to {end_date}):\n\n"

        for i, row in df.iterrows():
            post_date = row['timestamp'].strftime('%Y-%m-%d')
            caption_preview = row['caption'][:60] + '...' if len(str(row['caption'])) > 60 else row['caption']

            result += f"{post_date} | Likes: {row['likes']} | Comments: {row['comments']} | Reach: {row['reach']}\n"
            result += f"  Caption: {caption_preview}\n"
            result += f"  Link: {row['permalink']}\n\n"

        return result

    return StructuredTool.from_function(
        name="get_top_instagram_posts",
        func=get_top_instagram_posts,
        description="Get top performing Instagram posts sorted by likes, comments, reach, or engagement rate",
        args_schema=InstagramPostsInput
    )


class InstagramEngagementInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")


def create_get_instagram_engagement_summary_tool(df_posts: pd.DataFrame, df_comments: pd.DataFrame):
    """Get overall Instagram engagement summary."""

    def get_instagram_engagement_summary(start_date: str, end_date: str) -> str:
        if df_posts.empty:
            return "No Instagram data available. Please upload Instagram data first."

        df = df_posts.copy()

        # Filter by date (make timezone-aware for comparison)
        start = pd.to_datetime(start_date).tz_localize('UTC')
        end = pd.to_datetime(end_date).tz_localize('UTC')
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

        if df.empty:
            return f"No Instagram posts found between {start_date} and {end_date}"

        # Calculate metrics
        total_posts = len(df)
        total_likes = df['likes'].sum()
        total_comments = df['comments'].sum()
        total_reach = df['reach'].dropna().sum()
        total_saved = df['saved'].dropna().sum()
        avg_engagement_rate = df['engagement_rate'].dropna().mean()

        # Comment metrics
        df_comments_filtered = df_comments[
            (df_comments['timestamp'] >= start) & (df_comments['timestamp'] <= end)
        ]
        total_comment_count = len(df_comments_filtered)
        unique_commenters = df_comments_filtered['username'].nunique() if not df_comments_filtered.empty else 0

        result = f"Instagram Engagement Summary ({start_date} to {end_date}):\n\n"
        result += f"Posts: {total_posts}\n"
        result += f"Total Likes: {int(total_likes):,}\n"
        result += f"Total Comments: {int(total_comments):,}\n"
        result += f"Total Reach: {int(total_reach):,}\n"
        result += f"Total Saved: {int(total_saved):,}\n"
        result += f"Avg Engagement Rate: {avg_engagement_rate:.2f}%\n\n"
        result += f"Average per post:\n"
        result += f"  - Likes: {total_likes/total_posts:.1f}\n"
        result += f"  - Comments: {total_comments/total_posts:.1f}\n"
        result += f"  - Reach: {total_reach/total_posts:.1f}\n\n"
        result += f"Comment Analysis:\n"
        result += f"  - Unique commenters: {unique_commenters}\n"

        return result

    return StructuredTool.from_function(
        name="get_instagram_engagement_summary",
        func=get_instagram_engagement_summary,
        description="Get overall Instagram engagement metrics including likes, comments, reach, and saves",
        args_schema=InstagramEngagementInput
    )


class InstagramContentAnalysisInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")


def create_get_instagram_content_themes_tool(df_posts: pd.DataFrame):
    """Analyze Instagram content themes from AI descriptions."""

    def get_instagram_content_themes(start_date: str, end_date: str) -> str:
        if df_posts.empty:
            return "No Instagram data available. Please upload Instagram data first."

        df = df_posts.copy()

        # Filter by date (make timezone-aware for comparison)
        start = pd.to_datetime(start_date).tz_localize('UTC')
        end = pd.to_datetime(end_date).tz_localize('UTC')
        df = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

        if df.empty:
            return f"No Instagram posts found between {start_date} and {end_date}"

        # Analyze AI themes
        if 'ai_themes' not in df.columns or df['ai_themes'].isna().all():
            return f"No AI theme analysis available for posts between {start_date} and {end_date}. Run fetch with enable_vision_analysis=True to analyze themes."

        # Collect all themes
        all_themes = []
        for themes_str in df['ai_themes'].dropna():
            themes = [t.strip() for t in str(themes_str).split(',')]
            all_themes.extend(themes)

        # Count theme frequency
        from collections import Counter
        theme_counts = Counter(all_themes)

        # Analyze activity types
        activity_counts = df['ai_activity_type'].value_counts()

        result = f"Instagram Content Analysis ({start_date} to {end_date}):\n\n"
        result += f"Total posts analyzed: {len(df)}\n\n"

        result += "Top Content Themes:\n"
        for theme, count in theme_counts.most_common(10):
            pct = count / len(df) * 100
            result += f"  - {theme}: {count} posts ({pct:.1f}%)\n"

        result += "\nActivity Types:\n"
        for activity, count in activity_counts.head(5).items():
            pct = count / len(df) * 100
            result += f"  - {activity}: {count} posts ({pct:.1f}%)\n"

        return result

    return StructuredTool.from_function(
        name="get_instagram_content_themes",
        func=get_instagram_content_themes,
        description="Analyze Instagram content themes and activity types from AI-generated descriptions",
        args_schema=InstagramContentAnalysisInput
    )


# ============================================================================
# CREATE ALL TOOLS
# ============================================================================

class InstagramRevenueCorrelationInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    revenue_category: Optional[str] = Field(None, description="Optional: filter to a specific revenue category (e.g., 'Day Pass')")
    top_n: int = Field(5, description="Number of top revenue days to show")


def create_get_instagram_revenue_correlation_tool(df_posts: pd.DataFrame, df_transactions: pd.DataFrame):
    """Correlate Instagram posts with revenue to see which posts were on high revenue days."""

    def get_instagram_revenue_correlation(
        start_date: str,
        end_date: str,
        revenue_category: Optional[str] = None,
        top_n: int = 5
    ) -> str:
        if df_posts.empty:
            return "No Instagram data available. Please upload Instagram data first."

        # Filter Instagram posts by date
        posts_df = df_posts.copy()
        start = pd.to_datetime(start_date).tz_localize('UTC')
        end = pd.to_datetime(end_date).tz_localize('UTC')
        posts_df = posts_df[(posts_df['timestamp'] >= start) & (posts_df['timestamp'] <= end)]

        if posts_df.empty:
            return f"No Instagram posts found between {start_date} and {end_date}"

        # Get revenue by day
        revenue_df = df_transactions.copy()
        start_local = pd.to_datetime(start_date)
        end_local = pd.to_datetime(end_date)
        revenue_df = revenue_df[(revenue_df['Date'] >= start_local) & (revenue_df['Date'] <= end_local)]

        if revenue_category:
            revenue_df = revenue_df[revenue_df['revenue_category'] == revenue_category]

        # Group revenue by day
        daily_revenue = revenue_df.groupby(revenue_df['Date'].dt.date)['Total Amount'].sum().sort_values(ascending=False)

        if daily_revenue.empty:
            filter_str = f" ({revenue_category})" if revenue_category else ""
            return f"No revenue data found{filter_str} between {start_date} and {end_date}"

        # Get top N revenue days
        top_days = daily_revenue.head(top_n)

        # Build result
        category_str = f" ({revenue_category})" if revenue_category else ""
        result = f"Instagram Posts on High Revenue Days{category_str}:\n"
        result += f"Period: {start_date} to {end_date}\n\n"

        for day, revenue in top_days.items():
            result += f"{'='*70}\n"
            result += f"📅 {day} - Revenue: ${revenue:,.2f}\n"
            result += f"{'='*70}\n"

            # Find posts on this day
            day_start = pd.Timestamp(day).tz_localize('UTC')
            day_end = day_start + pd.Timedelta(days=1)
            day_posts = posts_df[(posts_df['timestamp'] >= day_start) & (posts_df['timestamp'] < day_end)]

            if len(day_posts) > 0:
                result += f"📸 {len(day_posts)} Instagram post(s) on this day:\n\n"
                for _, post in day_posts.iterrows():
                    result += f"  • Post ID: {post['post_id']}\n"
                    result += f"    Time: {post['timestamp'].strftime('%I:%M %p')}\n"
                    if pd.notna(post.get('caption')):
                        caption_preview = str(post['caption'])[:100] + "..." if len(str(post['caption'])) > 100 else str(post['caption'])
                        result += f"    Caption: {caption_preview}\n"

                    # Handle NaN values in metrics
                    likes = int(post['likes']) if pd.notna(post.get('likes')) else 0
                    comments = int(post['comments']) if pd.notna(post.get('comments')) else 0
                    reach = int(post['reach']) if pd.notna(post.get('reach')) else 0
                    engagement_rate = post['engagement_rate'] if pd.notna(post.get('engagement_rate')) else 0.0

                    result += f"    Likes: {likes:,} | Comments: {comments:,} | Reach: {reach:,}\n"
                    result += f"    Engagement Rate: {engagement_rate:.2f}%\n"
                    if pd.notna(post.get('ai_description')):
                        result += f"    AI Summary: {post['ai_description']}\n"
                    result += "\n"
            else:
                result += "  ⚠️ No Instagram posts on this day\n\n"

        # Summary stats
        days_with_posts = 0
        days_without_posts = 0
        for day in top_days.index:
            day_start = pd.Timestamp(day).tz_localize('UTC')
            day_end = day_start + pd.Timedelta(days=1)
            day_posts = posts_df[(posts_df['timestamp'] >= day_start) & (posts_df['timestamp'] < day_end)]
            if len(day_posts) > 0:
                days_with_posts += 1
            else:
                days_without_posts += 1

        result += f"\n📊 Summary:\n"
        result += f"Top {top_n} revenue days: {days_with_posts} had Instagram posts, {days_without_posts} did not\n"

        return result.strip()

    return StructuredTool.from_function(
        name="get_instagram_revenue_correlation",
        func=get_instagram_revenue_correlation,
        description="Find Instagram posts that were posted on high revenue days. Perfect for questions like 'which Instagram posts were on days with high day pass sales'.",
        args_schema=InstagramRevenueCorrelationInput
    )


def create_all_tools():
    """Create all analytical tools with loaded data."""
    print("Loading data from S3...")
    df_transactions, df_memberships, df_members, df_instagram_posts, df_instagram_comments = load_data_frames()
    print(f"Loaded {len(df_transactions)} transactions, {len(df_memberships)} memberships, {len(df_members)} members")

    if not df_instagram_posts.empty:
        print(f"Loaded {len(df_instagram_posts)} Instagram posts, {len(df_instagram_comments)} comments")

    tools = [
        # Revenue tools
        create_get_total_revenue_tool(df_transactions),
        create_get_revenue_breakdown_tool(df_transactions),
        create_get_revenue_by_time_period_tool(df_transactions),
        create_get_revenue_changes_tool(df_transactions),
        create_get_available_categories_tool(df_transactions),

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

    # Add Instagram tools if data is available
    if not df_instagram_posts.empty:
        tools.extend([
            create_get_top_instagram_posts_tool(df_instagram_posts),
            create_get_instagram_engagement_summary_tool(df_instagram_posts, df_instagram_comments),
            create_get_instagram_content_themes_tool(df_instagram_posts),
            create_get_instagram_revenue_correlation_tool(df_instagram_posts, df_transactions),
        ])

    return tools

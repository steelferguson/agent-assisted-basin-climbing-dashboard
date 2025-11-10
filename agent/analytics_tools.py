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
import uuid

# Chart output directory
CHART_OUTPUT_DIR = "outputs/charts"

# Global data registry for storing custom query results
# Format: {data_id: {'dataframe': pd.DataFrame, 'description': str, 'timestamp': datetime}}
_DATA_REGISTRY = {}


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

    # Facebook Ads data
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_facebook_ads)
        df_facebook_ads = uploader.convert_csv_to_df(csv_content)
        df_facebook_ads['date'] = pd.to_datetime(df_facebook_ads['date'], errors='coerce')
    except Exception as e:
        print(f"Warning: Could not load Facebook Ads data: {e}")
        df_facebook_ads = pd.DataFrame()

    # Capitan Check-in data
    try:
        csv_content = uploader.download_from_s3(config.aws_bucket_name, config.s3_path_capitan_checkins)
        df_checkins = uploader.convert_csv_to_df(csv_content)
        df_checkins['checkin_datetime'] = pd.to_datetime(df_checkins['checkin_datetime'], errors='coerce')
        df_checkins['customer_birthday'] = pd.to_datetime(df_checkins['customer_birthday'], errors='coerce')
    except Exception as e:
        print(f"Warning: Could not load check-in data: {e}")
        df_checkins = pd.DataFrame()

    return df_transactions, df_memberships, df_members, df_instagram_posts, df_instagram_comments, df_facebook_ads, df_checkins


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
        description="Get count of day pass TRANSACTIONS (not unique people) in a time period. NOTE: This counts transaction records, not unique customers. One person can have multiple transactions. To count unique people, you need check-in data with customer_id.",
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
        description="Get day pass revenue from TRANSACTION records (not unique people). NOTE: This analyzes payment transactions, not unique customers. One person can have multiple transactions. To analyze unique people, you need check-in data with customer_id.",
        args_schema=DayPassInput
    )


class UniqueDayPassCustomersInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    pass_count: Optional[int] = Field(None, description="Filter to customers who bought exactly this many passes (e.g., 1 for 'only one pass'). Leave empty to see all counts.")


def create_get_unique_day_pass_customers_tool(df_checkins: pd.DataFrame):
    """Get unique customers who purchased day passes, grouped by how many passes they bought."""

    def get_unique_day_pass_customers(
        start_date: str,
        end_date: str,
        pass_count: Optional[int] = None
    ) -> str:
        """
        Analyze UNIQUE CUSTOMERS (not transactions) who bought day passes.
        Groups customers by how many day passes they purchased.
        """
        df = df_checkins.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df['checkin_datetime'] = pd.to_datetime(df['checkin_datetime'])
        df = df[(df['checkin_datetime'] >= start) & (df['checkin_datetime'] <= end)]

        # Filter to day pass check-ins (not membership check-ins)
        day_pass_keywords = ['Day Pass', 'Punch Pass', 'Pass with Gear']
        mask = df['entry_method_description'].str.contains(
            '|'.join(day_pass_keywords),
            case=False,
            na=False,
            regex=True
        )
        df_day_passes = df[mask].copy()

        # Count how many day passes each unique customer bought
        customer_pass_counts = df_day_passes.groupby('customer_id').size()

        # If user wants specific count, filter to that
        if pass_count is not None:
            customers_with_count = customer_pass_counts[customer_pass_counts == pass_count]
            result = f"Unique customers who bought exactly {pass_count} day pass(es) from {start_date} to {end_date}: {len(customers_with_count)}\n\n"
            result += f"Total day pass check-ins: {len(df_day_passes)}\n"
            result += f"Total unique customers with day passes: {len(customer_pass_counts)}\n"
            return result

        # Otherwise, show breakdown by count
        count_distribution = customer_pass_counts.value_counts().sort_index()

        result = f"Unique day pass customers from {start_date} to {end_date}:\n\n"
        result += f"Total unique customers: {len(customer_pass_counts)}\n"
        result += f"Total day pass check-ins: {len(df_day_passes)}\n\n"
        result += "Distribution by number of passes purchased per customer:\n"

        for count, num_customers in count_distribution.items():
            result += f"  {count} pass(es): {num_customers} customers\n"

        return result

    return StructuredTool.from_function(
        name="get_unique_day_pass_customers",
        func=get_unique_day_pass_customers,
        description="Analyze UNIQUE CUSTOMERS (people) who bought day passes, NOT transactions. Use this to answer questions like 'how many PEOPLE bought exactly 1 day pass'. Groups customers by how many passes they purchased. Uses check-in data with customer_id to count unique people.",
        args_schema=UniqueDayPassCustomersInput
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


class InstagramPostsChartInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    grouping: Literal['day', 'week', 'month'] = Field(
        'week',
        description="Time period to group posts by (default: week)"
    )
    metric: Literal['post_count', 'likes', 'comments', 'reach', 'engagement_rate'] = Field(
        'post_count',
        description="Metric to display (default: post_count)"
    )


def create_instagram_posts_chart_tool(df_posts: pd.DataFrame):
    """Create a chart showing Instagram posts and engagement over time."""

    def create_instagram_posts_chart(
        start_date: str,
        end_date: str,
        grouping: str = 'week',
        metric: str = 'post_count'
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

        # Group by time period
        if grouping == 'day':
            df['period'] = df['timestamp'].dt.date
            period_format = '%Y-%m-%d'
        elif grouping == 'week':
            df['period'] = df['timestamp'].dt.to_period('W').apply(lambda r: r.start_time)
            period_format = 'Week of %Y-%m-%d'
        else:  # month
            df['period'] = df['timestamp'].dt.to_period('M').apply(lambda r: r.start_time)
            period_format = '%Y-%m'

        # Calculate metrics by period
        if metric == 'post_count':
            period_data = df.groupby('period').size().reset_index(name='value')
            y_label = 'Number of Posts'
            chart_title = f'Instagram Posts Over Time ({grouping.capitalize()})'
        elif metric == 'likes':
            period_data = df.groupby('period')['likes'].sum().reset_index(name='value')
            y_label = 'Total Likes'
            chart_title = f'Instagram Likes Over Time ({grouping.capitalize()})'
        elif metric == 'comments':
            period_data = df.groupby('period')['comments'].sum().reset_index(name='value')
            y_label = 'Total Comments'
            chart_title = f'Instagram Comments Over Time ({grouping.capitalize()})'
        elif metric == 'reach':
            period_data = df.groupby('period')['reach'].sum().reset_index(name='value')
            y_label = 'Total Reach'
            chart_title = f'Instagram Reach Over Time ({grouping.capitalize()})'
        elif metric == 'engagement_rate':
            period_data = df.groupby('period')['engagement_rate'].mean().reset_index(name='value')
            y_label = 'Average Engagement Rate (%)'
            chart_title = f'Instagram Engagement Rate Over Time ({grouping.capitalize()})'

        # Create figure
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=period_data['period'],
            y=period_data['value'],
            name=metric.replace('_', ' ').title(),
            mode='lines+markers',
            line=dict(color='#E1306C', width=3),  # Instagram brand color
            marker=dict(size=8),
            hovertemplate='<b>%{x}</b><br>' +
                         f'{y_label}: %{{y:,.0f}}<br>' +
                         '<extra></extra>'
        ))

        fig.update_layout(
            title=chart_title,
            xaxis_title='Date',
            yaxis_title=y_label,
            hovermode='x unified',
            template='plotly_white',
            showlegend=False
        )

        # Save chart
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_instagram_{metric}_{grouping}.html"
        filepath = os.path.join(CHART_OUTPUT_DIR, filename)
        fig.write_html(filepath)

        summary = f"Instagram chart created successfully!\n"
        summary += f"Saved to: {filepath}\n"
        summary += f"Metric: {metric}\n"
        summary += f"Grouping: {grouping}\n"
        summary += f"Date range: {start_date} to {end_date}\n"
        summary += f"Total periods: {len(period_data)}\n"

        if metric == 'post_count':
            total_posts = period_data['value'].sum()
            avg_posts = period_data['value'].mean()
            summary += f"Total posts: {int(total_posts)}\n"
            summary += f"Average posts per {grouping}: {avg_posts:.1f}\n"
        elif metric == 'engagement_rate':
            avg_engagement = period_data['value'].mean()
            summary += f"Average engagement rate: {avg_engagement:.2f}%\n"
        else:
            total_value = period_data['value'].sum()
            avg_value = period_data['value'].mean()
            summary += f"Total {metric}: {int(total_value):,}\n"
            summary += f"Average {metric} per {grouping}: {avg_value:,.1f}\n"

        return summary

    return StructuredTool.from_function(
        name="create_instagram_posts_chart",
        func=create_instagram_posts_chart,
        description="Create a time-series chart showing Instagram posts and engagement metrics (post count, likes, comments, reach, engagement rate) over time",
        args_schema=InstagramPostsChartInput
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


# ============================================================================
# FACEBOOK ADS TOOLS
# ============================================================================

class AdsPerformanceInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")


def create_get_ads_performance_summary_tool(df_ads: pd.DataFrame):
    """Get overall Facebook/Instagram Ads performance summary."""

    def get_ads_performance_summary(start_date: str, end_date: str) -> str:
        if df_ads.empty:
            return "No Facebook Ads data available. Please upload ads data first."

        df = df_ads.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['date'] >= start) & (df['date'] <= end)]

        if df.empty:
            return f"No ads data found between {start_date} and {end_date}"

        # Calculate metrics
        total_spend = df['spend'].sum()
        total_impressions = df['impressions'].sum()
        total_clicks = df['clicks'].sum()
        total_reach = df['reach'].sum()
        avg_ctr = df['ctr'].mean()
        avg_cpm = df['cpm'].mean()
        avg_cpc = df['cpc'].mean()

        # Conversion metrics (if available)
        link_clicks = df['link_clicks'].sum() if 'link_clicks' in df.columns else 0
        post_engagements = df['post_engagements'].sum() if 'post_engagements' in df.columns else 0

        result = f"Facebook/Instagram Ads Performance ({start_date} to {end_date}):\n\n"
        result += f"💰 Spend: ${total_spend:,.2f}\n"
        result += f"👁️  Impressions: {int(total_impressions):,}\n"
        result += f"🖱️  Clicks: {int(total_clicks):,}\n"
        result += f"📊 Reach: {int(total_reach):,}\n\n"

        result += f"Performance Metrics:\n"
        result += f"  - CTR (Click-Through Rate): {avg_ctr:.2f}%\n"
        result += f"  - CPM (Cost Per 1000 Impressions): ${avg_cpm:.2f}\n"
        result += f"  - CPC (Cost Per Click): ${avg_cpc:.2f}\n\n"

        if link_clicks > 0 or post_engagements > 0:
            result += f"Conversions:\n"
            if link_clicks > 0:
                result += f"  - Link Clicks: {int(link_clicks):,}\n"
            if post_engagements > 0:
                result += f"  - Post Engagements: {int(post_engagements):,}\n"

        return result

    return StructuredTool.from_function(
        name="get_ads_performance_summary",
        func=get_ads_performance_summary,
        description="Get overall Facebook/Instagram Ads performance including spend, impressions, clicks, CTR, CPM, and conversions",
        args_schema=AdsPerformanceInput
    )


class AdsCampaignInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    group_by: Literal['campaign', 'adset', 'ad'] = Field('campaign', description="Group by: 'campaign', 'adset', or 'ad'")


def create_get_ads_by_campaign_tool(df_ads: pd.DataFrame):
    """Get ads performance grouped by campaign, adset, or individual ad."""

    def get_ads_by_campaign(
        start_date: str,
        end_date: str,
        group_by: Literal['campaign', 'adset', 'ad'] = 'campaign'
    ) -> str:
        if df_ads.empty:
            return "No Facebook Ads data available. Please upload ads data first."

        df = df_ads.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['date'] >= start) & (df['date'] <= end)]

        if df.empty:
            return f"No ads data found between {start_date} and {end_date}"

        # Group by the specified level
        column_map = {
            'campaign': 'campaign_name',
            'adset': 'adset_name',
            'ad': 'ad_name'
        }
        group_col = column_map[group_by]

        # Aggregate metrics
        grouped = df.groupby(group_col).agg({
            'spend': 'sum',
            'impressions': 'sum',
            'clicks': 'sum',
            'reach': 'sum',
        }).round(2)

        # Calculate derived metrics
        grouped['ctr'] = (grouped['clicks'] / grouped['impressions'] * 100).round(2)
        grouped['cpm'] = (grouped['spend'] / grouped['impressions'] * 1000).round(2)
        grouped['cpc'] = (grouped['spend'] / grouped['clicks']).round(2)

        # Sort by spend (highest first)
        grouped = grouped.sort_values('spend', ascending=False)

        result = f"Ads Performance by {group_by.capitalize()} ({start_date} to {end_date}):\n\n"

        for name, row in grouped.iterrows():
            result += f"📢 {name}:\n"
            result += f"   Spend: ${row['spend']:,.2f} | Impressions: {int(row['impressions']):,} | Clicks: {int(row['clicks']):,}\n"
            result += f"   CTR: {row['ctr']:.2f}% | CPM: ${row['cpm']:.2f} | CPC: ${row['cpc']:.2f}\n\n"

        result += f"Total Spend: ${grouped['spend'].sum():,.2f}\n"
        result += f"Total Impressions: {int(grouped['impressions'].sum()):,}\n"
        result += f"Total Clicks: {int(grouped['clicks'].sum()):,}"

        return result

    return StructuredTool.from_function(
        name="get_ads_by_campaign",
        func=get_ads_by_campaign,
        description="Get ads performance grouped by campaign, adset, or individual ad. Shows spend, impressions, clicks, CTR, CPM, and CPC for each.",
        args_schema=AdsCampaignInput
    )


class AdsROASInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    revenue_category: Optional[str] = Field(None, description="Optional: filter revenue to a specific category (e.g., 'Day Pass', 'New Membership')")


def create_get_ads_roas_tool(df_ads: pd.DataFrame, df_transactions: pd.DataFrame):
    """Calculate Return on Ad Spend (ROAS) by comparing ad spend with revenue."""

    def get_ads_roas(
        start_date: str,
        end_date: str,
        revenue_category: Optional[str] = None
    ) -> str:
        if df_ads.empty:
            return "No Facebook Ads data available. Please upload ads data first."

        # Filter ads by date
        ads_df = df_ads.copy()
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        ads_df = ads_df[(ads_df['date'] >= start) & (ads_df['date'] <= end)]

        if ads_df.empty:
            return f"No ads data found between {start_date} and {end_date}"

        # Get revenue for same period
        revenue_df = df_transactions.copy()
        revenue_df = revenue_df[(revenue_df['Date'] >= start) & (revenue_df['Date'] <= end)]

        if revenue_category:
            revenue_df = revenue_df[revenue_df['revenue_category'] == revenue_category]

        # Calculate totals
        total_ad_spend = ads_df['spend'].sum()
        total_revenue = revenue_df['Total Amount'].sum()

        # Calculate ROAS
        roas = (total_revenue / total_ad_spend) if total_ad_spend > 0 else 0

        # Daily breakdown
        daily_ad_spend = ads_df.groupby('date')['spend'].sum()
        daily_revenue = revenue_df.groupby(revenue_df['Date'].dt.date)['Total Amount'].sum()

        # Merge and calculate daily ROAS
        daily_comparison = pd.DataFrame({
            'ad_spend': daily_ad_spend,
            'revenue': daily_revenue
        }).fillna(0)
        daily_comparison['roas'] = (daily_comparison['revenue'] / daily_comparison['ad_spend']).replace([float('inf'), -float('inf')], 0)

        # Sort by ROAS to find best days
        daily_comparison = daily_comparison.sort_values('roas', ascending=False)

        category_str = f" ({revenue_category})" if revenue_category else ""
        result = f"Return on Ad Spend (ROAS) Analysis{category_str}:\n"
        result += f"Period: {start_date} to {end_date}\n\n"

        result += f"Overall Metrics:\n"
        result += f"  Total Ad Spend: ${total_ad_spend:,.2f}\n"
        result += f"  Total Revenue{category_str}: ${total_revenue:,.2f}\n"
        result += f"  ROAS: {roas:.2f}x (${roas:.2f} revenue per $1 spent)\n\n"

        if roas >= 3:
            result += f"✅ Excellent ROAS! Your ads are highly profitable.\n\n"
        elif roas >= 2:
            result += f"✅ Good ROAS. Your ads are profitable.\n\n"
        elif roas >= 1:
            result += f"⚠️  Marginal ROAS. Revenue barely exceeds ad spend.\n\n"
        else:
            result += f"⚠️  Poor ROAS. Ad spend exceeds revenue.\n\n"

        result += f"Top 5 Days by ROAS:\n"
        for date, row in daily_comparison.head(5).iterrows():
            if row['ad_spend'] > 0:
                result += f"  {date}: {row['roas']:.2f}x (Spend: ${row['ad_spend']:.2f}, Revenue: ${row['revenue']:.2f})\n"

        return result

    return StructuredTool.from_function(
        name="get_ads_roas",
        func=get_ads_roas,
        description="Calculate Return on Ad Spend (ROAS) by comparing ad spend with revenue. Shows if ads are profitable and which days had best ROAS.",
        args_schema=AdsROASInput
    )


# ============================================================================
# CHECK-IN TOOLS
# ============================================================================

class CheckinInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    entry_type: Optional[str] = Field(None, description="Filter by entry type: 'member' (MEM), 'day_pass' (ENT), 'guest' (GUE), or leave blank for all")


def create_get_checkin_summary_tool(df_checkins: pd.DataFrame):
    """Get check-in summary with ability to distinguish members vs day passes."""

    def get_checkin_summary(
        start_date: str,
        end_date: str,
        entry_type: Optional[str] = None
    ) -> str:
        if df_checkins.empty:
            return "No check-in data available. Please upload check-in data first."

        df = df_checkins.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['checkin_datetime'] >= start) & (df['checkin_datetime'] <= end)]

        if df.empty:
            return f"No check-ins found between {start_date} and {end_date}"

        # Filter by entry type if specified
        if entry_type:
            type_map = {
                'member': 'MEM',
                'day_pass': 'ENT',
                'guest': 'GUE'
            }
            entry_code = type_map.get(entry_type.lower(), entry_type.upper())
            df = df[df['entry_method'] == entry_code]

        # Calculate statistics
        total_checkins = len(df)
        unique_customers = df['customer_id'].nunique()
        free_entries = df['is_free_entry'].sum()

        # Breakdown by entry method (MEM, ENT, GUE)
        entry_method_breakdown = df['entry_method'].value_counts()

        # Top entry method descriptions
        top_entry_descriptions = df['entry_method_description'].value_counts().head(10)

        # Daily average
        date_range_days = (end - start).days + 1
        daily_avg = total_checkins / date_range_days if date_range_days > 0 else 0

        entry_type_str = f" ({entry_type})" if entry_type else ""
        result = f"Check-in Summary{entry_type_str}:\n"
        result += f"Period: {start_date} to {end_date}\n\n"

        result += f"Overall Metrics:\n"
        result += f"  Total Check-ins: {total_checkins:,}\n"
        result += f"  Unique Customers: {unique_customers:,}\n"
        result += f"  Free Entries: {free_entries:,} ({free_entries/total_checkins*100:.1f}%)\n"
        result += f"  Daily Average: {daily_avg:.1f} check-ins/day\n\n"

        result += f"By Entry Type:\n"
        for method, count in entry_method_breakdown.items():
            method_name = {'MEM': 'Members', 'ENT': 'Day Passes/Entries', 'GUE': 'Guest Passes'}.get(method, method)
            result += f"  {method_name}: {count:,} ({count/total_checkins*100:.1f}%)\n"

        result += f"\nTop 10 Entry Methods:\n"
        for desc, count in top_entry_descriptions.items():
            result += f"  {desc}: {count:,}\n"

        return result

    return StructuredTool.from_function(
        name="get_checkin_summary",
        func=get_checkin_summary,
        description="""Get check-in summary with breakdowns by entry type (members vs day passes vs guests).

IMPORTANT - How to calculate percentages:
1. Call WITHOUT entry_type → gets TOTAL check-ins (denominator)
2. Call WITH entry_type → gets SPECIFIC type check-ins (numerator)
3. Calculate: (specific / total) * 100 = percentage

Examples:
- "What % of check-ins are from day passes?"
  → Call #1: get_checkin_summary(dates) → total=100
  → Call #2: get_checkin_summary(dates, entry_type='day_pass') → day_pass=30
  → Answer: (30/100)*100 = 30% are day passes

- "What % are from members week over week?"
  → Week 1: Call without filter (total=200), call with entry_type='member' (members=150) → 75%
  → Week 2: Call without filter (total=180), call with entry_type='member' (members=130) → 72%
  → Answer: "75% week 1, 72% week 2"

Entry types: 'member' (memberships), 'day_pass' (purchased day passes), 'guest' (guest passes)""",
        args_schema=CheckinInput
    )


class CheckinChartInput(BaseModel):
    start_date: str = Field(description="Start date in YYYY-MM-DD format")
    end_date: str = Field(description="End date in YYYY-MM-DD format")
    grouping: str = Field(default="month", description="Time grouping: 'day', 'week', or 'month' (default: month)")


def create_checkin_timeseries_chart_tool(df_checkins: pd.DataFrame):
    """Create a time-series chart showing check-ins over time with member vs non-member breakdown."""

    def create_checkin_timeseries_chart(
        start_date: str,
        end_date: str,
        grouping: str = "month"
    ) -> str:
        if df_checkins.empty:
            return "No check-in data available."

        df = df_checkins.copy()

        # Filter by date
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df = df[(df['checkin_datetime'] >= start) & (df['checkin_datetime'] <= end)]

        if df.empty:
            return f"No check-ins found between {start_date} and {end_date}"

        # Add entry type categories
        df['entry_category'] = df['entry_method'].map({
            'MEM': 'Members',
            'ENT': 'Day Passes',
            'GUE': 'Guest Passes',
            'FRE': 'Free Entry',
            'EVE': 'Events'
        }).fillna('Other')

        # Group by time period
        if grouping == 'day':
            df['period'] = df['checkin_datetime'].dt.date
            period_format = '%Y-%m-%d'
        elif grouping == 'week':
            df['period'] = df['checkin_datetime'].dt.to_period('W').apply(lambda r: r.start_time)
            period_format = 'Week of %Y-%m-%d'
        else:  # month
            df['period'] = df['checkin_datetime'].dt.to_period('M').apply(lambda r: r.start_time)
            period_format = '%Y-%m'

        # Calculate counts by period and entry category
        period_counts = df.groupby(['period', 'entry_category']).size().reset_index(name='count')

        # Calculate total per period for percentage
        total_per_period = df.groupby('period').size().reset_index(name='total')

        # Merge to get percentages
        period_counts = period_counts.merge(total_per_period, on='period')
        period_counts['percentage'] = (period_counts['count'] / period_counts['total'] * 100).round(1)

        # Create figure
        fig = go.Figure()

        # Add a line for each entry category
        categories = period_counts['entry_category'].unique()
        colors = {
            'Members': '#2E86AB',
            'Day Passes': '#A23B72',
            'Guest Passes': '#F18F01',
            'Free Entry': '#C73E1D',
            'Events': '#6A994E',
            'Other': '#BC4B51'
        }

        for category in sorted(categories):
            cat_data = period_counts[period_counts['entry_category'] == category].sort_values('period')

            fig.add_trace(go.Scatter(
                x=cat_data['period'],
                y=cat_data['count'],
                name=category,
                mode='lines+markers',
                line=dict(color=colors.get(category, '#666666'), width=2),
                marker=dict(size=6),
                hovertemplate=f'<b>{category}</b><br>' +
                             'Date: %{x}<br>' +
                             'Check-ins: %{y}<br>' +
                             '<extra></extra>'
            ))

        fig.update_layout(
            title=f'Check-ins by Entry Type Over Time ({grouping.capitalize()})',
            xaxis_title='Date',
            yaxis_title='Number of Check-ins',
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            template='plotly_white'
        )

        # Save chart
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{timestamp}_checkin_timeseries_{grouping}_{start_date}_{end_date}.html'
        filepath = os.path.join(CHART_OUTPUT_DIR, filename)
        fig.write_html(filepath)

        # Calculate summary statistics
        total_checkins = len(df)
        member_checkins = len(df[df['entry_category'] == 'Members'])
        non_member_checkins = total_checkins - member_checkins
        member_pct = (member_checkins / total_checkins * 100) if total_checkins > 0 else 0

        summary = f"Check-in timeseries chart created and saved to {filepath}\n\n"
        summary += f"Summary:\n"
        summary += f"- Total check-ins: {total_checkins:,}\n"
        summary += f"- Member check-ins: {member_checkins:,} ({member_pct:.1f}%)\n"
        summary += f"- Non-member check-ins: {non_member_checkins:,} ({100-member_pct:.1f}%)\n"
        summary += f"- Time periods: {len(period_counts['period'].unique())}"

        return summary

    return StructuredTool.from_function(
        name="create_checkin_timeseries_chart",
        func=create_checkin_timeseries_chart,
        description="""Create a time-series line chart showing check-ins over CALENDAR time (dates), broken down by entry type (members, day passes, guests).

X-axis is DATES (by day/week/month). Perfect for showing month-over-month trends in member vs non-member check-ins.

NOTE: This does NOT create hourly patterns or day-of-week analysis. For hourly breakdowns or day-of-week patterns, use execute_custom_query + create_generic_chart instead.""",
        args_schema=CheckinChartInput
    )


class InactiveMembersInput(BaseModel):
    start_date: str = Field(description="Start date for check-in window in YYYY-MM-DD format")
    end_date: str = Field(description="End date for check-in window in YYYY-MM-DD format")
    max_checkins: int = Field(default=1, description="Maximum number of check-ins to be considered 'inactive' (default: 1)")


def create_get_inactive_members_tool(df_checkins: pd.DataFrame, df_memberships: pd.DataFrame):
    """Find members with active memberships but few/no recent check-ins."""

    def get_inactive_members(
        start_date: str,
        end_date: str,
        max_checkins: int = 1
    ) -> str:
        if df_checkins.empty:
            return "No check-in data available."

        if df_memberships.empty:
            return "No membership data available."

        # Get active memberships
        today = pd.Timestamp.now()
        active_memberships = df_memberships[df_memberships['end_date'] >= today].copy()

        if active_memberships.empty:
            return "No active memberships found."

        # Count check-ins per customer in the time window
        df_checkins_filtered = df_checkins.copy()
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        df_checkins_filtered = df_checkins_filtered[
            (df_checkins_filtered['checkin_datetime'] >= start) &
            (df_checkins_filtered['checkin_datetime'] <= end)
        ]

        # Count check-ins per customer
        checkin_counts = df_checkins_filtered.groupby('customer_id').size().reset_index(name='checkin_count')

        # Merge with active memberships
        members_with_counts = active_memberships.merge(
            checkin_counts,
            on='customer_id',
            how='left'
        )

        # Fill NaN (members with zero check-ins) with 0
        members_with_counts['checkin_count'] = members_with_counts['checkin_count'].fillna(0).astype(int)

        # Filter to inactive members (max_checkins or fewer)
        inactive_members = members_with_counts[members_with_counts['checkin_count'] <= max_checkins].copy()

        if inactive_members.empty:
            return f"No members found with {max_checkins} or fewer check-ins between {start_date} and {end_date}"

        # Sort by checkin count (lowest first)
        inactive_members = inactive_members.sort_values('checkin_count')

        # Group by membership type
        by_membership_type = inactive_members.groupby('membership_size').size().sort_values(ascending=False)

        result = f"Inactive Members Report:\n"
        result += f"Period: {start_date} to {end_date}\n"
        result += f"Criteria: {max_checkins} or fewer check-ins\n\n"

        result += f"Summary:\n"
        result += f"  Total Inactive Members: {len(inactive_members):,}\n"
        result += f"  Total Active Memberships: {len(active_memberships):,}\n"
        result += f"  Inactive Rate: {len(inactive_members)/len(active_memberships)*100:.1f}%\n\n"

        result += f"By Membership Type:\n"
        for membership_type, count in by_membership_type.items():
            result += f"  {membership_type}: {count:,}\n"

        result += f"\nBreakdown by Check-in Count:\n"
        checkin_breakdown = inactive_members['checkin_count'].value_counts().sort_index()
        for count, num_members in checkin_breakdown.items():
            result += f"  {int(count)} check-ins: {num_members:,} members\n"

        result += f"\nSample of Inactive Members (first 10):\n"
        for idx, row in inactive_members.head(10).iterrows():
            result += f"  Customer {row['customer_id']}: {row['membership_size']}, {int(row['checkin_count'])} check-ins\n"

        return result

    return StructuredTool.from_function(
        name="get_inactive_members",
        func=get_inactive_members,
        description="Find members with active memberships but few/no recent check-ins. Useful for identifying at-risk members who may need engagement.",
        args_schema=InactiveMembersInput
    )


# ============================================================================
# GENERIC DATA QUERY AND CHARTING TOOLS
# ============================================================================


class CustomQueryInput(BaseModel):
    query_description: str = Field(description="Brief description of what this query does")
    pandas_code: str = Field(description="Pandas code to execute. Available DataFrames: df_transactions, df_memberships, df_members, df_instagram_posts, df_instagram_comments, df_facebook_ads, df_checkins. Code should create a variable named 'result' containing the final DataFrame.")


def create_execute_custom_query_tool(df_transactions, df_memberships, df_members, df_instagram_posts, df_instagram_comments, df_facebook_ads, df_checkins):
    """
    Execute custom pandas queries on any combination of data sources.
    Returns a data_id that can be used with create_generic_chart.
    """

    # Build schema documentation
    schema_doc = "\n\nAVAILABLE DATAFRAMES AND SCHEMAS:\n"
    schema_doc += "\n1. df_checkins - Check-in records\n"
    schema_doc += f"   Columns: {', '.join(df_checkins.columns.tolist())}\n"
    schema_doc += "\n2. df_memberships - Membership records\n"
    schema_doc += f"   Columns: {', '.join(df_memberships.columns.tolist())}\n"
    schema_doc += "\n3. df_members - Individual member records\n"
    schema_doc += f"   Columns: {', '.join(df_members.columns.tolist())}\n"
    schema_doc += "\n4. df_transactions - Transaction records\n"
    schema_doc += f"   Columns: {', '.join(df_transactions.columns.tolist())}\n"
    if not df_instagram_posts.empty:
        schema_doc += "\n5. df_instagram_posts - Instagram post data\n"
        schema_doc += f"   Columns: {', '.join(df_instagram_posts.columns.tolist())}\n"
    if not df_instagram_comments.empty:
        schema_doc += "\n6. df_instagram_comments - Instagram comment data\n"
        schema_doc += f"   Columns: {', '.join(df_instagram_comments.columns.tolist())}\n"
    if not df_facebook_ads.empty:
        schema_doc += "\n7. df_facebook_ads - Facebook ads performance data\n"
        schema_doc += f"   Columns: {', '.join(df_facebook_ads.columns.tolist())}\n"

    def execute_custom_query(query_description: str, pandas_code: str) -> str:
        """Execute pandas code and store result in registry."""
        import traceback
        from difflib import get_close_matches

        # Prepare the execution environment with all available DataFrames
        exec_globals = {
            'pd': pd,
            'df_transactions': df_transactions.copy(),
            'df_memberships': df_memberships.copy(),
            'df_members': df_members.copy(),
            'df_instagram_posts': df_instagram_posts.copy() if not df_instagram_posts.empty else pd.DataFrame(),
            'df_instagram_comments': df_instagram_comments.copy() if not df_instagram_comments.empty else pd.DataFrame(),
            'df_facebook_ads': df_facebook_ads.copy() if not df_facebook_ads.empty else pd.DataFrame(),
            'df_checkins': df_checkins.copy() if not df_checkins.empty else pd.DataFrame(),
            'datetime': datetime,
            'timedelta': timedelta,
        }

        try:
            # Execute the pandas code
            exec(pandas_code, exec_globals)

            # Check if 'result' variable was created
            if 'result' not in exec_globals:
                return "Error: Code must create a variable named 'result' containing the DataFrame"

            result_df = exec_globals['result']

            # Validate it's a DataFrame
            if not isinstance(result_df, pd.DataFrame):
                return f"Error: 'result' must be a DataFrame, got {type(result_df)}"

            if result_df.empty:
                return "Query executed successfully but returned an empty DataFrame"

            # Generate unique ID and store in registry
            data_id = f"query_{uuid.uuid4().hex[:8]}"
            _DATA_REGISTRY[data_id] = {
                'dataframe': result_df,
                'description': query_description,
                'timestamp': datetime.now()
            }

            # Return summary
            summary = f"Query executed successfully!\n"
            summary += f"Data ID: {data_id}\n"
            summary += f"Description: {query_description}\n\n"
            summary += f"Result shape: {result_df.shape[0]} rows × {result_df.shape[1]} columns\n"
            summary += f"Columns: {', '.join(result_df.columns.tolist())}\n\n"
            summary += f"First few rows:\n{result_df.head(5).to_string()}\n\n"
            summary += f"Use this data_id with create_generic_chart to visualize the results."

            return summary

        except KeyError as e:
            # Provide helpful suggestions for KeyError (likely wrong column name)
            error_column = str(e).strip("'\"")

            # Find which DataFrame was being accessed (simple heuristic)
            all_columns = {}
            all_columns['df_checkins'] = df_checkins.columns.tolist()
            all_columns['df_memberships'] = df_memberships.columns.tolist()
            all_columns['df_members'] = df_members.columns.tolist()
            all_columns['df_transactions'] = df_transactions.columns.tolist()
            if not df_instagram_posts.empty:
                all_columns['df_instagram_posts'] = df_instagram_posts.columns.tolist()
            if not df_instagram_comments.empty:
                all_columns['df_instagram_comments'] = df_instagram_comments.columns.tolist()
            if not df_facebook_ads.empty:
                all_columns['df_facebook_ads'] = df_facebook_ads.columns.tolist()

            # Find similar column names across all DataFrames
            suggestions = {}
            for df_name, columns in all_columns.items():
                matches = get_close_matches(error_column, columns, n=3, cutoff=0.6)
                if matches:
                    suggestions[df_name] = matches

            error_msg = f"Error: Column '{error_column}' not found.\n\n"
            if suggestions:
                error_msg += "Did you mean one of these?\n"
                for df_name, matches in suggestions.items():
                    error_msg += f"  {df_name}: {', '.join(matches)}\n"

            error_msg += f"\n{schema_doc}"
            return error_msg

        except Exception as e:
            error_msg = f"Error executing query:\n{str(e)}\n\nTraceback:\n{traceback.format_exc()}"
            return error_msg

    description = (
        "Execute custom pandas queries on any combination of data sources. "
        "Supports joins, aggregations, filters, and any pandas operations. "
        "Code must create a 'result' variable containing the final DataFrame. "
        "Returns a data_id that can be used with create_generic_chart.\n\n"
        "USE THIS TOOL FOR:\n"
        "- Custom aggregations not covered by specialized tools\n"
        "- Hourly patterns or day-of-week analysis\n"
        "- Complex groupings (e.g., 'hour of day by day of week')\n"
        "- Joining multiple data sources\n"
        "- Any analysis requiring custom pandas code"
        + schema_doc
    )

    return StructuredTool.from_function(
        name="execute_custom_query",
        func=execute_custom_query,
        description=description,
        args_schema=CustomQueryInput
    )


class GenericChartInput(BaseModel):
    data_id: str = Field(description="The data_id returned from execute_custom_query")
    chart_type: Literal["line", "bar", "scatter", "area"] = Field(description="Type of chart to create")
    x_column: str = Field(description="Column name for x-axis")
    y_column: str = Field(description="Column name for y-axis")
    title: Optional[str] = Field(default=None, description="Chart title (optional)")
    group_by_column: Optional[str] = Field(default=None, description="Column to group by for multiple lines/bars (optional)")


def create_generic_chart_tool():
    """Create a generic chart from any data stored in the registry."""

    def create_generic_chart(
        data_id: str,
        chart_type: str,
        x_column: str,
        y_column: str,
        title: Optional[str] = None,
        group_by_column: Optional[str] = None
    ) -> str:
        """Create a Plotly chart from data in the registry."""

        # Check if data_id exists
        if data_id not in _DATA_REGISTRY:
            available_ids = list(_DATA_REGISTRY.keys())
            return f"Error: data_id '{data_id}' not found in registry. Available IDs: {available_ids}"

        # Get the data
        registry_entry = _DATA_REGISTRY[data_id]
        df = registry_entry['dataframe']
        description = registry_entry['description']

        # Validate columns exist
        if x_column not in df.columns:
            return f"Error: Column '{x_column}' not found. Available columns: {', '.join(df.columns)}"
        if y_column not in df.columns:
            return f"Error: Column '{y_column}' not found. Available columns: {', '.join(df.columns)}"
        if group_by_column and group_by_column not in df.columns:
            return f"Error: Column '{group_by_column}' not found. Available columns: {', '.join(df.columns)}"

        # Create the chart
        fig = go.Figure()

        if title is None:
            title = f"{description} - {y_column} by {x_column}"

        if group_by_column:
            # Multiple traces grouped by column
            groups = df[group_by_column].unique()
            for group in groups:
                group_data = df[df[group_by_column] == group].sort_values(x_column)

                if chart_type == "line":
                    fig.add_trace(go.Scatter(x=group_data[x_column], y=group_data[y_column],
                                            mode='lines+markers', name=str(group)))
                elif chart_type == "bar":
                    fig.add_trace(go.Bar(x=group_data[x_column], y=group_data[y_column],
                                        name=str(group)))
                elif chart_type == "scatter":
                    fig.add_trace(go.Scatter(x=group_data[x_column], y=group_data[y_column],
                                            mode='markers', name=str(group)))
                elif chart_type == "area":
                    fig.add_trace(go.Scatter(x=group_data[x_column], y=group_data[y_column],
                                            fill='tonexty', name=str(group)))
        else:
            # Single trace
            sorted_data = df.sort_values(x_column)

            if chart_type == "line":
                fig.add_trace(go.Scatter(x=sorted_data[x_column], y=sorted_data[y_column],
                                        mode='lines+markers', name=y_column))
            elif chart_type == "bar":
                fig.add_trace(go.Bar(x=sorted_data[x_column], y=sorted_data[y_column],
                                    name=y_column))
            elif chart_type == "scatter":
                fig.add_trace(go.Scatter(x=sorted_data[x_column], y=sorted_data[y_column],
                                        mode='markers', name=y_column))
            elif chart_type == "area":
                fig.add_trace(go.Scatter(x=sorted_data[x_column], y=sorted_data[y_column],
                                        fill='tozeroy', name=y_column))

        # Update layout
        fig.update_layout(
            title=title,
            xaxis_title=x_column,
            yaxis_title=y_column,
            hovermode='x unified',
            template='plotly_white'
        )

        # Save the chart
        os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_custom_{chart_type}_{data_id}.html"
        filepath = os.path.join(CHART_OUTPUT_DIR, filename)
        fig.write_html(filepath)

        summary = f"Chart created successfully!\n"
        summary += f"Saved to: {filepath}\n"
        summary += f"Chart type: {chart_type}\n"
        summary += f"Data: {description}\n"
        summary += f"X-axis: {x_column}\n"
        summary += f"Y-axis: {y_column}\n"
        if group_by_column:
            summary += f"Grouped by: {group_by_column} ({len(df[group_by_column].unique())} groups)\n"

        return summary

    return StructuredTool.from_function(
        name="create_generic_chart",
        func=create_generic_chart,
        description="Create a chart from data stored in the registry (from execute_custom_query). Supports line, bar, scatter, and area charts. Can group data by a column to create multiple series.",
        args_schema=GenericChartInput
    )


def create_all_tools():
    """Create all analytical tools with loaded data."""
    print("Loading data from S3...")
    df_transactions, df_memberships, df_members, df_instagram_posts, df_instagram_comments, df_facebook_ads, df_checkins = load_data_frames()
    print(f"Loaded {len(df_transactions)} transactions, {len(df_memberships)} memberships, {len(df_members)} members")

    if not df_instagram_posts.empty:
        print(f"Loaded {len(df_instagram_posts)} Instagram posts, {len(df_instagram_comments)} comments")

    if not df_facebook_ads.empty:
        print(f"Loaded {len(df_facebook_ads)} Facebook Ads records")

    if not df_checkins.empty:
        print(f"Loaded {len(df_checkins)} check-in records from {df_checkins['customer_id'].nunique()} unique customers")

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
        create_get_unique_day_pass_customers_tool(df_checkins),

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
            create_instagram_posts_chart_tool(df_instagram_posts),
        ])

    # Add Facebook Ads tools if data is available
    if not df_facebook_ads.empty:
        tools.extend([
            create_get_ads_performance_summary_tool(df_facebook_ads),
            create_get_ads_by_campaign_tool(df_facebook_ads),
            create_get_ads_roas_tool(df_facebook_ads, df_transactions),
        ])

    # Add Check-in tools if data is available
    if not df_checkins.empty:
        tools.extend([
            create_get_checkin_summary_tool(df_checkins),
            create_get_inactive_members_tool(df_checkins, df_memberships),
            create_checkin_timeseries_chart_tool(df_checkins),
        ])

    # Add Generic Query and Charting tools (always available)
    tools.extend([
        create_execute_custom_query_tool(
            df_transactions, df_memberships, df_members,
            df_instagram_posts, df_instagram_comments,
            df_facebook_ads, df_checkins
        ),
        create_generic_chart_tool(),
    ])

    return tools

"""
Analytical tools for querying business data.

These tools provide pre-built functions for common analytical questions about:
- Revenue (total, breakdowns, comparisons)
- Memberships (counts, breakdowns, conversions)
- Day passes (counts, revenue)
- Generic pandas queries for ad-hoc analysis
"""

import pandas as pd
from typing import Optional, Literal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from langchain_core.tools import StructuredTool


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
    ]

    return tools

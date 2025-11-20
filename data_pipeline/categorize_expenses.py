"""
Expense categorization logic for QuickBooks data.

Maps granular QuickBooks expense categories into high-level business categories
for dashboard visualization and analysis.
"""

import pandas as pd
from typing import Dict, List


# Dashboard expense categories (only Payroll and Marketing shown)
EXPENSE_CATEGORY_MAPPINGS = {
    "Payroll": [
        "Salaries & wages",
        "Payroll taxes",
        "Payroll expenses",
        "Employee benefits",
        "Employee retirement plans",
        "Group term life insurance",
        "Health insurance & accident plans",
        "Workers' compensation insurance",
        "Health Reimbursements",
        "Health Reimbursement",
        "Payments to partners",
        "Reimbursement",
    ],

    "Marketing": [
        "Advertising & marketing",
        "Google Ads",
        "Listing fees",
        "Social media",
        "Website ads",
    ],
}


def categorize_expense(expense_category: str) -> str:
    """
    Map a QuickBooks expense category to a high-level business category.

    Only returns Payroll or Marketing. Returns None for all other categories.

    Args:
        expense_category: The raw expense category from QuickBooks

    Returns:
        'Payroll', 'Marketing', or None
    """
    if pd.isna(expense_category):
        return None

    expense_category = expense_category.strip()

    for high_level_category, subcategories in EXPENSE_CATEGORY_MAPPINGS.items():
        if expense_category in subcategories:
            return high_level_category

    # If not in Payroll or Marketing, return None
    return None


def add_expense_categories(df_expenses: pd.DataFrame) -> pd.DataFrame:
    """
    Add high-level category column to expense DataFrame.

    Args:
        df_expenses: DataFrame with 'expense_category' column

    Returns:
        DataFrame with added 'category_group' column
    """
    df_expenses = df_expenses.copy()
    df_expenses['category_group'] = df_expenses['expense_category'].apply(categorize_expense)
    return df_expenses


def get_category_summary(df_expenses: pd.DataFrame) -> pd.DataFrame:
    """
    Get expense summary for Payroll and Marketing categories only.

    Args:
        df_expenses: DataFrame with expense data

    Returns:
        DataFrame with columns: category_group, total_amount, transaction_count, avg_amount
    """
    df = df_expenses.copy()

    # Add categories if not already present
    if 'category_group' not in df.columns:
        df = add_expense_categories(df)

    # Filter to only Payroll and Marketing
    df = df[df['category_group'].isin(['Payroll', 'Marketing'])]

    # Group by category
    summary = df.groupby('category_group').agg({
        'amount': ['sum', 'count', 'mean']
    }).reset_index()

    summary.columns = ['category_group', 'total_amount', 'transaction_count', 'avg_amount']
    summary = summary.sort_values('total_amount', ascending=False)

    return summary


def get_monthly_expenses(df_expenses: pd.DataFrame) -> pd.DataFrame:
    """
    Get monthly expense totals for Payroll and Marketing only.

    Args:
        df_expenses: DataFrame with expense data (must have 'date' column)

    Returns:
        DataFrame with columns: year_month, category_group, total_amount
    """
    df = df_expenses.copy()

    # Add categories if not already present
    if 'category_group' not in df.columns:
        df = add_expense_categories(df)

    # Filter to only Payroll and Marketing
    df = df[df['category_group'].isin(['Payroll', 'Marketing'])]

    # Ensure date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])

    # Create year-month column
    df['year_month'] = df['date'].dt.to_period('M')

    # Group by month and category
    monthly = df.groupby(['year_month', 'category_group'])['amount'].sum().reset_index()
    monthly.columns = ['year_month', 'category_group', 'total_amount']

    # Convert period back to string for easier handling
    monthly['year_month'] = monthly['year_month'].astype(str)

    return monthly


def get_top_expenses_by_category(
    df_expenses: pd.DataFrame,
    category_group: str,
    top_n: int = 10
) -> pd.DataFrame:
    """
    Get top N expense line items within a specific category.

    Args:
        df_expenses: DataFrame with expense data
        category_group: High-level category to filter by
        top_n: Number of top expenses to return

    Returns:
        DataFrame with top expenses in that category
    """
    df = df_expenses.copy()

    # Add categories if not already present
    if 'category_group' not in df.columns:
        df = add_expense_categories(df)

    # Filter to category
    df_category = df[df['category_group'] == category_group]

    # Sort by amount and get top N
    top_expenses = df_category.nlargest(top_n, 'amount')

    return top_expenses[['date', 'vendor', 'expense_category', 'description', 'amount']]


if __name__ == "__main__":
    # Test the categorization
    import os

    print("=" * 60)
    print("Testing Expense Categorization")
    print("=" * 60)

    # Load expense data
    df = pd.read_csv('data/outputs/quickbooks_expenses.csv')
    print(f"\nLoaded {len(df)} expense line items")

    # Add categories
    df = add_expense_categories(df)

    # Show category distribution
    print("\n" + "=" * 60)
    print("Category Distribution")
    print("=" * 60)
    category_counts = df['category_group'].value_counts()
    for category, count in category_counts.items():
        total = df[df['category_group'] == category]['amount'].sum()
        print(f"{category:15} {count:4} transactions  ${total:,.2f}")

    # Show monthly summary
    print("\n" + "=" * 60)
    print("Monthly Expenses (Payroll & Marketing)")
    print("=" * 60)
    monthly = get_monthly_expenses(df)
    monthly_totals = monthly.groupby('year_month')['total_amount'].sum().sort_index()
    for month, total in monthly_totals.items():
        print(f"{month}:  ${total:,.2f}")

    # Show category summary
    print("\n" + "=" * 60)
    print("Category Summary (Payroll & Marketing)")
    print("=" * 60)
    summary = get_category_summary(df)
    print(summary.to_string(index=False))

    print("\n" + "=" * 60)
    print("✅ Categorization test complete!")
    print("=" * 60)

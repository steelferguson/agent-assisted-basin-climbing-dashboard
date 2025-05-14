
import pandas as pd


'''
This file is used to investigate the data and get the daily metrics.
It is used to get the daily metrics for a given day and category.
'''

def summarize_date_range(
    df: pd.DataFrame, 
    start_date: str, 
    end_date: str, 
    category: str = None, 
    sub_category: str = None,
    date_col: str = "Date",
    total_col: str = "Total Amount", 
    day_passes_col: str = "Day Pass Count",
    already_filtered: bool = False
) -> dict:
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    
    if not already_filtered:
        df = df[(df[date_col] >= pd.to_datetime(start_date)) & (df[date_col] <= pd.to_datetime(end_date))]

    if category:
        df = df[df['revenue_category'] == category]
    if sub_category:
        df = df[df['revenue_sub_category'] == sub_category]

    daily_totals = df.groupby(df[date_col].dt.date)[total_col].sum()
    daily_day_passes = df.groupby(df[date_col].dt.date)[day_passes_col].sum()

    return {
        "start_date": start_date,
        "end_date": end_date,
        "category": category or "All",
        "total_revenue": round(df[total_col].sum(), 2),
        "average_daily_revenue": round(daily_totals.mean(), 2) if not daily_totals.empty else 0,
        "average_daily_day_passes": round(daily_day_passes.mean(), 2) if not daily_day_passes.empty else 0,
        "num_days": len(daily_totals),
        "num_transactions": len(df),
        "daily_breakdown": daily_totals.to_dict(),
        "daily_day_passes_breakdown": daily_day_passes.to_dict(),
    }


def compare_categories(
        df: pd.DataFrame, 
        date_range: tuple[str, str],
        date_col: str = "Date"
    ) -> dict:
    """
    Compares revenue totals for all categories within a given date range.
    Returns a dictionary mapping each category to its revenue and transaction count.
    """
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    start_date, end_date = date_range
    df_filtered = df[(df[date_col] >= pd.to_datetime(start_date)) & (df[date_col] <= pd.to_datetime(end_date))]

    category_summary = (
        df_filtered.groupby('revenue_category')
        .agg(total_revenue=('Total Amount', 'sum'), num_transactions=('Total Amount', 'count'))
        .sort_values(by='total_revenue', ascending=False)
        .reset_index()
    )

    return category_summary.to_dict(orient='records')


def detect_anomalies(
        df: pd.DataFrame, 
        date_range: tuple[str, str] = None, 
        window_size: int = 120,
        date_col: str = "Date"
    ) -> list[dict]:
    """
    Flags days where revenue was a significant outlier based on z-score.
    Returns a list of dates and metrics where revenue was anomalously high or low.
    """
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    if date_range:
        window_size = pd.to_datetime(date_range[1]) - pd.to_datetime(date_range[0])
        end_date = pd.to_datetime(date_range[1])
        window_start_date = end_date - window_size
    else:
        end_date = df[date_col].max()
        window_start_date = end_date - window_size
    
    df_filtered = df[(df[date_col] >= window_start_date) & (df[date_col] <= end_date)]

    daily = df.groupby(df[date_col].dt.date)['Total Amount'].sum().reset_index()
    daily.columns = ['date', 'total_revenue']

    daily['z_score'] = (daily['total_revenue'] - daily['total_revenue'].mean()) / daily['total_revenue'].std()
    anomalies = daily[abs(daily['z_score']) > 2].sort_values(by='z_score', key=abs, ascending=False)
    # convert string date to datetime
    anomalies['date'] = pd.to_datetime(anomalies['date'])
    if date_range:
        # anomolies within date range
        anomalies = anomalies[anomalies['date'] >= pd.to_datetime(date_range[0])]
        anomalies = anomalies[anomalies['date'] <= pd.to_datetime(date_range[1])]

    return anomalies.to_dict(orient='records')

"""
Shared data loading functions for Basin Climbing dashboards.

These functions load data from S3 for use in both owner and crew dashboards.
"""
import pandas as pd
import io
import streamlit as st
from data_pipeline.upload_data import DataUploader
import data_pipeline.config as config


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_memberships() -> pd.DataFrame:
    """Load Capitan membership data from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_capitan_memberships
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    date_cols = ['membership_start', 'membership_end', 'created_at', 'updated_at']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    return df


@st.cache_data(ttl=300)
def load_members() -> pd.DataFrame:
    """Load Capitan member (customer) data from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_capitan_members
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    if 'birthday' in df.columns:
        df['birthday'] = pd.to_datetime(df['birthday'], errors='coerce', utc=True)

    return df


@st.cache_data(ttl=300)
def load_checkins() -> pd.DataFrame:
    """Load Capitan check-in data from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_capitan_checkins
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    if 'checkin_datetime' in df.columns:
        df['checkin_datetime'] = pd.to_datetime(df['checkin_datetime'], errors='coerce', utc=True)
    if 'created_at' in df.columns:
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True)

    return df


@st.cache_data(ttl=300)
def load_associations() -> pd.DataFrame:
    """Load Capitan associations (groups/tags) from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_capitan_associations
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    date_cols = ['created_at', 'updated_at']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    return df


@st.cache_data(ttl=300)
def load_association_members() -> pd.DataFrame:
    """Load Capitan association members (customer-to-group mappings) from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_capitan_association_members
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    date_cols = ['created_at', 'approved_at', 'last_reverified_at', 'next_automatic_removal_datetime']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    return df


@st.cache_data(ttl=300)
def load_events() -> pd.DataFrame:
    """Load Capitan events from S3."""
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_capitan_events
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    date_cols = ['start_datetime', 'end_datetime', 'created_at', 'updated_at']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)

    return df


@st.cache_data(ttl=300)
def load_transactions() -> pd.DataFrame:
    """
    Load transaction data from S3 (Stripe + Square combined).

    **NOTE**: This includes revenue data. Use only in owner dashboard.
    """
    uploader = DataUploader()
    csv_content = uploader.download_from_s3(
        config.aws_bucket_name,
        config.s3_path_combined
    )
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

    # Parse dates (handle timezone-aware datetimes)
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)

    return df


def refresh_all_data():
    """Clear all cached data to force refresh."""
    st.cache_data.clear()

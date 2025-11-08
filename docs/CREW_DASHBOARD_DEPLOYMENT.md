# Crew Dashboard Deployment Guide

## Overview

The Basin Climbing repository now contains **two separate dashboards**:

1. **Owner Dashboard** (`streamlit_app.py`) - Chat-based analytics agent with full data access including revenue
2. **Crew Dashboard** (`crew_app.py`) - Operational metrics dashboard WITHOUT financial data

Both dashboards pull from the same S3 data sources managed by the daily pipeline.

---

## Dashboard Comparison

| Feature | Owner Dashboard | Crew Dashboard |
|---------|----------------|----------------|
| **File** | `streamlit_app.py` | `crew_app.py` |
| **Type** | Chat interface (Cliff agent) | Traditional dashboard |
| **Revenue Data** | ✅ Yes | ❌ No |
| **Transactions** | ✅ Yes | ❌ No |
| **Pricing Info** | ✅ Yes | ❌ No |
| **Memberships** | ✅ Yes (with $) | ✅ Yes (counts only) |
| **Check-ins** | ✅ Yes | ✅ Yes |
| **Events** | ✅ Yes | ✅ Yes |
| **At-Risk Members** | ✅ Yes | ✅ Yes |
| **Associations** | ✅ Yes | ✅ Yes |

---

## What's In The Crew Dashboard?

### 📊 Today's Overview
- Today's check-in count
- Total active members
- This week's visits
- Upcoming events (next 7 days)

### 📈 Check-in Trends
- Daily check-ins for last 30 days
- Peak hours analysis
- Busiest days of the week

### 🎫 Membership Overview
- Active memberships by type (Monthly/Annual/Weekly)
- Memberships by size (Solo/Duo/Family)
- **No pricing or revenue information**

### ⚠️ At-Risk Members
- Members who haven't visited in 14+ days
- Memberships expiring in next 30 days
- Very dormant members (30+ days no visit)
- Detailed list of expiring memberships

### 🎟️ Day Pass Usage
- Total day passes used (last 30 days)
- Daily day pass trend
- Frequent day pass users (4+ visits) - potential membership leads

### 🎯 Upcoming Events
- All events in next 7 days
- Date, time, capacity, status

### 👥 Member Groups
- Top 10 associations by member count
- Shows groups like "Active Member", "Founders Team", "Students", etc.

---

## How to Deploy on Streamlit Cloud

### Step 1: Deploy Owner Dashboard (Already Done)
Your existing deployment at https://basin-climbing-analytics.streamlit.app should already be pointing to `streamlit_app.py`.

### Step 2: Deploy Crew Dashboard (New)

1. **Go to Streamlit Cloud**: https://share.streamlit.io/

2. **Click "New app"**

3. **Configure the app**:
   - **Repository**: `steelferguson/agent-assisted-basin-climbing-dashboard`
   - **Branch**: `main`
   - **Main file path**: `crew_app.py` ⬅️ **Key difference!**
   - **App URL**: Choose something like `basin-crew-dashboard` or `basin-ops`

4. **Advanced Settings** → **Secrets**:
   Copy the same secrets from your owner dashboard:
   ```toml
   AWS_ACCESS_KEY_ID = "your-key"
   AWS_SECRET_ACCESS_KEY = "your-secret"
   ```

5. **Deploy**

You'll now have **two separate URLs**:
- **Owner Dashboard**: `https://basin-climbing-analytics.streamlit.app` (chat agent)
- **Crew Dashboard**: `https://basin-crew-dashboard.streamlit.app` (metrics dashboard)

---

## Sharing Access

### Owner Dashboard
- Share with: Owners, investors, financial stakeholders
- Access level: Full revenue data, transactions, pricing

### Crew Dashboard
- Share with: Front desk staff, coaches, operations team
- Access level: Operational metrics, no financial data
- Safe to share more widely

---

## Local Testing

### Test Crew Dashboard Locally
```bash
# Make sure you're in the project directory
cd agent-assisted-basin-climbing-dashboard

# Set environment variables
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"

# Run crew dashboard
streamlit run crew_app.py

# Or run owner dashboard
streamlit run streamlit_app.py
```

Both will open in separate browser tabs on different ports.

---

## Maintenance

### Updating Both Dashboards
Since both dashboards share the same data pipeline, updates to data sources automatically benefit both:

1. **Data pipeline changes**: Both dashboards automatically use new data
2. **Crew dashboard UI changes**: Only affects `crew_app.py`
3. **Owner dashboard changes**: Only affects `streamlit_app.py`
4. **Shared utilities**: Changes to `shared/data_loader.py` affect both

### Adding New Data Sources

When adding new data to the pipeline:

1. **Add to** `data_pipeline/` (as usual)
2. **Add loader** to `shared/data_loader.py`
3. **Update crew dashboard** if appropriate (no revenue)
4. **Update owner dashboard** if needed

---

## Architecture

```
agent-assisted-basin-climbing-dashboard/
├── data_pipeline/              # Shared data fetching (S3)
│   ├── fetch_*.py
│   ├── pipeline_handler.py
│   └── config.py
├── shared/                     # Shared utilities
│   ├── data_loader.py         # S3 data loading for dashboards
│   └── __init__.py
├── streamlit_app.py            # Owner: Chat agent with revenue
├── crew_app.py                 # Crew: Metrics dashboard (no $)
└── requirements.txt            # Shared dependencies
```

**Data Flow:**
```
Daily Pipeline → S3 → shared/data_loader.py → Both Dashboards
```

---

## Security Notes

1. **No authentication built-in**: Streamlit Cloud apps are public by URL
2. **Obscurity-based**: Share URLs only with intended users
3. **Crew dashboard safe**: Contains no financial data, pricing, or transaction details
4. **Owner dashboard sensitive**: Contains revenue data - share URL carefully

If you need stronger security, consider:
- Streamlit's built-in authentication (paid feature)
- OAuth integration
- IP whitelisting via hosting provider

---

## Troubleshooting

### "Error loading dashboard"
- Check that AWS credentials are set in Streamlit Cloud secrets
- Verify S3 bucket has all required data files
- Check that daily pipeline ran successfully

### "No data available"
- Run `python run_daily_pipeline.py` to populate S3
- Or run `python backfill_checkin_history.py` for historical check-ins

### Data seems outdated
- Click "Refresh Data" button in sidebar
- Data is cached for 5 minutes for performance

### Import errors
- Ensure `requirements.txt` includes all dependencies
- Check that Streamlit Cloud rebuilt the app after updating requirements

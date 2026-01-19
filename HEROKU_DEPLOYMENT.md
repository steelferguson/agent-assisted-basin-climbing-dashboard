# Heroku Deployment Guide - Basin Climbing Dashboard

This guide explains how to deploy the main Basin Climbing dashboard (Streamlit app) to Heroku.

## Files for Heroku Deployment

The following files configure the Heroku deployment:

1. **`Procfile`** - Tells Heroku how to run the Streamlit app
2. **`setup.sh`** - Configures Streamlit settings for Heroku environment
3. **`runtime.txt`** - Specifies Python 3.12.8
4. **`requirements.txt`** - Python dependencies (already exists)
5. **`main_basin_dashboard.py`** - Main dashboard file (renamed from streamlit_dashboard.py)

## Prerequisites

1. **Heroku Account** - Sign up at https://heroku.com
2. **Heroku CLI** - Install from https://devcenter.heroku.com/articles/heroku-cli
3. **Git** - For deployment

## Deployment Steps

### 1. Login to Heroku

```bash
heroku login
```

### 2. Create a New Heroku App

```bash
heroku create basin-climbing-dashboard
```

Or use a custom name:
```bash
heroku create your-custom-name
```

### 3. Set Environment Variables

The dashboard needs AWS credentials to access S3 data:

```bash
heroku config:set AWS_ACCESS_KEY_ID=your_aws_access_key_id
heroku config:set AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
```

To verify environment variables are set:
```bash
heroku config
```

### 4. Deploy to Heroku

Push the code to Heroku:

```bash
git push heroku heroku-dashboard-deployment:main
```

Or if deploying from main branch:
```bash
git push heroku main
```

### 5. Open the Dashboard

```bash
heroku open
```

Or visit: `https://your-app-name.herokuapp.com`

## Monitoring and Logs

### View Logs
```bash
heroku logs --tail
```

### Check App Status
```bash
heroku ps
```

### Restart the App
```bash
heroku restart
```

## Updating the Dashboard

To deploy updates:

1. Make your code changes
2. Commit to git:
   ```bash
   git add .
   git commit -m "Update dashboard"
   ```
3. Push to Heroku:
   ```bash
   git push heroku main
   ```

## Troubleshooting

### App Crashes on Startup

Check logs:
```bash
heroku logs --tail
```

Common issues:
- Missing environment variables (AWS credentials)
- Python version mismatch
- Missing dependencies in requirements.txt

### Slow Loading

Heroku free dynos sleep after 30 minutes of inactivity. First load may be slow as the dyno wakes up. Consider upgrading to a paid dyno for production.

### Memory Issues

If the app uses too much memory, consider upgrading to a larger dyno:
```bash
heroku ps:scale web=1:standard-1x
```

## Dashboard vs Dash App

Note: This Procfile is configured for the **Streamlit dashboard** (`main_basin_dashboard.py`).

If you want to deploy the Dash dashboard instead (`dashboard/dashboard.py`), change the Procfile to:
```
web: gunicorn dashboard.app:server
```

## Cost Considerations

- **Free Tier**: Heroku offers free dynos with limitations (sleeps after 30 min inactivity)
- **Hobby Tier** ($7/month): No sleeping, custom domains
- **Standard Tier** ($25/month): Better performance, metrics

## Alternative: Streamlit Cloud

If you prefer to continue using Streamlit Cloud (currently hosting the dashboard):
1. Streamlit Cloud automatically detects Streamlit apps
2. Connects directly to GitHub repo
3. Free for public repos
4. Simpler deployment (no Procfile needed)

Choose Heroku if you need:
- More control over hosting environment
- Custom domain without Streamlit branding
- Integration with other Heroku services
- More flexible deployment options

## Support

- Heroku Documentation: https://devcenter.heroku.com/
- Streamlit on Heroku: https://devcenter.heroku.com/articles/python-streamlit
- Basin Climbing Dashboard Issues: https://github.com/steelferguson/agent-assisted-basin-climbing-dashboard/issues

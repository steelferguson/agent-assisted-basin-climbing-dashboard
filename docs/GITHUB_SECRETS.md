# GitHub Secrets Configuration

**Last Updated:** November 8, 2025

**IMPORTANT: This file lists secret names only. Never include actual secret values in this file.**

## Current GitHub Secrets

These secrets are configured in the GitHub repository settings for use by GitHub Actions (daily pipeline automation).

### Payment Processing APIs
1. **`STRIPE_PRODUCTION_API_KEY`**
   - Purpose: Fetch transaction data from Stripe
   - Used by: `data_pipeline/fetch_stripe_data.py`

2. **`SQUARE_PRODUCTION_API_TOKEN`**
   - Purpose: Fetch transaction data from Square
   - Used by: `data_pipeline/fetch_square_data.py`

### Membership & Check-in System
3. **`CAPITAN_API_TOKEN`**
   - Purpose: Fetch member, membership, check-in, and event data from Capitan
   - Used by: `data_pipeline/fetch_capitan_data.py`

### Social Media & Marketing
4. **`INSTAGRAM_ACCESS_TOKEN`**
   - Purpose: Fetch Instagram posts, comments, and insights
   - Type: Facebook Graph API long-lived token (60-day expiration)
   - Expires: ~December 31, 2025
   - Used by: `data_pipeline/fetch_instagram_data.py`, `data_pipeline/fetch_facebook_ads_data.py`
   - **Note:** Needs manual refresh every 60 days (see `INSTAGRAM_CREDENTIALS.md`)
   - **Note:** Same token used for Facebook Ads data

5. **`FACEBOOK_AD_ACCOUNT_ID`**
   - Purpose: Fetch Facebook/Instagram Ads performance data
   - Format: Numeric ID (e.g., `272120788771569`)
   - Used by: `data_pipeline/fetch_facebook_ads_data.py`
   - **Note:** Uses same access token as Instagram

6. **`MAILCHIMP_API_KEY`**
   - Purpose: Fetch email campaign data, automations, landing pages, and audience growth
   - Format: `{api_key}-{server_prefix}` (e.g., `xxxxx-us9`)
   - Used by: `data_pipeline/fetch_mailchimp_data.py`

### E-commerce
7. **`SHOPIFY_STORE_DOMAIN`**
   - Purpose: Fetch Shopify orders and manage customer tags
   - Format: Store domain (e.g., `basin-climbing.myshopify.com`)
   - Used by: `data_pipeline/fetch_shopify_data.py`, `data_pipeline/sync_flags_to_shopify.py`

8. **`SHOPIFY_ADMIN_TOKEN`**
   - Purpose: Shopify Admin API authentication
   - Used by: `data_pipeline/fetch_shopify_data.py`, `data_pipeline/sync_flags_to_shopify.py`

### Messaging
9. **`TWILIO_ACCOUNT_SID`**
   - Purpose: Fetch SMS message data for waiver tracking
   - Used by: `data_pipeline/sync_twilio_opt_ins.py`

10. **`TWILIO_AUTH_TOKEN`**
    - Purpose: Twilio API authentication
    - Used by: `data_pipeline/sync_twilio_opt_ins.py`

11. **`TWILIO_PHONE_NUMBER`**
    - Purpose: Identify Basin's phone number in Twilio data
    - Used by: `data_pipeline/sync_twilio_opt_ins.py`

12. **`SENDGRID_API_KEY`**
    - Purpose: Fetch email activity data for AB test experiment tracking
    - Used by: `data_pipeline/fetch_sendgrid_data.py`
    - **Note:** Tracks emails sent, delivered, opened, and clicked for conversion funnels

### Cloud Storage
13. **`AWS_ACCESS_KEY_ID`**
   - Purpose: Access AWS S3 for data storage and retrieval
   - Used by: All pipeline scripts and Streamlit dashboard

14. **`AWS_SECRET_ACCESS_KEY`**
   - Purpose: AWS authentication secret key
   - Used by: All pipeline scripts and Streamlit dashboard

## Optional Secrets (Not Currently Required)

These have default values in `config.py` but can be overridden:

- `INSTAGRAM_BUSINESS_ACCOUNT_ID` (default: `17841455043408233`)
- `MAILCHIMP_SERVER_PREFIX` (default: `us9`)
- `MAILCHIMP_AUDIENCE_ID` (default: `6113b6f2ca`)
- `OPENAI_API_KEY` (for AI content analysis - optional)
- `ANTHROPIC_API_KEY` (for AI content analysis - optional)

## Where Secrets Are Used

### GitHub Actions
- **Workflow:** `.github/workflows/daily_update.yml`
- **Schedule:** Daily at 6 AM UTC
- **Purpose:** Automatically fetch fresh data from all APIs and update S3

### Streamlit Cloud (Separate Configuration)
The Streamlit dashboard needs only AWS credentials (configured separately in Streamlit Cloud settings):
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

Dashboard only reads pre-processed data from S3, so it doesn't need API keys for Stripe, Square, Capitan, etc.

## Security Best Practices

- ✅ All secrets stored in GitHub repository settings (Settings > Secrets and variables > Actions)
- ✅ Secrets are encrypted and never exposed in logs
- ✅ Never commit actual secret values to the repository
- ✅ Keep credential documentation files (e.g., `INSTAGRAM_CREDENTIALS.md`, `MAILCHIMP_CREDENTIALS.md`) in `.gitignore`
- ⚠️ Instagram token expires every 60 days - set calendar reminder for renewal
- ⚠️ Rotate secrets if compromised
- ⚠️ Use principle of least privilege for all API keys

## Maintenance Schedule

### Monthly
- [ ] Verify all GitHub Actions are running successfully
- [ ] Check S3 bucket for data freshness

### Every 60 Days (Instagram Token Refresh)
- [ ] Generate new Instagram long-lived access token
- [ ] Update `INSTAGRAM_ACCESS_TOKEN` in GitHub Secrets
- [ ] Update token in `INSTAGRAM_CREDENTIALS.md` (local only)
- [ ] Test Instagram data fetch

### Annually
- [ ] Audit all API keys for usage and necessity
- [ ] Consider rotating all secrets as security best practice
- [ ] Review GitHub Actions logs for any authentication issues

## Adding or Updating Secrets

To add or update a secret in GitHub:

1. Go to repository: `https://github.com/steelferguson/agent-assisted-basin-climbing-dashboard`
2. Navigate to: Settings > Secrets and variables > Actions
3. Click "New repository secret" or edit existing secret
4. Enter secret name (must match exactly as shown above)
5. Paste secret value
6. Click "Add secret" or "Update secret"

## Troubleshooting

### Pipeline Fails with Authentication Error
1. Check GitHub Actions logs for which API is failing
2. Verify the corresponding secret is set correctly in GitHub
3. For Instagram: Check if token has expired (60-day limit)
4. Test API key manually using instructions in respective credential files

### Dashboard Can't Load Data
1. Verify AWS credentials are set in Streamlit Cloud settings
2. Check S3 bucket access permissions
3. Ensure data files exist in S3 (run pipeline manually if needed)

## Related Documentation

- `INSTAGRAM_CREDENTIALS.md` - Detailed Instagram API setup and token renewal
- `MAILCHIMP_CREDENTIALS.md` - Mailchimp API configuration and endpoints
- `.github/workflows/daily_update.yml` - Pipeline automation configuration
- `data_pipeline/config.py` - Environment variable definitions and defaults

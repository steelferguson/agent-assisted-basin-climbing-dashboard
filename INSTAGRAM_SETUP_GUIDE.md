# Instagram Data Integration Setup Guide

## Overview

This guide walks you through connecting Instagram post data to your analytics system using the Instagram Graph API.

## Prerequisites

✅ **What You Need:**
1. Instagram Business or Creator account (Basin Climbing's account)
2. Facebook Page connected to the Instagram account
3. Facebook Developer account
4. Admin access to both Instagram and Facebook accounts

## Step-by-Step Setup Process

### Phase 1: Prepare Instagram Account (15 minutes)

#### 1.1 Convert to Business Account (if not already)
1. Log into Basin Climbing's Instagram account
2. Go to **Settings** → **Account**
3. Click **Switch to Professional Account**
4. Select **Business** (for companies)
5. Choose a category (e.g., "Gym/Physical Fitness Center")
6. Skip contact information step (optional)

#### 1.2 Create/Link Facebook Page
1. Go to Facebook and create a Page for Basin Climbing (if you don't have one)
2. In Instagram settings, go to **Professional dashboard** → **Linked Accounts**
3. Click **Connect account** from Instagram
4. Select your Facebook Page to link

**⚠️ Important:** The Instagram account MUST be linked to a Facebook Page to use the Graph API.

---

### Phase 2: Create Facebook Developer App (20 minutes)

#### 2.1 Create Developer Account
1. Go to https://developers.facebook.com/
2. Click **Get Started** (if new) or **My Apps**
3. Complete developer registration if needed

#### 2.2 Create New App
1. Click **Create App**
2. Select **Business** as the app type
3. Fill in details:
   - **App Name:** "Basin Climbing Analytics" (or similar)
   - **App Contact Email:** Your email
   - **Business Account:** Select or create one
4. Click **Create App**

#### 2.3 Add Instagram Graph API Product
1. In your new app dashboard, find **Add Products**
2. Locate **Instagram Graph API**
3. Click **Set Up**
4. Also add **Facebook Login** (required for authentication)

---

### Phase 3: Configure App Settings (15 minutes)

#### 3.1 Get Access Tokens
1. In app dashboard, go to **Tools** → **Graph API Explorer**
2. Select your app from dropdown
3. Under **User or Page**, select your Instagram Business Account
4. Add these permissions:
   - `instagram_basic`
   - `instagram_manage_insights`
   - `pages_show_list`
   - `pages_read_engagement`
5. Click **Generate Access Token**
6. **Save this token securely!** (You'll need it for API calls)

#### 3.2 Get Instagram Business Account ID
1. In Graph API Explorer, make this request:
   ```
   GET /me/accounts
   ```
2. Find your Facebook Page ID in the response
3. Then make this request:
   ```
   GET /{page-id}?fields=instagram_business_account
   ```
4. **Save the `instagram_business_account.id`** - this is your IG Account ID

---

### Phase 4: Test API Access (10 minutes)

#### 4.1 Test Basic Query
Use Graph API Explorer to test:
```
GET /{ig-user-id}/media?fields=id,caption,media_type,media_url,timestamp,like_count,comments_count
```

#### 4.2 Test Insights Query
```
GET /{ig-user-id}/insights?metric=impressions,reach,profile_views&period=day
```

If both work, you're ready to integrate!

---

## Available Instagram Metrics

### Account-Level Metrics
- `impressions` - Total number of times posts were seen
- `reach` - Total unique accounts reached
- `profile_views` - Number of profile views
- `follower_count` - Total followers
- `email_contacts` - Number of email button taps
- `phone_call_clicks` - Number of call button taps
- `text_message_clicks` - Number of text button taps
- `get_directions_clicks` - Number of direction button taps
- `website_clicks` - Number of website link taps

### Post-Level Metrics
- `engagement` - Total likes, comments, saves
- `impressions` - Times post was viewed
- `reach` - Unique accounts reached
- `saved` - Number of saves
- `video_views` - Views (for video posts)
- `likes` - (available via media object)
- `comments` - (available via media object)

### Story Metrics (if needed)
- `exits` - Times someone swiped away
- `replies` - Direct message replies
- `taps_forward` - Taps to next story
- `taps_back` - Taps to previous story

---

## Integration Plan for Your Dashboard

### Architecture (Similar to Revenue Data Pipeline)

**Data Flow:**
```
Instagram API → fetch_instagram_data.py → S3 Storage → Analytics Agent → Dashboard
```

Just like your revenue data:
- Fetch new data daily/weekly from Instagram API
- Store in S3 as CSV (persistent storage)
- Analytics agent reads from S3 (not live API)
- Incremental updates: fetch only new posts, append to existing data

### Step 1: Create Instagram Data Fetcher
Create `data_pipeline/fetch_instagram_data.py`:

**Similar to your existing fetchers:**
- Class: `InstagramDataFetcher` (like `StripeFetcher`, `SquareFetcher`)
- Fetch recent posts with metadata (caption, media_url, timestamp)
- Fetch post insights (engagement, reach, impressions)
- Fetch account insights (follower growth, profile views)
- **AI Vision Enhancement:** Use Claude/GPT-4 Vision to analyze images
  - Generate descriptions of what's in the photo/video
  - Extract themes (climbing, fitness, community, etc.)
  - Identify activities shown
- Save to S3: `instagram/posts.csv`, `instagram/account_metrics.csv`

**Data Schema (posts.csv):**
```csv
post_id,timestamp,media_type,media_url,caption,likes,comments,saves,impressions,reach,engagement_rate,ai_description,ai_themes,hashtags
```

**Data Schema (account_metrics.csv):**
```csv
date,follower_count,profile_views,website_clicks,impressions,reach
```

### Step 2: Add to pipeline_handler.py
Add function similar to `upload_new_capitan_membership_data()`:
```python
def upload_new_instagram_data(save_local=False):
    # Fetch last 7 days of posts
    # Download existing data from S3
    # Append new data (remove duplicates)
    # Upload back to S3
    # Monthly snapshot on day 1
```

### Step 3: Add Instagram Tools to Analytics Agent
Add to `agent/analytics_tools.py`:
- `get_instagram_post_performance` - Top/bottom performing posts
- `get_instagram_engagement_rate` - Calculate engagement metrics
- `get_instagram_content_analysis` - AI-generated content themes
- `get_instagram_follower_growth` - Track follower trends
- `create_instagram_engagement_chart` - Visualize post performance
- `create_instagram_content_chart` - Chart by content type/theme

### Step 4: AI Image Analysis Details

**What We'll Extract from Images:**
1. **Visual Description:** "Photo shows person bouldering on an overhang with colorful holds"
2. **Activity Type:** Climbing, training, yoga, community event, etc.
3. **Setting:** Indoor gym, outdoor, classroom, social area
4. **People:** Solo climber, group, instructor, kids
5. **Mood/Vibe:** Energetic, focused, fun, challenging
6. **Gear Visible:** Shoes, harness, crash pads, etc.

**API Options for Vision:**
- Claude 3.5 Sonnet (Anthropic) - You already have the API key!
- GPT-4 Vision (OpenAI) - You might have this too
- Both can analyze image URLs directly

**Example:**
```python
# Instagram gives us: media_url = "https://instagram.com/p/abc123/media"
# We send to Claude Vision API with prompt:
"Analyze this climbing gym social media post. Describe what you see,
identify the activity, setting, and overall vibe. Extract key themes."
```

---

## Next Steps

1. **Complete Phase 1-3** (Set up Instagram Business account and Developer App)
2. **Save credentials securely:**
   - Access Token → Add to environment variables
   - Instagram Business Account ID → Add to config
3. **Share credentials with me** so I can:
   - Create the data fetcher script
   - Add Instagram analytics tools
   - Integrate with your existing pipeline

---

## Important Notes

### Access Token Longevity
- **Short-lived tokens** expire in 1 hour
- **Long-lived tokens** expire in 60 days
- You'll need to implement token refresh or generate new tokens periodically
- Consider using Facebook's token debugger to extend tokens

### Rate Limits
- 200 calls per hour per user
- Plan your data fetching accordingly
- Implement error handling for rate limit errors

### Data Retention
- Instagram stores insights data for 2 years
- Fetch historical data soon after setup
- Store data locally/S3 for longer retention

### App Review (For Production)
- Development mode works for your own accounts
- For public use or certain permissions, you need App Review
- For now, development mode should be sufficient

---

## Troubleshooting

**"Invalid OAuth access token"**
→ Token expired, generate a new one

**"Requires instagram_basic permission"**
→ Add permission in Graph API Explorer and regenerate token

**"Instagram account not found"**
→ Make sure account is Business/Creator and linked to Facebook Page

**"Insights not available"**
→ Wait 24 hours after posting for insights to populate

---

## Resources

- [Instagram Graph API Documentation](https://developers.facebook.com/docs/instagram-api/)
- [Meta for Developers Console](https://developers.facebook.com/)
- [Graph API Explorer](https://developers.facebook.com/tools/explorer/)
- [Token Debugger](https://developers.facebook.com/tools/debug/accesstoken/)

---

## Ready to Implement?

Once you complete the setup and have:
1. ✅ Access Token
2. ✅ Instagram Business Account ID

Let me know and I'll create the integration code to pull Instagram data into your analytics system!

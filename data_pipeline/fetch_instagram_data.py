"""
Instagram Data Fetcher

Fetches Instagram post data and metrics using the Instagram Graph API.
Includes AI-powered vision analysis using Claude to describe post content.
"""

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import os
from typing import Dict, List, Optional
from anthropic import Anthropic
import base64
import mimetypes


class InstagramDataFetcher:
    """
    A class for fetching and processing Instagram Business account data.

    Features:
    - Fetches posts with metadata (captions, media URLs, likes, comments)
    - Fetches insights (reach, saved) for posts
    - AI vision analysis to describe post content and extract themes
    - Smart incremental updates (only update recent posts to save API calls)
    """

    def __init__(self, access_token: str, business_account_id: str, anthropic_api_key: Optional[str] = None):
        """
        Initialize the Instagram data fetcher.

        Args:
            access_token: Instagram Graph API access token
            business_account_id: Instagram Business Account ID
            anthropic_api_key: Anthropic API key for vision analysis (optional)
        """
        self.access_token = access_token
        self.business_account_id = business_account_id
        self.base_url = "https://graph.facebook.com/v21.0"
        self.anthropic_api_key = anthropic_api_key

        if self.anthropic_api_key:
            self.anthropic_client = Anthropic(api_key=self.anthropic_api_key)
        else:
            self.anthropic_client = None
            print("Warning: No Anthropic API key provided. Vision analysis will be skipped.")

    def _make_api_request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Make API request to Instagram Graph API."""
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making API request to {url}: {e}")
            return None

    def get_account_info(self) -> Dict:
        """Get basic account information."""
        url = f"{self.base_url}/{self.business_account_id}"
        params = {
            'fields': 'username,followers_count,media_count',
            'access_token': self.access_token
        }
        return self._make_api_request(url, params)

    def get_posts(self, limit: int = 100, since: Optional[datetime] = None) -> List[Dict]:
        """
        Fetch Instagram posts with basic metadata.

        Args:
            limit: Maximum number of posts to fetch
            since: Only fetch posts created after this date (optional, filters client-side)

        Returns:
            List of post dictionaries
        """
        url = f"{self.base_url}/{self.business_account_id}/media"
        params = {
            'fields': 'id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count',
            'limit': min(limit, 100),  # API max per page is 100
            'access_token': self.access_token
        }

        # Note: Instagram API doesn't support 'since' parameter well
        # We'll filter client-side after fetching

        all_posts = []

        while url and len(all_posts) < limit:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break

            all_posts.extend(data['data'])

            # Stop if we've reached the limit
            if len(all_posts) >= limit:
                all_posts = all_posts[:limit]
                break

            # Check if there's a next page
            url = data.get('paging', {}).get('next')
            params = None  # Next URL already has params

            print(f"Fetched {len(all_posts)} posts so far...")

        print(f"Total posts fetched: {len(all_posts)}")

        # Filter by date if 'since' is provided (client-side filtering)
        if since and all_posts:
            filtered_posts = []
            for post in all_posts:
                timestamp_str = post['timestamp']
                if '+0000' in timestamp_str:
                    timestamp_str = timestamp_str.replace('+0000', '+00:00')
                post_date = datetime.fromisoformat(timestamp_str)

                if post_date >= since:
                    filtered_posts.append(post)

            print(f"Filtered to {len(filtered_posts)} posts since {since.date()}")
            return filtered_posts

        return all_posts

    def get_post_insights(self, post_id: str) -> Dict:
        """
        Fetch insights for a specific post.

        Args:
            post_id: Instagram post ID

        Returns:
            Dictionary with insight metrics
        """
        url = f"{self.base_url}/{post_id}/insights"
        params = {
            'metric': 'reach,saved',  # impressions removed in v22+
            'access_token': self.access_token
        }

        data = self._make_api_request(url, params)
        if not data or 'data' not in data:
            return {'reach': None, 'saved': None}

        # Parse insights response
        insights = {}
        for metric in data['data']:
            metric_name = metric['name']
            metric_value = metric['values'][0]['value'] if metric['values'] else None
            insights[metric_name] = metric_value

        return insights

    def get_post_comments(self, post_id: str) -> List[Dict]:
        """
        Fetch comments for a specific post.

        Args:
            post_id: Instagram post ID

        Returns:
            List of comment dictionaries
        """
        url = f"{self.base_url}/{post_id}/comments"
        params = {
            'fields': 'id,username,text,timestamp,like_count',
            'access_token': self.access_token
        }

        all_comments = []

        while url:
            data = self._make_api_request(url, params)
            if not data or 'data' not in data:
                break

            all_comments.extend(data['data'])

            # Check if there's a next page
            url = data.get('paging', {}).get('next')
            params = None  # Next URL already has params

        return all_comments

    def analyze_image_with_ai(self, image_url: str, caption: str = "") -> Dict:
        """
        Use Claude Vision API to analyze post image and extract insights.

        Downloads the image first and sends as base64 to avoid Instagram CDN blocking.

        Args:
            image_url: URL of the image to analyze
            caption: Post caption for context

        Returns:
            Dictionary with AI-generated description and themes
        """
        if not self.anthropic_client:
            return {
                'ai_description': None,
                'ai_themes': None,
                'ai_activity_type': None
            }

        try:
            # Download the image first (Instagram blocks Claude from directly accessing URLs)
            response = requests.get(image_url, timeout=10)
            response.raise_for_status()
            image_data = response.content

            # Detect media type
            content_type = response.headers.get('Content-Type', '')
            if not content_type or 'image' not in content_type:
                # Try to guess from URL
                content_type = mimetypes.guess_type(image_url)[0] or 'image/jpeg'

            # Encode as base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')

            prompt = f"""Analyze this climbing gym social media post image.

Caption: "{caption}"

Please provide:
1. **Visual Description** (2-3 sentences): What do you see in the image?
2. **Activity Type** (1-2 words): What activity is shown? (e.g., bouldering, training, yoga, community event, etc.)
3. **Themes** (comma-separated): Key themes or tags (e.g., climbing, fitness, community, youth, competition, etc.)

Format your response as:
DESCRIPTION: [your description]
ACTIVITY: [activity type]
THEMES: [comma-separated themes]"""

            message = self.anthropic_client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=300,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": content_type,
                                    "data": image_base64,
                                },
                            },
                            {
                                "type": "text",
                                "text": prompt
                            }
                        ],
                    }
                ],
            )

            # Parse response
            response_text = message.content[0].text

            description = None
            activity = None
            themes = None

            for line in response_text.split('\n'):
                if line.startswith('DESCRIPTION:'):
                    description = line.replace('DESCRIPTION:', '').strip()
                elif line.startswith('ACTIVITY:'):
                    activity = line.replace('ACTIVITY:', '').strip()
                elif line.startswith('THEMES:'):
                    themes = line.replace('THEMES:', '').strip()

            return {
                'ai_description': description,
                'ai_themes': themes,
                'ai_activity_type': activity
            }

        except Exception as e:
            print(f"Error analyzing image {image_url}: {e}")
            return {
                'ai_description': None,
                'ai_themes': None,
                'ai_activity_type': None
            }

    def should_update_post_metrics(self, post_timestamp: str) -> bool:
        """
        Determine if we should fetch fresh metrics for a post.
        Smart strategy to reduce API calls:
        - Posts <= 7 days old: Update daily
        - Posts 8-30 days old: Update weekly (on Mondays)
        - Posts > 30 days old: Skip (metrics stabilize)

        Args:
            post_timestamp: ISO 8601 timestamp string from Instagram

        Returns:
            True if we should fetch fresh metrics
        """
        # Instagram timestamps come in format: "2025-10-28T15:54:34+0000"
        timestamp_str = post_timestamp
        if '+0000' in timestamp_str:
            timestamp_str = timestamp_str.replace('+0000', '+00:00')
        post_date = datetime.fromisoformat(timestamp_str)
        post_age_days = (datetime.now(post_date.tzinfo) - post_date).days

        if post_age_days <= 7:
            return True  # Update daily for recent posts
        elif post_age_days <= 30:
            return datetime.now().weekday() == 0  # Update weekly (Mondays) for month-old posts
        else:
            return False  # Skip old posts

    def fetch_all_comments(self, posts: List[Dict]) -> pd.DataFrame:
        """
        Fetch comments for all posts and return as DataFrame.

        Args:
            posts: List of post dictionaries from get_posts()

        Returns:
            DataFrame with all comments
        """
        all_comments = []

        for i, post in enumerate(posts):
            post_id = post['id']
            print(f"Fetching comments for post {i+1}/{len(posts)}: {post_id}")

            comments = self.get_post_comments(post_id)

            for comment in comments:
                all_comments.append({
                    'post_id': post_id,
                    'comment_id': comment['id'],
                    'username': comment.get('username', ''),
                    'text': comment.get('text', ''),  # Some comments may not have text (e.g., emoji-only)
                    'timestamp': comment.get('timestamp', ''),
                    'comment_likes': comment.get('like_count', 0),
                    'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            if comments:
                print(f"  Found {len(comments)} comments")

        if not all_comments:
            print("No comments found on any posts.")
            return pd.DataFrame(columns=['post_id', 'comment_id', 'username', 'text', 'timestamp', 'comment_likes', 'fetched_at'])

        df = pd.DataFrame(all_comments)
        print(f"\n✅ Fetched {len(df)} total comments across {len(posts)} posts")
        return df

    def fetch_and_process_posts(
        self,
        limit: int = 100,
        since: Optional[datetime] = None,
        enable_vision_analysis: bool = True,
        fetch_comments: bool = True,
        existing_posts_df: Optional[pd.DataFrame] = None
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Fetch posts, insights, AI analysis, and comments.

        Args:
            limit: Maximum number of posts to fetch
            since: Only fetch posts created after this date
            enable_vision_analysis: Whether to run AI vision analysis
            fetch_comments: Whether to fetch comments for posts
            existing_posts_df: DataFrame of existing posts (to skip AI analysis if already done)

        Returns:
            Tuple of (posts_df, comments_df)
        """
        print(f"Fetching Instagram posts (limit={limit}, since={since})...")
        posts = self.get_posts(limit=limit, since=since)

        if not posts:
            print("No posts found.")
            empty_posts = pd.DataFrame()
            empty_comments = pd.DataFrame(columns=['post_id', 'comment_id', 'username', 'text', 'timestamp', 'comment_likes', 'fetched_at'])
            return empty_posts, empty_comments

        rows = []
        for i, post in enumerate(posts):
            print(f"\nProcessing post {i+1}/{len(posts)}: {post['id']}")

            # Basic post data
            post_data = {
                'post_id': post['id'],
                'timestamp': post['timestamp'],
                'media_type': post['media_type'],
                'media_url': post.get('media_url') or post.get('thumbnail_url'),
                'permalink': post['permalink'],
                'caption': post.get('caption', ''),
                'likes': post.get('like_count', 0),
                'comments': post.get('comments_count', 0),
            }

            # Calculate post age
            # Instagram timestamps come in format: "2025-10-28T15:54:34+0000"
            timestamp_str = post['timestamp']
            if '+0000' in timestamp_str:
                timestamp_str = timestamp_str.replace('+0000', '+00:00')
            post_date = datetime.fromisoformat(timestamp_str)
            post_age_days = (datetime.now(post_date.tzinfo) - post_date).days
            post_data['post_age_days'] = post_age_days

            # Fetch insights (if post is recent enough)
            if self.should_update_post_metrics(post['timestamp']):
                print(f"  Fetching insights (post is {post_age_days} days old)...")
                insights = self.get_post_insights(post['id'])
                post_data['reach'] = insights.get('reach')
                post_data['saved'] = insights.get('saved')
            else:
                print(f"  Skipping insights (post is {post_age_days} days old)")
                post_data['reach'] = None
                post_data['saved'] = None

            # AI vision analysis
            # Check if post already has AI data in existing dataset
            skip_ai = False
            if enable_vision_analysis and existing_posts_df is not None and not existing_posts_df.empty:
                existing_post = existing_posts_df[existing_posts_df['post_id'] == post['id']]
                if not existing_post.empty and pd.notna(existing_post.iloc[0].get('ai_description')):
                    # Post already has AI analysis, reuse it
                    print("  Skipping AI analysis (already exists)")
                    post_data['ai_description'] = existing_post.iloc[0].get('ai_description')
                    post_data['ai_themes'] = existing_post.iloc[0].get('ai_themes')
                    post_data['ai_activity_type'] = existing_post.iloc[0].get('ai_activity_type')
                    skip_ai = True

            if not skip_ai and enable_vision_analysis and post_data['media_url'] and self.anthropic_client:
                print("  Running AI vision analysis...")
                ai_analysis = self.analyze_image_with_ai(
                    post_data['media_url'],
                    post_data['caption']
                )
                post_data.update(ai_analysis)
            elif not skip_ai:
                post_data['ai_description'] = None
                post_data['ai_themes'] = None
                post_data['ai_activity_type'] = None

            # Calculate engagement rate
            if post_data['reach'] and post_data['reach'] > 0:
                engagement = post_data['likes'] + post_data['comments'] + (post_data['saved'] or 0)
                post_data['engagement_rate'] = round(engagement / post_data['reach'] * 100, 2)
            else:
                post_data['engagement_rate'] = None

            post_data['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            rows.append(post_data)

        posts_df = pd.DataFrame(rows)

        # Ensure consistent column order
        column_order = [
            'post_id', 'timestamp', 'post_age_days', 'media_type', 'media_url',
            'permalink', 'caption', 'likes', 'comments', 'reach', 'saved',
            'engagement_rate', 'ai_description', 'ai_themes', 'ai_activity_type',
            'last_updated'
        ]
        posts_df = posts_df[column_order]

        print(f"\n✅ Processed {len(posts_df)} posts successfully")

        # Fetch comments if enabled
        if fetch_comments:
            print("\nFetching comments for all posts...")
            comments_df = self.fetch_all_comments(posts)
        else:
            comments_df = pd.DataFrame(columns=['post_id', 'comment_id', 'username', 'text', 'timestamp', 'comment_likes', 'fetched_at'])

        return posts_df, comments_df

    def save_data(self, df: pd.DataFrame, file_name: str, output_dir: str = "data/outputs"):
        """Save DataFrame to CSV."""
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"{file_name}.csv")
        df.to_csv(filepath, index=False)
        print(f"Saved data to {filepath}")


def test_fetcher():
    """Test function to verify the Instagram fetcher works."""
    from data_pipeline import config

    # Get credentials from config or environment
    access_token = os.getenv('INSTAGRAM_ACCESS_TOKEN')
    business_account_id = os.getenv('INSTAGRAM_BUSINESS_ACCOUNT_ID', '17841455043408233')
    anthropic_api_key = config.anthropic_api_key

    if not access_token:
        print("Error: INSTAGRAM_ACCESS_TOKEN environment variable not set")
        return

    fetcher = InstagramDataFetcher(
        access_token=access_token,
        business_account_id=business_account_id,
        anthropic_api_key=anthropic_api_key
    )

    # Test account info
    print("Testing account info fetch...")
    account_info = fetcher.get_account_info()
    print(f"Account: @{account_info['username']}")
    print(f"Followers: {account_info['followers_count']}")
    print(f"Total Posts: {account_info['media_count']}")

    # Test fetching last 7 days of posts
    print("\nFetching last 7 days of posts...")
    since_date = datetime.now() - timedelta(days=7)
    posts_df, comments_df = fetcher.fetch_and_process_posts(
        limit=10,
        since=since_date,
        enable_vision_analysis=True,
        fetch_comments=True
    )

    print("\n=== POSTS DataFrame Preview ===")
    print(posts_df.head())
    print(f"\nTotal posts: {len(posts_df)}")

    print("\n=== COMMENTS DataFrame Preview ===")
    print(comments_df.head())
    print(f"\nTotal comments: {len(comments_df)}")

    # Save locally for inspection
    fetcher.save_data(posts_df, "instagram_posts_test")
    fetcher.save_data(comments_df, "instagram_comments_test")
    print("\n✅ Test complete!")


if __name__ == "__main__":
    test_fetcher()

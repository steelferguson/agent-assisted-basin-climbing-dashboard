import os

# data_pipeline configurations
stripe_key = os.getenv("STRIPE_PRODUCTION_API_KEY")
square_token = os.getenv("SQUARE_PRODUCTION_API_TOKEN")
capitan_token = os.getenv("CAPITAN_API_TOKEN")
instagram_access_token = os.getenv("INSTAGRAM_ACCESS_TOKEN")
instagram_business_account_id = os.getenv("INSTAGRAM_BUSINESS_ACCOUNT_ID", "17841455043408233")
facebook_ad_account_id = os.getenv("FACEBOOK_AD_ACCOUNT_ID", "272120788771569")
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
openai_api_key = os.getenv("OPENAI_API_KEY")
anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
df_path_recent_days = "data/outputs/stripe_and_square_combined_data_recent_days.csv"
df_path_combined = "data/outputs/stripe_and_square_combined_data.csv"
aws_bucket_name = "basin-climbing-data-prod"
s3_path_recent_days = "transactions/recent_days_combined_transaction_data.csv"
s3_path_combined = "transactions/combined_transaction_data.csv"
s3_path_capitan_memberships = "capitan/memberships.csv"
s3_path_capitan_members = "capitan/members.csv"
s3_path_capitan_membership_revenue_projection = (
    "capitan/membership_revenue_projection.csv"
)
s3_path_combined_snapshot = "transactions/snapshots/combined_transaction_data.csv"
s3_path_capitan_memberships_snapshot = "capitan/snapshots/memberships.csv"
s3_path_capitan_members_snapshot = "capitan/snapshots/members.csv"
s3_path_capitan_membership_revenue_projection_snapshot = (
    "capitan/snapshots/membership_revenue_projection.csv"
)
s3_path_instagram_posts = "instagram/posts_data.csv"
s3_path_instagram_comments = "instagram/comments_data.csv"
s3_path_instagram_posts_snapshot = "instagram/snapshots/posts_data.csv"
s3_path_instagram_comments_snapshot = "instagram/snapshots/comments_data.csv"
s3_path_facebook_ads = "facebook_ads/ads_data.csv"
s3_path_facebook_ads_snapshot = "facebook_ads/snapshots/ads_data.csv"
snapshot_day_of_month = 1
s3_path_text_and_metadata = "agent/text_and_metadata"

## Dictionaries for processing string in decripitions
revenue_category_keywords = {
    "day pass": "Day Pass",
    "team dues": "Team",
    "entry pass": "Day Pass",
    "initial payment": "New Membership",
    "renewal payment": "Membership Renewal",
    "membership renewal": "Membership Renewal",
    "new membership": "New Membership",
    "fitness": "Programming",
    "transformation": "Programming",
    "climbing technique": "Programming",
    "competition quality": "Retail",
    "comp": "Programming",
    "class": "Programming",
    "camp": "Programming",
    "event": "Event Booking",
    "birthday": "Event Booking",
    "retreat": "Event Booking",
    "pass": "Day Pass",
    "booking": "Event Booking",
    "gear upgrade": "Day Pass",
}
day_pass_sub_category_age_keywords = {
    "youth": "youth",
    "under 14": "youth",
    "Adult": "adult",
    "14 and up": "adult",
    "customer": "discounted day pass",
    "discounted day pass": "discounted day pass",
    "5 climb": "punch pass",
    "mid-day": "mid-day",
    "ladies night pass": "discounted day pass",
    "pj pass": "discounted day pass",
    "7 day pass": "7 day pass",
    "spectator day pass": "spectator day pass",
}
day_pass_sub_category_gear_keywords = {
    "gear upgrade": "gear upgrade",
    "with gear": "with gear",
}
membership_size_keywords = {
    "bcf family": "BCF Staff & Family",
    "bcf staff": "BCF Staff & Family",
    "duo": "Duo",
    "solo": "Solo",
    "family": "Family",
    "corporate": "Corporate",
}
membership_frequency_keywords = {
    "annual": "Annual",
    "weekly": "weekly",
    "monthly": "Monthly",
    "founders": "monthly",  # founders charged monthly
}
bcf_fam_friend_keywords = {
    "bcf family": True,
    "bcf staff": True,
}
founder_keywords = {
    "founder": True,
}
birthday_sub_category_patterns = {
    "Birthday Party- non-member": "second payment",
    "Birthday Party- Member": "second payment",
    "Birthday Party- additional participant": "second payment",
    "[Calendly] Basin 2 Hour Birthday": "initial payment",  # from calendly
    "Birthday Party Rental- 2 hours": "initial payment",  # from capitan (old)
    "Basin 2 Hour Birthday Party Rental": "initial payment",  # more flexible calendly pattern
}
fitness_patterns = {
    "HYROX CLASS": "hyrox",
    "week transformation": "transformation",
}

# Agent prompts
agent_identity_prompt = """
You are a data scientist at Basin Climbing.
You are responsible for analyzing the data and providing insights to the team.
You are also responsible for investigating any questions that are raised by the team.
You have the tools to investigate questions that can be answered with the data you have access to.
The data you have access to includes:
A summary view of transactions including their category, and sub-category.
The trends (momentum) of certain categories and sub-categories.
For questions that you think will be very helpful to the team but which you cannot answer,
you will be able to escalate those questions to the team.
You will be responsible to use tools you have and documents you have access to investigate,
and you will provide a clear and concise summary of your findings.
Please also be concise and always use dates in your answers where possible
"""
default_query = "please give insights into recent revenue trends"

# Debugging tools

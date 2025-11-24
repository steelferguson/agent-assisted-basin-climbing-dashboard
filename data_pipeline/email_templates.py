"""
Email template metadata and offer tracking.

Analyzes email campaigns once to determine what offers they contain,
then tracks which customers received which campaigns.
"""

import json
import os
from datetime import datetime
from typing import Dict, Optional
import anthropic


# Path to store analyzed templates
TEMPLATES_FILE = 'data/outputs/email_templates.json'


def load_email_templates() -> Dict:
    """Load cached email template analysis."""
    if os.path.exists(TEMPLATES_FILE):
        with open(TEMPLATES_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_email_templates(templates: Dict):
    """Save email template analysis to disk."""
    os.makedirs(os.path.dirname(TEMPLATES_FILE), exist_ok=True)
    with open(TEMPLATES_FILE, 'w') as f:
        json.dump(templates, f, indent=2)


def analyze_email_with_claude(
    campaign_id: str,
    campaign_title: str,
    email_subject: str,
    email_html: str,
    anthropic_api_key: str
) -> Dict:
    """
    Use Claude to analyze an email and extract offer information.

    Args:
        campaign_id: Mailchimp campaign ID
        campaign_title: Campaign name from Mailchimp
        email_subject: Email subject line
        email_html: Full HTML content of email
        anthropic_api_key: Anthropic API key

    Returns:
        Dict with template metadata and offer details
    """
    client = anthropic.Anthropic(api_key=anthropic_api_key)

    prompt = f"""Analyze this marketing email and extract offer information.

Campaign Title: {campaign_title}
Email Subject: {email_subject}

Email HTML Content:
{email_html[:4000]}

Please analyze this email and return a JSON object with:
{{
    "has_offer": true/false,
    "offer_type": "membership_discount" | "day_pass_discount" | "retail_discount" | "free_trial" | "other" | null,
    "offer_amount": "percentage or dollar amount like '20%' or '$50 off'" | null,
    "offer_code": "promo code if present" | null,
    "offer_expires": "expiration date if mentioned (YYYY-MM-DD format)" | null,
    "offer_description": "brief description of the offer" | null,
    "email_category": "welcome" | "promotional" | "newsletter" | "transactional" | "re-engagement" | "other"
}}

Return ONLY the JSON object, no other text."""

    message = client.messages.create(
        model="claude-3-haiku-20240307",  # Use Haiku for cost efficiency
        max_tokens=500,
        messages=[{
            "role": "user",
            "content": prompt
        }]
    )

    # Parse Claude's response
    response_text = message.content[0].text.strip()

    # Remove markdown code blocks if present
    if response_text.startswith('```'):
        response_text = response_text.split('```')[1]
        if response_text.startswith('json'):
            response_text = response_text[4:]

    analysis = json.loads(response_text)

    # Add metadata
    analysis['campaign_id'] = campaign_id
    analysis['campaign_title'] = campaign_title
    analysis['email_subject'] = email_subject
    analysis['analyzed_date'] = datetime.now().isoformat()
    analysis['analyzed_by'] = 'claude-3-haiku'

    return analysis


def get_campaign_template(
    campaign_id: str,
    campaign_title: str,
    email_subject: str,
    email_html: str,
    anthropic_api_key: str,
    force_reanalyze: bool = False
) -> Dict:
    """
    Get email template metadata, analyzing with Claude if not cached.

    Args:
        campaign_id: Mailchimp campaign ID
        campaign_title: Campaign name
        email_subject: Email subject line
        email_html: Email HTML content
        anthropic_api_key: Anthropic API key
        force_reanalyze: If True, re-analyze even if cached

    Returns:
        Dict with template metadata and offer details
    """
    templates = load_email_templates()

    # Check if already analyzed
    if campaign_id in templates and not force_reanalyze:
        print(f"✅ Using cached analysis for campaign {campaign_id}")
        return templates[campaign_id]

    # First time seeing this campaign - analyze with Claude
    print(f"🔍 Analyzing new campaign '{campaign_title}' ({campaign_id}) with Claude...")

    try:
        analysis = analyze_email_with_claude(
            campaign_id,
            campaign_title,
            email_subject,
            email_html,
            anthropic_api_key
        )

        # Cache the result
        templates[campaign_id] = analysis
        save_email_templates(templates)

        # Print summary
        if analysis.get('has_offer'):
            offer_desc = analysis.get('offer_description', 'No description')
            print(f"   ✅ Offer detected: {offer_desc}")
        else:
            print(f"   ℹ️  No offer detected")

        return analysis

    except Exception as e:
        print(f"   ❌ Error analyzing campaign: {e}")
        # Return minimal metadata on error
        return {
            'campaign_id': campaign_id,
            'campaign_title': campaign_title,
            'email_subject': email_subject,
            'analyzed_date': datetime.now().isoformat(),
            'has_offer': None,
            'error': str(e)
        }


def list_analyzed_campaigns() -> Dict:
    """
    Get all analyzed campaigns.

    Returns:
        Dict of campaign_id -> template metadata
    """
    return load_email_templates()


def get_campaigns_with_offers() -> Dict:
    """
    Get only campaigns that contain offers.

    Returns:
        Dict of campaign_id -> template metadata for campaigns with offers
    """
    templates = load_email_templates()
    return {
        cid: template
        for cid, template in templates.items()
        if template.get('has_offer') is True
    }


if __name__ == "__main__":
    # Test with a sample email
    print("Email Template Analyzer - Test Mode")
    print("=" * 60)

    # Example usage
    sample_html = """
    <html>
    <body>
        <h1>Welcome to Basin Climbing!</h1>
        <p>We're excited to have you join our community.</p>
        <p><strong>Special Offer:</strong> Use code WELCOME20 for 20% off your first day pass!</p>
        <p>This offer expires December 31, 2025.</p>
    </body>
    </html>
    """

    # Would normally use real API key from config
    # analysis = get_campaign_template(
    #     campaign_id='test123',
    #     campaign_title='Welcome Email',
    #     email_subject='Welcome to Basin!',
    #     email_html=sample_html,
    #     anthropic_api_key=config.anthropic_api_key
    # )

    print("\nTo use this module:")
    print("1. Call get_campaign_template() when processing Mailchimp campaigns")
    print("2. Template will be analyzed once and cached")
    print("3. Future sends of same campaign use cached analysis")
    print("4. Create customer events with offer details from template")

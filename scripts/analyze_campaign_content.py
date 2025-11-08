"""
Quick script to fetch and analyze Mailchimp campaign HTML content
"""
import os
from mailchimp_marketing import Client
from bs4 import BeautifulSoup
import re

# Mailchimp credentials from environment
api_key = os.getenv("MAILCHIMP_API_KEY")
server_prefix = os.getenv("MAILCHIMP_SERVER_PREFIX", "us9")

client = Client()
client.set_config({
    "api_key": api_key,
    "server": server_prefix
})

def clean_html_to_text(html_content: str) -> str:
    """Convert HTML to clean text for analysis."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for script in soup(["script", "style"]):
        script.decompose()

    # Get text
    text = soup.get_text()

    # Break into lines and remove leading/trailing space
    lines = (line.strip() for line in text.splitlines())
    # Break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # Drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text

def extract_links(html_content: str) -> list:
    """Extract all links/CTAs from HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)
        if text and not href.startswith('mailto:'):
            links.append(f"{text} -> {href}")
    return links

# Campaigns to analyze (variety of performance levels)
campaigns_to_analyze = [
    ("f40448d3ce", "1-Year Anniversary + Boulder Bash (High engagement)"),
    ("1ab2483231", "This Week at Basin (0% CTR despite 52% opens)"),
    ("120bc14dea", "September Climbing Classes (1.09% CTR)"),
    ("db677d9b82", "Black Friday Deals (0.69% CTR)"),
]

print("="*80)
print("MAILCHIMP CAMPAIGN CONTENT ANALYSIS")
print("="*80)

for campaign_id, description in campaigns_to_analyze:
    print(f"\n{'='*80}")
    print(f"Campaign: {description}")
    print(f"ID: {campaign_id}")
    print(f"{'='*80}\n")

    try:
        # Get campaign content
        content = client.campaigns.get_content(campaign_id)

        # Try plain text first
        plain_text = content.get('plain_text', '')
        html_content = content.get('html', '')

        # If no plain text, extract from HTML
        if not plain_text:
            plain_text = clean_html_to_text(html_content)

        # Extract links
        links = extract_links(html_content)

        print("CAMPAIGN TEXT:")
        print("-" * 80)
        if plain_text:
            print(plain_text[:1500])
            if len(plain_text) > 1500:
                print("...")
        else:
            print("[No text content found]")
        print()

        print(f"CALL-TO-ACTION LINKS ({len(links)} total):")
        print("-" * 80)
        # Filter out standard mailchimp links
        actionable_links = [l for l in links if 'UPDATE_PROFILE' not in l and 'UNSUB' not in l]

        if actionable_links:
            for link in actionable_links:
                print(f"  • {link}")
        else:
            print("  [No actionable CTAs found - only standard unsubscribe/preferences links]")
        print()

    except Exception as e:
        print(f"Error fetching campaign {campaign_id}: {e}\n")

print("="*80)
print("ANALYSIS COMPLETE")
print("="*80)

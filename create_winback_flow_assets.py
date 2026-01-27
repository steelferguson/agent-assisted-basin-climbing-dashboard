import requests
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()

KLAVIYO_PRIVATE_KEY = os.getenv('KLAVIYO_PRIVATE_KEY')

headers = {
    'Authorization': f'Klaviyo-API-Key {KLAVIYO_PRIVATE_KEY}',
    'revision': '2025-01-15',
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

LOGO_URL = 'https://d3k81ch9hvuctc.cloudfront.net/company/SvkYJz/images/8917ac69-a570-4518-b162-ad0eec4b083c.png'

def make_email_html(headline, body_paragraphs, cta_text=None, cta_url=None, footer_note=None):
    """Generate Basin-branded email HTML."""
    body_html = ''
    for p in body_paragraphs:
        body_html += f'<p style="color: #000000; font-size: 16px; line-height: 1.6; margin: 0 0 16px 0;">{p}</p>\n'
    
    cta_html = ''
    if cta_text and cta_url:
        cta_html = f'''
        <div style="text-align: center; margin: 32px 0;">
            <a href="{cta_url}"
               style="background-color: #e9c867; color: #000000; padding: 14px 28px;
                      text-decoration: none; display: inline-block; border-radius: 4px;
                      font-weight: bold; font-size: 16px;">
                {cta_text}
            </a>
        </div>'''
    
    footer_html = ''
    if footer_note:
        footer_html = f'<p style="color: #666666; font-size: 14px; font-style: italic; margin-top: 24px;">{footer_note}</p>'
    
    return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="background-color: #fffdf5; margin: 0; padding: 0; font-family: 'Helvetica Neue', Helvetica, Arial, Verdana, sans-serif;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <!-- Logo -->
        <div style="text-align: center; padding: 24px 0;">
            <img src="{LOGO_URL}" alt="Basin Climbing and Fitness"
                 style="display: inline-block; max-width: 180px; height: auto;">
        </div>
        <!-- Content -->
        <div style="background-color: #ffffff; padding: 36px 32px; border-radius: 4px;">
            <h1 style="color: #26241c; font-size: 28px; font-weight: bold; margin: 0 0 24px 0; line-height: 1.3;">
                {headline}
            </h1>
            {body_html}
            {cta_html}
            {footer_html}
        </div>
        <!-- Footer -->
        <div style="background-color: #f4f4f4; padding: 24px 32px; margin-top: 16px; border-radius: 4px; text-align: center;">
            <p style="color: #666666; font-size: 13px; margin: 0 0 8px 0;">
                Basin Climbing and Fitness<br>
                650 Alliance Parkway, Hewitt, TX 76643
            </p>
            <p style="color: #999999; font-size: 12px; margin: 0;">
                <a href="%tag_unsubscribe_url%" style="color: #999999;">Unsubscribe</a>
            </p>
        </div>
    </div>
</body>
</html>'''


# Step 1: Create the list
print("Creating list...")
list_resp = requests.post(
    'https://a.klaviyo.com/api/lists',
    headers=headers,
    json={
        'data': {
            'type': 'list',
            'attributes': {
                'name': 'Membership Cancellation - Win Back'
            }
        }
    }
)
print(f"List status: {list_resp.status_code}")
if list_resp.status_code in [200, 201]:
    list_id = list_resp.json()['data']['id']
    print(f"List ID: {list_id}")
else:
    print(f"List error: {list_resp.text}")
    list_id = None

time.sleep(0.5)

# Step 2: Create 4 email templates

templates = {}

# Template 1: Feedback Ask (Day 3)
print("\nCreating Template 1: Feedback Ask...")
t1_html = make_email_html(
    headline="We'd love to hear from you",
    body_paragraphs=[
        "We noticed you recently cancelled your Basin membership, and we wanted to reach out personally.",
        "Your experience at Basin matters to us — whether you were with us for a month or a year. We're always trying to get better, and honest feedback from members like you is the best way we know how.",
        "If you have a minute, we'd really appreciate hearing what worked, what didn't, or what we could do differently. No agenda here — just listening.",
        "However your climbing journey continues, we're grateful you were part of the Basin community."
    ],
    cta_text="Share Your Feedback",
    cta_url="https://basinclimbing.com/pages/contact-us",
    footer_note="Just reply to this email if that's easier — we read every response."
)

t1_resp = requests.post(
    'https://a.klaviyo.com/api/templates',
    headers=headers,
    json={
        'data': {
            'type': 'template',
            'attributes': {
                'name': 'Win-Back 1: Feedback Ask',
                'editor_type': 'CODE',
                'html': t1_html
            }
        }
    }
)
print(f"Template 1 status: {t1_resp.status_code}")
if t1_resp.status_code in [200, 201]:
    templates['feedback'] = t1_resp.json()['data']['id']
    print(f"Template 1 ID: {templates['feedback']}")
else:
    print(f"Error: {t1_resp.text[:300]}")

time.sleep(0.5)

# Template 2: The Offer (Day 7)
print("\nCreating Template 2: The Offer...")
t2_html = make_email_html(
    headline="Come back — we'll make it easy",
    body_paragraphs=[
        "We'd love to see you back on the wall.",
        "If you've been thinking about coming back to Basin, here's a little something to make the decision easier:",
        '<strong style="font-size: 18px;">10% off your membership + activation fee waived.</strong>',
        "That's less to think about and more time climbing. Whether it's been a week or a month, your holds are waiting.",
        "Just mention this offer when you come in, or reach out and we'll get you set up."
    ],
    cta_text="Rejoin Basin",
    cta_url="https://basinclimbing.com",
    footer_note="This offer is available for a limited time. Questions? Just reply to this email."
)

t2_resp = requests.post(
    'https://a.klaviyo.com/api/templates',
    headers=headers,
    json={
        'data': {
            'type': 'template',
            'attributes': {
                'name': 'Win-Back 2: The Offer',
                'editor_type': 'CODE',
                'html': t2_html
            }
        }
    }
)
print(f"Template 2 status: {t2_resp.status_code}")
if t2_resp.status_code in [200, 201]:
    templates['offer'] = t2_resp.json()['data']['id']
    print(f"Template 2 ID: {templates['offer']}")
else:
    print(f"Error: {t2_resp.text[:300]}")

time.sleep(0.5)

# Template 3: Social Proof + Reminder (Day 14)
print("\nCreating Template 3: Social Proof...")
t3_html = make_email_html(
    headline="Here's what's been happening at Basin",
    body_paragraphs=[
        "A lot has been going on since you've been away — and we didn't want you to miss out.",
        "🧗 <strong>Fresh routes</strong> — Our setters have been busy. New problems across all levels, from beginner-friendly to project-worthy.",
        "💪 <strong>Community events</strong> — Comp nights, clinics, and group sessions that bring everyone together.",
        "👨‍👩‍👧‍👦 <strong>Growing community</strong> — New faces, familiar ones, and the same welcoming energy you helped build.",
        "Your spot in the Basin community is always here. And your <strong>10% off + waived activation fee</strong> offer is still available if you want to come back."
    ],
    cta_text="Come See What's New",
    cta_url="https://basinclimbing.com",
    footer_note="Miss the crew? Drop in for a day pass anytime — no commitment needed."
)

t3_resp = requests.post(
    'https://a.klaviyo.com/api/templates',
    headers=headers,
    json={
        'data': {
            'type': 'template',
            'attributes': {
                'name': 'Win-Back 3: Social Proof',
                'editor_type': 'CODE',
                'html': t3_html
            }
        }
    }
)
print(f"Template 3 status: {t3_resp.status_code}")
if t3_resp.status_code in [200, 201]:
    templates['social_proof'] = t3_resp.json()['data']['id']
    print(f"Template 3 ID: {templates['social_proof']}")
else:
    print(f"Error: {t3_resp.text[:300]}")

time.sleep(0.5)

# Template 4: Graceful Close (Day 35)
print("\nCreating Template 4: Graceful Close...")
t4_html = make_email_html(
    headline="The door's always open",
    body_paragraphs=[
        "This is our last note — we promise we're not going to keep filling your inbox.",
        "We just wanted you to know: there's no expiration on being part of the Basin community. Whether it's next week, next month, or next year — you're always welcome.",
        "Not ready for a membership? No problem. Grab a <strong>day pass</strong> anytime you want to climb, work out, or just hang with the crew. No strings attached.",
        "Thanks for being part of Basin. We hope to see you on the wall again someday. 🤙"
    ],
    cta_text="Grab a Day Pass",
    cta_url="https://basinclimbing.com",
    footer_note=None
)

t4_resp = requests.post(
    'https://a.klaviyo.com/api/templates',
    headers=headers,
    json={
        'data': {
            'type': 'template',
            'attributes': {
                'name': 'Win-Back 4: Graceful Close',
                'editor_type': 'CODE',
                'html': t4_html
            }
        }
    }
)
print(f"Template 4 status: {t4_resp.status_code}")
if t4_resp.status_code in [200, 201]:
    templates['close'] = t4_resp.json()['data']['id']
    print(f"Template 4 ID: {templates['close']}")
else:
    print(f"Error: {t4_resp.text[:300]}")

# Print summary
print("\n\n=== SUMMARY ===")
print(f"List ID: {list_id}")
print(f"Templates: {json.dumps(templates, indent=2)}")

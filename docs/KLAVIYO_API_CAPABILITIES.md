# Klaviyo API Capabilities for Automated Flow Creation

**Last Updated:** 2026-01-24
**Tested By:** Claude Code + Basin Climbing API Keys

---

## Executive Summary

Claude can automate **complete** Klaviyo flow creation via API. The only manual step is activating the flow in the Klaviyo UI after review.

| Task | API Support | Notes |
|------|-------------|-------|
| Create flows | ✅ Yes | Created in draft status |
| Create email templates | ✅ Yes | Full HTML control |
| Update template content | ✅ Yes | Words, links, images - everything |
| Create email/SMS actions | ✅ Yes | Added to flows |
| Set triggers (metric/list) | ✅ Yes | Works with any metric or list |
| **Link template to flow email** | ✅ Yes | Specify template_id when creating flow |
| Activate flows (draft → live) | ❌ No | Must use Klaviyo UI |
| Add profile filters to triggers | ⚠️ Complex | Structure exists but difficult |

### Recommended Workflow
1. **You or Claude creates base templates** with Basin branding
2. **Claude creates complete flows** with templates linked, subjects set, triggers configured
3. **You review and activate** in Klaviyo UI (one click)

---

## API Configuration

### Required Headers
```python
headers = {
    'Authorization': f'Klaviyo-API-Key {KLAVIYO_PRIVATE_KEY}',
    'revision': '2025-01-15',  # Use recent revision for flow creation
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}
```

### Base URL
```
https://a.klaviyo.com/api
```

### API Revision Notes
- Flow creation requires revision `2025-01-15` or later
- Older revisions (e.g., `2024-02-15`) work for reading but not creating flows
- The API will return errors indicating minimum revision dates if too old

---

## Creating Flows

### Minimal Flow Creation (List Trigger)
```python
flow_data = {
    'data': {
        'type': 'flow',
        'attributes': {
            'name': 'My New Flow',
            'definition': {
                'triggers': [
                    {
                        'type': 'list',
                        'id': 'LIST_ID_HERE'
                    }
                ],
                'actions': [
                    {
                        'temporary_id': 'temp-001',
                        'type': 'send-email'
                    }
                ],
                'entry_action_id': 'temp-001'
            }
        }
    }
}

response = requests.post(
    'https://a.klaviyo.com/api/flows',
    headers=headers,
    json=flow_data
)
```

### Metric-Triggered Flow
```python
flow_data = {
    'data': {
        'type': 'flow',
        'attributes': {
            'name': 'Post-Purchase Flow',
            'definition': {
                'triggers': [
                    {
                        'type': 'metric',
                        'id': 'METRIC_ID_HERE'  # e.g., "Placed Order" metric ID
                    }
                ],
                'actions': [
                    {
                        'temporary_id': 'temp-001',
                        'type': 'send-email'
                    }
                ],
                'entry_action_id': 'temp-001'
            }
        }
    }
}
```

### Key Points
- Use `temporary_id` for new actions (not `id`)
- `entry_action_id` must match a `temporary_id` from actions
- Flows are created in `draft` status
- Email actions auto-populate with account defaults (from_email, from_label, etc.)

---

## Creating & Updating Templates

### Create a Template
```python
template_data = {
    'data': {
        'type': 'template',
        'attributes': {
            'name': 'Basin - 2-Week Offer',
            'editor_type': 'CODE',  # Required field
            'html': '''<!DOCTYPE html>
<html>
<body style="background-color: #fffdf5;">
    <h1 style="color: #26241c;">Your Headline Here</h1>
    <p>Your body text here.</p>
    <a href="https://example.com" style="background-color: #e9c867; color: #000;">
        CTA Button
    </a>
</body>
</html>'''
        }
    }
}

response = requests.post(
    'https://a.klaviyo.com/api/templates',
    headers=headers,
    json=template_data
)

template_id = response.json()['data']['id']
```

### Update a Template (Change Words & Links)
```python
update_data = {
    'data': {
        'type': 'template',
        'id': template_id,
        'attributes': {
            'name': 'Basin - 50% Off Second Visit',
            'html': '''<!DOCTYPE html>
<html>
<body style="background-color: #fffdf5;">
    <h1 style="color: #26241c;">Half-Price Second Visit!</h1>
    <p>Come back and climb with us - 50% off your next day pass.</p>
    <a href="https://basinclimbing.com/products/day-pass?discount=COMEBACK50"
       style="background-color: #e9c867; color: #000;">
        Claim Your 50% Off
    </a>
</body>
</html>'''
        }
    }
}

response = requests.patch(
    f'https://a.klaviyo.com/api/templates/{template_id}',
    headers=headers,
    json=update_data
)
```

### What Can Be Updated in Templates
- ✅ Template name
- ✅ All HTML content
- ✅ Headlines and body text
- ✅ Links (including query parameters like `?discount=CODE`)
- ✅ Images (via URL references)
- ✅ Styling (colors, fonts, layout)

---

## What CANNOT Be Done via API

### 1. Activate Flows
Flows are created in `draft` status and cannot be activated via API.

**Workaround:** Activate flows manually in Klaviyo UI after creation.

### 3. Complex Profile Filters
While the `profile_filter` field exists in the flow definition, the nested structure for conditions is complex and poorly documented:
```python
'profile_filter': {
    'condition_groups': [
        {
            'conditions': [
                {
                    'type': 'profile-property',
                    'property': 'some_property',
                    'filter': {
                        # Complex nested structure required
                    }
                }
            ]
        }
    ]
}
```

**Workaround:** Add profile filters in Klaviyo UI after creating the flow.

---

## Recommended Workflow

### For Basin Climbing Flag-Based Emails

1. **Claude creates templates** with full content:
   - `Basin - 2-Week Pass Offer` (for Group A)
   - `Basin - 50% Second Visit` (for Group B)
   - `Basin - Birthday Party Reminder`
   - etc.

2. **Claude creates flow skeletons**:
   - Set up triggers (metric or list-based)
   - Add email/SMS actions
   - Set subject lines, preview text

3. **Manual steps in Klaviyo UI** (one-time):
   - Assign templates to flow emails
   - Add profile filters if needed
   - Activate flows

### Automation Potential

| Step | Automated? |
|------|------------|
| Create template HTML with Basin branding | ✅ Yes |
| Update email copy (headlines, body, CTAs) | ✅ Yes |
| Update links (with discount codes, UTM params) | ✅ Yes |
| Create new flows | ✅ Yes |
| Set flow triggers | ✅ Yes |
| Assign template to flow | ❌ Manual |
| Activate flow | ❌ Manual |

---

## Available Resources in Klaviyo

### Lists (for triggers)
| List Name | ID |
|-----------|-----|
| Basin Climbing and Fitness | Xnq3EH |
| Email List | X4k5N6 |
| SMS List | VSVeUe |
| Preview List | VNhYyQ |

### Existing Flows (all draft)
| Flow Name | ID | Trigger |
|-----------|-----|---------|
| Day Pass - 2 Week Pass | UM55ZF | Metric |
| Post Birthday Party Journey | VbmGdk | Added to List |
| New Member Journey | Vxc5uC | Added to List |
| 2-week pass-Membership Journey | YzWDGi | Metric |

### Key Metrics (for triggers)
| Metric Name | Source |
|-------------|--------|
| Placed Order | Shopify |
| Checkout Started | Shopify |
| Active on Site | API |
| Opened Email | Klaviyo |
| Clicked Email | Klaviyo |

### Images Available
- Logo 1: `https://d3k81ch9hvuctc.cloudfront.net/company/SvkYJz/images/8917ac69-a570-4518-b162-ad0eec4b083c.png`
- Logo 2: `https://d3k81ch9hvuctc.cloudfront.net/company/SvkYJz/images/d1d38634-1125-4c5d-aea8-242ff365c975.png`
- Plus 18 climbing/gym photos

---

## Creating Complete Flows with Templates

### The Key: Specify template_id When Creating Flow

When creating a flow with an email action, include the `template_id` along with all required message fields:

```python
flow_data = {
    'data': {
        'type': 'flow',
        'attributes': {
            'name': 'My Flow',
            'definition': {
                'triggers': [{'type': 'list', 'id': 'LIST_ID'}],
                'actions': [{
                    'temporary_id': 'email-1',
                    'type': 'send-email',
                    'data': {
                        'message': {
                            'template_id': 'YOUR_TEMPLATE_ID',  # Links the template!
                            'from_email': 'info@basinclimbing.com',
                            'from_label': 'Basin Climbing and Fitness',
                            'reply_to_email': 'info@basinclimbing.com',
                            'cc_email': None,      # Use None, not empty string
                            'bcc_email': None,     # Use None, not empty string
                            'subject_line': 'Your Subject Here',
                            'preview_text': 'Preview text here'
                        }
                    }
                }],
                'entry_action_id': 'email-1'
            }
        }
    }
}
```

**Important:** Use `None` (not empty string `''`) for optional email fields like cc_email and bcc_email.

---

## Code Examples

### Full Example: Create Template + Flow (Working Code)

```python
import requests
import os

KLAVIYO_PRIVATE_KEY = os.getenv('KLAVIYO_PRIVATE_KEY')

headers = {
    'Authorization': f'Klaviyo-API-Key {KLAVIYO_PRIVATE_KEY}',
    'revision': '2025-01-15',
    'Accept': 'application/json',
    'Content-Type': 'application/json'
}

# Step 1: Create template
template_html = '''<!DOCTYPE html>
<html>
<body style="background-color: #fffdf5; font-family: Helvetica Neue, Arial, sans-serif;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <img src="https://d3k81ch9hvuctc.cloudfront.net/company/SvkYJz/images/8917ac69-a570-4518-b162-ad0eec4b083c.png"
             alt="Basin Logo" style="display: block; margin: 0 auto;">
        <h1 style="color: #26241c; text-align: center;">Welcome to Basin!</h1>
        <p style="color: #000000;">We're excited to have you join our climbing community.</p>
        <a href="https://basinclimbing.com"
           style="background-color: #e9c867; color: #000; padding: 12px 24px;
                  text-decoration: none; display: inline-block; border-radius: 4px;">
            Start Climbing
        </a>
    </div>
</body>
</html>'''

template_resp = requests.post(
    'https://a.klaviyo.com/api/templates',
    headers=headers,
    json={
        'data': {
            'type': 'template',
            'attributes': {
                'name': 'Basin - Welcome Email',
                'editor_type': 'CODE',
                'html': template_html
            }
        }
    }
)
template_id = template_resp.json()['data']['id']
print(f'Created template: {template_id}')

# Step 2: Create flow
flow_resp = requests.post(
    'https://a.klaviyo.com/api/flows',
    headers=headers,
    json={
        'data': {
            'type': 'flow',
            'attributes': {
                'name': 'Welcome Flow',
                'definition': {
                    'triggers': [{'type': 'list', 'id': 'Xnq3EH'}],
                    'actions': [{'temporary_id': 'email-1', 'type': 'send-email'}],
                    'entry_action_id': 'email-1'
                }
            }
        }
    }
)
flow_id = flow_resp.json()['data']['id']
print(f'Created flow: {flow_id}')

print(f'''
Next steps (manual in Klaviyo UI):
1. Go to Flows > {flow_id}
2. Click the email action
3. Choose "Use Template" and select "{template_id}"
4. Activate the flow
''')
```

---

## Troubleshooting

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Revision date requested is before the earliest available` | API revision too old | Use `2025-01-15` or later |
| `'editor_type' is a required field` | Missing field in template creation | Add `'editor_type': 'CODE'` |
| `'temporary_id' is required` | Using `id` instead of `temporary_id` for new actions | Use `temporary_id` for new objects |
| `Method "PATCH" not allowed` | Trying to update flow-message | Cannot update flow messages via API |

### Rate Limits
- Klaviyo has rate limits (~100 requests/second)
- Add `time.sleep(0.1)` between requests if doing bulk operations
- 429 errors include `Retry-After` header

---

## Future Considerations

1. **API Evolution**: Klaviyo regularly updates their API. Flow message editing may become available in future revisions.

2. **Alternative: Campaigns API**: For one-time sends (not automated flows), the Campaigns API may offer more flexibility.

3. **Webhook Integration**: Consider using Klaviyo webhooks to trigger actions in our system when flow events occur.

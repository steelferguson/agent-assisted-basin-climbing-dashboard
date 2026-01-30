# How to Replicate Klaviyo Flows via API

This guide explains how to create a new Klaviyo flow based on an existing one, with a different trigger.

## Overview

Klaviyo does NOT allow changing a flow's trigger after creation. To change a trigger, you must:
1. Get the existing flow's definition
2. Create a new flow with the modified trigger

## Prerequisites

- `KLAVIYO_PRIVATE_KEY` environment variable set
- Python with `requests` library

## Step 1: List All Flows

First, find the flow you want to replicate:

```python
import os
import requests

KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")
headers = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2025-01-15",
    "Accept": "application/json"
}

url = "https://a.klaviyo.com/api/flows"
response = requests.get(url, headers=headers)
flows = response.json().get('data', [])

for flow in flows:
    print(f"ID: {flow['id']} - {flow['attributes']['name']} ({flow['attributes']['trigger_type']})")
```

## Step 2: Get Flow Definition

Use the **beta revision header** to get the full flow definition:

```python
headers = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2024-10-15.pre",  # BETA revision required!
    "Accept": "application/json"
}

flow_id = "YOUR_FLOW_ID"
url = f"https://a.klaviyo.com/api/flows/{flow_id}?additional-fields[flow]=definition"
response = requests.get(url, headers=headers)
flow = response.json()

# Save to file for reference
import json
with open('flow_definition.json', 'w') as f:
    json.dump(flow, f, indent=2)
```

## Step 3: Modify the Definition for Creation

When creating a new flow, you must:

### 3a. Change Trigger Type

**List Trigger** (triggers when profile added to list):
```python
"triggers": [
    {
        "type": "list",
        "id": "YOUR_LIST_ID"  # Klaviyo list ID
    }
]
```

**Metric Trigger** (triggers on event):
```python
"triggers": [
    {
        "type": "metric",
        "id": "METRIC_ID",
        "trigger_filter": null
    }
]
```

### 3b. Convert Action IDs to Temporary IDs

Replace all `"id": "12345"` with `"temporary_id": "action_1"`, `"action_2"`, etc.

Also update all `"links"` references to use the new temporary IDs:
```python
# Before
"id": "98050397",
"links": {"next": "98050512"}

# After
"temporary_id": "action_1",
"links": {"next": "action_2"}
```

### 3c. Required Fields for Emails

All email actions MUST include these fields:
```python
"message": {
    "from_email": "info@basinclimbing.com",
    "from_label": "Basin Climbing and Fitness",
    "reply_to_email": "info@basinclimbing.com",  # Required!
    "cc_email": None,      # Required (can be None)
    "bcc_email": None,     # Required (can be None)
    "subject_line": "Your subject",
    "preview_text": "",    # Required (can be empty string)
    "template_id": "TEMPLATE_ID",
    "smart_sending_enabled": True,
    "name": "Email #1"
}
```

### 3d. Time Delay Rules

- `delay_until_weekdays` is ONLY allowed when `unit` is `"days"`
- For `unit: "hours"`, do NOT include `delay_until_weekdays`

```python
# Hours - NO weekdays
{
    "type": "time-delay",
    "data": {
        "unit": "hours",
        "value": 2,
        "secondary_value": 0,
        "timezone": "profile"
    }
}

# Days - weekdays allowed
{
    "type": "time-delay",
    "data": {
        "unit": "days",
        "value": 1,
        "timezone": "profile",
        "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    }
}
```

### 3e. Set Entry Action

Point to the first action's temporary_id:
```python
"entry_action_id": "action_1"
```

## Step 4: Create the New Flow

```python
headers = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2024-10-15.pre",  # BETA revision required!
    "Content-Type": "application/json",
    "Accept": "application/json"
}

flow_definition = {
    "data": {
        "type": "flow",
        "attributes": {
            "name": "Your New Flow Name",
            "definition": {
                "triggers": [...],
                "profile_filter": None,  # Or keep existing filter
                "actions": [...],
                "entry_action_id": "action_1",
                "reentry_criteria": {
                    "duration": 1,
                    "unit": "alltime"
                }
            }
        }
    }
}

url = "https://a.klaviyo.com/api/flows"
response = requests.post(url, headers=headers, json=flow_definition)

if response.status_code in [200, 201]:
    result = response.json()
    print(f"Flow created: {result['data']['id']}")
else:
    print(f"Error: {response.status_code}")
    print(response.text)
```

## Step 5: Add Profiles to List (for List-Triggered Flows)

```python
def get_or_create_profile(email):
    """Get or create a Klaviyo profile."""
    url = f"https://a.klaviyo.com/api/profiles?filter=equals(email,\"{email}\")"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json().get('data', [])
        if data:
            return data[0]['id']

    # Create if not found
    payload = {
        "data": {
            "type": "profile",
            "attributes": {"email": email}
        }
    }
    response = requests.post("https://a.klaviyo.com/api/profiles", headers=headers, json=payload)
    return response.json()['data']['id']


def add_to_list(email, list_id):
    """Add profile to list to trigger flow."""
    profile_id = get_or_create_profile(email)

    url = f"https://a.klaviyo.com/api/lists/{list_id}/relationships/profiles"
    payload = {
        "data": [{"type": "profile", "id": profile_id}]
    }
    response = requests.post(url, headers=headers, json=payload)
    return response.status_code in [200, 201, 202, 204]
```

## Common Action Types

### Send SMS
```python
{
    "temporary_id": "action_X",
    "type": "send-sms",
    "data": {
        "message": {
            "body": "Your message text",
            "shorten_links": True,
            "add_org_prefix": True,
            "add_info_link": True,
            "add_opt_out_language": True,
            "smart_sending_enabled": True,
            "sms_quiet_hours_enabled": True,
            "name": "Text message #1"
        },
        "status": "draft"
    },
    "links": {"next": "action_Y"}
}
```

### Send Email
```python
{
    "temporary_id": "action_X",
    "type": "send-email",
    "data": {
        "message": {
            "from_email": "info@basinclimbing.com",
            "from_label": "Basin Climbing and Fitness",
            "reply_to_email": "info@basinclimbing.com",
            "cc_email": None,
            "bcc_email": None,
            "subject_line": "Subject here",
            "preview_text": "",
            "template_id": "TEMPLATE_ID",
            "smart_sending_enabled": True,
            "name": "Email #1"
        },
        "status": "draft"
    },
    "links": {"next": "action_Y"}
}
```

### Conditional Split
```python
{
    "temporary_id": "action_X",
    "type": "conditional-split",
    "data": {
        "profile_filter": {
            "condition_groups": [
                {
                    "conditions": [
                        {
                            "type": "profile-property",
                            "property": "properties['Shopify Tags']",
                            "filter": {
                                "type": "list",
                                "operator": "contains",
                                "value": "some-tag"
                            }
                        }
                    ]
                }
            ]
        }
    },
    "links": {
        "next_if_true": "action_Y",
        "next_if_false": None  # Or another action
    }
}
```

## Example Scripts

See these files in the project:
- `get_flow_definition.py` - Fetch a flow's full definition
- `create_list_triggered_flow.py` - Create a flow with list trigger
- `add_to_klaviyo_list.py` - Add profiles to a list
- `list_klaviyo_flows.py` - List all flows

## Important Notes

1. **Beta API**: Flow creation uses beta endpoints (`revision: 2024-10-15.pre`)
2. **Flows are created in Draft**: You must manually turn them on in Klaviyo UI
3. **Cannot update flows**: Once created, you cannot modify flow structure via API
4. **Template IDs**: Keep the same template IDs to preserve email designs
5. **List IDs**: Create lists first using `setup_klaviyo_flow_lists.py`

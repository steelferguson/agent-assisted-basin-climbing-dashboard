"""
Create a new Day Pass - 2 Week Pass flow with List trigger.

Based on the existing flow but triggered by list entry instead of metric.
"""
import os
import requests
import json

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")

# Beta revision required for flow creation
headers = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2024-10-15.pre",
    "Content-Type": "application/json",
    "Accept": "application/json"
}

# List ID for "Day Pass - 2 Week Offer"
LIST_ID = "RX9TsQ"

# Build the new flow with list trigger
flow_definition = {
    "data": {
        "type": "flow",
        "attributes": {
            "name": "Day Pass - 2 Week Pass (List Trigger)",
            "definition": {
                "triggers": [
                    {
                        "type": "list",
                        "id": LIST_ID
                    }
                ],
                "profile_filter": None,
                "actions": [
                    {
                        "temporary_id": "action_1",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Hey! Thanks again for climbing with us 👋 If you want to come back, our 2-Week Unlimited Climbing Pass is $29 (gear included). Come twice & it's worth it → \n\nhttps://climber.hellocapitan.com/basin/membership/enter-owner-info/1333/",
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
                        "links": {
                            "next": "action_2"
                        }
                    },
                    {
                        "temporary_id": "action_2",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 2,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_3"
                        }
                    },
                    {
                        "temporary_id": "action_3",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "How was your climb today? 😊",
                                "preview_text": "",
                                "template_id": "YkBQTW",
                                "smart_sending_enabled": True,
                                "name": "Email #1"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_4"
                        }
                    },
                    {
                        "temporary_id": "action_4",
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
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": "action_5",
                            "next_if_false": None
                        }
                    },
                    {
                        "temporary_id": "action_5",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 1,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_6"
                        }
                    },
                    {
                        "temporary_id": "action_6",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Hey there! If you're thinking about returning, the $29 two-week pass is the easiest way to explore the gym more without committing to a membership.\n\nUnlimited visits • Free rentals • Come at your pace.\n👉 Activate here:\nhttps://mcsms.io/w4kzkw\n\nSee you soon,\nThe Basin Climbing Team",
                                "shorten_links": True,
                                "add_org_prefix": True,
                                "add_info_link": True,
                                "add_opt_out_language": True,
                                "smart_sending_enabled": True,
                                "sms_quiet_hours_enabled": True,
                                "name": "Text message #2"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_7"
                        }
                    },
                    {
                        "temporary_id": "action_7",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 3,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_8"
                        }
                    },
                    {
                        "temporary_id": "action_8",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Ready to come back and climb again? 🧗‍♀️",
                                "preview_text": "",
                                "template_id": "YrRm7C",
                                "smart_sending_enabled": True,
                                "name": "Email #2"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_9"
                        }
                    },
                    {
                        "temporary_id": "action_9",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 1,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_10"
                        }
                    },
                    {
                        "temporary_id": "action_10",
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
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": "action_11",
                            "next_if_false": None
                        }
                    },
                    {
                        "temporary_id": "action_11",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Your $29 climbing pass is still available",
                                "preview_text": "",
                                "template_id": "XQ9yzb",
                                "smart_sending_enabled": True,
                                "name": "Email #3"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_12"
                        }
                    },
                    {
                        "temporary_id": "action_12",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 3,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_13"
                        }
                    },
                    {
                        "temporary_id": "action_13",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Quick reminder: your $29 two-week climbing pass link is still active, but expires soon. \n\nUnlimited climbs + free rentals →\nhttps://mcsms.io/0iczz4",
                                "shorten_links": True,
                                "add_org_prefix": True,
                                "add_info_link": True,
                                "add_opt_out_language": True,
                                "smart_sending_enabled": True,
                                "sms_quiet_hours_enabled": True,
                                "name": "Text message #3"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_14"
                        }
                    },
                    {
                        "temporary_id": "action_14",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 2,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_15"
                        }
                    },
                    {
                        "temporary_id": "action_15",
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
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": "action_16",
                            "next_if_false": None
                        }
                    },
                    {
                        "temporary_id": "action_16",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Quick math: come climb 3× and it's basically free",
                                "preview_text": "",
                                "template_id": "SjRTup",
                                "smart_sending_enabled": True,
                                "name": "Email #4"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_17"
                        }
                    },
                    {
                        "temporary_id": "action_17",
                        "type": "time-delay",
                        "data": {
                            "unit": "days",
                            "value": 1,
                            "timezone": "profile",
                            "delay_until_weekdays": ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                        },
                        "links": {
                            "next": "action_18"
                        }
                    },
                    {
                        "temporary_id": "action_18",
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
                                                    "value": "active-membership"
                                                }
                                            }
                                        ]
                                    }
                                ]
                            }
                        },
                        "links": {
                            "next_if_true": "action_19",
                            "next_if_false": None
                        }
                    },
                    {
                        "temporary_id": "action_19",
                        "type": "send-sms",
                        "data": {
                            "message": {
                                "body": "Basin:\n\nLast day to grab your $29 unlimited 2-week climbing pass. Link expires tonight →\nhttps://mcsms.io/w63a4a\n",
                                "shorten_links": True,
                                "add_org_prefix": True,
                                "add_info_link": True,
                                "add_opt_out_language": True,
                                "smart_sending_enabled": True,
                                "sms_quiet_hours_enabled": True,
                                "name": "Text message #4"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": "action_20"
                        }
                    },
                    {
                        "temporary_id": "action_20",
                        "type": "time-delay",
                        "data": {
                            "unit": "hours",
                            "value": 1,
                            "secondary_value": 0,
                            "timezone": "profile"
                        },
                        "links": {
                            "next": "action_21"
                        }
                    },
                    {
                        "temporary_id": "action_21",
                        "type": "send-email",
                        "data": {
                            "message": {
                                "from_email": "info@basinclimbing.com",
                                "from_label": "Basin Climbing and Fitness",
                                "reply_to_email": "info@basinclimbing.com",
                                "cc_email": None,
                                "bcc_email": None,
                                "subject_line": "Last chance! $29 climbing pass expires tonight",
                                "preview_text": "If you haven't already- take advantage of this offer",
                                "template_id": "XXS55i",
                                "smart_sending_enabled": True,
                                "name": "Email #5"
                            },
                            "status": "draft"
                        },
                        "links": {
                            "next": None
                        }
                    }
                ],
                "entry_action_id": "action_1",
                "reentry_criteria": {
                    "duration": 1,
                    "unit": "alltime"
                }
            }
        }
    }
}

print("Creating new flow with list trigger...")
print(f"List ID: {LIST_ID}")

url = "https://a.klaviyo.com/api/flows"
response = requests.post(url, headers=headers, json=flow_definition, timeout=60)

if response.status_code in [200, 201]:
    result = response.json()
    flow_id = result['data']['id']
    flow_name = result['data']['attributes']['name']
    print(f"\n✅ Flow created successfully!")
    print(f"   ID: {flow_id}")
    print(f"   Name: {flow_name}")
    print(f"   Status: draft (ready to be turned on)")
else:
    print(f"\n❌ Error creating flow: {response.status_code}")
    print(response.text)

"""List all Klaviyo flows."""
import os
import requests
import json

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

KLAVIYO_API_KEY = os.getenv("KLAVIYO_PRIVATE_KEY")
headers = {
    "Authorization": f"Klaviyo-API-Key {KLAVIYO_API_KEY}",
    "revision": "2025-01-15",
    "Accept": "application/json"
}

# Get all flows
url = "https://a.klaviyo.com/api/flows"
response = requests.get(url, headers=headers, timeout=30)

if response.status_code == 200:
    flows = response.json().get('data', [])
    print(f"Found {len(flows)} flows:\n")
    for flow in flows:
        attrs = flow['attributes']
        print(f"ID: {flow['id']}")
        print(f"   Name: {attrs['name']}")
        print(f"   Status: {attrs['status']}")
        print(f"   Trigger Type: {attrs.get('trigger_type', 'N/A')}")
        print()
else:
    print(f"Error: {response.status_code}")
    print(response.text)

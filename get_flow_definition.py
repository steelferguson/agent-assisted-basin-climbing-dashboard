"""Get full flow definition from Klaviyo."""
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
    "revision": "2024-10-15.pre",  # Beta revision for flow definition
    "Accept": "application/json"
}

flow_id = "UM55ZF"  # Day Pass - 2 Week Pass

# Get flow with full definition
url = f"https://a.klaviyo.com/api/flows/{flow_id}?additional-fields[flow]=definition"
response = requests.get(url, headers=headers, timeout=30)

if response.status_code == 200:
    flow = response.json()
    print(json.dumps(flow, indent=2))
else:
    print(f"Error: {response.status_code}")
    print(response.text)

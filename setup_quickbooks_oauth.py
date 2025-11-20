"""
QuickBooks OAuth Setup Script

This script helps you complete the OAuth flow to get access tokens
for the QuickBooks API.

Steps:
1. Run this script
2. Click the authorization URL it prints
3. Log into QuickBooks and approve the app
4. Copy the full redirect URL you're sent to
5. Paste it back into this script
6. Your tokens will be saved to INTUIT_QUICKBOOKS_CREDENTIALS.md
"""

from intuitlib.client import AuthClient
from intuitlib.enums import Scopes
import sys

# Your credentials
CLIENT_ID = 'ABGH71AmkTpOcNVtSYtBAGG0EQ3TN48EAkUNYlO3lwt5giEpQE'
CLIENT_SECRET = 'FCmxDTLD1RY1bCDN5kRkapKlJHKGYw85RPxdfYjH'

# Redirect URI - this MUST match what's configured in your QuickBooks app settings
# For local testing, this is typical. Check your app settings at developer.intuit.com
REDIRECT_URI = 'http://localhost:8000/callback'

# Environment: 'sandbox' or 'production'
ENVIRONMENT = 'production'  # Change to 'sandbox' if testing

def main():
    print("=" * 70)
    print("QuickBooks OAuth Setup")
    print("=" * 70)
    print()

    # Create auth client
    auth_client = AuthClient(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        environment=ENVIRONMENT,
        redirect_uri=REDIRECT_URI,
    )

    # Scopes we need
    scopes = [
        Scopes.ACCOUNTING  # Access to accounting data (expenses, bills, etc.)
    ]

    # Generate authorization URL
    auth_url = auth_client.get_authorization_url(scopes)

    print("STEP 1: Authorize the application")
    print("-" * 70)
    print("Click this URL to authorize QuickBooks access:")
    print()
    print(auth_url)
    print()
    print("After authorizing, you'll be redirected to a URL that looks like:")
    print("http://localhost:8000/callback?code=...&realmId=...&state=...")
    print()
    print("IMPORTANT: Your browser may show an error (site can't be reached).")
    print("That's OK! Just copy the FULL URL from your browser's address bar.")
    print()
    print("-" * 70)
    print()

    # Get the callback URL from user
    callback_url = input("Paste the full redirect URL here: ").strip()

    if not callback_url:
        print("❌ No URL provided. Exiting.")
        sys.exit(1)

    try:
        # Parse the callback URL to get the auth code and realm ID
        auth_client.get_bearer_token(callback_url)

        # Get tokens
        access_token = auth_client.access_token
        refresh_token = auth_client.refresh_token
        realm_id = auth_client.realm_id

        print()
        print("=" * 70)
        print("✅ SUCCESS! Tokens obtained")
        print("=" * 70)
        print()
        print(f"Company ID (Realm ID): {realm_id}")
        print(f"Access Token: {access_token[:20]}...{access_token[-20:]}")
        print(f"Refresh Token: {refresh_token[:20]}...{refresh_token[-20:]}")
        print()

        # Save to credentials file
        print("Updating INTUIT_QUICKBOOKS_CREDENTIALS.md with tokens...")

        with open('INTUIT_QUICKBOOKS_CREDENTIALS.md', 'r') as f:
            content = f.read()

        # Append tokens to the file
        token_section = f"""

## Access Tokens (Generated {auth_client.id_token.get('iat', 'unknown')})

**IMPORTANT:** These tokens expire. Access tokens last 1 hour, refresh tokens last 100 days.

**Company ID (Realm ID):**
```
{realm_id}
```

**Access Token:**
```
{access_token}
```

**Refresh Token:**
```
{refresh_token}
```

**Token Expiration:**
- Access Token expires: 1 hour from generation
- Refresh Token expires: 100 days from generation

**To refresh tokens:** Run `refresh_quickbooks_tokens.py` before access token expires
"""

        with open('INTUIT_QUICKBOOKS_CREDENTIALS.md', 'w') as f:
            f.write(content + token_section)

        print("✅ Tokens saved to INTUIT_QUICKBOOKS_CREDENTIALS.md")
        print()
        print("Next steps:")
        print("1. Test the API connection with: python test_quickbooks_api.py")
        print("2. Set up automatic token refresh (tokens expire!)")
        print("3. Integrate with your dashboard")
        print()

    except Exception as e:
        print()
        print(f"❌ Error: {e}")
        print()
        print("Common issues:")
        print("- Make sure the redirect URI matches your app settings")
        print("- Check that you copied the FULL URL including ?code=... etc.")
        print("- Verify you're using the correct environment (sandbox vs production)")
        sys.exit(1)

if __name__ == "__main__":
    main()

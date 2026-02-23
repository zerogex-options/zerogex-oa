"""
TradeStation OAuth Token Generator

Interactive script to get OAuth tokens from TradeStation.
This must be run manually during the initial setup of the application.

This process reads the TradeStation API client ID and secret from your
local .env file, prints out the authorization URL to access via web
browser, waits for you to input the callback URL provided from the web
browser session, then exchanges for tokens and saves the refresh token
to your local .env file.

For more detailed information, review the TradeStation API
documentation for Auth Code Flow:
https://api.tradestation.com/docs/fundamentals/authentication/auth-code
"""

import os
import requests
from urllib.parse import urlencode, urlparse, parse_qs
from dotenv import load_dotenv

print("\n" + "="*60)
print("TradeStation OAuth Setup")
print("="*60)

# Load .env file
load_dotenv()

# Configuration
CLIENT_ID = os.getenv('TRADESTATION_CLIENT_ID')
CLIENT_SECRET = os.getenv('TRADESTATION_CLIENT_SECRET')
REDIRECT_URI = "http://localhost:3000"
USE_SANDBOX = os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'

# Check that client ID and secret are specified in .env
if not CLIENT_ID or not CLIENT_SECRET:
    print("❌ Error: TRADESTATION_CLIENT_ID and TRADESTATION_CLIENT_SECRET must be set in .env file")
    print("\nPlease add these lines to /home/ubuntu/zerogex-oa/.env:")
    print("TRADESTATION_CLIENT_ID=your_client_id_here")
    print("TRADESTATION_CLIENT_SECRET=your_client_secret_here\n")
    exit(1)

print(f"✅ Loaded credentials from .env file")
print(f"   Client ID: {CLIENT_ID[:20]}...")

# OAuth URLS
AUTH_URL = "https://signin.tradestation.com/authorize"
TOKEN_URL = "https://signin.tradestation.com/oauth/token"
if USE_SANDBOX:
    AUTH_URL = "https://sim-signin.tradestation.com/authorize"
    TOKEN_URL = "https://sim-signin.tradestation.com/oauth/token"

# Generate authorization URL
params = {
    'response_type': 'code',
    'client_id': CLIENT_ID,
    'audience' : 'https://api.tradestation.com',
    'redirect_uri': REDIRECT_URI,
    'state' : 'gex',
    'scope': 'openid offline_access profile MarketData ReadAccount Trade OptionSpreads'
}

auth_url = f"{AUTH_URL}?{urlencode(params)}"

print("\n" + "="*60)
print("STEP 1: Visit this URL in your browser:\n")
print(auth_url)

print("\n" + "="*60)
print("STEP 2: After authorizing, you'll be redirected to a URL (e.g. http://localhost:3000/callback?code=XXXXX")
print("\nThe page won't load (that's OK). Just copy the ENTIRE URL from your browser.")

callback_url = input("\nPaste the callback URL here: ").strip()

# Extract code
parsed = urlparse(callback_url)
params = parse_qs(parsed.query)

if 'code' not in params:
    print("❌ No authorization code found in URL\n")
    exit(1)

auth_code = params['code'][0]
print(f"\n✅ Received authorization code:")
print(f"   {auth_code[:20]}...")

# Exchange for tokens
print("\n🔄 Exchanging code for tokens...")

data = {
    'grant_type': 'authorization_code',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'code': auth_code,
    'redirect_uri': REDIRECT_URI
}

response = requests.post(TOKEN_URL, data=data)

if response.status_code == 200:

    # Parse tokens from JSON response
    tokens = response.json()

    # Validate that tokens were successfully
    # parsed from JSON response
    token_types = ["access_token", "refresh_token", "expires_in"]
    if all(key in tokens for key in token_types):

        # Get relevant bits from JSON response
        access_token = tokens.get('access_token')
        refresh_token = tokens.get('refresh_token')
        expires_in = tokens.get('expires_in')

        print("\n✅ Tokens received!")
        print(f"   Access Token: {access_token[:20]}...")
        print(f"   Refresh Token: {refresh_token[:20]}...")
        print(f"   Expires in: {expires_in} seconds")

        # Read lines from .env
        env_path = '/home/ubuntu/zerogex-oa/.env'
        with open(env_path, 'r') as f:
            lines = f.readlines()

        # Update .env with updated refresh tokens
        with open(env_path, 'w') as f:
            for line in lines:
                if line.startswith('TRADESTATION_REFRESH_TOKEN='):
                    f.write(f"TRADESTATION_REFRESH_TOKEN={refresh_token}\n")
                else:
                    f.write(line)

        print(f"\n💾 Refresh token saved to {env_path}")
        print("\n✅ Done! You can now start your services.")

    else:
        print(f"❌ Failed: Could not parse tokens from JSON response\n")

else:
    print(f"❌ Failed: {response.status_code}")
    print(response.text)
    print("\n")

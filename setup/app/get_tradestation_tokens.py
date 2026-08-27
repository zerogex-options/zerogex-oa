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

import argparse
import os
import shutil
import sys
import time
import requests
from urllib.parse import urlencode, urlparse, parse_qs
from dotenv import load_dotenv

# Repo root, so the identity helper is importable and .env is found wherever
# the repo is checked out rather than only under /home/ubuntu.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _REPO_ROOT)

_parser = argparse.ArgumentParser(description="Mint a TradeStation refresh token.")
_parser.add_argument(
    "--var",
    default="TRADESTATION_REFRESH_TOKEN",
    help="Which .env variable to write. Use TRADESTATION_FUTURES_REFRESH_TOKEN "
    "when authorising the SECOND username (the one holding real-time CME) so "
    "the main credential is left alone.",
)
_parser.add_argument(
    "--no-write", action="store_true", help="Print the token; do not touch .env."
)
_parser.add_argument(
    "--print-token",
    action="store_true",
    help="Print the FULL refresh token. It is a secret — it will be in your "
    "shell scrollback.",
)
_ARGS = _parser.parse_args()

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

        # WHICH USERNAME did you just authorise? Market-data entitlements
        # attach to the username, not to the API application, so a token minted
        # from the wrong browser session is the single easiest mistake to make
        # here — and without this line it is invisible until prices come back
        # stale days later.
        username = None
        try:
            from src.tools.tradestation_whoami import decode_jwt_payload, username_of

            username = username_of(decode_jwt_payload(access_token))
        except Exception:
            pass
        print(f"   Username:      {username or '(could not be determined)'}")

        if _ARGS.print_token or _ARGS.no_write:
            print(f"\n   FULL refresh token (secret — now in your scrollback):\n   {refresh_token}")

        if _ARGS.no_write:
            print("\n--no-write: .env not modified.")
            print(f"Set it yourself:  {_ARGS.var}=<the token above>")
            raise SystemExit(0)

        env_path = os.path.join(_REPO_ROOT, '.env')
        with open(env_path, 'r') as f:
            lines = f.readlines()

        # BACK UP FIRST. This script rewrites a live production credential, and
        # the value it replaces cannot be recovered from anywhere else — a
        # refresh token is shown once. Overwriting the main credential while
        # meaning to add a second one is a real incident, not a hypothetical.
        backup = f"{env_path}.bak.{time.strftime('%Y%m%d-%H%M%S')}"
        shutil.copy2(env_path, backup)
        os.chmod(backup, 0o600)

        prefix = f"{_ARGS.var}="
        previous = next((ln for ln in lines if ln.startswith(prefix)), None)
        replaced = False
        with open(env_path, 'w') as f:
            for line in lines:
                if line.startswith(prefix):
                    f.write(f"{prefix}{refresh_token}\n")
                    replaced = True
                else:
                    f.write(line)
            # Append when the variable is absent. Without this, --var for a new
            # variable matched no line and the script reported success having
            # written nothing at all.
            if not replaced:
                if lines and not lines[-1].endswith("\n"):
                    f.write("\n")
                f.write(f"{prefix}{refresh_token}\n")

        print(f"\n💾 {_ARGS.var} {'updated in' if replaced else 'ADDED to'} {env_path}")
        print(f"   Previous .env backed up to {backup}")
        if replaced and previous:
            print(f"   It REPLACED an existing {_ARGS.var} — recover it from the backup")
            print("   above if that was not what you intended.")
        print("\nConfirm which username each feed now runs as:")
        print("   make ts-whoami")
        print("\n✅ Done! You can now start your services.")

    else:
        print(f"❌ Failed: Could not parse tokens from JSON response\n")

else:
    print(f"❌ Failed: {response.status_code}")
    print(response.text)
    print("\n")

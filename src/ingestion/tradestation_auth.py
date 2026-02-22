"""
TradeStation Authentication Manager

Handles OAuth2 authentication with TradeStation API.
"""

import os
import requests
import time
from datetime import datetime, timedelta
from src.utils import get_logger

# Initialize logger
logger = get_logger(__name__)

class TradeStationAuth:
    """Manage TradeStation API authentication"""

    TOKEN_URL = "https://signin.tradestation.com/oauth/token"
    SANDBOX_TOKEN_URL = "https://sim-signin.tradestation.com/oauth/token"

    def __init__(self, client_id: str, client_secret: str, refresh_token: str, sandbox: bool = False):
        """
        Initialize auth manager

        Args:
            client_id: TradeStation API client ID
            client_secret: TradeStation API client secret
            refresh_token: Refresh token for obtaining access tokens
            sandbox: Use sandbox environment (default False)
        """
        logger.debug("Initializing TradeStationAuth...")

        if not client_id or not client_secret or not refresh_token:
            logger.critical("Missing required authentication credentials!")
            raise ValueError("Client ID, Client Secret, and Refresh Token are required")

        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self.sandbox = sandbox

        self.token_url = self.SANDBOX_TOKEN_URL if sandbox else self.TOKEN_URL

        # Cached access token
        self.access_token = None
        self.token_expiry = None

        logger.info(f"TradeStationAuth initialized for {'sandbox' if sandbox else 'production'}")

    def get_access_token(self) -> str:
        """
        Get valid access token, refreshing if necessary

        Returns:
            Valid access token
        """
        logger.debug("Checking access token validity...")

        # If we already have an access token and it's not expired
        # or coming up for expiry, then return it
        # Otherwise, refresh it
        if self.access_token and self.token_expiry:
            time_until_expiry = (self.token_expiry - datetime.now()).total_seconds()
            logger.debug(f"Token expires in {time_until_expiry:.0f} seconds")

            if time_until_expiry > 5*60:
                logger.debug("Using cached access token")
                return self.access_token
            elif time_until_expiry > 0:
                logger.debug("Access token will expire in <5 minutes, refreshing...")
            else:
                logger.info("Access token expired, refreshing...")
        else:
            logger.info("No cached token, obtaining new access token...")

        return self._refresh_access_token()

    def _refresh_access_token(self) -> str:
        """
        Refresh access token using refresh token

        Returns:
            New access token
        """
        logger.debug(f"Requesting new access token from {self.token_url}...")

        # Generate JSON payload for refresh_token request
        # grant_type:    'refresh_token'
        # client_id:     client ID or API key from .env
        # client_secret: client secrect from .env
        # refresh_token: refresh token from .env
        payload = {
            'grant_type': 'refresh_token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token
        }

        try:

            # Make refresh token request to https://signin.tradestation.com/oauth/token
            # (or for sandbox: https://sim-signin.tradestation.com/oauth/token)
            response = requests.post(self.token_url, data=payload, timeout=10)

            logger.debug(f"Token request status code: {response.status_code}")

            if response.status_code != 200:
                logger.error(f"Token refresh failed with status {response.status_code}")
                logger.error(f"Response: {response.text}")
                response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Pull access token from JSON response
            # Access tokens have a 20-minute lifetime
            # For more details, see:
            # https://api.tradestation.com/docs/fundamentals/authentication/refresh-tokens
            self.access_token = data['access_token']
            expires_in = data.get('expires_in', 1200)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            logger.info(f"✅ Access token refreshed successfully (expires in {expires_in}s)")
            logger.debug(f"Token expiry set to: {self.token_expiry}")

            return self.access_token

        except requests.exceptions.Timeout:
            logger.error("Token refresh request timed out")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Token refresh request failed: {e}")
            raise
        except KeyError as e:
            logger.error(f"Unexpected token response format, missing key: {e}")
            logger.debug(f"Response data: {data}")
            raise
        except Exception as e:
            logger.critical(f"Unexpected error during token refresh: {e}", exc_info=True)
            raise

    def get_headers(self) -> dict:
        """
        Get authorization headers for API requests

        Returns:
            Dictionary with Authorization header
            containing the access token
        """
        token = self.get_access_token()
        headers = {'Authorization': f'Bearer {token}'}
        logger.debug("Generated authorization headers")
        return headers


def main():

    print("\n" + "="*60)
    print("TradeStation API Authentication...")
    print("="*60 + "\n")

    auth = TradeStationAuth(
        os.getenv('TRADESTATION_CLIENT_ID'),
        os.getenv('TRADESTATION_CLIENT_SECRET'),
        os.getenv('TRADESTATION_REFRESH_TOKEN'),
        sandbox=os.getenv('TRADESTATION_USE_SANDBOX', 'false').lower() == 'true'
    )

    try:

        token = auth.get_access_token()
        headers = auth.get_headers()

        print(f"\n✅ Access token obtained")
        print(f"   Token: {token[:50]}...")
        print(f"\n✅ Headers generated")
        print(f"   Authorization: Bearer {headers['Authorization'][7:50]}...")

    except Exception as e:
        print(f"❌ Authentication failed: {e}")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()

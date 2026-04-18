"""
TradeStation Authentication Manager

Handles OAuth2 authentication with TradeStation API.
"""

import logging
import os
import requests
import time
import json
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from threading import Lock
import fcntl
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
        self._token_lock = Lock()
        self._last_refresh_epoch: float = 0.0
        self.refresh_buffer_seconds = int(
            os.getenv("TS_REFRESH_BUFFER_SECONDS", "30")
        )
        self.min_force_refresh_interval_seconds = int(
            os.getenv("TS_MIN_FORCE_REFRESH_INTERVAL_SECONDS", "60")
        )
        token_cache_name = "tradestation_token_cache_sandbox.json" if sandbox else "tradestation_token_cache.json"
        lock_cache_name = "tradestation_token_cache_sandbox.lock" if sandbox else "tradestation_token_cache.lock"
        self._token_cache_path = Path(tempfile.gettempdir()) / token_cache_name
        self._token_cache_lock_path = Path(tempfile.gettempdir()) / lock_cache_name

        logger.info(f"TradeStationAuth initialized for {'sandbox' if sandbox else 'production'}")

    def _load_cached_token_from_disk(self) -> bool:
        """Best-effort load of a still-valid token from shared disk cache."""
        try:
            if not self._token_cache_path.exists():
                return False
            data = json.loads(self._token_cache_path.read_text())
            token = data.get("access_token")
            expiry_epoch = float(data.get("expiry_epoch", 0))
            if not token or expiry_epoch <= 0:
                return False
            expires_in = expiry_epoch - time.time()
            if expires_in <= self.refresh_buffer_seconds:
                return False
            self.access_token = token
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            self._last_refresh_epoch = time.time()
            logger.debug("Loaded TradeStation token from shared cache")
            return True
        except Exception as e:
            logger.debug(f"Unable to load shared token cache: {e}")
            return False

    def _persist_cached_token_to_disk(self, expires_in: int) -> None:
        """Best-effort write of current token to shared disk cache."""
        if not self.access_token:
            return
        payload = {
            "access_token": self.access_token,
            "expiry_epoch": time.time() + max(0, int(expires_in)),
            "updated_epoch": time.time(),
        }
        tmp_path = self._token_cache_path.with_suffix(".tmp")
        # Write with mode 0o600 so the access token is not world-readable in
        # the shared tempdir.  os.open lets us set the mode atomically before
        # writing any bytes.
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        fd = os.open(str(tmp_path), flags, 0o600)
        try:
            with os.fdopen(fd, "w") as f:
                f.write(json.dumps(payload))
        except Exception:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise
        os.replace(str(tmp_path), str(self._token_cache_path))

    def get_access_token(self) -> str:
        """
        Get valid access token, refreshing if necessary

        Returns:
            Valid access token
        """
        with self._token_lock:
            logger.debug("Checking access token validity...")

            # If we already have an access token and it's not expired
            # or coming up for expiry, then return it
            # Otherwise, refresh it
            if self.access_token and self.token_expiry:
                time_until_expiry = (self.token_expiry - datetime.now()).total_seconds()
                logger.debug(f"Token expires in {time_until_expiry:.0f} seconds")

                if time_until_expiry > self.refresh_buffer_seconds:
                    logger.debug("Using cached access token")
                    return self.access_token
                elif time_until_expiry > 0:
                    logger.debug(
                        "Access token expires soon (<=%ss), refreshing...",
                        self.refresh_buffer_seconds,
                    )
                else:
                    logger.info("Access token expired, refreshing...")
            else:
                if self._load_cached_token_from_disk():
                    return self.access_token
                logger.info("No cached token, obtaining new access token...")

            return self._refresh_access_token()

    def force_refresh_access_token(self) -> str:
        """Force-refresh access token (used when API returns 401)."""
        with self._token_lock:
            now_epoch = time.time()
            if (
                self.access_token
                and self.token_expiry
                and (self.token_expiry - datetime.now()).total_seconds() > self.refresh_buffer_seconds
                and (now_epoch - self._last_refresh_epoch) < self.min_force_refresh_interval_seconds
            ):
                logger.warning(
                    "Skipping force-refresh: last refresh %.1fs ago and token still valid",
                    now_epoch - self._last_refresh_epoch,
                )
                return self.access_token
            logger.warning("Forcing TradeStation access token refresh after auth failure")
            return self._refresh_access_token()

    def invalidate_token(self):
        """Invalidate cached token so the next request must refresh."""
        with self._token_lock:
            self.access_token = None
            self.token_expiry = None

    def token_seconds_remaining(self) -> float:
        """Return seconds until token expiry (negative means expired/unknown)."""
        with self._token_lock:
            if not self.access_token or not self.token_expiry:
                return -1.0
            return (self.token_expiry - datetime.now()).total_seconds()

    def should_refresh_soon(self, buffer_seconds: int = 120) -> bool:
        """Whether token is missing/expired/near expiry and should be refreshed."""
        return self.token_seconds_remaining() <= buffer_seconds

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
            with open(self._token_cache_lock_path, "a+") as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)

                # Another process may have refreshed while we waited for the lock.
                if self._load_cached_token_from_disk():
                    return self.access_token

                # Make refresh token request to https://signin.tradestation.com/oauth/token
                # (or for sandbox: https://sim-signin.tradestation.com/oauth/token)
                response = requests.post(self.token_url, data=payload, timeout=10)

                logger.debug(f"Token request status code: {response.status_code}")

                if response.status_code != 200:
                    logger.error(f"Token refresh failed with status {response.status_code}")
                    # Avoid logging the response body at error level — TradeStation
                    # may echo back parts of the request payload.  Surface it only
                    # when DEBUG is enabled and operators have opted in to verbose
                    # logging.
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Token refresh response body: {response.text}")
                    response.raise_for_status()

                # Parse JSON response
                data = response.json()
                # Never log the raw `data` dict — it contains the bearer access
                # token.  Log only non-sensitive metadata.
                logger.debug(
                    "Token refresh response received (keys=%s)",
                    sorted(data.keys()) if isinstance(data, dict) else type(data).__name__,
                )

                # Pull access token from JSON response
                # Access tokens have a 20-minute lifetime
                # For more details, see:
                # https://api.tradestation.com/docs/fundamentals/authentication/refresh-tokens
                self.access_token = data['access_token']
                expires_in = data.get('expires_in', 1200)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
                self._last_refresh_epoch = time.time()
                self._persist_cached_token_to_disk(expires_in)
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
            # Log only the key set, never the values (which include the bearer token).
            try:
                key_summary = sorted(data.keys()) if isinstance(data, dict) else type(data).__name__
            except Exception:
                key_summary = "unavailable"
            logger.debug(f"Response key summary: {key_summary}")
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

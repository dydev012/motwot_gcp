import os
import logging
from typing import Optional, Dict, Any
from pathlib import Path
import msal
import atexit
import time
import json
from _base import ENV

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MOTOAuth2Client(ENV):
    """
    Government OAuth2 authentication client using MSAL with token caching.
    Designed for UK Government services using Azure AD.
    """

    def __init__(self, cache_file: str = "token_cache.json"):
        """
        Initialize the OAuth2 client with configuration from environment variables.

        Args:
            cache_file: Path to the token cache file
        """
        ENV.__init__(self)

        self.client_id = os.getenv("MOT_CLIENT_ID")
        self.client_secret = os.getenv("MOT_CLIENT_SECRET")
        self.scope_url = os.getenv("MOT_SCOPE_URL")
        self.token_url = os.getenv("MOT_TOKEN_URL")
        self.api_key = os.getenv("MOT_API_KEY")
        self.dir = Path(__file__).parent
        self.cache_file = Path(os.path.join(self.dir, cache_file))

        # Validate required environment variables
        if not all([self.client_id, self.client_secret, self.token_url]):
            raise ValueError("Missing required environment variables: MOT_CLIENT_ID, MOT_CLIENT_SECRET, MOT_TOKEN_URL")

        # Extract tenant ID from token URL
        self.tenant_id = self._extract_tenant_id(self.token_url)
        # Initialize MSAL client
        self.app = self._create_msal_app()

        logger.info("GovOAuth2Client initialized successfully")

    def _extract_tenant_id(self, token_url: str) -> str:
        """Extract tenant ID from the token URL."""
        try:
            # Extract tenant ID from URL like: https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token
            parts = token_url.split('/')
            tenant_index = parts.index('login.microsoftonline.com') + 1
            return parts[tenant_index]
        except (ValueError, IndexError):
            raise ValueError(f"Cannot extract tenant ID from token URL: {token_url}")

    def _init_cache(self):
        self.cache = msal.SerializableTokenCache()
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache.deserialize(f.read())
                logger.info(f"Loaded existing token cache from {self.cache_file}")
            except Exception as e:
                logger.warning(f"Failed to load cache file: {e}")

        # Hint: The following optional line persists only when state changed
        atexit.register(
            lambda: open("my_cache.bin", "w").write(self.cache.serialize()) if self.cache.has_state_changed else None
        )

    def _create_msal_app(self) -> msal.ConfidentialClientApplication:
        """Create and configure the MSAL application with token cache."""
        self._init_cache()

        app = msal.ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            token_cache=self.cache,
        )

        return app

    def _save_cache(self) -> None:
        """Save the token cache to file."""
        if self.app.token_cache.has_state_changed:
            try:
                with open(self.cache_file, 'w+') as f:
                    f.write(self.app.token_cache.serialize())
                logger.info(f"Token cache saved to {self.cache_file}")
            except Exception as e:
                logger.error(f"Failed to save cache: {e}")

    def _is_token_expired(self) -> bool:
        """
        Check if the cached token is expired by examining the cache file.

        Returns:
            True if token is expired or doesn't exist, False otherwise
        """
        try:
            if not self.cache_file.exists():
                return True

            with open(self.cache_file, "r") as f:
                cache_data = json.load(f)

            # Look for AccessToken in cache
            access_tokens = cache_data.get("AccessToken", {})
            if not access_tokens:
                return True

            # Check expiry of the first (and likely only) token
            for token_key, token_data in access_tokens.items():
                expires_on = token_data.get("expires_on")
                if expires_on:
                    current_time = int(time.time())
                    # Add 60 second buffer to avoid edge cases
                    return current_time >= (int(expires_on) - 60)

            return True  # Default to expired if we can't determine

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning(f"Error checking token expiry: {e}")
            return True  # Default to expired if we can't read the cac

    def get_access_token(self, force_refresh: bool = False) -> Optional[str]:
        """
        Get a valid access token, using cache when possible.

        Args:
            force_refresh: If True, force token refresh even if cached token is valid

        Returns:
            Access token string or None if authentication fails
        """
        scopes = [self.scope_url]

        # Try to get token from cache first (unless force refresh is requested)
        if not force_refresh:
            accounts = self.app.get_accounts()
            if accounts:
                try:
                    result = self.app.acquire_token_silent(scopes, account=accounts[0])
                    if result and "access_token" in result:
                        logger.info("Access token acquired from cache")
                        return result["access_token"]
                    elif result and "error" in result:
                        logger.warning(f"Silent token acquisition failed: {result['error']}")
                except Exception as e:
                    logger.warning(f"Error during silent token acquisition: {e}")

        # If cache miss or force refresh, acquire new token
        try:
            result = self.app.acquire_token_for_client(scopes=scopes)

            if "access_token" in result:
                self._save_cache()
                return result["access_token"]
            else:
                error_msg = result.get("error", "Unknown error")
                error_desc = result.get("error_description", "No description")
                logger.error(f"Token acquisition failed: {error_msg} - {error_desc}")
                return None

        except Exception as e:
            logger.error(f"Exception during token acquisition: {e}")
            return None

    def get_token_info(self) -> Optional[Dict[str, Any]]:
        """
        Get detailed token information including expiration time.

        Returns:
            Dictionary with token information or None if no token available
        """
        try:
            result = self.app.acquire_token_for_client(scopes=[self.scope_url])
            if "access_token" in result:
                return {
                    "access_token": result["access_token"],
                    "token_type": result.get("token_type", "Bearer"),
                    "expires_in": result.get("expires_in"),
                    "scope": result.get("scope"),
                    "expires_on": result.get("expires_on"),
                }
        except Exception as e:
            logger.error(f"Error getting token info: {e}")
        return None

    def clear_cache(self) -> None:
        """Clear the token cache."""
        try:
            if self.cache_file.exists():
                self.cache_file.unlink()
                logger.info("Token cache cleared")

            # Clear in-memory cache
            accounts = self.app.get_accounts()
            for account in accounts:
                self.app.remove_account(account)

        except Exception as e:
            logger.error(f"Error clearing cache: {e}")

    def is_authenticated(self) -> bool:
        """
        Check if the client has a valid token.

        Returns:
            True if authenticated, False otherwise
        """
        token = self.get_access_token()
        return token is not None

    def get_auth_headers(self) -> Optional[Dict[str, str]]:
        """
        Get authorization header for API requests.

        Returns:
            Dictionary with Authorization header or None if not authenticated
        """
        token = self.get_access_token()
        if token:
            return {"Authorization": f"Bearer {token}", "X-API-Key": self.api_key, "accept": "application/json"}
        return None


if __name__ == "__main__":

    client = MOTOAuth2Client(cache_file="token_cache.json")
    token = client.get_access_token()
    if token:
        print("✅ Authentication successful!")
        token_info = client.get_token_info()
        if token_info:
            print(f"Token expires in: {token_info.get('expires_in')} seconds")
            print(f"Token scope: {token_info.get('scope')}")
        auth_header = client.get_auth_headers()
    else:
        print("❌ Authentication failed!")

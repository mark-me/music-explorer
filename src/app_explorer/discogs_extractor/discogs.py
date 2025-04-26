from celery import Celery
from discogs_client import Client
from discogs_client.exceptions import HTTPError

from log_config import logging
from utils import SecretsYAML

from .derive import DiscogsDerive
from .extractor_collection import ETLCollection

logger = logging.getLogger(__name__)


class Discogs:
    """Handles Discogs API authentication and initiates the ETL process.

    This class manages user authentication with the Discogs API, including
    checking for existing tokens, requesting user access, and saving user tokens.
    It also starts the ETL process for extracting and processing Discogs data.
    """
    def __init__(self, file_secrets: str, file_db: str) -> None:
        """Initializes Discogs API client and checks for user tokens.

        This method sets up the Discogs API client with consumer key and secret,
        initializes user secrets, and checks for existing user tokens.

        Args:
            file_secrets (str): File containing user secrets
            file_db (str): File location of the duckdb
        """
        self.file_db = file_db
        self.consumer_key = "zvHFpFQWJrdDfCwoLalG"
        self.consumer_secret = "FzRxDEGBbvWZpAmkQKBYHYeNdIjKxnVO"
        self.secrets = {"name": None, "secret": None, "token": None, "user": None}
        self.user_secrets_file = SecretsYAML(
            file_path=file_secrets,
            app="discogs",
            expected_keys=set(self.secrets.keys()),
        )
        self.user_agent = "boelmuziek"
        self.client_discogs = Client(
            self.user_agent,
            consumer_key=self.consumer_key,
            consumer_secret=self.consumer_secret,
        )
        self.check_user_tokens()

    def check_user_tokens(self) -> bool:
        """Checks for existing user tokens and sets them in the Discogs client.

        This method reads user secrets from the secrets file and, if found,
        sets the token and secret in the Discogs client.

        Returns:
            bool: True if user tokens are found and set, False otherwise.
        """
        result = self.user_secrets_file.read_secrets()
        if result is not None:
            logger.info("Found user token in config file config/secrets.yml")
            self.client_discogs.set_token(token=result["token"], secret=result["secret"])
            return True
        else:
            logger.warning("No user token found, user needs to authenticate the app use on Discogs")
            return False

    def request_user_access(self, url_callback: str = None) -> str:
        """Requests user access to their Discogs account.

        This method initiates the OAuth flow by requesting an authorization URL
        from the Discogs API. This URL is then used to redirect the user to
        Discogs for authentication.

        Args:
            url_callback (str, optional): The callback URL to redirect to after
                authentication. Defaults to None.

        Returns:
            str: The authorization URL.
        """

        logger.info(f"Requesting user access to Discogs account with callback {url_callback}")
        self._user_token, self._user_secret, url = self.client_discogs.get_authorize_url(
            callback_url=url_callback
        )
        return url

    def save_user_token(self, verification_code: str) -> dict:
        """Saves the user token and secret after successful authentication.

        This method receives the verification code from Discogs, exchanges it for
        an access token and secret, and saves these credentials to the secrets file.

        Args:
            verification_code (str): The verification code received from Discogs.

        Returns:
            dict: A dictionary containing the status code and message indicating
                success or failure of the authentication process.
        """

        oauth_verifier = verification_code
        try:
            logger.info("Receiving confirmation of access to the user's Discogs account")
            self._user_token, self._user_secret = self.client_discogs.get_access_token(
                oauth_verifier
            )
        except HTTPError:
            logger.error("Failed to authenticate.")
            return {"status_code": 401, "detail": "Unable to authenticate."}
        user = (
            self.client_discogs.identity()
        )  # Fetch the identity object for the current logged in user.
        # Write to secrets file
        dict_user = {
            "token": self._user_token,
            "secret": self._user_secret,
            "user": user.username,
            "name": user.name,
        }
        self.user_secrets_file.write_secrets(dict_secrets=dict_user)
        logger.info(f"Connected and written user credentials of {user.name}.")
        return {"status_code": 200, "message": f"User {user.username} connected."}

    def start_ETL(self, app_celery: Celery):
        """Starts the ETL process for Discogs data.

        This method initiates the extraction, transformation, and loading of
        Discogs data, including collection information and derived data.

        Args:
            app_celery (Celery): Celery application instance for task management.
        """
        progress = {
            "collection_value": {"current": 0, "total": 1, "item": "None"},
            "collection_items": {"current": 0, "total": 1, "item": "None"},
            "collection_artists": {"current": 0, "total": 1, "item": "None"},
            "derive": {"current": 0, "total": 1, "item": "None"},
        }
        collection = ETLCollection(
            discogs_client=self.client_discogs,
            file_db=self.file_db,
            app_celery=app_celery,
            progress=progress,
        )
        self.celery.update_state(state="PROGRESS")
        collection.process()
        derive = DiscogsDerive(file_db=self.file_db)
        derive.start()
        self.celery.update_state(state="SUCCESS")

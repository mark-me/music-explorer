from discogs_client import Client
from discogs_client.exceptions import HTTPError

from log_config import logging
from utils import SecretsYAML

from .derive import DiscogsDerive
from .extractor_collection import ETLCollection

logger = logging.getLogger(__name__)


class Discogs:
    def __init__(self, file_secrets: str, file_db: str) -> None:
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

    def check_user_tokens(self) -> dict:
        result = self.user_secrets_file.read_secrets()
        if result is not None:
            logger.info("Found user token in config file config/secrets.yml")
            self.client_discogs.set_token(
                token=result["token"], secret=result["secret"]
            )
            return True
        else:
            logger.warning(
                "No user token found, user needs to authenticate the app use on Discogs"
            )
            return False

    def request_user_access(self, callback_url: str = None) -> dict:
        """Prompt your user to "accept" the terms of your application. The application
        will act on behalf of their discogs.com account."""
        logger.info(
            f"Requesting user access to Discogs account with callback {callback_url}"
        )
        self._user_token, self._user_secret, url = (
            self.client_discogs.get_authorize_url(callback_url=callback_url)
        )
        return {
            "message": "Authorize BoelMuziek for access to your Discogs account :",
            "url": url,
        }

    def save_user_token(self, verification_code: str) -> dict:
        """If the user accepts, discogs displays a key to the user that is used for
        verification. The key is required in the 2nd phase of authentication."""
        oauth_verifier = verification_code
        try:
            logger.info(
                "Receiving confirmation of access to the user's Discogs account"
            )
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

    def start_ETL(self):
        collection = ETLCollection(
            discogs_client=self.client_discogs, file_db=self.file_db
        )
        collection.process()
        derive = DiscogsDerive(file_db=self.file_db)
        derive.start()

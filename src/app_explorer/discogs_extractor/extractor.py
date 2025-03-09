from celery import Celery
from db_operations import DBStorage
from log_config import logging

logger = logging.getLogger(__name__)


class DiscogsETL:
    """A virtual class for extracting, processing and storing a user's collection data from Discogs"""

    def __init__(self, file_db: str, app_celery: Celery) -> None:
        self.file_db = file_db
        self.celery = app_celery
        self.db = DBStorage(file_db=file_db)

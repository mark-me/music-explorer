from celery import Celery
from db_operations import DBStorage
from log_config import logging

logger = logging.getLogger(__name__)


class DiscogsETL:
    """Base class for Discogs ETL processes.

    This class provides common functionality for extracting, transforming, and loading
    Discogs data, including database connection, Celery app integration, and progress tracking.
    """
    def __init__(self, file_db: str, app_celery: Celery, progress: dict) -> None:
        """Initializes the DiscogsETL class.

        This method sets up the database connection, Celery app, and progress
        dictionary for the ETL process.
        """
        self.file_db = file_db
        self.celery = app_celery
        self.progress = progress
        self.db = DBStorage(file_db=file_db)

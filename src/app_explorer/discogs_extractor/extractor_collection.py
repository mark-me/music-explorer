import datetime as dt

import polars as pl
from celery import Celery
from discogs_client import Client, models

from log_config import logging

from .extractor import DiscogsETL
from .extractor_release import ETLRelease

logger = logging.getLogger(__name__)


class ETLCollection(DiscogsETL):
    """Extracts, transforms, and loads Discogs collection data.

    This class handles the ETL process for a user's Discogs collection,
    including collection value, items, and artists.
    """
    def __init__(self, discogs_client: Client, file_db: str, app_celery: Celery, progress: dict):
        """Initializes ETLCollection with Discogs client and database information.

        This method sets up the Discogs client, database connection, Celery app,
        and progress dictionary for collection data extraction.
        """
        super().__init__(file_db, app_celery=app_celery)
        self.discogs_client = discogs_client
        self.user = discogs_client.identity()
        self.progress = progress

    def process(self):
        """Processes collection value and items.

        This method orchestrates the extraction and loading of collection value
        and collection items data.
        """
        logger.info("Started ETL for collection")
        self.collection_value(target_table="collection_value")
        self.collection_items(target_table="collection_items")

    def collection_value(self, target_table: str) -> None:
        """Extracts and stores the user's collection value.

        This method retrieves the user's collection value statistics, such as
        minimum, median, and maximum values, and stores them in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """

        logger.info("Retrieve collection value")
        self.progress.update({"collection_value": {"current": 0, "total": 1, "item": ""}})
        self.celery.update_state(state="PROGRESS", meta=self.progress)
        collection_value = self.user.collection_value
        df_stats = pl.DataFrame(
            [
                {
                    "dt_loaded": dt.datetime.now(),
                    "qty_collection_items": self.user.num_collection,
                    "amt_maximum": collection_value.maximum,
                    "amt_median": collection_value.median,
                    "amt_minimum": collection_value.minimum,
                }
            ]
        )
        self.db.store_append(df=df_stats, name_table=target_table)
        self.progress.update({"collection_value": {"current": 1, "total": 1, "item": ""}})
        self.celery.update_state(state="PROGRESS", meta=self.progress)

    def collection_items(self, target_table: str) -> None:
        """Extracts and stores the user's collection items.

        This method retrieves the user's collection items, including details like
        release ID, date added, title, and rating, and stores them in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        logger.info("Process collection items")
        name_table = "collection_items"
        self.db.drop_table(name_table=name_table)
        qty_items = self.user.collection_folders[0].count
        lst_releases = self.user.collection_folders[0].releases
        for i, item in enumerate(lst_releases):
            self.progress.update(
                {
                    "collection_items": {
                        "current": i,
                        "total": qty_items,
                        "item": item.data["basic_information"]["title"],
                    }
                }
            )
            self.celery.update_state(state="PROGRESS", meta=self.progress)
            self._collection_item(collection_item=item, target_table=target_table)

    def _collection_item(
        self, collection_item: models.CollectionItemInstance, target_table: str
    ) -> None:
        """Extracts and stores a single collection item.

        This method retrieves details for a specific collection item, such as
        release ID, date added, title, and rating, and stores them in the specified table.
        It also triggers the extraction of release details.

        Args:
            collection_item (models.CollectionItemInstance): The collection item object.
            target_table (str): The name of the table to store the data in.
        """
        data = collection_item.data
        logging.info(f"Extracting collection item {data['basic_information']['title']}")
        dict_item = {
            "id_release": data["id"],
            "date_added": data["date_added"],
            "id_instance": data["instance_id"],
            "title": data["basic_information"]["title"],
            "id_master": data["basic_information"]["master_id"],
            "api_master": data["basic_information"]["master_url"],
            "api_release": data["basic_information"]["resource_url"],
            "url_thumbnail": data["basic_information"]["thumb"],
            "url_cover": data["basic_information"]["cover_image"],
            "year_released": data["basic_information"]["year"],
            "rating": data["rating"],
            "dt_loaded": dt.datetime.now(),
        }
        df = pl.DataFrame([dict_item])
        self.db.store_append(df, name_table=target_table)
        release = ETLRelease(collection_item.release, file_db=self.file_db)
        release.process()

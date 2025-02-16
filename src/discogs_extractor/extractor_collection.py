import datetime as dt

import polars as pl
from discogs_client import Client, models
from tqdm import tqdm

from log_config import logging

from .extractor import DiscogsETL
from .extractor_release import ETLRelease

logger = logging.getLogger(__name__)


class ETLCollection(DiscogsETL):
    def __init__(self, discogs_client: Client, file_db: str):
        super().__init__(file_db)
        self.discogs_client = discogs_client
        self.user = discogs_client.identity()

    def process(self):
        """Starting point of all collection"""
        logger.info("Started ETL for collection")
        self.collection_value(target_table="collection_value")
        self.collection_items(target_table="collection_items")

    def collection_value(self, target_table: str) -> None:
        """Collection value"""
        logger.info("Retrieve collection value")
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

    def collection_items(self, target_table: str) -> None:
        """Process the user's collection items"""
        logger.info("Process collection items")
        name_table = "collection_items"
        self.db.drop_table(name_table=name_table)
        qty_items = self.user.collection_folders[0].count
        lst_releases = self.user.collection_folders[0].releases
        for item in tqdm(
            lst_releases,
            total=qty_items,
            desc="Collection items",
        ):
            self._collection_item(collection_item=item, target_table=target_table)

    def _collection_item(self, collection_item: models.CollectionItemInstance, target_table: str) -> None:
        """Extract data for collection item

        Args:
            collection_item (models.CollectionItemInstance): A Discogs API representation of a collection item
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

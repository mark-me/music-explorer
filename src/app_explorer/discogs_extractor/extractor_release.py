import datetime as dt
from json.decoder import JSONDecodeError

import polars as pl
from celery import Celery
from discogs_client import models

from log_config import logging

from .extractor_artist import ETLArtist
from .extractor_master import ETLMaster

logger = logging.getLogger(__name__)


class ETLRelease(ETLMaster):
    """A class that processes release related data"""

    def __init__(self, release: models.Release, file_db: str, app_celery: Celery) -> None:
        super().__init__(release=release, file_db=file_db, app_celery=app_celery)
        self.obj_discogs = release
        self.artist_etl = ETLArtist(artists=release.artists, file_db=self.file_db, app_celery=app_celery)

    def process(self) -> None:
        logger.info(f"Extracting info from release {self.obj_discogs.title}")
        self.extract_stats(target_table="release_stats")
        exists = self.db.is_value_present(
            name_table="release", name_column="id_release", value=self.obj_discogs.id
        )
        if not exists:
            self.extract_release(target_table="release")
            self.extract_artists(target_table="release_artists")
            self.extract_labels(target_table="release_labels")
            self.extract_formats(target_table="release_formats")
            self.extract_genres(target_table="release_genres")
            self.extract_styles(target_table="release_styles")
            self.extract_credits(target_table="release_credits")
            self.extract_tracks(target_table="release_tracks")
            self.extract_track_artists(target_table="release_track_artists")
            self.extract_videos(target_table="release_videos")
            self.artist_etl.process()

    def extract_release(self, target_table: str) -> None:
        data = self.obj_discogs.data
        data.update(
            {
                "url_release": self.obj_discogs.url,
                "country": self.obj_discogs.country,
                "dt_loaded": dt.datetime.now(),
            }
        )
        df_release = pl.DataFrame([data])
        cols_release = [
            "id",
            "master_id",
            "title",
            "thumb",
            "cover_image",
            "year",
            "uri",
            "country",
        ]
        cols = list(set(cols_release) & set(df_release.columns))
        df_release = df_release[cols]
        dict_rename = {
            "id": "id_release",
            "master_id": "id_master",
            "cover_image": "url_cover",
            "uri": "url_release",
        }
        if "thumb" in df_release.columns:
            dict_rename.update({"thumb": "url_thumbnail"})
        df_release = df_release.rename(dict_rename)
        self.db.store_append(df=df_release, name_table=target_table)

    def extract_artists(self, target_table: str) -> None:
        lst_artists = []
        for artist in self.obj_discogs.artists:
            lst_artists.append(
                {
                    "id_artist": artist.id,
                    "id_release": self.obj_discogs.id,
                    "dt_loaded": dt.datetime.now(),
                }
            )
        df_artists = pl.DataFrame(lst_artists)
        if df_artists.shape[0] > 0:
            self.db.store_append(df=df_artists, name_table=target_table)

    def extract_labels(self, target_table: str) -> None:
        lst_labels = []
        for label in self.obj_discogs.labels:
            data = label.data
            data.update(
                {
                    "id_release": self.obj_discogs.id,
                    "dt_loaded": dt.datetime.now(),
                }
            )
            lst_labels.append(data)
        if len(lst_labels) > 0:
            df_labels = pl.DataFrame(lst_labels)
            df_labels = df_labels[["id_release", "id", "name", "catno", "dt_loaded"]]
            df_labels = df_labels.rename(
                {
                    "id": "id_label",
                    "name": "name_label",
                }
            )
            self.db.store_append(df=df_labels, name_table=target_table)

    def extract_formats(self, target_table: str) -> None:
        lst_formats = []
        for format in self.obj_discogs.formats:
            format.update({"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()})
            lst_formats.append(format)
        if len(lst_formats) > 0:
            df_formats = pl.DataFrame(lst_formats)
            df_formats = df_formats[["id_release", "name", "qty", "dt_loaded"]]
            df_formats = df_formats.rename({"name": "name_format", "qty": "qty_format"})
            self.db.store_append(df=df_formats, name_table=target_table)

    def extract_credits(self, target_table: str) -> pl.DataFrame:
        lst_artists = []
        for artist in self.obj_discogs.credits:
            data = artist.data
            data.update({"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()})
            lst_artists.append(data)
        if len(lst_artists) > 0:
            df_artists = pl.DataFrame(lst_artists)
            df_artists = df_artists[
                ["id_release", "name", "role", "id", "resource_url", "dt_loaded"]
            ]
            df_artists = df_artists.rename(
                {
                    "name": "name_artist",
                    "id": "id_artist",
                    "resource_url": "api_artist",
                }
            )
            self.db.store_append(df=df_artists, name_table=target_table)

    def extract_stats(self, target_table: str) -> pl.DataFrame:
        marketplace = self.obj_discogs.marketplace_stats
        dict_marketplace = marketplace.data
        community = self.obj_discogs.community
        dict_community = community.data
        try:
            num_for_sale = marketplace.num_for_sale
        except JSONDecodeError:
            logger.error(f"Can't extract market qty for sale from {self.obj_discogs.title}")
            return
        dict_stats = {
            "id_release": dict_marketplace["id"],
            "dt_loaded": dt.datetime.now(),
            "qty_for_sale": num_for_sale,
            "amt_price_lowest": dict_marketplace["lowest_price"]["value"]
            if dict_marketplace["lowest_price"] is not None
            else None,
            "qty_has": dict_community["have"],
            "qty_want": dict_community["want"],
            "avg_rating": dict_community["rating"]["average"],
            "qty_ratings": dict_community["rating"]["count"],
        }
        df_stats = pl.DataFrame([dict_stats])
        self.db.store_append(df=df_stats, name_table=target_table)

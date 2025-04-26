import datetime as dt

import polars as pl
from celery import Celery
from discogs_client import models

from log_config import logging

from .extractor import DiscogsETL

logger = logging.getLogger(__name__)


class ETLMaster(DiscogsETL):
    """Extracts, transforms, and loads Discogs master release data.

    This class handles the ETL process for Discogs master releases, including
    information like stats, genres, styles, tracks, artists, and videos.
    """
    def __init__(self, release: models.Release, file_db: str, app_celery: Celery, progress: dict) -> None:
        """Initializes ETLMaster with release data and database information.

        This method sets up the release object, database connection, Celery app,
        and progress dictionary for master release data extraction.

        Args:
            release (models.Release): The release object to extract data from.
            file_db (str): The path to the database file.
            app_celery (Celery): The Celery application instance.
            progress (dict): A dictionary to track the progress of the extraction.
        """
        super().__init__(file_db, app_celery=app_celery, progress=progress)
        self.obj_discogs = release

    def process(self) -> None:
        """Processes master release data and stores it in the database.

        This method extracts and stores various information related to a master release,
        including stats, genres, styles, tracks, artists, and videos,
        only if the master release has not been processed before.
        """
        self.extract_stats(target_table="master_stats")
        exists = self.db.is_value_present(
            name_table="master", name_column="id_master", value=self.obj_discogs.id
        )
        if not exists:
            logger.info(f"Extracting master info for '{self.obj_discogs.title}'")
            self.master()
            self.extract_genres(target_table="master_genres")
            self.extract_styles(target_table="master_styles")
            self.extract_tracks(target_table="master_tracks")
            self.extract_track_artists(target_table="master_track_artists")
            self.extract_videos(target_table="master_videos")
        else:
            logger.info(f"Already extract master info for '{self.obj_discogs.title}', skipped")

    def master(self) -> None:
        pass

    def extract_stats(self, target_table: str) -> None:
        """Extracts and stores master release statistics.

        This method retrieves community statistics for the master release,
        such as the number of users who want and have it, and stores them
        in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        stats = self.obj_discogs.data["stats"]
        df = pl.DataFrame[
            {
                "id_master": self.obj_discogs.id,
                "qty_wants": stats["community"]["in_wantlist"],
                "qty_has": stats["community"]["in_collection"],
                "dt_loaded": dt.datetime.now(),
            }
        ]
        self.db.store_append(df=df, name_table=target_table)

    def extract_styles(self, target_table: str) -> None:
        """Extracts and stores master release styles.

        This method retrieves the musical styles associated with the master release
        and stores them in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        if self.obj_discogs.styles is not None:
            if lst_styles := [
                {
                    "id_release": self.obj_discogs.id,
                    "name_style": style,
                    "dt_loaded": dt.datetime.now(),
                }
                for style in self.obj_discogs.styles
            ]:
                df = pl.DataFrame(lst_styles)
                self.db.store_append(df=df, name_table=target_table)

    def extract_genres(self, target_table: str) -> None:
        """Extracts and stores master release genres.

        This method retrieves the musical genres associated with the master release
        and stores them in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        lst_genres = []
        lst_genres.extend(
            {
                "id_release": self.obj_discogs.id,
                "name_genre": genre,
                "dt_loaded": dt.datetime.now(),
            }
            for genre in self.obj_discogs.genres
        )
        if lst_genres:
            df = pl.DataFrame(lst_genres)
            self.db.store_append(df=df, name_table=target_table)

    def extract_tracks(self, target_table: str) -> None:
        """Extracts and stores master release tracks.

        This method retrieves the tracklist of the master release,
        including track position, title, and duration, and stores them
        in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        lst_tracks = []
        for track in self.obj_discogs.tracklist:
            data = track.data
            data.update(
                {
                    "id_release": self.obj_discogs.id,
                    "dt_loaded": dt.datetime.now(),
                }
            )
            lst_tracks.append(data)
        if lst_tracks:
            df = pl.DataFrame(lst_tracks)
            df = df[["id_release", "position", "title", "duration", "dt_loaded"]]
            self.db.store_append(df=df, name_table=target_table)

    def extract_track_artists(self, target_table: str) -> None:
        """Extracts and stores artists contributing to master release tracks.

        This method retrieves artists associated with each track of the master release,
        including their name, role, ID, and resource URL, and stores them in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        lst_artists = []
        for track in self.obj_discogs.tracklist:
            if "extraartists" in track.data:
                artists = track.data["extraartists"]
                artists = [dict(item, position=track.data["position"]) for item in artists]
                artists = [dict(item, id_release=self.obj_discogs.id) for item in artists]
                artists = [dict(item, dt_loaded=dt.datetime.now()) for item in artists]
                lst_artists = lst_artists + artists
        if len(lst_artists) > 0:
            df = pl.DataFrame(lst_artists)
            df = df[["id_release", "name", "role", "id", "resource_url", "position", "dt_loaded"]]
            df = df.rename(
                {
                    "name": "name_artist",
                    "id": "id_artist",
                    "resource_url": "api_artist",
                }
            )
            self.db.store_append(df=df, name_table=target_table)

    def extract_videos(self, target_table: str) -> None:
        """Extracts and stores master release videos.

        This method retrieves video data associated with the master release,
        including video URL, title, duration, and stores them in the specified table.

        Args:
            target_table (str): The name of the table to store the data in.
        """
        lst_videos = []
        for video in self.obj_discogs.videos:
            dict_video = video.data
            dict_video.update({"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()})
            lst_videos.append(dict_video)
        if lst_videos:
            df = pl.DataFrame(lst_videos)
            df = df[["id_release", "uri", "title", "duration", "dt_loaded"]]
            df = df.rename({"uri": "url_video"})
            self.db.store_append(df=df, name_table=target_table)

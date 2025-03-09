import datetime as dt

import polars as pl
from celery import Celery
from discogs_client import models

from log_config import logging

from .extractor import DiscogsETL

logger = logging.getLogger(__name__)


class ETLMaster(DiscogsETL):
    def __init__(self, release: models.Release, file_db: str, app_celery: Celery) -> None:
        super().__init__(file_db, app_celery=app_celery)
        self.obj_discogs = release

    def process(self) -> None:
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
        """_summary_

        Args:
            target_table (str): Table where result is stored
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
        """_summary_

        Args:
            target_table (str): Table where result is stored
        """
        lst_styles = []
        if self.obj_discogs.styles is not None:
            for style in self.obj_discogs.styles:
                lst_styles.append(
                    {
                        "id_release": self.obj_discogs.id,
                        "name_style": style,
                        "dt_loaded": dt.datetime.now(),
                    }
                )
            if len(lst_styles) > 0:
                df = pl.DataFrame(lst_styles)
                self.db.store_append(df=df, name_table=target_table)

    def extract_genres(self, target_table: str) -> None:
        """_summary_

        Args:
            target_table (str): Table where result is stored
        """
        lst_genres = []
        for genre in self.obj_discogs.genres:
            lst_genres.append(
                {
                    "id_release": self.obj_discogs.id,
                    "name_genre": genre,
                    "dt_loaded": dt.datetime.now(),
                }
            )
        if len(lst_genres) > 0:
            df = pl.DataFrame(lst_genres)
            self.db.store_append(df=df, name_table=target_table)

    def extract_tracks(self, target_table: str) -> None:
        """_summary_

        Args:
            target_table (str): Table where result is stored
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
        if len(lst_tracks) > 0:
            df = pl.DataFrame(lst_tracks)
            df = df[["id_release", "position", "title", "duration", "dt_loaded"]]
            self.db.store_append(df=df, name_table=target_table)

    def extract_track_artists(self, target_table: str) -> None:
        """_summary_

        Args:
            target_table (str): Table where result is stored
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
        """_summary_

        Args:
            target_table (str): Table where result is stored
        """
        lst_videos = []
        for video in self.obj_discogs.videos:
            dict_video = video.data
            dict_video.update({"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()})
            lst_videos.append(dict_video)
        if len(lst_videos) > 0:
            df = pl.DataFrame(lst_videos)
            df = df[["id_release", "uri", "title", "duration", "dt_loaded"]]
            df = df.rename({"uri": "url_video"})
            self.db.store_append(df=df, name_table=target_table)

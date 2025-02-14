import datetime as dt
from json.decoder import JSONDecodeError

import polars as pl
from tqdm import tqdm
from discogs_client import Client
from discogs_client import models
from discogs_client.exceptions import HTTPError

from db_operations import DBStorage
from log_config import logging

logger = logging.getLogger(__name__)


class DiscogsETL:
    """A virtual class for extracting, processing and storing a user's collection data from Discogs"""

    def __init__(self, file_db: str) -> None:
        self.file_db = file_db
        self.db = DBStorage(file_db=file_db)


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
                    "time_value_retrieved": dt.datetime.now(),
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


class ETLMaster(DiscogsETL):
    def __init__(self, release: models.Release, file_db: str) -> None:
        super().__init__(file_db)
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
            logger.info(
                f"Already extract master info for '{self.obj_discogs.title}', skipped"
            )

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
            df = df[["position", "title", "duration"]]
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
                artists = [
                    dict(item, position=track.data["position"]) for item in artists
                ]
                artists = [
                    dict(item, id_release=self.obj_discogs.id) for item in artists
                ]
                artists = [dict(item, dt_loaded=dt.datetime.now()) for item in artists]
                lst_artists = lst_artists + artists
        if len(lst_artists) > 0:
            df = pl.DataFrame(lst_artists)
            df = df[["id_release", "name", "role", "id", "resource_url", "position"]]
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
            dict_video.update(
                {"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()}
            )
            lst_videos.append(dict_video)
        if len(lst_videos) > 0:
            df = pl.DataFrame(lst_videos)
            df = df[["uri", "title", "duration"]]
            df = df.rename({"uri": "url_video"})
            self.db.store_append(df=df, name_table=target_table)


class ETLRelease(ETLMaster):
    """A class that processes release related data"""

    def __init__(self, release: models.Release, file_db: str) -> None:
        super().__init__(release=release, file_db=file_db)
        self.obj_discogs = release
        self.db = DBStorage(file_db=file_db)
        self.artist_etl = ETLArtist(artists=self.obj_discogs.artists, file_db=self.file_db)

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
        df_release = df_release.rename(
            {
                "id": "id_release",
                "master_id": "id_master",
                "cover_image": "url_cover",
                "thumb": "url_thumbnail",
                "uri": "url_release",
            }
        )
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
            df_labels = df_labels[["id_release", "id", "name", "catno"]]
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
            format.update(
                {"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()}
            )
            lst_formats.append(format)
        if len(lst_formats) > 0:
            df_formats = pl.DataFrame(lst_formats)
            df_formats = df_formats[["id_release", "name", "qty"]]
            df_formats = df_formats.rename({"name": "name_format", "qty": "qty_format"})
            self.db.store_append(df=df_formats, name_table=target_table)

    def extract_credits(self, target_table: str) -> pl.DataFrame:
        lst_artists = []
        for artist in self.obj_discogs.credits:
            data = artist.data
            data.update(
                {"id_release": self.obj_discogs.id, "dt_loaded": dt.datetime.now()}
            )
            lst_artists.append(data)
        if len(lst_artists) > 0:
            df_artists = pl.DataFrame(lst_artists)
            df_artists = df_artists[["name", "role", "id", "resource_url"]]
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
        dict_stats = {
            "id_release": dict_marketplace["id"],
            "dt_loaded": dt.datetime.now(),
            "qty_for_sale": marketplace.num_for_sale,
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


class ETLArtist(DiscogsETL):
    """A class that processes artist related data"""

    def __init__(self, artists: models.Artist, file_db: str) -> None:
        super().__init__(file_db)
        self.obj_discogs = artists
        self.process_masters = True

    def process(self) -> None:
        for artist in tqdm(
            self.obj_discogs, total=len(self.obj_discogs), desc="Artists"
        ):
            exists = self.db.is_value_present(
                name_table="artist", name_column="id_artist", value=artist.id
            )
            if not exists:
                logger.info(f"Started processing data for artist '{artist.name}'")
                self.artist(artist=artist, target_table="artist")
                if self.process_masters:
                    self.masters(artist=artist, target_table="artist_masters")
                self.images(artist=artist, target_table="artist_images")
                self.aliases(artist=artist, target_table="artist_aliases")
                self.groups(artist=artist, target_table="artist_groups")
                self.members(artist=artist, target_table="artist_members")
                self.urls(artist=artist, target_table="artist_urls")
            else:
                logger.info(
                    f"Previously processed data for artist '{artist.name}', skipping"
                )

    def artist(self, artist: models.Artist, target_table: str) -> pl.DataFrame:
        df = pl.DataFrame([{"id_artist": artist.id, "name_artist": artist.name}])
        self.db.store_append(df=df, name_table=target_table)

    def masters(self, artist: models.Artist, target_table: str) -> None:
        lst_masters = []
        try:
            qty_pages = artist.releases.pages
        except HTTPError:
            logger.error(f"Could not find artist '{artist.name}' on Discogs")
            return
        except JSONDecodeError:
            logger.error(f"Couldn't process response for masters of artist '{artist.name}'")
            return
        for page_no in tqdm(
            range(1, qty_pages),
            total=qty_pages - 1,
            desc=artist.name + " - Masters",
        ):
            try:
                page = artist.releases.page(page_no)
            except JSONDecodeError:
                logger.error(f"Couldn't process response for masters of artist '{artist.name}'")
                return
            lst_masters = lst_masters + [master.data for master in page]
        lst_masters = [dict(item, id_artist=artist.id) for item in lst_masters]
        lst_masters = [dict(item, dt_loaded=dt.datetime.now()) for item in lst_masters]
        df = pl.DataFrame(lst_masters)
        if len(lst_masters) > 0:
            df = df[
                [
                    "id_artist",
                    "id",
                    "title",
                    "type",
                    "main_release",
                    "artist",
                    "role",
                    "year",
                    "thumb",
                ]
            ]
            df = df.rename(
                {
                    "id": "id_master",
                    "main_release": "id_main_release",
                    "artist": "name_artist",
                    "thumb": "url_thumb",
                }
            )
            self.db.store_append(df=df, name_table=target_table)

    def images(self, artist: models.Artist, target_table: str) -> None:
        lst_images = []
        try:
            if artist.images is None:
                logger.warning(f"No images found for artist '{artist.name}'")
                return
        except HTTPError:
            logger.warning(f"Artist not found '{artist.name}'")
            return
        for image in artist.images:
            image.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
            lst_images.append(image)
        if len(lst_images) > 0:
            df = pl.DataFrame(lst_images)
            df = df[["id_artist", "type", "uri", "uri150", "width", "height"]]
            df = df.rename(
                {
                    "uri": "url_image",
                    "uri150": "url_image_150",
                    "width": "width_image",
                    "height": "height_image",
                }
            )
            self.db.store_append(df=df, name_table=target_table)

    def groups(self, artist: models.Artist, target_table: str) -> None:
        lst_groups = []
        for group in artist.groups:
            data = group.data
            data.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
            lst_groups.append(data)
        if len(lst_groups) > 0:
            df = pl.DataFrame(lst_groups)
            df = df.rename(
                {
                    "id": "id_group",
                    "name": "name_group",
                    "resource_url": "api_group",
                    "active": "is_active",
                    "thumbnail_url": "url_thumbnail",
                }
            )
            self.db.store_append(df=df, name_table=target_table)

    def aliases(self, artist: models.Artist, target_table: str) -> None:
        lst_aliases = []
        for alias in artist.aliases:
            data = alias.data
            data.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
            lst_aliases.append(data)
        if len(lst_aliases) > 0:
            df = pl.DataFrame(lst_aliases)
            df = df.rename(
                {
                    "id": "id_alias",
                    "name": "name_alias",
                    "resource_url": "api_alias",
                    "thumbnail_url": "url_thumbnail",
                }
            )
            self.db.store_append(df=df, name_table=target_table)

    def members(self, artist: models.Artist, target_table: str) -> None:
        lst_members = []
        for member in artist.members:
            data = member.data
            data.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
            lst_members.append(data)
        if len(lst_members) > 0:
            df = pl.DataFrame(lst_members)
            dict_rename = {
                "id": "id_member",
                "name": "name_member",
                "resource_url": "api_member",
                "active": "is_active",
            }
            if "thumbnail_url" in df.columns:
                dict_rename.update({"thumbnail_url": "url_thumbnail"})
            df = df.rename(dict_rename)
            self.db.store_append(df=df, name_table=target_table)

    def urls(self, artist: models.Artist, target_table: str) -> None:
        lst_urls = []
        if artist.urls is None:
            logger.warning(f"None URLs found for {artist.name}")
            return
        for url in artist.urls:
            lst_urls.append(
                {"id_artist": artist.id, "url": url, "dt_loaded": dt.datetime.now()}
            )
        if len(lst_urls) > 0:
            df = pl.DataFrame(lst_urls)
            self.db.store_append(df=df, name_table=target_table)

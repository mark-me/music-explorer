import datetime as dt
from json.decoder import JSONDecodeError

import polars as pl
from discogs_client import models
from discogs_client.exceptions import HTTPError
from tqdm import tqdm

from log_config import logging

from .extractor import DiscogsETL

logger = logging.getLogger(__name__)


class ETLArtist(DiscogsETL):
    """A class that processes artist related data"""

    def __init__(self, artists: models.Artist, file_db: str) -> None:
        super().__init__(file_db)
        self.obj_discogs = artists
        self.process_masters = True

    def process(self) -> None:
        for artist in self.obj_discogs:
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
                logger.info(f"Previously processed data for artist '{artist.name}', skipping")

    def artist(self, artist: models.Artist, target_table: str) -> pl.DataFrame:
        try:
            profile = artist.profile
        except HTTPError:
            logger.error(f"Could not find artist '{artist.name}' on Discogs")
            return
        df = pl.DataFrame(
            [{"id_artist": artist.id, "name_artist": artist.name, "profile": profile}]
        )
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
                    "dt_loaded",
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
            df = df[["id_artist", "type", "uri", "uri150", "width", "height", "dt_loaded"]]
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
        try:
            for group in artist.groups:
                data = group.data
                data.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
                lst_groups.append(data)
        except HTTPError:
            logger.warning(f"Group not found for '{artist.name}'")
            return
        if len(lst_groups) > 0:
            df = pl.DataFrame(lst_groups)
            dict_rename = {
                "id": "id_group",
                "name": "name_group",
                "resource_url": "api_group",
                "active": "is_active",
            }
            if "thumbnail_url" in df.columns:
                dict_rename.update({"thumbnail_url": "url_thumbnail"})
            df = df.rename(dict_rename)
            self.db.store_append(df=df, name_table=target_table)

    def aliases(self, artist: models.Artist, target_table: str) -> None:
        lst_aliases = []
        try:
            for alias in artist.aliases:
                data = alias.data
                data.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
                lst_aliases.append(data)
        except HTTPError:
            logger.error(f"Could not find artist {artist.name} for aliases.")
            return
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
        try:
            for member in artist.members:
                data = member.data
                data.update({"id_artist": artist.id, "dt_loaded": dt.datetime.now()})
                lst_members.append(data)
        except HTTPError:
            logger.error(f"Could not find artist {artist.name} for members.")
            return
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
        try:
            if artist.urls is None:
                logger.warning(f"None URLs found for {artist.name}")
                return
        except HTTPError:
            logger.error(f"Could not find artist {artist.name} for urls.")
            return
        for url in artist.urls:
            lst_urls.append({"id_artist": artist.id, "url": url, "dt_loaded": dt.datetime.now()})
        if len(lst_urls) > 0:
            df = pl.DataFrame(lst_urls)
            self.db.store_append(df=df, name_table=target_table)

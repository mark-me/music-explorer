from db_operations import DBStorage


class Releases(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)

    def get_release(self, id_release: int) -> dict:
        release = Release(id_release=id_release, file_db=self.file_db)
        dict_release = release.data()
        return dict_release


class Release(DBStorage):
    def __init__(self, id_release: int, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.id_release = id_release

    def data(self) -> dict:
        dict_release = {}
        dict_release = self.release()
        dict_release.update(
            {
                "artists": self.artists(),
                "videos": self.videos(),
                "tracks": self.tracks(),
                "formats": self.formats(),
                "labels": self.labels(),
                "genres": self.genres(),
                "styles": self.styles(),
            }
        )
        return dict_release

    def release(self) -> dict:
        sql = f"""
            SELECT
                r.id_release
                ,r.title
                ,r.year
                ,r.country
            FROM collection.main.release r
            WHERE r.id_release = {self.id_release}
        """
        dict_release = self.read_sql(sql=sql).to_dicts()[0]
        return dict_release

    def artists(self) -> list:
        sql = f"""
        SELECT
            a.name_artist
            ,a.profile
            ,ai.url_image
            ,ai.url_image_150
            ,ai.width_image
        FROM collection.main.release_artists ra
        LEFT JOIN collection.main.artist a
            ON a.id_artist = ra.id_artist
        LEFT JOIN collection.main.artist_images as ai
            ON ai.id_artist = a.id_artist
        WHERE
            ( img.type = 'primary' OR img.type IS NULL )
            AND ra.id_release = {self.id_release}
        """
        lst_artists = self.read_sql(sql=sql).to_dicts()
        return lst_artists

    def videos(self) -> list:
        sql = f"""
            SELECT
                rv.url_video
                ,rv.title
            FROM release_videos rv
            WHERE id_release={self.id_release}"""
        lst_videos = self.read_sql(sql=sql).to_dicts()
        return lst_videos

    def tracks(self) -> list:
        sql = f"""
            SELECT
                rt.position
                ,rt.title
                ,rt.duration
            FROM collection.main.release_tracks rt
            WHERE id_release={self.id_release}"""
        lst_tracks = self.read_sql(sql=sql).to_dicts()
        return lst_tracks

    def formats(self) -> list:
        sql = f"""
            SELECT
                rf.name_format,
                ,rf.qty_format
            FROM collection.main.release_formats rf
            WHERE id_release={self.id_release}"""
        lst_formats = self.read_sql(sql=sql).to_dicts()
        return lst_formats

    def genres(self) -> list:
        sql = f"""
            SELECT
                rg.name_genre
            FROM collection.main.release_genres rg
            WHERE id_release={self.id_release}"""
        lst_genres = self.read_sql(sql=sql).to_dicts()
        return lst_genres

    def styles(self) -> list:
        sql = f"""
            SELECT
                rg.name_style
            FROM collection.main.release_styles rs
            WHERE id_release={self.id_release}"""
        lst_styles = self.read_sql(sql=sql).to_dicts()
        return lst_styles

    def labels(self) -> list:
        sql = f"""
            SELECT
                rl.name_label
            FROM collection.main.release_labels rl
            WHERE id_release={self.id_release}"""
        lst_labels = self.read_sql(sql=sql).to_dicts()
        return lst_labels
import polars as pl

from db_operations import DBStorage


class Collection(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                ci.id_release,
                ci.title,
                ra.id_artist,
                a.name_artist,
                ci.url_thumbnail,
                ci.url_cover,
                ci.year_released,
                rf.name_format,
                ci.id_master
            FROM collection.main.collection_items ci
            LEFT JOIN collection.main.release_formats rf
            ON rf.id_release = ci.id_release
            LEFT JOIN collection.main.release_artists ra
            ON ra.id_release = ci.id_release
            LEFT JOIN collection.main.artist a
            ON a.id_artist = ra.id_artist
            WHERE ( rf.name_format = 'Vinyl' or rf.name_format IS NULL )
        """
        self.sql_tracks = """
        """

    def all(self) -> list:
        sql = self.sql_all + " ORDER BY ci.title"
        lst_dicts = self.read_sql(sql=sql).to_dicts()
        return lst_dicts

    def all_top_10(self) -> list:
        sql = self.sql_all + " ORDER BY ci.title LIMIT 10"
        lst_dicts = self.read_sql(sql=sql).to_dicts()
        return lst_dicts

    def random(self, qty_sample: int = 20) -> list:
        df = self.read_sql(sql=self.sql_all)
        lst_dicts = df.sample(n=qty_sample).to_dicts()
        return lst_dicts

    def artist(self, id_artist: str) -> list:
        sql = self.sql_all + f" AND a.id_artist={id_artist} ORDER BY ci.year_released"
        df_releases = self.read_sql(sql=sql)
        lst_release_id = pl.Series(df_releases.select("id_release")).to_list()
        lst_dicts = df_releases.to_dicts()

        # Retrieve tracks
        lst_release_id = [str(i) for i in lst_release_id]
        str_release_ids = ", ".join(lst_release_id)
        sql = f"""
        SELECT
            rt.id_release,
            rt.position,
            rt.title
        FROM release_tracks rt
        WHERE rt.id_release IN ({str_release_ids} )
        """
        lst_tracks = self.read_sql(sql=sql).to_dicts()
        dict_tracks = {}
        for track in lst_tracks:
            id_release = str(track["id_release"])
            if id_release not in dict_tracks:
                dict_tracks.update(
                    {id_release: [{"position": track["position"], "title": track["title"]}]}
                )
            else:
                dict_tracks[id_release].append(
                    {"position": track["position"], "title": track["title"]}
                )

        # Embed tracks in releases
        for i, release in enumerate(lst_dicts):
            lst_dicts[i].update({"tracks": dict_tracks[str(release["id_release"])]})

        return lst_dicts

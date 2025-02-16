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
            FROM test.main.collection_items ci
            LEFT JOIN test.main.release_formats rf
            ON rf.id_release = ci.id_release
            LEFT JOIN test.main.release_artists ra
            ON ra.id_release = ci.id_release
            LEFT JOIN test.main.artist a
            ON a.id_artist = ra.id_artist
            WHERE ( rf.name_format = 'Vinyl' or rf.name_format IS NULL )
        """

    def all(self) -> pl.DataFrame:
        sql = self.sql_all + " ORDER BY ci.title"
        df = self.read_sql(sql=sql)
        return df

    def all_top_10(self) -> pl.DataFrame:
        sql = self.sql_all + " ORDER BY ci.title LIMIT 10"
        df = self.read_sql(sql=sql)
        return df

    def random(self, qty_sample: int = 20) -> pl.DataFrame:
        df = self.all()
        df = df.sample(n=qty_sample)
        return df

    def artist(self, id_artist: str) -> pl.DataFrame:
        sql = self.sql_all + f" AND a.id_artist={id_artist} ORDER BY ci.year_released"
        df = self.read_sql(sql=sql)
        #lst_items = self.read_sql(sql=sql).to_dicts()
        #for i, item in enumerate(lst_items):

        return df
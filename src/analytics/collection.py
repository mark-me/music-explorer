import polars as pl

from db_operations import DBStorage

class Collection(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                c.id_release,
                c.title,
                m.name_artist,
                c.url_thumbnail,
                c.url_cover,
                c.year_released,
                c.id_master
            FROM test.main.collection_items c
            LEFT JOIN test.main.artist_masters	m
            ON m.id_master = c.id_master
        """

    def all(self) -> pl.DataFrame:
        df = self.read_sql(sql=self.sql_all)
        return df

    def random(self, qty_sample: int = 20) -> pl.DataFrame:
        df = self.all()
        df = df.sample(n=qty_sample)
        return df

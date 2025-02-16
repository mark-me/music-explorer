import polars as pl
import duckdb

from db_operations import DBStorage


class Artists(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                a.id_artist,
                a.name_artist,
                img.url_image,
                img.url_image_150,
                img.width_image,
                COUNT(DISTINCT ci.id_instance) AS qty_collection_items
            FROM test.main.artist a
            LEFT JOIN test.main.artist_images as img
            ON img.id_artist = a.id_artist
            LEFT JOIN test.main.release_artists ra
            ON ra.id_artist = a.id_artist
            INNER JOIN test.main.collection_items ci
            ON ci.id_release = ra.id_release
            WHERE ( img.type = 'primary' OR img.type IS NULL )
            GROUP BY
                a.id_artist,
                a.name_artist,
                img.url_image,
                img.url_image_150,
                img.width_image
        """

    def artist(self, id_artist: int) -> pl.DataFrame:
        df = self.read_sql(sql=self.sql_all)
        df = df.filter(pl.col("id_artist") == id_artist)
        return df

    def all(self) -> pl.DataFrame:
        sql = self.sql_all + " ORDER BY UPPER(a.name_artist)"
        df = self.read_sql(sql=sql)
        return df

    def all_top_10(self) -> pl.DataFrame:
        sql = self.sql_all + " ORDER BY UPPER(a.name_artist) LIMIT 10"
        df = self.read_sql(sql=sql)
        return df

    def top_collected(self) -> pl.DataFrame:
        df = self.all()
        df.sort("qty_collection_items", descending=True)
        return df

    def random(self, qty_sample: int = 20) -> pl.DataFrame:
        df = self.all()
        df = df.sample(n=qty_sample)
        return df


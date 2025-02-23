import polars as pl

from db_operations import DBStorage


class Artists(DBStorage):
    def __init__(self, file_db, schema="main"):
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                a.id_artist,
                a.name_artist,
                a.profile,
                img.url_image,
                img.url_image_150,
                img.width_image,
                COUNT(DISTINCT rf.id_release) AS qty_collection_items
            FROM collection.main.artist a
            LEFT JOIN collection.main.artist_images as img
            ON img.id_artist = a.id_artist
            LEFT JOIN collection.main.release_artists ra
            ON ra.id_artist = a.id_artist
            INNER JOIN collection.main.collection_items ci
            ON ci.id_release = ra.id_release
            LEFT JOIN collection.main.release_formats rf
            ON rf.id_release = ci.id_release
            WHERE ( img.type = 'primary' OR img.type IS NULL )
            AND (rf.name_format = 'Vinyl' or rf.name_format IS NULL)
            GROUP BY
                a.id_artist,
                a.name_artist,
                a.profile,
                img.url_image,
                img.url_image_150,
                img.width_image
        """

    def artist(self, id_artist: int) -> dict:
        df = self.read_sql(sql=self.sql_all)
        artist = df.filter(pl.col("id_artist") == id_artist).to_dicts()[0]
        sql_urls = f"SELECT url FROM artist_urls WHERE id_artist={id_artist}"
        lst_urls = self.read_sql(sql=sql_urls).to_dicts()
        artist.update({"urls": lst_urls})
        return artist

    def all(self) -> list:
        sql = (
            self.sql_all
            + """
        ORDER BY
            LTRIM(
                LTRIM(
                    LTRIM(
                    UPPER(a.name_artist),
                    '.'),
                    ''''
                    ), '"'
                    )"""
        )
        lst_dicts = self.read_sql(sql=sql).to_dicts()
        return lst_dicts

    def all_top_10(self) -> list:
        sql = (
            self.sql_all
            + """
        ORDER BY
            LTRIM(
                LTRIM(
                    LTRIM(
                    UPPER(a.name_artist),
                    '.'),
                    ''''
                    ), '"'
                    ) LIMIT 10"""
        )
        lst_dicts = self.read_sql(sql=sql).to_dicts()
        return lst_dicts

    def top_collected(self) -> list:
        df = self.all()
        df.sort("qty_collection_items", descending=True)
        lst_dicts = df.to_dicts()
        return lst_dicts

    def random(self, qty_sample: int = 20) -> list:
        df = self.read_sql(sql=self.sql_all)
        lst_dicts = df.sample(n=qty_sample).to_dicts()
        return lst_dicts

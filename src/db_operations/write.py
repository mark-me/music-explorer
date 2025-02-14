import polars as pl
import sqlite3

from .db import DBStorage


class CollectionWriter(DBStorage):
    """A class for storing collection item data
    """
    def __init__(self, file_db) -> None:
        super().__init__(file_db)
        self.create_table_artist_write_attempts()

    def drop_tables(self) -> None:
        self.drop_table(name_table='collection_items')

    def create_views(self) -> None:
        db_con = sqlite3.connect(self.file_db)
        cursor = db_con.cursor()
        sql_file = open("loading/sql/views_collection.sql")
        sql_as_string = sql_file.read()
        cursor.executescript(sql_as_string)

    def create_table_artist_write_attempts(self) -> None:
        if not self.table_exists('artist_write_attempts'):
            db_con = sqlite3.connect(self.file_db)
            cursor = db_con.cursor()
            cursor.execute("CREATE TABLE artist_write_attempts ( id_artist INTEGER, qty_attempts INTEGER )")
            cursor.close()

    def artist_write_attempts(self, df_write_attempts: pl.DataFrame) -> None:
        if df_write_attempts.shape[0] > 0:
            self.store_replace(df=df_write_attempts, name_table='artist_write_attempts')

    def create_table(self, name_table: str) -> None:
        """Creates artist related tables in the database if they don't exist"""
        db_con = sqlite3.connect(self.file_db)
        cursor = db_con.cursor()
        if not self.table_exists(name_table=name_table):
            sql = ""
            if name_table == 'artist':
                sql = "CREATE TABLE artist(name_artist TEXT, id_artist INT, role TEXT);"
            elif name_table == 'artist_masters':
                sql = "CREATE TABLE artist_masters (id_artist INTEGER, id_master INTEGER, title TEXT, type TEXT, id_main_release INTEGER,\
                    name_artist TEXT, role TEXT, year INTEGER, url_thumb TEXT)"
            elif name_table == 'artist_aliases':
                sql = "CREATE TABLE artist_aliases (id_alias INTEGER, name_alias TEXT, api_alias TEXT, id_artist INTEGER,\
                    url_thumbnail TEXT);"
            elif name_table == 'artist_members':
                sql = "CREATE TABLE artist_members (id_member INTEGER, name_member TEXT, api_member TEXT, is_active INTEGER, id_artist INTEGER,\
                    url_thumbnail TEXT)"
            elif name_table == 'artist_groups':
                sql = "CREATE TABLE artist_groups (id_group INTEGER, name_group TEXT, api_group TEXT, is_active INTEGER, id_artist INTEGER,\
                    url_thumbnail TEXT)"
            elif name_table == 'artist_images':
                sql = "CREATE TABLE artist_images (id_artist INTEGER, type TEXT,\
                    url_image TEXT, url_image_150 TEXT, width_image INTEGER, height_image INTEGER)"
            elif name_table == 'artist_urls':
                sql = "CREATE TABLE artist_urls (url_artist TEXT, id_artist INTEGER)"
            cursor.execute(sql)

class ArtistNetworkWriter(DBStorage):
    def __init__(self, file_db) -> None:
        super().__init__(file_db)

    def vertices(self, df_vertices: pl.DataFrame) -> None:
        pass

    def edges(self, df_edges: pl.DataFrame) -> None:
        pass

    def community_hierarchy(self, df_hierarchy: pl.DataFrame) -> None:
        if df_hierarchy.shape[0] > 0:
            self.drop_table(name_table='artist_community_hierarchy')
            self.store_append(df=df_hierarchy, name_table='artist_community_hierarchy')
        pass

    def centrality(self, df_centrality: pl.DataFrame) -> None:
        pass

    def cluster_navigation(self, df_navigation: pl.DataFrame) -> None:
        pass

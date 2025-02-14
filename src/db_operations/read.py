import polars as pl

from .db import DBStorage

class Collection(DBStorage):
    def __init__(self, file_db) -> None:
        super().__init__(file_db)

    def get_artists(self, id_artist: str=None) -> pl.DataFrame:
        name_source = "vw_artists_qty_in_collection"
        df = pl.DataFrame()
        if id_artist is None:
            df = self.read_table(name_table=name_source)
        else:
            sql = f"SELECT * FROM {name_source} WHERE id_artist={id_artist}"
            df = self.read_sql(sql=sql)
        return df

    def get_releases(self, id_artist: str=None) -> list:
        name_source = "vw_artist_collection_releases"
        df = pl.DataFrame()
        if id_artist is None:
            df = self.read_table(name_table=name_source)
        else:
            sql = f"SELECT * FROM {name_source} WHERE id_artist={id_artist}"
            df = self.read_sql(sql=sql)
        return df

    def get_release_videos(self, id_release: int) -> list:
        name_source = "release_videos"
        sql = f"SELECT * FROM {name_source} WHERE id_release={id_release}"
        df = self.read_sql(sql=sql)
        return df

    def get_dendrogram_vertices(self, id_hierarchy: int) -> list:
        name_source = "community_dendrogram_vertices"
        sql = f"SELECT * FROM {name_source} WHERE id_hierarchy={id_hierarchy + 1}"
        df = self.read_sql(sql=sql)
        return df

    def get_dendrogram_edges(self, id_hierarchy: int) -> list:
        name_source = "community_dendrogram_edges"
        sql = f"SELECT * FROM {name_source} WHERE id_hierarchy={id_hierarchy}"
        df = self.read_sql(sql=sql)
        return df

    def get_spindler_random(self) -> list:
        name_source = "vw_spinder_artist"
        df = self.read_table(name_table=name_source)
        return df

    def get_spindler_artist(self, id_artist: str) -> list:
        name_source = "vw_spinder_artist"
        sql = f"SELECT * FROM {name_source} WHERE id_artist={id_artist}"
        df = self.read_sql(sql=sql)
        return df

    def create_view_artists_not_added(self) -> None:
        name_view = 'vw_artists_not_added'
        self.drop_view(name_view=name_view)
        sql_definition = "SELECT DISTINCT id_artist\
            FROM (\
                SELECT id_artist FROM artist_masters\
                WHERE role IN ('Main', 'Appearance', 'TrackAppearance')\
                UNION\
                    SELECT id_alias FROM artist_aliases\
                    UNION\
                        SELECT id_member FROM artist_members\
                        UNION\
                            SELECT id_group FROM artist_groups\
                                UNION\
                                    SELECT id_artist FROM release_artists )\
            WHERE id_artist NOT IN ( SELECT id_artist FROM artist ) AND\
					id_artist NOT IN ( SELECT id_artist FROM artist_ignore) AND\
                    id_artist NOT IN ( SELECT id_artist FROM artist_write_attempts WHERE qty_attempts > 1)"
        self.create_view(name_view=name_view, sql_definition=sql_definition)

    def get_artists_not_added(self) -> list:
        return self.read_table(name_table='vw_artists_not_added')

    def get_qty_artists_not_added(self) -> int:
        sql = "SELECT COUNT(*) FROM vw_artists_not_added;"
        df = self.read_sql(sql=sql)
        # FIXME: Select only one value
        return df

    def get_artists_write_attempts(self) -> list:
        return self.read_table(name_table='artist_write_attempts')


class Artists(DBStorage):
    def __init__(self, file_db) -> None:
        super().__init__(file_db)

    def get_artists(self) -> pl.DataFrame:
        sql = "SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist"
        df = self.read_sql(sql=sql)
        return df

    def get_vertices(self) -> pl.DataFrame:
        sql = "SELECT *, IIF(qty_collection_items > 0, 1, 0) AS in_collection FROM artist"
        df = self.read_sql(sql=sql)
        return df

    def get_edges(self) -> pl.DataFrame:
        return self.read_table(name_table='artist_relations')

    def get_community_hierarchy_edges(self) -> pl.DataFrame:
        return self.read_table(name_table='community_dendrogram_edges')

    def get_community_hierarchy_vertices(self) -> pl.DataFrame:
        return self.read_table(name_table='community_dendrogram_vertices')

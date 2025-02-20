import polars as pl

from db_operations import DBStorage


class DeriveArtist(DBStorage):
    def __init__(self, file_db) -> None:
        super().__init__(file_db)

    def process(self) -> None:
        self._artist_is_groups()
        self._artist_thumbnails()
        self._artist_qty_collection_items()
        self._artist_relationships()

    def _artist_is_groups(self) -> None:
        self.column_add(name_table="artist", name_column="is_group", type_data="INT")
        # sql = "CREATE INDEX IF NOT EXISTS idx_artist_members_id_artist ON artist_members (id_artist);"
        # self.execute_sql(sql=sql)
        sql = """
        UPDATE artist
        SET is_group = (SELECT 1 FROM artist_members WHERE id_artist = artist.id_artist LIMIT 1);
        """
        self.execute_sql(sql=sql)
        sql = """
        UPDATE artist
        SET is_group = 0
        WHERE is_group IS NULL;
        """
        self.execute_sql(sql=sql)

    def _artist_thumbnails(self) -> None:
        self.column_add(name_table="artist", name_column="url_thumbnail", type_data="VARCHAR")
        self.drop_table(name_table="thumbnails")
        sql_statement = """
        CREATE TABLE thumbnails AS
        SELECT id_alias AS id_artist, url_thumbnail FROM artist_aliases
        UNION
        SELECT id_member, url_thumbnail FROM artist_members
        UNION
        SELECT id_group, url_thumbnail FROM artist_groups;
        """
        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET url_thumbnail = ( SELECT url_thumbnail WHERE id_artist = artist.id_artist );
        """
        self.execute_sql(sql=sql_statement)

    def _artist_qty_collection_items(self) -> None:
        self.column_add(name_table="artist", name_column="qty_collection_items", type_data="INT")

        self.drop_table(name_table="qty_collection_items")
        sql_statement = """
        CREATE TABLE qty_collection_items AS
            SELECT release_artists.id_artist AS id_artist, COUNT(*) AS qty_items
            FROM collection_items
            INNER JOIN release_artists
                ON release_artists.id_release = collection_items.id_release
            GROUP BY release_artists.id_artist;
        """

        self.execute_sql(sql=sql_statement)
        sql_statement = """
        UPDATE artist
        SET qty_collection_items = (SELECT qty_items
                                    FROM qty_collection_items
                                    WHERE  qty_collection_items.id_artist = artist.id_artist);
        """
        self.execute_sql(sql=sql_statement)
        self.drop_table(name_table="qty_collection_items")

    def _artist_relationships(self) -> None:
        self.drop_table(name_table="artist_relations")
        sql_statement = """
        CREATE TABLE artist_relations AS
            SELECT DISTINCT id_artist_from,
                            id_artist_to,
                            relation_type
            FROM (  SELECT id_member AS id_artist_from,
                        id_artist as id_artist_to,
                        'group_member' as relation_type
                    FROM artist_members
                UNION
                    SELECT id_artist as id_artist_from,
                        id_group AS id_artist_to,
                        'group_member' as relation_type
                    FROM artist_groups
                UNION
                    SELECT a.id_alias,
                        a.id_artist,
                        'artist_alias'
                    FROM artist_aliases a
                    LEFT JOIN artist_aliases b
                        ON a.id_artist = b.id_alias AND
                            a.id_alias = b.id_artist
                    WHERE a.id_artist > b.id_artist OR
                        b.id_artist IS NULL
                UNION
                    SELECT a.id_artist,
                        b.id_artist,
                        'co_appearance'
                    FROM release_artists a
                    INNER JOIN release_artists b
                        ON b.id_release = a.id_release
                    WHERE a.id_artist != b.id_artist
                UNION
                    SELECT a.id_artist,
                        b.id_artist,
                        'release_role'
                    FROM release_artists a
                    INNER JOIN release_credits b
                        ON b.id_release = a.id_release
                    INNER JOIN role c
                        ON c.role = b.role
                    WHERE a.id_artist = b.id_artist AND
                        as_edge = 1
                )
            INNER JOIN artist a
                ON a.id_artist = id_artist_from
            INNER JOIN artist b
                ON b.id_artist = id_artist_to
            GROUP BY id_artist_from,
                id_artist_to,
                relation_type;
        """
        self.execute_sql(sql=sql_statement)

    def _get_artists_not_added(self) -> pl.DataFrame:
        df = pl.DataFrame()
        sql = """
            SELECT DISTINCT id_artist
            FROM (
                SELECT id_artist FROM artist_masters
                WHERE role IN ('Main', 'Appearance', 'TrackAppearance')
                    UNION
                SELECT id_alias FROM artist_aliases
                    UNION
                SELECT id_member FROM artist_members
                    UNION
                SELECT id_group FROM artist_groups
                    UNION
                SELECT id_artist FROM release_artists )
            WHERE id_artist NOT IN ( SELECT id_artist FROM artist )
                AND id_artist NOT IN ( SELECT id_artist FROM artist_ignore)
                AND id_artist NOT IN ( SELECT id_artist FROM artist_write_attempts WHERE qty_attempts > 1)
        """
        df = self.read_sql(sql=sql)
        return df

    def _get_qty_artists_not_added(self) -> int:
        sql = "SELECT COUNT(*) AS qty_artists_not_added FROM vw_artists_not_added;"
        qty = self.read_sql(sql=sql).item(0, 0)
        return qty

    def _get_artists_write_attempts(self) -> pl.DataFrame:
        return self.read_table(name_table='artist_write_attempts')
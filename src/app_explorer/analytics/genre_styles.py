from db_operations import DBStorage

class Genre_Styles(DBStorage):
    def __init__(self, file_db, schema = "main"):
        super().__init__(file_db, schema)

    def all(self):
        sql = """
            SELECT
                rg.name_genre,
                rs.name_style,
                COUNT(DISTINCT ci.id_release) AS qty_collection_items,
                COUNT(DISTINCT ra.id_artist) AS qty_collected_artists
            FROM collection_items ci
            INNER JOIN release_artists ra
            ON ra.id_release = ci.id_release
            LEFT JOIN release_genres rg
            ON rg.id_release = ci.id_release
            LEFT JOIN release_styles rs
            ON rs.id_release = ci.id_release
            GROUP BY
                rg.name_genre,
                rs.name_style
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        lst_results = self._add_nested_information(lst_results)
        return lst_results

    def artist_similarity(self, id_artist: int):
        if not self.table_exists("artist_similarity_genres"):
            sql_genres = """
                CREATE TABLE artist_similarity_genres AS
                SELECT a.id_artist,
                    b.id_artist,
                    SUM(CAST(a.name_genre = b.name_genre AS INT)) AS qty_similar,
                    COUNT(*) AS qty_all,
                    (qty_similar/qty_all) as similarity_jaccard
                FROM (
                            SELECT DISTINCT
                                ra.id_artist,
                                rg.name_genre
                            FROM collection_items ci
                            INNER JOIN release_artists ra
                            ON ra.id_release = ci.id_release
                            LEFT JOIN release_genres rg
                            ON rg.id_release = ci.id_release
                            LEFT JOIN release_styles rs
                            ON rs.id_release = ci.id_release
                ) AS a
                FULL JOIN (
                            SELECT DISTINCT
                                ra.id_artist,
                                rg.name_genre
                            FROM collection_items ci
                            INNER JOIN release_artists ra
                            ON ra.id_release = ci.id_release
                            LEFT JOIN release_genres rg
                            ON rg.id_release = ci.id_release
                            LEFT JOIN release_styles rs
                            ON rs.id_release = ci.id_release
                ) as b
                ON a.id_artist <> b.id_artist
                GROUP BY
                        a.id_artist,
                        b.id_artist
            """
            self.execute_sql(sql=sql_genres)

        if not self.table_exists("artist_similarity_styles"):
            sql_styles = """
                SELECT a.id_artist,
                    b.id_artist,
                    SUM(CAST(a.name_style = b.name_style AS INT)) AS qty_similar,
                    COUNT(*) AS qty_all,
                    (qty_similar/qty_all) as similarity_jaccard
                FROM (
                            SELECT DISTINCT
                                ra.id_artist,
                                rs.name_style
                            FROM collection_items ci
                            INNER JOIN release_artists ra
                            ON ra.id_release = ci.id_release
                            LEFT JOIN release_styles rs
                            ON rs.id_release = ci.id_release
                ) AS a
                FULL JOIN (
                            SELECT DISTINCT
                                ra.id_artist,
                                rs.name_style
                            FROM collection_items ci
                            INNER JOIN release_artists ra
                            ON ra.id_release = ci.id_release
                            LEFT JOIN release_styles rs
                            ON rs.id_release = ci.id_release
                ) as b
                ON a.id_artist <> b.id_artist
                GROUP BY
                        a.id_artist,
                        b.id_artist
            """
            self.execute_sql(sql=sql_styles)

        sql_combine = f"""
            SELECT
                asg.id_artist,
                asg.id_artist_1,
                asg.qty_all as qty_all_genres,
                asg.similarity_jaccard as similarity_genres,
                ass.qty_all as qty_all_styles,
                ass.similarity_jaccard as similarity_styles
            FROM artist_similarity_genres asg
            INNER JOIN artist_similarity_styles ass
            ON ass.id_artist = asg.id_artist
            AND ass.id_artist_1 = asg.id_artist_1
            WHERE
                asg.id_artist={id_artist}
        """
        lst_results = self.read_sql(sql=sql_combine).to_dicts
        return lst_results
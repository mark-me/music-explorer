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

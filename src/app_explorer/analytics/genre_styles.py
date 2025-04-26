from db_operations import DBStorage

class Genre_Styles(DBStorage):
    """Provides methods for accessing and analyzing genre and style data.

    This class interacts with the database to retrieve and process information about
    genres and styles associated with the user's music collection.
    """
    def __init__(self, file_db, schema = "main"):
        """Initializes the Genre_Styles class with database connection details.

        Args:
            file_db: Path to the database file.
            schema (str, optional): The database schema to use. Defaults to "main".
        """
        super().__init__(file_db, schema)

    def all(self):
        """Retrieves all genres and styles in the collection.

        This method retrieves all combinations of genres and styles present in the user's collection,
        along with the number of collection items and collected artists associated with each combination.

        Returns:
            list: A list of dictionaries, where each dictionary represents a genre-style combination
                and its associated counts.
        """
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

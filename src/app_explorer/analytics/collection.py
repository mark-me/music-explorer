import polars as pl

from db_operations import DBStorage


class Collection(DBStorage):
    """Provides methods for accessing and analyzing collection data.

    This class interacts with the database to retrieve and process information about
    the user's music collection, including items, artists, formats, genres, and styles.
    """
    def __init__(self, file_db, schema="main"):
        """Initializes the Collection class with database connection details.

        Args:
            file_db: Path to the database file.
            schema (str, optional): The database schema to use. Defaults to "main".
        """
        super().__init__(file_db, schema)
        self.sql_all = """
            SELECT
                ci.id_release,
                ci.title,
                ci.url_thumbnail,
                ci.url_cover,
                ci.year_released,
                ci.id_master
            FROM collection.main.collection_items ci
        """

    def all(self) -> list:
        """Retrieves all collection items.

        This method retrieves all items from the user's collection, ordered alphabetically by title,
        and includes nested information such as artists, formats, genres, and styles.

        Returns:
            list: A list of dictionaries, each representing a collection item.
        """
        sql = (
            self.sql_all
            + """
        ORDER BY
            LTRIM(
                LTRIM(
                    LTRIM(
                    UPPER(ci.title),
                    '.'),
                    ''''
                    ), '"'
                    )"""
        )
        lst_items = self.read_sql(sql=sql).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def all_top_10(self) -> list:
        """Retrieves the top 10 collection items.

        This method retrieves the first 10 items from the user's collection, ordered alphabetically by title,
        and includes nested information such as artists, formats, genres, and styles.

        Returns:
            list: A list of dictionaries, each representing a collection item.
        """
        sql = (
            self.sql_all
            + """
        ORDER BY
            REGEXP_REPLACE(UPPER(ci.title), '([.''"])', '')
        LIMIT 10"""
        )
        lst_items = self.read_sql(sql=sql).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def random(self, qty_sample: int = 20) -> list:
        """Retrieves a random sample of collection items.

        This method retrieves a random sample of items from the user's collection,
        including nested information such as artists, formats, genres, and styles.

        Args:
            qty_sample (int, optional): The number of items to sample. Defaults to 20.

        Returns:
            list: A list of dictionaries, each representing a collection item.
        """
        df = self.read_sql(sql=self.sql_all)
        lst_items = df.sample(n=qty_sample).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def search(self, text_search: str) -> list:
        """Searches for collection items by title.

        This method searches the user's collection for items with titles matching the given search text.
        The results are ordered alphabetically by title and include nested information such as artists,
        formats, genres, and styles.

        Args:
            text_search (str): The text to search for in the titles.

        Returns:
            list: A list of dictionaries, each representing a matching collection item.
        """
        sql = f"""
            SELECT
                ci.id_release,
                ci.title,
                ci.url_thumbnail,
                ci.url_cover,
                ci.year_released,
                ci.id_master
            FROM collection.main.collection_items ci
            WHERE ci.title ILIKE '%{text_search}%'
            ORDER BY
                REGEXP_REPLACE(UPPER(ci.title), '([.''"])', '')
        """
        lst_items = self.read_sql(sql=sql).to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)
        return lst_items

    def artist(self, id_artist: str) -> list:
        """Retrieves collection items by artist ID.

        This method retrieves all releases in the user's collection associated with a given artist ID.
        The results are ordered by release year and include nested information such as artists, formats,
        genres, styles, and tracks.

        Args:
            id_artist (str): The ID of the artist.

        Returns:
            list: A list of dictionaries, each representing a release in the collection by the specified artist.
        """
        sql = (
            self.sql_all
            + f"""
                INNER JOIN collection.main.release_artists ra
                ON ci.id_release = ra.id_release
                WHERE ra.id_artist={id_artist} ORDER BY ci.year_released
            """
        )
        df_releases = self.read_sql(sql=sql)
        lst_release_id = pl.Series(df_releases.select("id_release")).to_list()
        lst_items = df_releases.to_dicts()
        lst_items = self._add_nested_information(lst_items=lst_items)

        # Retrieve tracks
        lst_release_id = [str(i) for i in lst_release_id]
        str_release_ids = ", ".join(lst_release_id)
        sql = f"""
            SELECT
                rt.id_release,
                rt.position,
                rt.title
            FROM collection.main.release_tracks rt
            WHERE rt.id_release IN ({str_release_ids})
        """
        lst_tracks = self.read_sql(sql=sql).to_dicts()
        dict_tracks = {}
        for track in lst_tracks:
            id_release = str(track["id_release"])
            if id_release not in dict_tracks:
                dict_tracks[id_release] = [
                    {"position": track["position"], "title": track["title"]}
                ]
            else:
                dict_tracks[id_release].append(
                    {"position": track["position"], "title": track["title"]}
                )

        # Embed tracks in releases
        for i, release in enumerate(lst_items):
            lst_items[i].update({"tracks": dict_tracks[str(release["id_release"])]})

        return lst_items

    def formats(self) -> list:
        """Retrieves collection formats and their counts.

        This method retrieves the different formats present in the user's collection
        (e.g., Vinyl, CD, Cassette) along with the number of releases in each format.
        The results are ordered by count in descending order.

        Returns:
            list: A list of dictionaries, where each dictionary represents a format and its count.
        """
        sql = """
            SELECT
                rf.name_format,
                COUNT(*) as qty_collection_items
            FROM collection.main.collection_items ci
            INNER JOIN collection.main.release_formats rf
            ON rf.id_release = ci.id_release
            GROUP BY
                rf.name_format
            ORDER BY
                qty_collection_items DESC
        """
        lst_formats = self.read_sql(sql=sql).to_dicts()
        return lst_formats


    def _add_nested_information(self, lst_items: list) -> list:
        """Adds nested information to collection items.

        This method enriches collection items with related data such as artists, formats,
        genres, and styles, retrieved from the database.

        Args:
            lst_items (list): A list of collection item dictionaries.

        Returns:
            list: The updated list of collection item dictionaries with nested information.
        """
        str_release_ids = ", ".join([str(i["id_release"]) for i in lst_items])
        dict_artists = self._artists(str_release_ids=str_release_ids)
        dict_formats = self._formats(str_release_ids=str_release_ids)
        dict_genres = self._genres(str_release_ids=str_release_ids)
        dict_styles = self._styles(str_release_ids=str_release_ids)

        # Adding nested information
        for i, item in enumerate(lst_items):
            id_release = item["id_release"]
            if id_release in dict_artists:
                lst_items[i].update({"artists": dict_artists[id_release]})
            if id_release in dict_formats:
                lst_items[i].update({"formats": dict_formats[id_release]})
            if id_release in dict_genres:
                lst_items[i].update({"genres": dict_genres[id_release]})
            if id_release in dict_styles:
                lst_items[i].update({"styles": dict_styles[id_release]})
        return lst_items

    def _artists(self, str_release_ids: str) -> dict:
        """Retrieves artists associated with given release IDs.

        This method retrieves artist information for the specified release IDs, including
        artist ID and name, and organizes the results into a dictionary keyed by release ID.

        Args:
            str_release_ids (str): A comma-separated string of release IDs.

        Returns:
            dict: A dictionary where keys are release IDs and values are lists of artists
                associated with each release.
        """
        sql = f"""
            SELECT
                ra.id_release,
                ra.id_artist,
                a.name_artist,
            FROM collection.main.release_artists ra
            LEFT JOIN collection.main.artist a
            ON a.id_artist = ra.id_artist
            WHERE ra.id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results

    def _formats(self, str_release_ids: str) -> dict:
        """Retrieves formats associated with given release IDs.

        This method retrieves format information for the specified release IDs
        and organizes the results into a dictionary keyed by release ID.

        Args:
            str_release_ids (str): A comma-separated string of release IDs.

        Returns:
            dict: A dictionary where keys are release IDs and values are lists of
                formats associated with each release.
        """
        sql = f"""
            SELECT
                id_release,
                name_format
            FROM collection.main.release_formats
            WHERE id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results

    def _genres(self, str_release_ids: str) -> dict:
        """Retrieves genres associated with given release IDs.

        This method retrieves genre information for the specified release IDs
        and organizes the results into a dictionary keyed by release ID.

        Args:
            str_release_ids (str): A comma-separated string of release IDs.

        Returns:
            dict: A dictionary where keys are release IDs and values are lists of
                genres associated with each release.
        """
        sql = f"""
            SELECT
                ci.id_release,
                rg.name_genre
            FROM collection_items ci
            INNER JOIN release_genres rg
            ON rg.id_release = ci.id_release
            WHERE ci.id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results

    def _styles(self, str_release_ids: str) -> dict:
        """Retrieves styles associated with given release IDs.

        This method retrieves style information for the specified release IDs
        and organizes the results into a dictionary keyed by release ID.

        Args:
            str_release_ids (str): A comma-separated string of release IDs.

        Returns:
            dict: A dictionary where keys are release IDs and values are lists of
                styles associated with each release.
        """
        sql = f"""
            SELECT
                ci.id_release,
                rs.name_style
            FROM collection_items ci
            INNER JOIN release_styles rs
            ON rs.id_release = ci.id_release
            WHERE ci.id_release IN ({str_release_ids})
        """
        lst_results = self.read_sql(sql=sql).to_dicts()
        dict_results = self._dicts_to_dict(key_field="id_release", lst_dicts=lst_results)
        return dict_results
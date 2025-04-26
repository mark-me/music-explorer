from db_operations import DBStorage


class Releases(DBStorage):
    """Provides methods for accessing release data.

    This class interacts with the database to retrieve information about
    music releases, including details and associated artists, videos, tracks,
    formats, labels, genres, and styles.
    """
    def __init__(self, file_db, schema="main"):
        """Initializes Releases with database connection details.

        Args:
            file_db: Path to the database file.
            schema (str, optional): The database schema to use. Defaults to "main".
        """
        super().__init__(file_db, schema)

    def get_release(self, id_release: int) -> dict:
        """Retrieves a specific release by ID.

        This method retrieves detailed information about a single release from the database,
        including its artists, videos, tracks, formats, labels, genres, and styles.

        Args:
            id_release (int): The ID of the release to retrieve.

        Returns:
            dict: A dictionary containing the release information.
        """
        release = Release(id_release=id_release, file_db=self.file_db)
        dict_release = release.data()
        return dict_release


class Release(DBStorage):
    """Provides methods for accessing data about a specific release.

    This class interacts with the database to retrieve detailed information about
    a given music release, including its artists, videos, tracks, formats, labels,
    genres, and styles.
    """
    def __init__(self, id_release: int, file_db, schema="main"):
        """Initializes Release with database connection details and release ID.

        Args:
            id_release (int): The ID of the release.
            file_db: Path to the database file.
            schema (str, optional): The database schema to use. Defaults to "main".
        """
        super().__init__(file_db, schema)
        self.id_release = id_release

    def data(self) -> dict:
        """Retrieves comprehensive data for the release.

        This method retrieves all details associated with the release, including
        basic information, artists, videos, tracks, formats, labels, genres, and styles.

        Returns:
            dict: A dictionary containing the complete release data.
        """
        dict_release = {}
        dict_release = self.release()
        dict_release.update(
            {
                "artists": self.artists(),
                "videos": self.videos(),
                "tracks": self.tracks(),
                "formats": self.formats(),
                "labels": self.labels(),
                "genres": self.genres(),
                "styles": self.styles(),
            }
        )
        return dict_release

    def release(self) -> dict:
        """Retrieves basic information for the release.

        This method retrieves core details about the release, such as title, year,
        thumbnail URL, cover URL, and country, from the 'release' table.

        Returns:
            dict: A dictionary containing the basic release information.
        """
        sql = f"""
            SELECT
                r.id_release
                ,r.title
                ,r.url_thumbnail
                ,r.url_cover
                ,r.year
                ,r.country
            FROM collection.main.release r
            WHERE r.id_release = {self.id_release}
        """
        dict_release = self.read_sql(sql=sql).to_dicts()[0]
        return dict_release

    def artists(self) -> list:
        """Retrieves artists associated with the release.

        This method retrieves information about artists involved in the release,
        including their ID, name, profile, image URL, and image dimensions.

        Returns:
            list: A list of dictionaries, where each dictionary represents an artist
                associated with the release.
        """
        sql = f"""
        SELECT
            ra.id_artist
            ,a.name_artist
            ,a.profile
            ,ai.url_image
            ,ai.url_image_150
            ,ai.width_image
        FROM collection.main.release_artists ra
        LEFT JOIN collection.main.artist a
            ON a.id_artist = ra.id_artist
        LEFT JOIN collection.main.artist_images as ai
            ON ai.id_artist = a.id_artist
        WHERE
            ( ai.type = 'primary' OR ai.type IS NULL )
            AND ra.id_release = {self.id_release}
        """
        lst_artists = self.read_sql(sql=sql).to_dicts()
        return lst_artists

    def videos(self) -> list:
        """Retrieves videos associated with the release.

        This method retrieves video data for the release, including video URL and title.

        Returns:
            list: A list of dictionaries, where each dictionary represents a video
                associated with the release.
        """
        sql = f"""
            SELECT
                rv.url_video
                ,rv.title
            FROM release_videos rv
            WHERE id_release={self.id_release}"""
        lst_videos = self.read_sql(sql=sql).to_dicts()
        return lst_videos

    def tracks(self) -> list:
        """Retrieves tracks associated with the release.

        This method retrieves track information for the release, including track position, title, and duration.

        Returns:
            list: A list of dictionaries, where each dictionary represents a track
                associated with the release.
        """
        sql = f"""
            SELECT
                rt.position
                ,rt.title
                ,rt.duration
            FROM collection.main.release_tracks rt
            WHERE id_release={self.id_release}"""
        lst_tracks = self.read_sql(sql=sql).to_dicts()
        return lst_tracks

    def formats(self) -> list:
        """Retrieves formats associated with the release.

        This method retrieves the formats (e.g., Vinyl, CD) in which the release was issued,
        along with the quantity of each format.

        Returns:
            list: A list of dictionaries, where each dictionary represents a format and its quantity
                associated with the release.
        """
        sql = f"""
            SELECT
                rf.name_format
                ,rf.qty_format
            FROM collection.main.release_formats rf
            WHERE id_release={self.id_release}"""
        lst_formats = self.read_sql(sql=sql).to_dicts()
        return lst_formats

    def genres(self) -> list:
        """Retrieves genres associated with the release.

        This method retrieves the musical genres associated with the release.

        Returns:
            list: A list of dictionaries, where each dictionary represents a genre
                associated with the release.
        """
        sql = f"""
            SELECT
                rg.name_genre
            FROM collection.main.release_genres rg
            WHERE id_release={self.id_release}"""
        lst_genres = self.read_sql(sql=sql).to_dicts()
        return lst_genres

    def styles(self) -> list:
        """Retrieves styles associated with the release.

        This method retrieves the musical styles associated with the release.

        Returns:
            list: A list of dictionaries, where each dictionary represents a style
                associated with the release.
        """
        sql = f"""
            SELECT
                rs.name_style
            FROM collection.main.release_styles rs
            WHERE id_release={self.id_release}"""
        lst_styles = self.read_sql(sql=sql).to_dicts()
        return lst_styles

    def labels(self) -> list:
        """Retrieves labels associated with the release.

        This method retrieves the names of the record labels associated with the release.

        Returns:
            list: A list of dictionaries, where each dictionary represents a label
                associated with the release.
        """
        sql = f"""
            SELECT
                rl.name_label
            FROM collection.main.release_labels rl
            WHERE id_release={self.id_release}"""
        lst_labels = self.read_sql(sql=sql).to_dicts()
        return lst_labels
import polars as pl

from db_operations import DBStorage


class DeriveRelease(DBStorage):
    """Derives and stores release-related information.

    This class handles the derivation and storage of release data, including loading release roles
    and processing artist information derived from groups and memberships.
    """
    def __init__(self, file_db: str) -> None:
        """Initializes DeriveRelease with database information.

        This method sets up the database connection for release data derivation.

        Args:
            file_db (str): Path to the database file.
        """
        super().__init__(file_db)

    def process(self) -> None:
        """Processes release data derivations.

        This method loads release roles and processes artist information derived from groups and memberships.
        """
        self._load_release_roles()

    def _load_release_roles(self) -> None:
        """Loads release roles into the 'role' table.

        This method creates and populates the 'role' table with distinct roles from release credits,
        marking specific roles as edges in the artist collaboration network.
        """
        has_table = self.table_exists(name_table="role")
        if not has_table:
            sql_statement = """
            CREATE TABLE role AS
                SELECT DISTINCT
                    role,
                    0 AS as_edge
                FROM release_credits
            """
            self.execute_sql(sql=sql_statement)
        list_value_part = [
            "piano",
            "vocal",
            "perform",
            "bass",
            "viol",
            "drum",
            "keyboard",
            "guitar",
            "sax",
            "music",
            "written",
            "arrange",
            "lyric",
            "word",
            "compose",
            "song",
            "accordion",
            "chamberlin",
            "clarinet",
            "banjo",
            "band",
            "bongo",
            "bell",
            "bouzouki",
            "brass",
            "cello",
            "cavaquinho",
            "celest",
            "choir",
            "chorus",
            "handclap",
            "conduct",
            "conga",
            "percussion",
            "trumpet",
            "cornet",
            "djembe",
            "dobro",
            "organ",
            "electron",
            "horn",
            "fiddle",
            "flute",
            "recorder",
            "glocken",
            "gong",
            "guest",
            "vibra",
            "harmonium",
            "harmonica",
            "harp",
            "beatbox",
            "leader",
            "loop",
            "MC",
            "mellotron",
            "melod",
            "mixed",
            "oboe",
            "orchestra",
            "recorded",
            "remix",
            "saw",
            "score",
            "sitar",
            "strings",
            "synth",
            "tabla",
            "tambourine",
            "theremin",
            "timbales",
            "timpani",
            "whistle",
            "triangle",
            "trombone",
            "tuba",
            "vocoder",
            "voice",
            "phone",
            "woodwind",
        ]
        last_value = list_value_part.pop(-1)
        sql_start = "UPDATE role SET as_edge = 1 WHERE "
        sql_statement = (
            sql_start
            + "".join(
                f"""role LIKE "%{value_part}%" OR """ for value_part in list_value_part
            )
            + f"""role LIKE "%{last_value}%" """
        )
        self.execute_sql(sql=sql_statement)

    def artists_from_group_and_membership(self) -> None:
        """Process artist information derived from groups and memberships.

        This method retrieves artists not yet added to the database from related tables
        (groups, members, etc.), fetches their details from Discogs, and updates the database.
        It also tracks write attempts to prevent infinite loops for artists that cannot be retrieved.
        """
        self._extract_artist_to_ignore()
        qty_artists_not_added = self.read_sql(
            sql="SELECT COUNT(*) FROM vw_artists_not_added;"
        )  # db_reader.qty_artists_not_added()
        while qty_artists_not_added > 0:
            df_write_attempts = self.read_table(name_table="artist_write_attempts")
            df_artists_new = self.read_table(name_table="vw_artists_not_added")
            artists = []
            for index, row in df_artists_new.iterrows():
                artists.append(self.client_discogs.artist(id=row["id_artist"]))
                df_write_attempts = pl.concat(
                    [
                        df_write_attempts,
                        pl.DataFrame.from_records(
                            [{"id_artist": row["id_artist"], "qty_attempts": 1}]
                        ),
                    ]
                )

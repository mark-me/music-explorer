from .derive_artists import DeriveArtist
from .derive_artist_network import DeriveArtistNetwork
from .derive_release import DeriveRelease


class DiscogsDerive:
    """Orchestrates the derivation of Discogs data.

    This class initializes and runs derivation processes for artists and releases,
    generating derived data like artist statistics and release details.
    """
    def __init__(self, file_db: str) -> None:
        """Initializes DiscogsDerive with database information.

        This method sets up the derivation classes for artists, releases, and artist networks,
        preparing for data derivation processes.

        Args:
            file_db (str): The path to the database file.
        """
        self.artist = DeriveArtist(file_db=file_db)
        self.release = DeriveRelease(file_db=file_db)
        self.artist_network = DeriveArtistNetwork(file_db=file_db)

    def start(self):
        """Starts the derivation processes.

        This method triggers the data derivation for artists, releases, and artist networks.
        """
        self.artist.process()
        self.release.process()
        self.artist_network.process()

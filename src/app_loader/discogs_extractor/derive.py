from .derive_artists import DeriveArtist
from .derive_artist_network import DeriveArtistNetwork
from .derive_release import DeriveRelease


class DiscogsDerive:
    def __init__(self, file_db) -> None:
        self.artist = DeriveArtist(file_db=file_db)
        self.release = DeriveRelease(file_db=file_db)
        #self.artist_network = DeriveArtistNetwork(file_db=file_db)

    def start(self):
        self.artist.process()
        self.release.process()
        #self.artist_network.process()

from .derive_artists import DeriveArtist
from .derive_network import DeriveNetwork
from .derive_release import DeriveRelease


class DiscogsDerive:
    def __init__(self, file_db) -> None:
        self.artist = DeriveArtist(file_db=file_db)
        self.release = DeriveRelease(file_db=file_db)
        #self.network = DeriveNetwork(file_db=file_db)

    def start(self):
        self.artist.process()
        self.release.process()
        #self.network.process()

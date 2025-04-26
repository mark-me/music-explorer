import os
import shutil
import time

from .backup_cleaner import BackupCleaner

class ManageDB:
    """Manages database operations, including copying, replacing, and backing up.

    This class provides methods for creating copies of the database for loading data,
    replacing the original database with the loaded copy, and creating backups of the database.
    """
    def __init__(self, file_db: str) -> None:
        """Initializes ManageDB with the database file path.

        Args:
            file_db (str): The path to the database file.
        """
        self._db_path = file_db
        self._db_dir = os.path.dirname(file_db)
        filename, self.__db_ext = os.path.splitext(file_db)
        self._db_file = os.path.basename(filename)
        self._db_load = f'{filename}_load{self.__db_ext}'
        self._exists = os.path.exists(self._db_path)

    def create_load_copy(self) -> str:
        """Creates a copy of the database for loading data.

        Returns:
            str: The path to the newly created database copy.
        """
        if self._exists:
            shutil.copyfile(self._db_path, self._db_load)
        return self._db_load

    def replace_db(self) -> None:
        """Replaces the original database with the loaded copy.

        This method copies the contents of the load database to the original database path
        and then removes the load database file.
        """
        shutil.copyfile(self._db_load, self._db_path)
        os.remove(self._db_load)

    def create_backup(self) -> None:
        """Creates a backup of the database.

        This method creates a timestamped backup of the database file in a 'backup' directory
        within the database's directory. Old backups are cleaned up based on a retention policy.
        """
        if self._exists:
            dir_backup = f'{self._db_dir}/backup'
            if not os.path.exists(dir_backup):
                os.makedirs(dir_backup)
            file_backup = (
                f'{dir_backup}/{self._db_file}_'
                + time.strftime("%Y%m%d_%H%M%S")
                + '.db'
            )
            shutil.copyfile(self._db_path, file_backup)
            # TODO: Add retention policy
            #backup_cleaner = BackupCleaner(dir_backup=dir_backup)
            #backup_cleaner.clean_old_backups()



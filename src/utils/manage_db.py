import os
import shutil
import time
from pathlib import Path

class ManageDB:
    def __init__(self, file_db: str) -> None:
        self._db_path = file_db
        self._db_dir = os.path.dirname(file_db)
        filename, self.__db_ext = os.path.splitext(file_db)
        self._db_file = os.path.basename(filename)
        self._db_load = filename + '_load' +  self.__db_ext
        self._exists = os.path.exists(self._db_path)

    def create_load_copy(self) -> str:
        """Create a copy of the database for loading and return it's location"""
        if self._exists:
            shutil.copyfile(self._db_path, self._db_load)
        return self._db_load

    def replace_db(self) -> None:
        """Replace the database with the copy used for loading"""
        shutil.copyfile(self._db_load, self._db_path)
        os.remove(self._db_load)

    def create_backup(self) -> None:
        """Create a backup of the database"""
        if self._exists:
            dir_backup = self._db_dir + '/backup'
            if not os.path.exists(dir_backup):
                os.makedirs(dir_backup)
            file_backup = dir_backup + '/' + self._db_file + '_' + time.strftime("%Y%m%d_%H%M%S") + '.db'
            shutil.copyfile(self._db_path, file_backup)
            # TODO: Add retention policy



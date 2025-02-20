from db_operations import DBStorage

class Releases(DBStorage):
    def __init__(self, file_db, schema = "main"):
        super().__init__(file_db, schema)

    def get_release_videos(self, id_release: int) -> list:
        name_source = "release_videos"
        sql = f"SELECT * FROM {name_source} WHERE id_release={id_release}"
        lst_data = self.read_sql(sql=sql).to_dicts()
        return lst_data
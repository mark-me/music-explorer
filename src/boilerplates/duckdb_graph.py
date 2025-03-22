from db_operations import DBStorage


class DBStorageGraph(DBStorage):
    def __init__(self, file_db, schema = "main"):
        super().__init__(file_db, schema)
        self._install_extension()
        self._load_group_members()

    def _install_extension(self):
        sql = """
        INSTALL duckpgq FROM community;
        LOAD duckpgq;
        """
        self.execute_sql(sql)

    def _graph_exists(self, name_graph: str) -> bool:
        return self.is_value_present(name_table="__duckpgq_internal", name_column="property_graph", value=name_graph)

    def _load_group_members(self):
        if self._graph_exists(name_graph="snb"):
            return
        sql = """
        CREATE PROPERTY GRAPH snb
        VERTEX TABLES (
            artist
        )
        EDGE TABLES (
            artist_members  SOURCE
                                KEY (id_artist) REFERENCES artist (id_artist)
                            DESTINATION
                                KEY (id_member) REFERENCES artist (id_artist)
            LABEL member_of
        );
        """
        self.execute_sql(sql)

    def group_members(self):
        sql = """
        FROM GRAPH_TABLE (snb
            MATCH (a:artist)-[k:member_of]->(b:artist)
            COLUMNS (a.name_artist, b.name_artist)
        )
        """
        lst = self.read_sql(sql).to_dicts()
        return lst

    def connected(self):
        sql = """
        FROM GRAPH_TABLE (snb
            MATCH p = ANY SHORTEST (a:artist)-[k:member_of]->{1,3}(b:artist)
            COLUMNS (a.name_artist, b.name_artist, path_length(p))
        )
        """
        lst = self.read_sql(sql).to_dicts()
        return lst


if __name__ == "__main__":
    db = DBStorageGraph(file_db="/home/mark/Downloads/collection_graph.db")
    lst = db.group_members()
    lst = db.connected()
from collections import defaultdict
import polars as pl
from sqlalchemy import create_engine, text


class DBStorage:
    def __init__(self, file_db, schema: str = "main") -> None:
        self.file_db = file_db
        self.schema = schema
        self.engine = create_engine(f"duckdb:///{file_db}")

    def create_view(self, name_view: str, sql_definition: str) -> None:
        sql = text(f"CREATE VIEW {self.schema}.{name_view} AS {sql_definition};")
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def drop_view(self, name_view: str) -> None:
        sql = text(f"DROP VIEW IF EXISTS {self.schema}.{name_view};")
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def execute_sql(self, sql: str) -> None:
        sql = text(sql)
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def execute_sql_file(self, file_name: str) -> None:
        with open(file_name) as sql_file:
            sql = text(sql_file.read())
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def table_exists(self, name_table: str) -> bool:
        """Checks whether a table exists"""
        sql = f"SELECT count(name) AS is_present FROM sqlite_master WHERE type='table' AND name='{name_table}'"
        df = pl.read_database(sql, connection=self.engine.connect())
        does_exist = df.item(0, 0) > 0
        return does_exist

    def column_exists(self, name_table: str, name_column: str) -> bool:
        """Checks whether a table column exists"""
        exists = False
        sql = f"PRAGMA table_info({name_table})"
        series_names = pl.read_database(sql, connection=self.engine.connect())["name"]
        columns = series_names.to_list()
        exists = name_column in columns
        return exists

    def column_add(self, name_table: str, name_column: str, type_data: str) -> None:
        if not self.column_exists(name_table=name_table, name_column=name_column):
            sql = text(f"ALTER TABLE {name_table} ADD COLUMN {name_column} {type_data}")
            with self.engine.connect() as con:
                con.execute(sql)
                con.commit()

    def view_exists(self, name_view: str) -> bool:
        """Checks whether a view exists"""
        sql = f"SELECT count(name) FROM sqlite_master WHERE type='view' AND name='{name_view}'"
        df = pl.read_database(sql, connection=self.engine.connect())
        does_exist = not df.is_empty()
        return does_exist

    def drop_table(self, name_table: str) -> None:
        """Dropping a table"""
        if self.table_exists(name_table):
            sql = text(f"DROP TABLE {name_table};")
            with self.engine.connect() as con:
                con.execute(sql)
                con.commit()

    def store_replace(self, df: pl.DataFrame, name_table: str) -> None:
        """Storing data to a table"""
        df.write_database(
            table_name=name_table, con=self.engine.connect(), if_table_exists="replace"
        )

    def store_append(self, df: pl.DataFrame, name_table: str) -> None:
        df.write_database(
            table_name=name_table,
            connection=self.engine.connect(),
            if_table_exists="append",
        )

    def read_view(self, name_view: str) -> pl.DataFrame:
        return self.read_table(name_table=name_view)

    def read_table(self, name_table: str) -> pl.DataFrame:
        sql = "SELECT * FROM " + name_table
        df = pl.read_database(sql, connection=self.engine.connect())
        return df

    def read_sql(self, sql: str) -> pl.DataFrame:
        df = pl.read_database(sql, connection=self.engine.connect())
        return df

    def is_value_present(self, name_table: str, name_column: str, value: str):
        is_present = False
        if self.table_exists(name_table=name_table):
            sql = f"SELECT COUNT(*) AS qty_present FROM {name_table} WHERE {name_column}={value}"
            df = pl.read_database(sql, connection=self.engine.connect())
            is_present = df.item(0, 0) > 0
        return is_present

    def _dicts_to_dict(self, key_field: str, lst_dicts: list) -> dict:
        dict_results = defaultdict(list)
        for entry in lst_dicts:
            key_value = entry[key_field]
            del entry[key_field]
            dict_results[key_value].append(entry)
        # Convert defaultdict to a regular dict
        dict_results = dict(dict_results)
        return dict_results

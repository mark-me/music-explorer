from collections import defaultdict
import polars as pl
from sqlalchemy import create_engine, text


class DBStorage:
    """Provides methods for interacting with a DuckDB database.

    This class simplifies common database operations such as creating views, executing SQL queries,
    reading and writing data, checking for table/column existence, and managing database schemas.
    """
    def __init__(self, file_db, schema: str = "main") -> None:
        """Initializes a DBStorage instance.

        Args:
            file_db: The path to the DuckDB database file.
            schema (str, optional): The database schema to use. Defaults to "main".
        """
        self.file_db = file_db
        self.schema = schema
        self.engine = create_engine(f"duckdb:///{file_db}")

    def create_view(self, name_view: str, sql_definition: str) -> None:
        """Creates a database view.

        This method creates a new view in the database with the specified name and SQL definition.

        Args:
            name_view (str): The name of the view to create.
            sql_definition (str): The SQL query that defines the view.
        """
        sql = text(f"CREATE VIEW {self.schema}.{name_view} AS {sql_definition};")
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def drop_view(self, name_view: str) -> None:
        """Drops a database view if it exists.

        Args:
            name_view (str): The name of the view to drop.
        """
        sql = text(f"DROP VIEW IF EXISTS {self.schema}.{name_view};")
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def execute_sql(self, sql: str) -> None:
        """Executes a SQL query.

        This method executes a given SQL query against the database that doesn't return data.

        Args:
            sql (str): The SQL query to execute.
        """
        sql = text(sql)
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def execute_sql_file(self, file_name: str) -> None:
        """Executes a SQL file.

        This method reads and executes a SQL query from the specified file against the database and doesn't return data.

        Args:
            file_name (str): The path to the SQL file.
        """
        with open(file_name) as sql_file:
            sql = text(sql_file.read())
        with self.engine.connect() as con:
            con.execute(sql)
            con.commit()

    def table_exists(self, name_table: str) -> bool:
        """Checks if a table exists in the database.

        Args:
            name_table (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        sql = f"SELECT count(name) AS is_present FROM sqlite_master WHERE type='table' AND name='{name_table}'"
        df = pl.read_database(sql, connection=self.engine.connect())
        does_exist = df.item(0, 0) > 0
        return does_exist

    def column_exists(self, name_table: str, name_column: str) -> bool:
        """Checks if a column exists in a table.

        Args:
            name_table (str): The name of the table.
            name_column (str): The name of the column.

        Returns:
            bool: True if the column exists, False otherwise.
        """
        sql = f"PRAGMA table_info({name_table})"
        series_names = pl.read_database(sql, connection=self.engine.connect())["name"]
        columns = series_names.to_list()
        exists = name_column in columns
        return exists

    def column_add(self, name_table: str, name_column: str, type_data: str) -> None:
        """Adds a column to a table if it doesn't exist.

        Args:
            name_table (str): The name of the table.
            name_column (str): The name of the column to add.
            type_data (str): The data type of the column.
        """
        if not self.column_exists(name_table=name_table, name_column=name_column):
            sql = text(f"ALTER TABLE {name_table} ADD COLUMN {name_column} {type_data}")
            with self.engine.connect() as con:
                con.execute(sql)
                con.commit()

    def view_exists(self, name_view: str) -> bool:
        """Checks if a view exists in the database.

        Args:
            name_view (str): The name of the view to check.

        Returns:
            bool: True if the view exists, False otherwise.
        """
        sql = f"SELECT count(name) FROM sqlite_master WHERE type='view' AND name='{name_view}'"
        df = pl.read_database(sql, connection=self.engine.connect())
        does_exist = not df.is_empty()
        return does_exist

    def drop_table(self, name_table: str) -> None:
        """Drops a table from the database if it exists.

        Args:
            name_table (str): The name of the table to drop.
        """
        if self.table_exists(name_table):
            sql = text(f"DROP TABLE {name_table};")
            with self.engine.connect() as con:
                con.execute(sql)
                con.commit()

    def store_replace(self, df: pl.DataFrame, name_table: str) -> None:
        """Stores a DataFrame into a table, replacing the existing table.

        Args:
            df (pl.DataFrame): The DataFrame to store.
            name_table (str): The name of the table to store the data in.
        """
        df.write_database(
            table_name=name_table, con=self.engine.connect(), if_table_exists="replace"
        )

    def store_append(self, df: pl.DataFrame, name_table: str) -> None:
        """Stores a DataFrame into a table, appending to existing data.

        Args:
            df (pl.DataFrame): The DataFrame to store.
            name_table (str): The name of the table to store the data in.
        """
        df.write_database(
            table_name=name_table,
            connection=self.engine.connect(),
            if_table_exists="append",
        )

    def read_view(self, name_view: str) -> pl.DataFrame:
        """Reads data from a database view.

        Args:
            name_view (str): The name of the view to read.

        Returns:
            pl.DataFrame: A DataFrame containing the data from the view.
        """
        return self.read_table(name_table=name_view)

    def read_table(self, name_table: str) -> pl.DataFrame:
        """Reads data from a database table.

        Args:
            name_table (str): The name of the table to read.

        Returns:
            pl.DataFrame: A DataFrame containing the data from the table.
        """
        sql = f"SELECT * FROM {name_table}"
        df = pl.read_database(sql, connection=self.engine.connect())
        return df

    def read_sql(self, sql: str) -> pl.DataFrame:
        """Reads data from the database using a SQL query.

        Args:
            sql (str): The SQL query to execute.

        Returns:
            pl.DataFrame: A DataFrame containing the results of the query.
        """
        df = pl.read_database(sql, connection=self.engine.connect())
        return df

    def is_value_present(self, name_table: str, name_column: str, value: str) -> bool:
        """Checks if a value exists in a specific column of a table.

        Args:
            name_table (str): The name of the table.
            name_column (str): The name of the column.
            value (str): The value to check for.

        Returns:
            bool: True if the value is present, False otherwise.
        """
        is_present = False
        if self.table_exists(name_table=name_table):
            if isinstance(value, str):
                sql = f"SELECT COUNT(*) AS qty_present FROM {name_table} WHERE {name_column}='{value}'"
            elif isinstance(value, int):
                sql = f"SELECT COUNT(*) AS qty_present FROM {name_table} WHERE {name_column}={value}"
            df = pl.read_database(sql, connection=self.engine.connect())
            is_present = df.item(0, 0) > 0
        return is_present

    def _dicts_to_dict(self, key_field: str, lst_dicts: list) -> dict:
        """Converts a list of dictionaries to a dictionary of lists, grouped by a key field.

        Args:
            key_field (str): The field to group the dictionaries by.
            lst_dicts (list): The list of dictionaries to convert.

        Returns:
            dict: A dictionary where keys are the unique values of the key field and values
                are lists of dictionaries with that key value.
        """
        dict_results = defaultdict(list)
        for entry in lst_dicts:
            key_value = entry[key_field]
            del entry[key_field]
            dict_results[key_value].append(entry)
        # Convert defaultdict to a regular dict
        dict_results = dict(dict_results)
        return dict_results

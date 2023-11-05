"""
This module defines an abstraction layer for accessing to database.
"""

from dataclasses import dataclass
import psycopg2
from psycopg2 import sql


def with_cursor(func):
    """A wrapper interface to acquire and release the cursor
    for a function that uses the cursor.
    """

    def wrapper(self, *args, **kwargs):
        self.acquire_cursor()
        result = func(self, *args, **kwargs)
        self.release_cursor()
        return result

    return wrapper


@dataclass(kw_only=True)
class DatabaseSettings:
    """This class holds the database settings."""

    name: str
    host: str
    port: int
    password: str
    username: str


class DatabaseHandler:
    """This class handles all database interactions."""

    def __init__(self, settings: DatabaseSettings) -> None:
        # Holds the connection and cursor interfaces.
        self.db_conn = None
        self.db_cursor = None
        # Holds the database settings.
        self.settings: DatabaseSettings = settings

    def connect(self) -> None:
        """Connects to the database.
        :return: None
        """
        self.db_conn = psycopg2.connect(
            database=self.settings.name,
            host=self.settings.host,
            port=self.settings.port,
            user=self.settings.username,
            password=self.settings.password,
        )

    def disconnect(self) -> None:
        """Disconnect from the database.
        :return: None
        """
        # Close the cursor if available.
        if self.db_cursor:
            self.db_cursor.close()
        # Close the connection.
        self.db_conn.close()

    def acquire_cursor(self) -> None:
        """Acquire the cursor.
        :return: None
        """
        if not self.db_cursor:
            self.db_cursor = self.db_conn.cursor()

    def release_cursor(self) -> None:
        """Release the cursor.
        :return: None
        """
        self.db_cursor.close()
        self.db_cursor = None

    @with_cursor
    def check_table_exists(self, table_name: str) -> bool:
        """Checks if there is a table with the given name in the database.
        Returns true if exists, false otherwise.
        """
        self.db_cursor.execute(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name=%s)",
            (table_name,),
        )
        exists = self.db_cursor.fetchone()[0]
        return exists

    @with_cursor
    def create_table(self, table_name: str, columns: dict[str, str]) -> None:
        """Create a table in the database."""
        # Create the table using the constructed column definitions.
        column_definitaions = ", ".join(
            [f"{name} {datatype}" for name, datatype in columns.items()]
        )
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitaions})"

        self.db_cursor.execute(create_table_query)
        self.db_conn.commit()
        self.db_conn.rollback()

    @with_cursor
    def insert_into_table(self, table_name: str, column_values: dict):
        """Insert values into a table dynamically."""
        columns = ", ".join(column_values.keys())
        placeholders = ", ".join(str(value) for value in column_values.values())
        values = tuple(column_values.values())

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        self.db_cursor.execute(insert_query, values)
        self.db_conn.commit()
        self.db_conn.rollback()

    @with_cursor
    def get_columns_from_table(
        self, table_name: str, column_names: str, extras: str = ""
    ) -> list:
        """Execute a query.
        :param table_name: The name of the table.
        :param column_names: The names of the columns to be selected.
        :param extras: The extra conditions.
        :return: The rows of the query.
        """
        self.db_cursor.execute(f"SELECT {column_names} FROM {table_name} {extras}")
        rows = self.db_cursor.fetchall()
        return rows

    @with_cursor
    def check_value_exists(self, table_name: str, column_name: str, value: str) -> bool:
        """Check if a value exists in a specific column of a table.

        :param table_name: The name of the table.
        :param column_name: The name of the column.
        :param value: The value to check.
        :return: True if the value exists, False otherwise.
        """
        self.db_cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} = %s", (value,)
        )
        count = self.db_cursor.fetchone()[0]
        return count > 0


# This is a singleton pattern implementation.
def create_database_handler(settings: DatabaseSettings = None) -> DatabaseHandler:
    """Factory function for creating a DatabaseHandler instance
    only once as a singleton object.

    :param settings: The database settings.
    :return: The DatabaseHandler instance.
    """
    if not hasattr(create_database_handler, "database_handler"):
        # If settings are not provided, raise an error.
        if not settings:
            raise ValueError("Database settings not provided.")
        # Create the DatabaseHandler instance.
        create_database_handler.database_handler = DatabaseHandler(settings)

    # Return the DatabaseHandler instance that is created before.
    return create_database_handler.database_handler
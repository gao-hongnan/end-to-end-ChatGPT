from __future__ import annotations

from typing import Any, Optional, Type, TypeVar

import psycopg2
from psycopg2.extensions import connection

Connection = TypeVar("Connection", bound=connection)

# pylint: disable=redefined-outer-name, too-many-arguments
class DatabaseConnection:
    """Singleton class for database connection."""

    _instance: Optional[DatabaseConnection] = None

    def __new__(
        cls: Type[DatabaseConnection], *args: Any, **kwargs: Any
    ) -> DatabaseConnection:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.init_connection(*args, **kwargs)
        return cls._instance

    def init_connection(
        self, user: str, password: str, host: str, port: str, database: str
    ) -> None:
        """
        Initialize the database connection.

        :param user: The database user.
        :param password: The password for the database user.
        :param host: The database host.
        :param port: The port to connect to the database.
        :param database: The name of the database.
        """
        # pylint: disable=attribute-defined-outside-init
        self.conn: Connection = psycopg2.connect(
            user=user, password=password, host=host, port=port, database=database
        )

    def get_connection(self) -> Connection:
        """
        Get the database connection.

        :return: The psycopg2 connection object.
        """
        return self.conn

    def close(self) -> None:
        """Close the database connection."""
        if self.conn:
            self.conn.close()

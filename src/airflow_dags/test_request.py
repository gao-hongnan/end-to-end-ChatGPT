from __future__ import annotations
import json
import os
import subprocess
import sys
import traceback
from typing import Any, Dict, List
from typing import Any, Optional, Type, TypeVar, Tuple

import psycopg2
from psycopg2.extensions import connection

import httpx
import pendulum
import requests

from airflow import settings
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# temp workaround for src module import
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from src.database.client import Connection, DatabaseConnection
# from src.database.fetch_store_train_evaluate import save_data_to_db
import psycopg2

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


def execute_query(
    connection: Connection, query: str, params: Tuple[Any, ...] = ()
) -> None:
    cur = connection.cursor()

    cur.execute(query, params)

    connection.commit()
    cur.close()


def save_data_to_db(
    insert_query: str, data: List[Dict[str, Any]], connection: Connection
) -> None:
    for item in data:
        params = tuple(item.values())
        execute_query(connection, insert_query, params)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Singapore"),
    catchup=False,
)
def dataops():

    # step 1. fetch live data = raw data
    @task
    def extract_live_data() -> List[Dict[str, Any]]:
        try:
            print("Starting fetch_live_data")

            # Use curl to send an HTTP request
            cmd = ["curl", "-s", "https://jsonplaceholder.typicode.com/todos"]
            response = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )

            # Parse the response JSON
            data = json.loads(response.stdout)
            return data

        except Exception as error:
            # traceback.print_exc()
            raise AirflowFailException("Failed to fetch live data") from error

    # step 2. store raw data into database
    # (but in reality raw data can be dumped into data lake, see goku).
    # @task
    # def load_raw_data(data: List[Dict[str, Any]]) -> None:
    #     print("load_raw_data")
    #     data_copy = data
    #     print(data_copy)

    @task
    def load_raw_data(data: List[Dict[str, Any]]) -> None:
        print("Starting load_raw_data...")
        # Connect to the PostgreSQL database
        # calls psycopg2.connect() under the hood!
        # pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        connection = DatabaseConnection(
            user="airflow_user",
            password="airflow_pass",
            host="127.0.0.1",
            port="5432",
            database="airflow_db",
        ).get_connection()

        insert_query = """
        INSERT INTO todo_table (user_id, id, title, completed)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """

        save_data_to_db(insert_query, data, connection)
        print("Finished load_raw_data")

    # extract_live_data() >> load_raw_data()
    raw_data = extract_live_data()
    load_raw_data(raw_data)
    # extract_live_data() >> load_raw_data()


dataops_dag = dataops()

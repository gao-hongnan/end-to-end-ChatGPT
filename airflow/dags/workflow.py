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

# print(os.environ["AIRFLOW_HOME"])


# sys.path.append(os.path.abspath(os.environ["AIRFLOW_HOME"]))
from src.database.client import Connection, DatabaseConnection
from src.database.fetch_store_train_evaluate import save_data_to_db


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Singapore"),
    catchup=False,
)
def dataopss():

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

    raw_data = extract_live_data()
    load_raw_data(raw_data)


dataops_dag = dataopss()

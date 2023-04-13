from __future__ import annotations

import json
from typing import Any, List, Dict, Tuple
import requests
import pandas as pd
import psycopg2

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

from src.database.client import DatabaseConnection, Connection


class MockTaskInstance:
    def __init__(self):
        self.xcom_data = {}

    def xcom_push(self, key, value):
        self.xcom_data[key] = value

    def xcom_pull(self, key):
        return self.xcom_data.get(key)


def execute_query(
    connection: Connection, query: str, params: Tuple[Any, ...] = ()
) -> None:
    cur = connection.cursor()

    cur.execute(query, params)

    connection.commit()
    cur.close()


def create_table(connection: Connection, create_table_query: str) -> None:
    execute_query(connection, create_table_query)


def save_data_to_db(
    insert_query: str, data: List[Dict[str, Any]], connection: Connection
) -> None:
    for item in data:
        params = tuple(item.values())
        execute_query(connection, insert_query, params)


def preprocess_data(connection: Connection, data_pool: MockTaskInstance) -> None:

    df = pd.read_sql("SELECT * FROM todo_table", connection)
    print(df.head())
    connection.close()

    # Feature engineering
    df["title_length"] = df["title"].apply(len)

    # Train-test split
    X = df[["user_id", "title_length"]]
    y = df["completed"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Save train and test sets to XCom
    data_pool.xcom_push(key="X_train", value=X_train)
    data_pool.xcom_push(key="X_test", value=X_test)
    data_pool.xcom_push(key="y_train", value=y_train)
    data_pool.xcom_push(key="y_test", value=y_test)


def train_model(data_pool: MockTaskInstance) -> None:
    # Retrieve train and test sets from XCom
    X_train = data_pool.xcom_pull(key="X_train")
    y_train = data_pool.xcom_pull(key="y_train")

    # Train a logistic regression model
    model = LogisticRegression(random_state=42)
    model.fit(X_train, y_train)

    # Save the trained model to XCom
    data_pool.xcom_push(key="model", value=model)


def evaluate_model(connection: Connection, data_pool: MockTaskInstance) -> None:
    # Retrieve test sets and model from XCom
    X_test = data_pool.xcom_pull(key="X_test")
    y_test = data_pool.xcom_pull(key="y_test")
    model = data_pool.xcom_pull(key="model")
    # Make predictions and evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    print("Accuracy:", accuracy)
    print("Classification Report:\n", report)

    # Save evaluation results to XCom
    data_pool.xcom_push(key="accuracy", value=accuracy)
    data_pool.xcom_push(key="report", value=report)

    # Save evaluation results to PostgreSQL
    connection = psycopg2.connect(
        user="airflow_user",
        password="airflow_pass",
        host="127.0.0.1",
        port="5432",
        database="airflow_db",
    )

    cur = connection.cursor()

    # Create a table if it doesn't exist
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS model_evaluation (
        id SERIAL PRIMARY KEY,
        accuracy FLOAT,
        report JSON
    )
    """
    )

    # Insert evaluation results into the table
    cur.execute(
        """
    INSERT INTO model_evaluation (accuracy, report)
    VALUES (%s, %s)
    """,
        (accuracy, json.dumps(report)),
    )

    connection.commit()
    cur.close()
    connection.close()


def fetch_live_data() -> List[Dict[str, Any]]:
    response = requests.get("https://jsonplaceholder.typicode.com/todos", timeout=5)
    data = response.json()
    return data


if __name__ == "__main__":

    # Fetch data from the API
    data = fetch_live_data()
    data_pool = MockTaskInstance()

    db_instance = DatabaseConnection(
        user="airflow_user",
        password="airflow_pass",
        host="127.0.0.1",
        port="5432",
        database="airflow_db",
    )

    connection = db_instance.get_connection()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS todo_table (
        id SERIAL PRIMARY KEY,
        user_id INTEGER,
        title VARCHAR(255),
        completed BOOLEAN
    )
    """

    insert_query = """
    INSERT INTO todo_table (user_id, id, title, completed)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """

    # Create the table
    create_table(connection, create_table_query)

    # Save data to the PostgreSQL
    save_data_to_db(insert_query, data, connection)

    # Preprocess data
    preprocess_data(connection, data_pool)

    # Train a model
    train_model(data_pool)

    # Evaluate the model
    evaluate_model(connection, data_pool)

"""
├── src
│   ├── database
│   │   ├── client.py
│   │   └── fetch_store_train_evaluate.py
│   ├── __init__.py
│   └── airflow_dags
│       ├── test_request.py
│       └── ...
├── airflow
│   ├── airflow.cfg
│   └── ...

airflow dags trigger dataops # to trigger the dag on cli

ask docker to check for me my workflow?
"""

# Configurations
# this step must be run to indicate where the airflow home directory is! so if i am
# in examples/ then the airflow home directory is examples/airflow.

export AIRFLOW_HOME=${PWD}/airflow

AIRFLOW_VERSION=2.5.3
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Airflow (may need to upgrade pip)
# MAKE SURE you are in the virtual environment
pip install "apache-airflow[postgres]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"



export AIRFLOW_DB_HOST=localhost
export AIRFLOW_DB_PORT=5432
export AIRFLOW_DB_NAME=airflow_db
export AIRFLOW_DB_USER=airflow_user
export AIRFLOW_DB_PASSWORD=airflow_pass

# Start the web server, default port is 8080???
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@${AIRFLOW_DB_HOST}:${AIRFLOW_DB_PORT}/${AIRFLOW_DB_NAME}"
echo $(airflow config get-value database sql_alchemy_conn)

export PYTHONPATH=.
# Initialize DB (SQLite by default) and will create airflow folder IFF AIRFLOW_HOME is set!
airflow db init

# airflow db reset # to reset the db

# create airflow dags folder to whereever you want so long airflow points to it
AIRFLOW_DAGS_FOLDER="$(pwd)/src/airflow_dags"
mkdir -p ${AIRFLOW_DAGS_FOLDER}

# TODO: automate this
# PYTHONPATH=/Users/reighns/gaohn/end-to-end-ChatGPT/src
# export AIRFLOW__CORE__DAGS_FOLDER=${AIRFLOW_DAGS_FOLDER} # or just set it auto in airflow.cfg
# -i '' is for macos ?? automate this
sed -i '' -E \
-e "s|dags_folder = .+|dags_folder = ${AIRFLOW_DAGS_FOLDER}|" \
-e 's|load_examples = .+|load_examples = False|' \
-e 's|default_timezone = .+|default_timezone = Asia/Singapore|' \
-e 's|default_ui_timezone = .+|default_ui_timezone = Asia/Singapore|' \
-e 's|executor = .+|executor = LocalExecutor|' \
${AIRFLOW_HOME}/airflow.cfg


# We'll be prompted to enter a password
airflow users create \
    --username admin \
    --firstname HN \
    --lastname G \
    --role Admin \
    --email hongnan@aisingapore.org


airflow webserver --port 8080
airflow scheduler

python -m py_compile workflow.py # to debug syntax errors if not it wont run

# sudo lsof -i :8793
# sudo kill <PID>
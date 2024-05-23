import pendulum
import os

import requests
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def insert_data_to_posgres():
    data_path = "/opt/airflow/dags/data/Iris.csv"

    postgres_hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(data_path, "r") as file:
        cur.copy_expert(
            "COPY iris FROM STDIN WITH CSV HEADER DELIMITER AS ',' ",
            file,
        )
    conn.commit()
    cur.close()
    conn.close()

def data_processing():
    query = """
            SELECT *
            FROM iris;
        """
    postgres_hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()

    # preprocess data using pandas
    # do some preprocessing here...
    iris = pd.DataFrame(data = data, columns = ['iris_id', 'iris_sepal_length', 'iris_sepal_width',
                 'iris_petal_length', 'iris_petal_width', 'iris_variety'])
    iris = iris[
        (iris['iris_sepal_length'] > 5) &
        (iris['iris_sepal_width'] == 3) &
        (iris['iris_petal_length'] > 3) &
        (iris['iris_petal_width'] == 1.5)
    ]
    iris.to_csv(('/opt/airflow/dags/data/Iris_after_process.csv'), index=False)
    
    # close database connection
    cur.close()
    conn.close()

def insert_data_to_posgres_after():
    data_path = "/opt/airflow/dags/data/Iris_after_process.csv"
    postgres_hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(data_path, "r") as file:
        cur.copy_expert(
            "COPY iris_after FROM STDIN WITH CSV HEADER DELIMITER AS ',' ",
            file,
        )
    conn.commit()
    cur.close()
    conn.close()

# Define DAG
dag = DAG(
    dag_id = 'workshop_postgres_operator',
    default_args = {'owner': 'Duangjai'},
    tags = ["workshop_postgres"],
    schedule_interval = '0 0 * * *',
    start_date = datetime(2023, 4, 30),
    catchup = False
)

# Define operators and tasks
create_postgres_table = PostgresOperator(
    task_id = 'create_postgres_table',
    postgres_conn_id = 'postgres_localhost',
    sql = """
        create table if not exists iris(
	    iris_id SERIAL PRIMARY KEY,
	    iris_sepal_length REAL,
	    iris_sepal_width REAL,
	    iris_petal_length REAL,
	    iris_petal_width REAL,
	    iris_variety VARCHAR(16)
        )
    """
)

insert_data_posgres = PythonOperator(
	task_id = 'insert_data_posgres', 
    python_callable = insert_data_to_posgres, 
    dag=dag
)

data_process = PythonOperator(
	task_id = 'data_process', 
    python_callable = data_processing, 
    dag = dag
)

create_table_after= PostgresOperator(
    task_id = 'create_table_after',
    postgres_conn_id = 'postgres_localhost',
    sql = """
        create table if not exists iris_after(
	    iris_id SERIAL PRIMARY KEY,
	    iris_sepal_length REAL,
	    iris_sepal_width REAL,
	    iris_petal_length REAL,
	    iris_petal_width REAL,
	    iris_variety VARCHAR(16)
        )
    """
)

insert_data_posgres_after = PythonOperator(
	task_id = 'insert_data_posgres_after', 
    python_callable = insert_data_to_posgres_after, 
    dag = dag
)

create_postgres_table >> insert_data_posgres >> data_process >> create_table_after >> insert_data_posgres_after

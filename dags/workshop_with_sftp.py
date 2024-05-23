import csv
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.utils.dates import days_ago
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta
import pandas as pd 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

def data_processing():
    # preprocess data using pandas
    # do some preprocessing here...
    iris = pd.read_csv('/opt/airflow/dags/data/Iris_output.csv')
    iris = iris[
        (iris['iris_sepal_length'] > 5) &
        (iris['iris_sepal_width'] == 3) &
        (iris['iris_petal_length'] > 3) &
        (iris['iris_petal_width'] == 1.5)
    ]
    iris.to_csv(('/opt/airflow/dags/data/Iris_sftp_after.csv'), index=False)


dag = DAG(
    dag_id = 'workshop_with_sftp',
    default_args = {'owner': 'Jilasak'},
    tags = ["workshop_sftp"],
    schedule_interval = '0 0 * * *',
    start_date = datetime(2023, 4, 30),
    catchup = False
)

download_file = SFTPOperator(
    task_id = "download-file",
    ssh_hook = SSHHook(ssh_conn_id = "sftp_localhost"),
    remote_filepath = "/data/Iris.csv",
    local_filepath = "/opt/airflow/dags/data/Iris_output.csv", 
    operation = "get",
    create_intermediate_dirs = True
)
    
data_preprocess = PythonOperator(
	task_id = 'data_preprocess', 
    python_callable = data_processing, 
    dag = dag
)

upload_file_after = SFTPOperator(
    task_id = "upload-file",
    ssh_hook = SSHHook(ssh_conn_id = "sftp_localhost"),
    remote_filepath = "/data/Iris_after_process.csv",
    local_filepath = "/opt/airflow/dags/data/Iris_sftp_after.csv", 
    operation = "put",
    create_intermediate_dirs = True
)


create_table_after_output= PostgresOperator(
    task_id = 'create_table_after',
    postgres_conn_id = 'postgres_localhost',
    sql = """
        create table if not exists iris_after_few(
	    iris_id SERIAL PRIMARY KEY,
	    iris_sepal_length REAL,
	    iris_sepal_width REAL,
	    iris_petal_length REAL,
	    iris_petal_width REAL,
	    iris_variety VARCHAR(16)
        )
    """
)

def insert_data_to_posgres_after():
    data_path = "/opt/airflow/dags/data/Iris_after_process.csv"
    postgres_hook = PostgresHook(postgres_conn_id = "postgres_localhost")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(data_path, "r") as file:
        cur.copy_expert(
            "COPY iris_after_few FROM STDIN WITH CSV HEADER DELIMITER AS ',' ",
            file,
        )
    conn.commit()
    cur.close()
    conn.close()

insert_data_posgres_after_output = PythonOperator(
	task_id = 'insert_data_posgres_after', 
    python_callable = insert_data_to_posgres_after, 
    dag = dag
)





download_file >> data_preprocess >> upload_file_after >> create_table_after_output >> insert_data_posgres_after_output
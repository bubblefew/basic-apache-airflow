from airflow import DAG
from airflow.providers.amazon.aws.transfers.s3_to_athena import S3ToAthenaOperator
from airflow.providers.amazon.aws.transfers.athena_to_s3 import AthenaToS3QueryResults
from airflow.providers.mssql.operators.mssql import MsSqlOperator
from datetime import datetime

# Define your DAG
dag = DAG(
    's3_to_athena_to_sql_server',
    schedule_interval=None,  # Set your desired schedule interval
    start_date=datetime(2023, 1, 1),  # Set your desired start date
)

# Define tasks
s3_to_athena_task = S3ToAthenaOperator(
    task_id='s3_to_athena',
    # bucket_name='your-s3-bucket',
    destination_path='s3://aws-athena-query-results-246898065194-ap-southeast-1',
    aws_conn_id='aws_default',  # Airflow connection ID for AWS
    database='pwb_mdc',
    query='select * from pwb_mdc.sales_order_address limit 1',
    dag=dag,
)

athena_to_sql_server_task = MsSqlOperator(
    task_id='athena_to_sql_server',
    sql='INSERT INTO your_sql_server_table SELECT * FROM your_athena_output_table',
    mssql_conn_id='mssql_default',  # Airflow connection ID for SQL Server
    dag=dag,
)

# Set task dependencies
s3_to_athena_task >> athena_to_sql_server_task

# Importing Packages
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import timezone
import logging

# Define DAG
dag = DAG(
    dag_id = 'helloX',
    default_args = {'owner': 'Jilasak'},
    schedule_interval = '*/5 * * * *',
    start_date = timezone.datetime(2023, 3, 31),
    catchup = False
)

def _hello():
    return 'Hello, Python'

def _print_log_messages():
    logging.debug('This is a debug message')
    logging.info('This is a info message')
    logging.warning('This is a warning message')
    logging.error('This is a error message')
    logging.critical('This is a critical message')

    return 'Whatever is returned also gets printed in the logs'


# Define operators and tasks
start = DummyOperator(
    task_id='start',
    dag=dag,
)

echo_hello = BashOperator(
    task_id = 'echo_hello',
    bash_command = 'echo hello',
    dag = dag
)

say_hello = PythonOperator(
    task_id = 'say_hello',
    python_callable = _hello,
    dag = dag
)

print_log_messages = PythonOperator(
    task_id = 'print_log_messages',
    python_callable = _print_log_messages,
    dag = dag
)

end = DummyOperator(
    task_id = 'end',
    dag = dag
)

# Define dependencies
start >> echo_hello >> say_hello >> print_log_messages >> end
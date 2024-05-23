# Importing Packages
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

# Define DAG
dag = DAG (
    dag_id = 'my_first_dag',
    default_args = {'owner': 'Duangjai'},
    schedule_interval = '*/5 * * * *',
    start_date = timezone.datetime(2023, 3, 31),
    catchup = False
)


# Define operators and tasks
t1 = DummyOperator(
    task_id = 'my_1st_dummy_task',
    dag = dag
)

t2 = DummyOperator(
    task_id ='my_2nd_dummy_task',
    dag = dag
)

# Define dependencies
t1 >> t2
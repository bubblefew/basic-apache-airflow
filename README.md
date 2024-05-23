# airflow-cluster-docker
Make an airflow cluster in your localhost with docker

## Component

* Apache Airflow 2.xx with
    * __airflow-scheduler__ The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
    * __airflow-webserver__ The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
    * __airflow-worker__ The worker that executes the tasks given by the scheduler.
    * __airflow-triggerer__ The triggerer runs an event loop for deferrable tasks.
    * __airflow-init__ The initialization service.
    * __postgres__ The database for storing airflow's metadata.
    * __redis__ broker that forwards messages from scheduler to worker.

## how to use

To start Workbench
```
docker-compose up -d
```
Try to use airflow DAGs by put some example (DAGs Example folder) to dags folder

## Interfaces
* Airflow webserver UI : http://localhost:8080

## stop workbench and terminate

```
docker-compose down
```

## Reference
* https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
* https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html


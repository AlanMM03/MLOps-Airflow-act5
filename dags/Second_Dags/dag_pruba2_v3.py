import sys 
from airflow.decorators import dag,task
from datetime import datetime

""" from common.add_task import task_virtualenv """


@task.docker(image="python:3.9-slim-bullseye")
def task_docker():
    import sys
    print("Versión Python (DockerHub): ")
    print(sys.version)

@dag(
    dag_id='taskflow_v3',
    start_date= datetime(2022,1,1),
    schedule=None
)

def mydag():
    task_docker()

third_dag=mydag()
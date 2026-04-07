import sys 
from airflow.decorators import dag,task
from datetime import datetime

from common.add_task import task_virtualenv


@task.docker(image="mlops-image-docker:latest")
def task_docker():
    import sys
    print("Versión Python (imagen personalizada): ")
    print(sys.version)

@dag(
    dag_id='taskflow_v4',
    start_date= datetime(2022,1,1),
    schedule=None
)

def mydag():
    task_docker()

fourth_dag=mydag()
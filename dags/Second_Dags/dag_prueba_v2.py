import sys 
from airflow.decorators import dag
from datetime import datetime

from common.add_task import task_virtualenv


@dag(
    dag_id='taskflow_v2',
    start_date= datetime(2022,1,1),
    schedule=None
)

def mydag():
    task_virtualenv()

second_dag=mydag()
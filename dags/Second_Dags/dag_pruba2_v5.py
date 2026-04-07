import sys 
from airflow.decorators import dag,task
from datetime import datetime

from common.add_task import task_training_model

@dag(
    dag_id='taskflow_v5',
    start_date= datetime(2022,1,1),
    schedule=None
)

def mydag():
    task_training_model()
fifth_dag=mydag()
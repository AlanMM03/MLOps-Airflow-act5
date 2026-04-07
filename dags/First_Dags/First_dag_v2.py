from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os
import pytz

TZ= os.getenv('TZ')
timezone=pytz.timezone(TZ)


default_args = {
    'owner': 'MLOps-AlanMociños',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="first_dag-v2",
    default_args=default_args,
    description= "DAG DE PRUEBA",
    start_date=datetime(2026,2,11,1,tzinfo=timezone),
    schedule="@once",
    catchup=False
)as dag:
    
    task1=BashOperator(
        task_id="first_task_1",
        bash_command="echo hello world -- task 1 [$TZ]"
    )
    task2=BashOperator(
        task_id="first_task_2",
        bash_command="echo hello world -- task 2"
    )
    task3=BashOperator(
        task_id="first_task_3",
        bash_command="echo hello world -- task 3"
    )

    """Métodos para ejecutar las tareas de manera sincronizada después de ejecutar la primera. Forma lineal. 
    task1.set_downstream(task2)
    task1.set_downstream(task3)
    
    task1 >> task2
    task1 >> task3 """

    task1>>[task2,task3]
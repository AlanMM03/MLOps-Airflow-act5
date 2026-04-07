from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os
import pytz
from airflow.models import Variable

TZ= os.getenv('TZ')
timezone=pytz.timezone(TZ)

VAR1=Variable.get("AIRFLOW_VAR_1")
VAR2=Variable.get("AIRFLOW_VAR_2",deserialize_json=True)

VAR2_STRING=VAR2.get('data').get('string')


default_args = {
    'owner': 'MLOps-AlanMociños',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id="first_dag-v4",
    default_args=default_args,
    description= "DAG DE PRUEBA",
    start_date=datetime(2026,2,11,1,tzinfo=timezone),
    schedule="@once",
    catchup=False,
    params={"name":"MLOps-AlanMociños", "project":'Introducción a AirFlow'}
)as dag:
    
    task1=BashOperator(
        task_id="first_task_1",
        bash_command=f"echo hello world -- task 1 [{VAR1}]"
    )
    task2=BashOperator(
        task_id="first_task_2",
        bash_command="echo hello world -- task 2[$AIRFLOW_HOME]"
    )
    task3=BashOperator(
        task_id="first_task_3",
        bash_command="echo hello world -- task 3 [{{var.json.AIRFLOW_VAR_2.data.array}}]"
    )

    """Métodos para ejecutar las tareas de manera sincronizada después de ejecutar la primera. Forma lineal. 
    task1.set_downstream(task2)
    task1.set_downstream(task3)
    
    task1 >> task2
    task1 >> task3 """

    task1>>[task2,task3]
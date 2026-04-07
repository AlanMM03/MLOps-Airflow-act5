import sys 
from airflow import DAG
from datetime import datetime,timedelta

from airflow.utils.email import send_email
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def success_email(context):
    task_instance = context['task_instance']
    task_status = 'Success'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
           f'The task execution date is: {context["execution_date"]}\n'\
           f'Log url: {task_instance.log_url}\n\n'
    to_email = 'alanperfect2003@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

def failure_email(context):
    task_instance = context['task_instance']
    task_status = 'Failed'
    subject = f'Airflow Task {task_instance.task_id} {task_status}'
    body = f'The task {task_instance.task_id} completed with status : {task_status}. \n\n'\
           f'The task execution date is: {context["execution_date"]}\n'\
           f'Log url: {task_instance.log_url}\n\n'
    to_email = 'alanperfect2003@gmail.com' #recepient mail
    send_email(to = to_email, subject = subject, html_content = body)

def python_command1():
    print("Hola mundo")

def python_command2():
    raise ValueError("Forzando error")

default_args = {
    'owner': 'datapath',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_success': True,
    'email_on_retry': False
}


with DAG(
    'email_v1',
    default_args=default_args,
    description= "Email test",
    schedule= None,
    on_failure_callback = lambda context: failure_email(context),
    on_success_callback = lambda context: failure_email(context)
)as dag:
    task1=PythonOperator(
        task_id="execute_command_1",
        python_callable=python_command1
    )
    task2=PythonOperator(
        task_id="execute_command_2",
        python_callable=python_command2
    )
    task1>>task2
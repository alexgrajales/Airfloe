from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from util_slack import tell_slack_success as slack_success
from airflow.models import Variable

#XCOM abbreviation for cross communication
#push and pull

def tell_slack_success(context):
    return slack_success(context)

default_args = {
    'owner': 'Owner',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 22),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
}
dag = DAG(
    'Hello', default_args=default_args, schedule_interval="0 * * * *")

Stage1 = BashOperator(
    task_id='Hello',
    on_success_callback=tell_slack_success,
    bash_command='echo {{ var.value.Nombre}}',
    dag=dag)

Stage2 = BashOperator(
    task_id='World',
    on_success_callback=tell_slack_success,
    bash_command='echo world',
    dag=dag)
#function to get the number 7
def seven():
    return 7
#First Way to push using xcom
Stage3 = PythonOperator(
     task_id = 'try_xcom7',
    on_success_callback=tell_slack_success,
     python_callable = seven,
     xcom_push=True,
     dag = dag)

def pushnine(**context):
    context['ti'].xcom_push(key='keyNINE', value=9)

#second way to push
Stage5 = PythonOperator(
    task_id = 'push9',
    on_success_callback=tell_slack_success,
    python_callable = pushnine,
    dag = dag
    )

def getNINE(**context):
    value = context['ti'].xcom_pull(key='keyNINE',task_ids='push9')
    print (value)
    return value

#Pull values 
Stage4 = PythonOperator(
    task_id ='pull_xcom9',
    on_success_callback=tell_slack_success,
    python_callable=getNINE,
    provide_context=True,
    dag=dag
    )


Stage1 >> Stage2

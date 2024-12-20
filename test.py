from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import json
from datetime import datetime

def debug_variable(**kwargs):
    keyfile_json = Variable.get('SERVICE_ACCOUNT_JSON')
    keyfile_dict = json.loads(keyfile_json)
    print(keyfile_dict)

with DAG(
    'debug_variable_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None
) as dag:
    debug_task = PythonOperator(
        task_id='debug_service_account_json',
        python_callable=debug_variable
    )

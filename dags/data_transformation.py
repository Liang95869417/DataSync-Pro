from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG('data_transformation_dag', start_date=datetime(2024, 1, 1), schedule_interval='@daily')

dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run',
    dag=dag,
)

dbt_run_task

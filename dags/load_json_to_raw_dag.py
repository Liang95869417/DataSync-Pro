import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from scripts.load_json_to_bigquery import load_json_to_bigquery


default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('load_json_to_raw_dag',
         default_args=default_args,
         schedule_interval='@weekly',
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    load_json_task = PythonOperator(
        task_id='load_json_to_raw',
        python_callable=load_json_to_bigquery,
        op_kwargs={
            'bucket_name': 'ingested-data-1',
            'source_prefix': 'test/',
            'dataset_id': 'raw_dataset',
        }
    )

    end = EmptyOperator(task_id='end')

    start >> load_json_task >> end
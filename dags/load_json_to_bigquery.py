import os
import json
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from scripts.load_json_to_bigquery import load_json_to_bigquery

cutoff_date = datetime.now(timezone.utc) - timedelta(days=2)
date_str = cutoff_date.strftime('%Y%m%d')

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('load_json_to_raw_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    load_json_task = PythonOperator(
        task_id='load_json_to_raw',
        python_callable=load_json_to_bigquery,
        op_kwargs={
            'bucket_name': 'ingested-data-1',
            'source_prefix': 'production/',
            'dataset_id': 'production_dataset',
            'date_str': date_str,
        }
    )

    end = EmptyOperator(task_id='end')

    start >> load_json_task >> end
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from google.cloud import bigquery

def load_json_to_bigquery():
    client = bigquery.Client()
    dataset_id = 'raw_dataset'
    table_id = 'raw_table'
    uri = 'gs://ingested-data-1/kassal_product_data_test.json'

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
    )

    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )
    load_job.result()

dag = DAG('data_ingestion_and_transformation', start_date=datetime(2024, 1, 1), schedule_interval='@daily')

start = EmptyOperator(task_id='start', dag=dag)

load_json_task = PythonOperator(
    task_id='load_json_to_bigquery',
    python_callable=load_json_to_bigquery,
    dag=dag,
)

transform_task = BashOperator(
    task_id='dbt_run',
    bash_command='dbt run --profiles-dir /dbt',
    dag=dag,
)

start >> load_json_task >> transform_task

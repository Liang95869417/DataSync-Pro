from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
from scripts.api_ingestion_kassal import ingest_kassal_product_data_since, ingest_kassal_store_data_all
from scripts.api_ingestion_vda import ingest_VDA_product_data
from scripts.file_processing import process_uploaded_files
from scripts.gcs_upload import upload_to_gcs
from airflow.models.baseoperator import chain
from scripts.load_json_to_bigquery import load_json_to_bigquery
import subprocess

cutoff_date = datetime.now(timezone.utc) - timedelta(days=5)
date_str = cutoff_date.strftime('%Y%m%d')
kassal_temp_file = f'kassal_product_data_{date_str}.ndjson'
vda_ltemp_file = f'vda_product_data_{date_str}.ndjson'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def run_dbt_task(dbt_task_name):
    subprocess.run(['dbt', dbt_task_name, '--profiles-dir', 'dbt', '--project-dir', 'dbt/etl'], check=True)

dag = DAG(
    'data_ingestion_dag',
    default_args=default_args,
    description='A simple data ingestion DAG',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

ingest_kassal_product_data_task = PythonOperator(
    task_id='ingest_kassal_product_data',
    python_callable=lambda: ingest_kassal_product_data_since(cutoff_date.date(),
                                                             kassal_temp_file),
    dag=dag,
)

# # it seems that we don't need to update store data weekly
# ingest_kassal_store_data_task = PythonOperator(
#     task_id='ingest_kassal_store_data',
#     python_callable=ingest_kassal_store_data_all,
#     dag=dag,
# )

ingest_vda_product_data_task = PythonOperator(
    task_id='ingest_vda_product_data',
    python_callable=lambda: ingest_VDA_product_data(kassal_temp_file,
                                                    vda_ltemp_file),
    dag=dag,
)

upload_kassal_product_data_task = PythonOperator(
    task_id='upload_kassal_product_data_to_gcs',
    python_callable=lambda: upload_to_gcs(kassal_temp_file, 
                                          f'production/{kassal_temp_file}'),
    dag=dag,
)

# upload_kassal_store_data_task = PythonOperator(
#     task_id='upload_kassal_store_data_to_gcs',
#     python_callable=lambda: upload_to_gcs('/tmp/kassal_store_data_production.ndjson', 
#                                           'production/kassal_store_data_production.ndjson'),
#     dag=dag,
# )

upload_vda_product_data_task = PythonOperator(
    task_id='upload_vda_product_data_to_gcs',
    python_callable=lambda: upload_to_gcs(vda_ltemp_file, 
                                          f'production/{vda_ltemp_file}'),
    dag=dag,
)

load_json_task = PythonOperator(
        task_id='load_json_to_bigquery',
        python_callable=load_json_to_bigquery,
        op_kwargs={
            'bucket_name': 'ingested-data-1',
            'source_prefix': 'production/',
            'dataset_id': 'production_dataset',
            'date_str': date_str,
        }
    )

dbt_run = PythonOperator(
    task_id='dbt_run',
    python_callable=run_dbt_task,
    op_args=['run']
)

dbt_test = PythonOperator(
    task_id='dbt_test',
    python_callable=run_dbt_task,
    op_args=['test']
)

## Define task dependencies
chain(ingest_kassal_product_data_task,
      upload_kassal_product_data_task,
      ingest_vda_product_data_task,
      upload_vda_product_data_task,
      load_json_task,
      dbt_run,
      dbt_test)
# chain(ingest_kassal_store_data_task,
#       upload_kassal_store_data_task)

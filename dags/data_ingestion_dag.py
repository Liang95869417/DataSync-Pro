from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.api_ingestion import (ingest_kassal_product_data, 
                                   ingest_kassal_store_data, 
                                   ingest_VDA_product_data)
from scripts.file_processing import process_uploaded_files
from scripts.gcs_upload import upload_to_gcs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

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
    python_callable=ingest_kassal_product_data,
    dag=dag,
)

ingest_kassal_store_data_task = PythonOperator(
    task_id='ingest_kassal_store_data',
    python_callable=ingest_kassal_store_data,
    dag=dag,
)

ingest_vda_product_data_task = PythonOperator(
    task_id='ingest_vda_product_data',
    python_callable=ingest_VDA_product_data,
    dag=dag,
)

upload_kassal_product_data_task = PythonOperator(
    task_id='upload_kassal_product_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/kassal_product_data_test.ndjson', 'test/kassal_product_data_test.ndjson'),
    dag=dag,
)

upload_kassal_store_data_task = PythonOperator(
    task_id='upload_kassal_store_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/kassal_store_data_test.ndjson', 'test/kassal_store_data_test.ndjson'),
    dag=dag,
)

upload_vda_product_data_task = PythonOperator(
    task_id='upload_vda_product_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/vda_product_data_test.ndjson', 'test/vda_product_data_test.ndjson'),
    dag=dag,
)

# Define task dependencies
ingest_kassal_product_data_task >> upload_kassal_product_data_task >> ingest_vda_product_data_task >> upload_vda_product_data_task
ingest_kassal_store_data_task >> upload_kassal_store_data_task


# ## task for ingesting data uploaded from platform
# process_files_task = PythonOperator(
#     task_id='process_uploaded_files',
#     python_callable=process_uploaded_files,
#     dag=dag,
# )

# upload_processed_files_task = PythonOperator(
#     task_id='upload_processed_files_to_gcs',
#     python_callable=lambda: upload_to_gcs('/tmp/processed_files/', 'processed_files/'),
#     dag=dag,
# )

# process_files_task >> upload_processed_files_task

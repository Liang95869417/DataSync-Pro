from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.api_ingestion_vda import ingest_VDA_product_data
from scripts.api_ingestion_kassal import ingest_kassal_product_data_all, ingest_kassal_store_data_all
from scripts.gcs_upload import upload_to_gcs
from scripts.load_json_to_bigquery import load_json_to_bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'data_ingestion_first_run_dag',
    default_args=default_args,
    description='A simple data ingestion DAG',
    schedule_interval=None,  # manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

ingest_kassal_product_data_all_task = PythonOperator(
    task_id='ingest_kassal_product_data',
    python_callable=ingest_kassal_product_data_all,
    dag=dag,
)

ingest_kassal_store_data_all_task = PythonOperator(
    task_id='ingest_kassal_store_data',
    python_callable=ingest_kassal_store_data_all,
    dag=dag,
)

ingest_vda_product_data_all_task = PythonOperator(
    task_id='ingest_vda_product_data',
    python_callable=lambda: ingest_VDA_product_data('/tmp/kassal_product_data.ndjson',
                                                    '/tmp/vda_product_data.ndjson'),
    dag=dag,
)

upload_kassal_product_data_task = PythonOperator(
    task_id='upload_kassal_product_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/kassal_product_data.ndjson', 
                                          'production/kassal_product_data.ndjson'),
    dag=dag,
)

upload_kassal_store_data_task = PythonOperator(
    task_id='upload_kassal_store_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/kassal_store_data.ndjson', 
                                          'production/kassal_store_data_test.ndjson'),
    dag=dag,
)

upload_vda_product_data_task = PythonOperator(
    task_id='upload_vda_product_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/vda_product_data.ndjson', 
                                          'production/vda_product_data_test.ndjson'),
    dag=dag,
)

load_json_task = PythonOperator(
        task_id='load_json_to_bigquery',
        python_callable=load_json_to_bigquery,
        op_kwargs={
            'bucket_name': 'ingested-data-1',
            'source_prefix': 'production/',
            'dataset_id': 'production_dataset',
        }
    )

# Define task dependencies
ingest_kassal_product_data_all_task >> upload_kassal_product_data_task >> ingest_vda_product_data_all_task >> upload_vda_product_data_task >> load_json_task
ingest_kassal_store_data_all_task >> upload_kassal_store_data_task >> load_json_task


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

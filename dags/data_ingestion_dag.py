from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.api_ingestion import ingest_api_data
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

api_key = Variable.get('KASSAL_API_KEY', default_var=None)

## task for ingesting api data
ingest_api_task = PythonOperator(
    task_id='ingest_api_data',
    python_callable=ingest_api_data,
    op_args=[api_key],
    dag=dag,
)

upload_api_data_task = PythonOperator(
    task_id='upload_api_data_to_gcs',
    python_callable=lambda: upload_to_gcs('/tmp/kassal_api_data.json', 'kassal_api_data.json'),
    dag=dag,
)
# Define the dependencies
ingest_api_task >> upload_api_data_task

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

from google.cloud import bigquery
from utils import kassal_product_schema, kassal_store_schema, vda_product_schema


def load_data_from_gcs(project_id, dataset_id, table_id, gcs_uri):
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        schema=kassal_store_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        f"{client.project}.{dataset_id}.{table_id}",
        job_config=job_config
    )

    print(f"Starting job {load_job.job_id}")

    load_job.result()  # Waits for the job to complete

    print(f"Job finished. Loaded {load_job.output_rows} rows into {dataset_id}:{table_id}.")

if __name__ == "__main__":
    project_id = 'datasync-pro'
    dataset_id = 'production_dataset'
    table_id = 'kassal_store_data' 
    gcs_uri = 'gs://ingested-data-1/production/kassal_store_data.ndjson'

    load_data_from_gcs(project_id, dataset_id, table_id, gcs_uri)

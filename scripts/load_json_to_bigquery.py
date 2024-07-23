import os
import logging
from google.cloud import bigquery
from google.cloud import storage

def load_json_to_raw(bucket_name, source_prefix, dataset_id):
    client = bigquery.Client()
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        logging.info(f"Found blob: {blob.name}")
        if blob.name.endswith('.ndjson'):
            uri = f"gs://{bucket_name}/{blob.name}"
            table_name = os.path.splitext(os.path.basename(blob.name))[0]
            table_id = f"{dataset_id}.{table_name}"

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                autodetect=True,
            )
            logging.info(f"Loading data from {uri} into {table_id}")

            load_job = client.load_table_from_uri(
                uri,
                table_id,
                job_config=job_config,
            )
            load_job.result()
            logging.info(f"Loaded {blob.name} into {dataset_id}.{table_id}")
        else:
            logging.info(f"Skipping non-JSON file: {blob.name}")

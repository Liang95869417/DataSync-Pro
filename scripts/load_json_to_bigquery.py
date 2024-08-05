import os
import json
import logging
from io import BytesIO
from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import GoogleAPIError
from scripts.utils import kassal_product_schema, kassal_store_schema, vda_product_schema

def get_schema_and_unique_key_for_blob(blob_name):
    if "kassal_product" in blob_name or "kassal_store" in blob_name:
        schema = kassal_product_schema if "kassal_product" in blob_name else kassal_store_schema
        unique_key = "id"
    elif "vda_product" in blob_name:
        schema = vda_product_schema
        unique_key = "gtin"
    else:
        raise ValueError(f"No schema defined for blob: {blob_name}")
    return schema, unique_key

def preprocess_blob(blob):
    try:
        content = blob.download_as_string().decode('utf-8')
        return [line for line in content.splitlines() if line]
    except Exception as e:
        logging.error(f"Error processing blob {blob.name}: {e}")
        return []

def load_data_to_bigquery(client, project_id, dataset_id, 
                          table_id, lines, schema):
    try:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        data_stream = BytesIO('\n'.join(lines).encode('utf-8'))

        logging.info(f"Loading data into {table_id}")

        load_job = client.load_table_from_file(
            data_stream,
            f"{project_id}.{dataset_id}.{table_id}",
            job_config=job_config,
        )
        load_job.result()
        logging.info(f"Loaded data into {project_id}.{dataset_id}.{table_id}")

    except GoogleAPIError as e:
        logging.error(f"Failed to load data into {project_id}.{dataset_id}.{table_id}: {e}")

def load_json_to_bigquery(bucket_name: str, source_prefix: str, dataset_id: str, date_str: str):
    project_id = "datasync-pro"
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        logging.info(f"Found blob: {blob.name}")
        if blob.name.endswith(f'{date_str}.ndjson'):
            len_target = len(date_str) + 1
            table_name = os.path.splitext(os.path.basename(blob.name))[0][:-len_target]

            try:
                schema, unique_key = get_schema_and_unique_key_for_blob(blob.name)
                processed_lines = preprocess_blob(blob)
                logging.info(f"Blob length for {blob.name}: {len(processed_lines)}")

                if processed_lines:
                    load_data_to_bigquery(client, project_id, dataset_id, 
                                          table_name, processed_lines, schema)
                else:
                    logging.warning(f"No data to load for {blob.name}")

            except ValueError as e:
                logging.error(f"Error processing blob {blob.name}: {e}")
        else:
            logging.info(f"Skipping non-Target file: {blob.name}")


import os
import json
import logging
import tempfile
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
        lines = content.splitlines()
        processed_lines = [line for line in lines if line]
        return processed_lines
    except Exception as e:
        logging.error(f"Error processing blob {blob.name}: {e}")
        return []

def load_blob_to_bigquery(client, dataset_id, table_id, processed_lines, schema, unique_key):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.ndjson') as temp_file:
        temp_file_path = temp_file.name
        temp_file.write('\n'.join(processed_lines).encode('utf-8'))

        try:
            temp_table_id = f"{table_id}_temp"

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            logging.info(f"Loading data from {temp_file_path} into {temp_table_id}")

            with open(temp_file_path, 'rb') as temp_file:
                load_job = client.load_table_from_file(
                    temp_file,
                    temp_table_id,
                    job_config=job_config,
                )
                load_job.result()

            logging.info(f"Loaded data into {dataset_id}.{temp_table_id}")

            merge_job = client.query(f"""
                MERGE `{dataset_id}.{table_id}` T
                USING `{dataset_id}.{temp_table_id}` S
                ON T.{unique_key} = S.{unique_key}
                WHEN NOT MATCHED THEN
                  INSERT ROW
            """)
            merge_job.result()

            logging.info(f"Merged data from {temp_table_id} into {table_id}")

            client.delete_table(temp_table_id)

        except GoogleAPIError as e:
            logging.error(f"Failed to load data from {temp_file_path} into {table_id}: {e}")

        finally:
            os.remove(temp_file_path)

def load_json_to_bigquery(bucket_name: str, source_prefix: str, dataset_id: str):
    client = bigquery.Client()
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        logging.info(f"Found blob: {blob.name}")
        if blob.name.endswith('.ndjson'):
            table_name = os.path.splitext(os.path.basename(blob.name))[0]
            table_id = f"{dataset_id}.{table_name}"

            schema, unique_key = get_schema_and_unique_key_for_blob(blob.name)
            processed_lines = preprocess_blob(blob)
            logging.info(f"blob length for {blob.name}: {len(processed_lines)}")

            load_blob_to_bigquery(client, dataset_id, table_id, processed_lines, schema, unique_key)
        else:
            logging.info(f"Skipping non-JSON file: {blob.name}")


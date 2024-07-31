import os
import json
import logging
import tempfile
from google.cloud import bigquery
from google.cloud import storage


def convert_comma_to_dot_in_json(json_data):
    for entry in json_data:
        if 'mengde' in entry and isinstance(entry['mengde'], str):
            try:
                # Replace comma with dot and convert to float
                entry['mengde'] = float(entry['mengde'].replace(',', '.'))
            except ValueError:
                logging.error(f"Invalid mengde value: {entry['mengde']}")
                entry['mengde'] = None  # or handle this case as needed
    return json_data

def preprocess_blob(blob):
    content = blob.download_as_string().decode('utf-8')
    lines = content.splitlines()
    processed_lines = []
    for line in lines:
        json_data = json.loads(line)
        processed_json_data = convert_comma_to_dot_in_json([json_data])
        processed_lines.append(json.dumps(processed_json_data[0]))
    return '\n'.join(processed_lines)

def load_json_to_bigquery(bucket_name: str, source_prefix: str, dataset_id: str):
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

            # Preprocess the blob content
            processed_content = preprocess_blob(blob)

            # Create a temporary file for the processed content
            with tempfile.NamedTemporaryFile(delete=False, suffix='.ndjson') as temp_file:
                temp_file_path = temp_file.name
                temp_file.write(processed_content.encode('utf-8'))

            try:
                # Load the processed file into BigQuery
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    autodetect=True,
                )
                logging.info(f"Loading data from {temp_file_path} into {table_id}")

                with open(temp_file_path, 'rb') as temp_file:
                    load_job = client.load_table_from_file(
                        temp_file,
                        table_id,
                        job_config=job_config,
                    )
                    load_job.result()

                logging.info(f"Loaded {blob.name} into {dataset_id}.{table_id}")

            except Exception as e:
                logging.error(f"Failed to load data from {temp_file_path} into {table_id}: {e}")

            finally:
                # Clean up the temporary file
                os.remove(temp_file_path)
        else:
            logging.info(f"Skipping non-JSON file: {blob.name}")

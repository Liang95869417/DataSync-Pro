import os
import json
import logging
import tempfile
from google.cloud import bigquery
from google.cloud import storage
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.api_core.exceptions import GoogleAPIError

CHUNK_SIZE = 100

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

def validate_json_line(line):
    try:
        json_data = json.loads(line)
        return json.dumps(convert_comma_to_dot_in_json([json_data])[0])
    except (json.JSONDecodeError, ValueError) as e:
        logging.error(f"Invalid JSON line: {line} - Error: {e}")
        return None

def preprocess_blob(blob):
    try:
        content = blob.download_as_string().decode('utf-8')
        lines = content.splitlines()
        processed_lines = [validate_json_line(line) for line in lines]
        return [line for line in processed_lines if line is not None]
    except Exception as e:
        logging.error(f"Error processing blob {blob.name}: {e}")
        return []

def load_chunk_to_bigquery(client, dataset_id, table_id, chunk):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.ndjson') as temp_file:
        temp_file_path = temp_file.name
        temp_file.write('\n'.join(chunk).encode('utf-8'))

    try:
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

        logging.info(f"Loaded chunk into {dataset_id}.{table_id}")

    except GoogleAPIError as e:
        logging.error(f"Failed to load data from {temp_file_path} into {table_id}: {e}")

    finally:
        os.remove(temp_file_path)

def load_json_to_bigquery(bucket_name: str, source_prefix: str, dataset_id: str):
    client = bigquery.Client()
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_prefix)

    with ThreadPoolExecutor(max_workers=4) as executor: 
        futures = []

        for blob in blobs:
            logging.info(f"Found blob: {blob.name}")
            if blob.name.endswith('.ndjson'):
                table_name = os.path.splitext(os.path.basename(blob.name))[0]
                table_id = f"{dataset_id}.{table_name}"

                processed_lines = preprocess_blob(blob)

                # Split processed lines into chunks and load them concurrently
                for i in range(0, len(processed_lines), CHUNK_SIZE):
                    chunk = processed_lines[i:i + CHUNK_SIZE]
                    futures.append(executor.submit(load_chunk_to_bigquery, client, dataset_id, table_id, chunk))
            else:
                logging.info(f"Skipping non-JSON file: {blob.name}")

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Chunk processing failed: {e}")


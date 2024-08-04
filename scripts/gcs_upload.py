import logging
import os
from google.cloud import storage

def upload_to_gcs(local_file_path: str, gcs_path: str):
    logging.info(f'Starting upload of {local_file_path} to GCS at {gcs_path}')
    try:
        client = storage.Client()
        bucket = client.get_bucket('ingested-data-1')
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_file_path)
        logging.info(f'Successfully uploaded {local_file_path} to GCS at {gcs_path}')

        # Delete the local file after successful upload
        os.remove(local_file_path)
        logging.info(f'Successfully deleted local file {local_file_path}')
    except Exception as e:
        logging.error(f'Failed to upload {local_file_path} to GCS: {e}')

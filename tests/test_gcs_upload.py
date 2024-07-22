from google.cloud import storage
import logging

def test_upload_to_gcs():
    logging.info('Starting GCS upload test')
    try:
        client = storage.Client()
        bucket = client.get_bucket('ingested-data-1')
        blob = bucket.blob('test_file.txt')
        blob.upload_from_string('This is a test file.')
        logging.info('Successfully uploaded test file to GCS')
    except Exception as e:
        logging.error(f'Failed to upload test file to GCS: {e}')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_upload_to_gcs()
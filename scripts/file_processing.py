import os
from google.cloud import storage


## TODO: Discuss with Torben and David, what data formats will we accept from file upload?
def process_uploaded_files():
    client = storage.Client()
    bucket = client.get_bucket('your_bucket_name')

    # Assuming files are uploaded to a specific directory in GCS
    blobs = bucket.list_blobs(prefix='uploaded_files/')
    for blob in blobs:
        if blob.name.endswith('.csv'):
            # Download and process CSV file
            download_path = f'/tmp/{os.path.basename(blob.name)}'
            blob.download_to_filename(download_path)
            # Add file processing logic here
            process_csv(download_path)
        
def process_csv(file_path):
    # Implement CSV processing logic here
    pass
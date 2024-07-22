import requests
import json
import logging

def ingest_api_data(api_key: str):
    logging.info(f"the api key for kassal is {api_key}")
    url = f"https://kassal.app/api/v1/products"
    headers = {'Authorization': f'Bearer {api_key}'}
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        logging.error(f'Failed to fetch data from API. Status code: {response.status_code}')
        return
    data = response.json()
    # Save data locally as JSON
    local_path = '/tmp/kassal_api_data.json'
    with open(local_path, 'w') as f:
        json.dump(data, f)
    
    logging.info(f'Data fetched from kassal: {data}')
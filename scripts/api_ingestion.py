import requests
import json
import logging
from typing import List, Union, Optional
from airflow.models import Variable
from google.cloud import storage

# Get API keys from Airflow variables
api_key = Variable.get('KASSAL_API_KEY', default_var=None)
client_id = Variable.get('VDA_CLIENT_ID', default_var=None)
client_secret = Variable.get('VDA_CLIENT_SECRET', default_var=None)


def save_to_ndjson(path: str, data: Union[dict, List[dict]]) -> None:
    with open(path, 'w') as f:
        if isinstance(data, dict):
            f.write(json.dumps(data) + '\n')
        elif isinstance(data, list):
            for record in data:
                f.write(json.dumps(record) + '\n')

### get data from kassal
def get_kassal_repsonse(url: str, params: dict = {}) -> Optional[dict]:
    headers = {'Authorization': f'Bearer {api_key}'}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        logging.error(f'Failed to fetch data from API. Status code: {response.status_code}')
        return
    else:
        return response.json()

def ingest_kassal_product_data() -> None:
    url = f"https://kassal.app/api/v1/products"
    params = {"size": 100, "sort": "date_desc"}
    response = get_kassal_repsonse(url, params)

    if response:
        local_path = '/tmp/kassal_product_data_test.ndjson'
        save_to_ndjson(local_path, response["data"])
        logging.info(f'Data fetched from Kassal product endpoint: {response}')
    else:
        logging.error('Failed to fetch Kassal product data')

def ingest_kassal_store_data() -> None:
    url = f"https://kassal.app/api/v1/physical-stores"
    params = {
            'group': "MENY_NO",
            'page': 1,
            'size': 100
        }
    response = get_kassal_repsonse(url, params)

    if response:
        local_path = '/tmp/kassal_store_data_test.ndjson'
        save_to_ndjson(local_path, response["data"])
        logging.info(f'Data fetched from Kassal store endpoint: {response}')
    else:
        logging.error('Failed to fetch Kassal store data')


### get data from VDA
def get_access_token() -> str:
    url = 'https://login.windows.net/trades.no/oauth2/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'resource': 'https://trades.no/TradesolutionApi',
        'client_secret': client_secret
    }
    response = requests.post(url, data=data)
    if response.status_code != 200:
        logging.error(f'Failed to get access token. Status code: {response.status_code}')
        return None
    return response.json().get('access_token')

def get_gtin_list(api_response: dict) -> List[int]:
    product_data = api_response
    gtin_list = [int(item['ean']) for item in product_data if 'ean' in item] 
    return gtin_list

def get_VDA_response(gtin: int) -> Optional[dict]:
    url = 'https://api.vetduat.no/api/products/usersearch'
    token = get_access_token()
    headers = {'Authorization': f'Bearer {token}'}

    payload = {'GTIN': gtin}
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code != 200:
        logging.error(f'Failed to fetch VDA data for GTIN {gtin}. Status code: {response.status_code}')
        return None
    return response.json()

def ingest_VDA_product_data() -> None:
    try:
        with open("/tmp/kassal_product_data_test.ndjson", "r") as f:
            kassal_product_data = [json.loads(line) for line in f]
    except Exception as e:
        logging.error(f'Failed to load Kassal product data: {e}')
        return
    
    gtin_list = get_gtin_list(kassal_product_data)
    results = []

    for gtin in gtin_list:
        response = get_VDA_response(gtin)
        if response:
            results.extend(response)
            logging.info(f"Processed following GTIN: {gtin}")
            logging.info(f"Got response data: {response}")

    local_path = '/tmp/vda_product_data_test.ndjson'
    save_to_ndjson(local_path, results)

import requests
import json
import logging
from typing import Set, Optional
from airflow.models import Variable
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential
from scripts.utils import save_to_ndjson

# Get credentials from Airflow variables
client_id = Variable.get('VDA_CLIENT_ID', default_var=None)
client_secret = Variable.get('VDA_CLIENT_SECRET', default_var=None)

def convert_comma_to_dot_in_json(data_entry: dict):
    if 'mengde' in data_entry and isinstance(data_entry['mengde'], str):
        try:
            # Replace comma with dot and convert to float
            data_entry['mengde'] = float(data_entry['mengde'].replace(',', '.'))
        except ValueError:
            logging.error(f"Invalid mengde value: {data_entry['mengde']}")
            data_entry['mengde'] = None  # or handle this case as needed
    return data_entry

### get data from VDA
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
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

def get_gtin_list(api_response: dict) -> Set[int]:
    product_data = api_response
    gtin_list = {int(item['ean']) for item in product_data if 'ean' in item and item['ean']} 
    return gtin_list

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10))
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

def ingest_VDA_product_data(kassal_temp_file: str, vda_temp_file: str) -> None:
    try:
        with open(kassal_temp_file, "r") as f:
            kassal_product_data = [json.loads(line) for line in f]
    except Exception as e:
        logging.error(f'Failed to load Kassal product data: {e}')
        return
    
    gtin_list = get_gtin_list(kassal_product_data)
    results = []

    max_workers = 5  

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_gtin = {executor.submit(get_VDA_response, gtin): gtin for gtin in gtin_list}
        for future in as_completed(future_to_gtin):
            gtin = future_to_gtin[future]
            try:
                response = future.result()
                if response:
                    response_with_dot = convert_comma_to_dot_in_json(response)
                    results.extend(response_with_dot)
                    logging.info(f"Processed GTIN: {gtin}")
                    logging.info(f"Got response data: {response}")
            except Exception as e:
                logging.error(f'Error processing GTIN {gtin}: {e}')

    save_to_ndjson(vda_temp_file, results)


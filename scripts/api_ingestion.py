import requests
import json
import logging
from typing import List, Union, Optional
from airflow.models import Variable
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential

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
def get_kassal_response(url: str, params: dict = {}) -> Optional[dict]:
    headers = {'Authorization': f'Bearer {api_key}'}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f'Failed to fetch data from API. Error: {e}')
        return None

def ingest_kassal_product_data_all() -> None:
    all_products: List[dict] = []
    url = f"https://kassal.app/api/v1/products?page=1"
    params = {"size": 100, "sort": "date_desc"}
    while url:
        response = get_kassal_response(url, params)
        if not response:
            break
        product_info = response.get('data', [])
        all_products.extend(product_info)
        url = response['links'].get('next')

    if all_products:
        local_path = '/tmp/kassal_product_data_all.ndjson'
        save_to_ndjson(local_path, all_products)
        logging.info(f'Data fetched from Kassal product endpoint: {response}')
    else:
        logging.error('Failed to fetch Kassal product data')

def ingest_kassal_store_data_all() -> None:
    all_stores: List[dict] = []
    base_url = "https://kassal.app/api/v1/physical-stores"
    groups = [
    "MENY_NO", "SPAR_NO", "JOKER_NO", "ODA_NO", "ENGROSSNETT_NO", "NAERBUTIKKEN",
    "BUNNPRIS", "KIWI", "REMA_1000", "EUROPRIS_NO", "HAVARISTEN", "HOLDBART",
    "FUDI", "COOP_NO", "COOP_MARKED", "MATKROKEN", "COOP_MEGA", "COOP_PRIX",
    "COOP_OBS", "COOP_EXTRA", "COOP_BYGGMIX", "COOP_OBS_BYGG", "COOP_ELEKTRO",
    "ARK", "NORLI", "ADLIBRIS"
]
    for group in groups:
        url = f"{base_url}?page=1"
        while url:
            params = {
                'group': group,
                'size': '100'
            }
            response = get_kassal_response(url, params)
            if not response:
                break
            store_info = response.get('data', [])
            all_stores.extend(store_info)
            url = response['links'].get('next')

    if all_stores:
        local_path = 'kassal_store_data.ndjson'
        save_to_ndjson(local_path, all_stores)
        logging.info(f'Data fetched from Kassal store endpoint: {all_stores}')
    else:
        logging.error('Failed to fetch Kassal store data')


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

def get_gtin_list(api_response: dict) -> List[int]:
    product_data = api_response
    gtin_list = [int(item['ean']) for item in product_data if 'ean' in item and item['ean']] 
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

def ingest_VDA_product_data_all() -> None:
    try:
        with open("/tmp/kassal_product_data.ndjson", "r") as f:
            kassal_product_data = [json.loads(line) for line in f]
    except Exception as e:
        logging.error(f'Failed to load Kassal product data: {e}')
        return
    
    gtin_list = get_gtin_list(kassal_product_data)
    results = []

    max_workers = 10  # Set the number of workers based on machine's capability and API rate limits

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_gtin = {executor.submit(get_VDA_response, gtin): gtin for gtin in gtin_list}
        for future in as_completed(future_to_gtin):
            gtin = future_to_gtin[future]
            try:
                response = future.result()
                if response:
                    results.extend(response)
                    logging.info(f"Processed GTIN: {gtin}")
                    logging.info(f"Got response data: {response}")
            except Exception as e:
                logging.error(f'Error processing GTIN {gtin}: {e}')

    local_path = '/tmp/vda_product_data.ndjson'
    save_to_ndjson(local_path, results)

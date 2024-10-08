import requests
import logging
from typing import List, Optional
from datetime import datetime, timezone
from airflow.models import Variable
from scripts.utils import save_to_ndjson

# Get API keys from Airflow variables
api_key = Variable.get('KASSAL_API_KEY', default_var=None)

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

def ingest_kassal_product_data_since(cutoff_date: datetime, temp_file: str) -> None:
    new_products: List[dict] = []
    url = f"https://kassal.app/api/v1/products?page=1"
    params = {"size": 100, "sort": "date_desc"}
    more_data = True
    logging.info(f"Start to extract latest data since {cutoff_date}")

    while url and more_data:
        response = get_kassal_response(url, params)
        if not response:
            break
        product_info = response.get('data', [])
        # Filter products by cutoff date and stop if all products are older
        for product in product_info:
            product_created_at = datetime.fromisoformat(product['created_at']).date()
            if product_created_at >= cutoff_date:
                logging.info(f"Fetched data from {product_created_at}")
                new_products.append(product)
            else:
                more_data = False
                break
        
        url = response['links'].get('next')

    if new_products:
        save_to_ndjson(temp_file, new_products)
        logging.info(f'Data fetched and filtered from Kassal product endpoint: {len(new_products)} records')
    else:
        logging.error('No new data to fetch since the cutoff date')

def ingest_kassal_product_data() -> None:
    url = f"https://kassal.app/api/v1/products?page=1"
    params = {"size": 100, "sort": "date_desc"}
    response = get_kassal_response(url, params)
    if response:
        product_info = response.get('data', [])
        temp_file = '/tmp/kassal_product_data_test.ndjson'
        save_to_ndjson(temp_file, product_info)
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
        temp_file = '/tmp/kassal_store_data.ndjson'
        save_to_ndjson(temp_file, all_stores)
        logging.info(f'Data fetched from Kassal store endpoint: {all_stores}')
    else:
        logging.error('Failed to fetch Kassal store data')

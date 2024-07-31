import json
from typing import List, Union
from google.cloud import bigquery


def save_to_ndjson(path: str, data: Union[dict, List[dict]]) -> None:
    with open(path, 'w') as f:
        if isinstance(data, dict):
            f.write(json.dumps(data) + '\n')
        elif isinstance(data, list):
            for record in data:
                f.write(json.dumps(record) + '\n')

kassal_product_schema = [
    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("nutrition", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("store", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("logo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("allergens", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("contains", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("code", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("current_price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("current_unit_price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_history", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("vendor", "STRING", mode="NULLABLE"),  
    bigquery.SchemaField("ingredients", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("labels", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("icon", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("png", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("svg", "STRING", mode="NULLABLE"),
        ]),
        bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("about", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("year_established", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("organization", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("alternative_names", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("display_name", "STRING", mode="NULLABLE"),
    ]), 
    bigquery.SchemaField("category", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("depth", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("image", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("weight_unit", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("weight", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ean", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("brand", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE")
]

kassal_store_schema = [
    bigquery.SchemaField("openingHours", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("sunday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("friday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("thursday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("saturday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("monday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("wednesday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("tuesday", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("detailUrl", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("fax", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("group", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("logo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("position", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("lng", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("lat", "FLOAT", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("website", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("id", "INTEGER", mode="NULLABLE"),
]

vda_product_schema = [
    bigquery.SchemaField("deklarasjoner", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("verdi", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("deklarasjon", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("merkeordninger", "STRING", mode="REPEATED"),
    bigquery.SchemaField("bildeUrl", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("allergener", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("verdi", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("allergen", "STRING", mode="NULLABLE"),
    ]),
    bigquery.SchemaField("varegruppenavn", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("informasjonstekst", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("mengdeType", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("mengde", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ingredienser", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("epdNr", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("firmaNavn", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gln", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("sistEndret", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("minimumsTemperaturCelcius", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("gtin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("produksjonsland", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("maksimumsTemperaturCelcius", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("produktnavn", "STRING", mode="NULLABLE"),
]
import json
from typing import List, Union


def save_to_ndjson(path: str, data: Union[dict, List[dict]]) -> None:
    with open(path, 'w') as f:
        if isinstance(data, dict):
            f.write(json.dumps(data) + '\n')
        elif isinstance(data, list):
            for record in data:
                f.write(json.dumps(record) + '\n')
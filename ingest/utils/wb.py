import math

import requests
from fastapi import HTTPException


def fetch_world_bank_page(url: str, page: int):
    paged_url = f"{url}&page={page}"

    try:
        response = requests.get(paged_url, timeout=200)
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail=response.text)

        return response.json()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error API World Bank: {str(e)}")


def fetch_all_world_bank_data(base_url: str):
    first_page = fetch_world_bank_page(base_url, 1)

    if not isinstance(first_page, list) or len(first_page) < 2:
        raise HTTPException(status_code=500, detail="Respuesta inesperada del Banco Mundial")

    metadata = first_page[0]
    data = first_page[1]

    total_pages = metadata.get("pages", 1)

    for p in range(2, total_pages + 1):
        page_json = fetch_world_bank_page(base_url, p)
        if len(page_json) >= 2:
            data.extend(page_json[1])

    return data


def wb_sanitize(obj):
    if isinstance(obj, dict):
        return {k: wb_sanitize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [wb_sanitize(x) for x in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif obj is None:
        return None
    else:
        return obj

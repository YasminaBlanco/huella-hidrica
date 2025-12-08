import os
import boto3
from fastapi import HTTPException
from utils.s3_utils import list_gold_files, read_parquet_from_s3
from utils.cache_utils import cache_get, cache_set

S3_KPI_KEYS = {
    "kpi1": "gold/model/kpi01_climate_water/part-00000-7a6e5092-b758-40fe-9ebc-de04e6d7645e-c000.snappy.parquet",
    "kpi2": "gold/model/kpi02_water_mobility/part-00000-22eabb39-cde4-4a7c-9c27-4bf43bd4fd8a-c000.snappy.parquet",
    "kpi3": "gold/model/kpi03_critical_zones/part-00000-89b8ab06-5f3f-4591-9f53-8d403a133902-c000.snappy.parquet",
    "kpi4": "gold/model/kpi04_weighted_health_risk_index/part-00000-2e71a2a5-c3d3-496f-a977-6c309e31dc88-c000.snappy.parquet",
    "kpi5": "gold/model/kpi05_urban_rural_gap_water/part-00000-8c33a279-b9c4-4740-9768-c5d931f2453b-c000.snappy.parquet",
    "kpi6": "gold/model/kpi06_water_gdp_corr/part-00000-3b9acfb3-9e77-4aa5-a5ef-81b5bdf1a56c-c000.snappy.parquet",
    "kpi7": "gold/model/kpi07_water_sanitation_gap/part-00000-7c893d94-aa92-429f-becf-bc608b29dced-c000.snappy.parquet",
}

def get_gold_files_service():
    cache_key = "gold_files"
    cached = cache_get(cache_key)
    if cached:
        return {"files": cached, "cached": True}
    files = list_gold_files()
    cache_set(cache_key, files, ttl_seconds=300)
    return {"files": files, "cached": False}

def get_gold_data_service(kpi: str, limit: int = None):
    if kpi not in S3_KPI_KEYS:
        raise HTTPException(status_code=400, detail=f"KPI desconocido: {kpi}")
    
    cache_key = f"gold_data:{kpi}:{limit}"
    cached = cache_get(cache_key)
    if cached:
        return {"data": cached, "cached": True}

    df = read_parquet_from_s3(S3_KPI_KEYS[kpi])
    if limit:
        df = df.head(limit)

    data_json = df.to_dict(orient="records")
    cache_set(cache_key, data_json, ttl_seconds=3600)
    return {"data": data_json, "cached": False}

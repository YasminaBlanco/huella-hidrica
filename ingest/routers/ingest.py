# from utils.s3_utils import list_gold_files, read_parquet_from_s3
# from utils.redis_utils import cache_get, cache_set
# from decimal import Decimal

# def convert_decimals(obj):
#     if isinstance(obj, list):
#         return [convert_decimals(x) for x in obj]
#     elif isinstance(obj, dict):
#         return {k: convert_decimals(v) for k, v in obj.items()}
#     elif isinstance(obj, Decimal):
#         return float(obj)
#     else:
#         return obj

# def get_gold_files_service():
#     cache_key = "gold_files"
#     cached = cache_get(cache_key)
#     if cached is not None:
#         return {"files": cached, "cached": True}
#     files = list_gold_files()
#     cache_set(cache_key, files, ttl_seconds=300)
#     return {"files": files, "cached": False}

# def get_gold_data_service(kpi: str, limit: int = None):
#     bucket_keys = {
#         "kpi4":"gold/model/kpi04_health_risk_population/part-00000-f9e797cf-15b2-481f-bd22-d5eb55a6a595-c000.snappy.parquet",
#         "kpi5":"gold/model/kpi05_urban_rural_gap_water/part-00000-aa37e629-ab2b-4d1c-867e-bf7258c709cb-c000.snappy.parquet",
#         "kpi6":"gold/model/kpi06_water_gdp_corr/part-00000-3b9acfb3-9e77-4aa5-a5ef-81b5bdf1a56c-c000.snappy.parquet",
#         "kpi7":"gold/model/kpi07_water_sanitation_gap/part-00000-7c893d94-aa92-429f-becf-bc608b29dced-c000.snappy.parquet",
#     }

#     key = bucket_keys.get(kpi)
#     if not key:
#         return {"error": f"KPI desconocido: {kpi}"}

#     cache_key = f"gold_data:{kpi}:{limit}"
#     cached = cache_get(cache_key)
#     if cached is not None:
#         return {"data": cached, "cached": True}

#     df = read_parquet_from_s3(key)
#     if limit:
#         df = df.head(limit)
#     data_json = df.to_dict(orient="records")
#     data_json = convert_decimals(data_json)
#     cache_set(cache_key, data_json, ttl_seconds=3600)
#     return {"data": data_json, "cached": False}


from datetime import date
from enum import Enum

# routers/ingest_router.py
from fastapi import APIRouter, Query

from services.ingest_svc import (
    ingest_jmp_service,
    ingest_open_meteo_service,
    ingest_open_meteo_simple_service,
    ingest_world_bank_service,
)

router = APIRouter(
    prefix="/ingest",
    tags=["Data Ingestion"],
)


# Enum para países LATAM
class LatamCountry(str, Enum):
    ARG = "ARG"
    BOL = "BOL"
    BRA = "BRA"
    CHL = "CHL"
    COL = "COL"
    CRI = "CRI"
    CUB = "CUB"
    DOM = "DOM"
    ECU = "ECU"
    SLV = "SLV"
    GTM = "GTM"
    HND = "HND"
    MEX = "MEX"
    NIC = "NIC"
    PAN = "PAN"
    PRY = "PRY"
    PER = "PER"
    URY = "URY"
    VEN = "VEN"


@router.get("/world-bank", summary="Ingesta de datos del World Bank")
def ingest_world_bank(
    countries: str = Query(..., description="Ej: ARG,BRA,CHL"),
    indicators: str = Query(..., description="Ej: NY.GDP.PCAP.CD,SP.POP.TOTL"),
    start_year: int = Query(..., description="Año inicio (YYYY)"),
    end_year: int = Query(..., description="Año fin (YYYY)"),
    isTest: bool = Query(False, description="Test conexión"),
):
    return ingest_world_bank_service(
        countries=countries,
        indicators=indicators,
        start_year=start_year,
        end_year=end_year,
        isTest=isTest,
    )


@router.get("/open-meteo-simple", summary="Ingesta de datos de Open-Meteo Simple")
def ingest_open_meteo_simple(
    country: LatamCountry = Query(..., description="Código ISO3 del país LATAM"),
    latitude: float = Query(..., ge=-90, le=90, description="Latitud (-90 a 90)"),
    longitude: float = Query(..., ge=-180, le=180, description="Longitud (-180 a 180)"),
    start_date: date = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Fecha fin (YYYY-MM-DD)"),
    timezone_utc: bool = Query(False, description="Timezone (por defecto UTC)"),
    isTest: bool = Query(False, description="Test conexión"),
):
    return ingest_open_meteo_simple_service(
        country=country,
        latitude=latitude,
        longitude=longitude,
        start_date=start_date,
        end_date=end_date,
        timezone_utc=timezone_utc,
        isTest=isTest,
    )


@router.get("/open-meteo", summary="Ingesta de datos de Open-Meteo por provincias")
async def ingest_open_meteo(
    country: LatamCountry = Query(..., description="Código ISO3 del país LATAM"),
    start_date: date = Query(..., description="Fecha inicio, formato YYYY-MM-DD"),
    end_date: date = Query(..., description="Fecha fin, formato YYYY-MM-DD"),
    timezone_utc: bool = Query(False, description="Timezone (por defecto UTC)"),
    isTest: bool = Query(False, description="Test conexión"),
):
    return await ingest_open_meteo_service(
        country=country,
        start_date=start_date,
        end_date=end_date,
        timezone_utc=timezone_utc,
        isTest=isTest,
    )


@router.get("/jmp", summary="Ingesta de datos JMP desde CSV local")
def ingest_jmp(
    csv_path: str = Query(
        "data/jmp_latam_2019_2024.csv", description="Ruta local del archivo JMP csv"
    ),
    isTest: bool = Query(False, description="Test conexión"),
):
    return ingest_jmp_service(
        csv_path=csv_path,
        isTest=isTest,
    )

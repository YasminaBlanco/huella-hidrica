import asyncio
import io
import os
from datetime import date, datetime, timezone
from enum import Enum
from urllib.parse import urlencode

import boto3
import httpx
import pandas as pd
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

from utils.om import om_to_dataframe
from utils.wb import fetch_all_world_bank_data, wb_sanitize  # fetch_world_bank_page,

LATAM_COUNTRIES = [
    "ARG",
    "BOL",
    "BRA",
    "CHL",
    "COL",
    "CRI",
    "CUB",
    "DOM",
    "ECU",
    "SLV",
    "GTM",
    "HND",
    "MEX",
    "NIC",
    "PAN",
    "PRY",
    "PER",
    "URY",
    "VEN",
]

COUNTRY_TIMEZONE = {
    "ARG": "America/Argentina/Buenos_Aires",
    "BOL": "America/La_Paz",
    "BRA": "America/Sao_Paulo",
    "CHL": "America/Santiago",
    "COL": "America/Bogota",
    "CRI": "America/Costa_Rica",
    "CUB": "America/Havana",
    "DOM": "America/Santo_Domingo",
    "ECU": "America/Guayaquil",
    "SLV": "America/El_Salvador",
    "GTM": "America/Guatemala",
    "HND": "America/Tegucigalpa",
    "MEX": "America/Mexico_City",
    "NIC": "America/Managua",
    "PAN": "America/Panama",
    "PRY": "America/Asuncion",
    "PER": "America/Lima",
    "URY": "America/Montevideo",
    "VEN": "America/Caracas",
}

COUNTRY_PROVINCES_MAPPING = {
    "ARG": {
        "Buenos Aires": {"latitude": -36.5, "longitude": -60.0},
        "Catamarca": {"latitude": -28.5, "longitude": -65.8},
        "Chaco": {"latitude": -27.0, "longitude": -59.0},
        "Chubut": {"latitude": -43.3, "longitude": -65.1},
        "Córdoba": {"latitude": -31.4, "longitude": -64.2},
        "Corrientes": {"latitude": -28.9, "longitude": -58.0},
        "Entre Ríos": {"latitude": -31.6, "longitude": -60.7},
        "Formosa": {"latitude": -26.2, "longitude": -58.2},
        "Jujuy": {"latitude": -23.7, "longitude": -65.3},
        "La Pampa": {"latitude": -36.5, "longitude": -65.0},
        "La Rioja": {"latitude": -29.4, "longitude": -66.8},
        "Mendoza": {"latitude": -32.9, "longitude": -68.8},
        "Misiones": {"latitude": -27.5, "longitude": -55.9},
        "Neuquén": {"latitude": -38.9, "longitude": -68.1},
        "Río Negro": {"latitude": -39.8, "longitude": -67.5},
        "Salta": {"latitude": -24.8, "longitude": -65.4},
        "San Juan": {"latitude": -31.5, "longitude": -68.5},
        "San Luis": {"latitude": -33.3, "longitude": -66.3},
        "Santa Cruz": {"latitude": -50.9, "longitude": -68.6},
        "Santa Fe": {"latitude": -31.6, "longitude": -60.7},
        "Santiago del Estero": {"latitude": -27.8, "longitude": -64.3},
        "Tierra del Fuego": {"latitude": -54.8, "longitude": -67.6},
        "Tucumán": {"latitude": -26.8, "longitude": -65.2},
    },
    "MEX": {
        "Aguascalientes": {"latitude": 21.8760, "longitude": -102.2960},
        "Baja California": {"latitude": 30.8406, "longitude": -115.2838},
        "Baja California Sur": {"latitude": 26.0444, "longitude": -111.6661},
        "Campeche": {"latitude": 19.8301, "longitude": -90.5349},
        "Coahuila": {"latitude": 27.0587, "longitude": -101.7068},
        "Colima": {"latitude": 19.1223, "longitude": -103.9160},
        "Chiapas": {"latitude": 16.7569, "longitude": -93.1292},
        "Chihuahua": {"latitude": 28.6320, "longitude": -106.0691},
        "Ciudad de México": {"latitude": 19.4326, "longitude": -99.1332},
        "Durango": {"latitude": 24.5593, "longitude": -104.6588},
        "Guanajuato": {"latitude": 21.0180, "longitude": -101.2590},
        "Guerrero": {"latitude": 17.5390, "longitude": -99.5451},
        "Hidalgo": {"latitude": 20.4590, "longitude": -98.8804},
        "Jalisco": {"latitude": 20.6597, "longitude": -103.3496},
        "México": {"latitude": 19.3257, "longitude": -99.6760},
        "Michoacán": {"latitude": 19.5665, "longitude": -101.7068},
        "Morelos": {"latitude": 18.6813, "longitude": -99.1013},
        "Nayarit": {"latitude": 21.7514, "longitude": -104.8455},
        "Nuevo León": {"latitude": 25.5922, "longitude": -99.9962},
        "Oaxaca": {"latitude": 17.0732, "longitude": -96.7266},
        "Puebla": {"latitude": 19.0460, "longitude": -98.2063},
        "Querétaro": {"latitude": 20.5888, "longitude": -100.3899},
        "Quintana Roo": {"latitude": 19.1817, "longitude": -88.4791},
        "San Luis Potosí": {"latitude": 22.1565, "longitude": -100.9855},
        "Sinaloa": {"latitude": 25.1721, "longitude": -107.4795},
        "Sonora": {"latitude": 29.1026, "longitude": -110.9773},
        "Tabasco": {"latitude": 17.8409, "longitude": -92.6189},
        "Tamaulipas": {"latitude": 24.2669, "longitude": -98.8363},
        "Tlaxcala": {"latitude": 19.3182, "longitude": -98.2378},
        "Veracruz": {"latitude": 19.4000, "longitude": -96.8500},
        "Yucatán": {"latitude": 20.7099, "longitude": -89.0943},
        "Zacatecas": {"latitude": 22.7709, "longitude": -102.5833},
    },
}

WB_DEFAULT_INDICATORS = [
    "NY.GDP.PCAP.CD",  # PIB per cápita
    "SP.POP.TOTL",  # Población total
    "SH.STA.BASS.ZS",  # Acceso a saneamiento básico
    "SH.H2O.BASW.ZS" "SI.POV.DDAY" "SH.DYN.MORT",  # Acceso a agua básica
]


BASE_WB_URL = "http://api.worldbank.org/v2"  # url base wb
BASE_OM_URL = "https://archive-api.open-meteo.com/v1/archive"  # url base om

S3_BRONZE_PREFIX = "bronze"

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

LatamCountry = Enum("LatamCountry", {country: country for country in LATAM_COUNTRIES})

app = FastAPI(
    title="API - Agua Latam",
    description="Endpoints para extraer datos y cargarlos en S3",
)

try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )
except Exception as e:
    raise RuntimeError(f"No se pudo inicializar el cliente S3: {e}")


def upload_to_s3(data_bytes, key, content_type="application/octet-stream", metadata=None):
    try:
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=key,
            Body=data_bytes,
            ContentType=content_type,
            Metadata=metadata or {},
        )
        return f"s3://{S3_BUCKET_NAME}/{key}"
    except Exception as e:
        print(f"Error al subir a S3: {e}")
        raise HTTPException(status_code=500, detail=f"Fallo de S3: {str(e)}")


@app.get("/ingest/world-bank")
def ingest_world_bank_data(
    countries: str = Query(..., description="Ej: ARG,BRA,CHL"),
    indicators: str = Query(..., description="Ej: NY.GDP.PCAP.CD,SP.POP.TOTL"),
    start_year: int = Query(..., description="Año inicio (YYYY)"),
    end_year: int = Query(..., description="Año fin (YYYY)"),
    isTest: bool = Query(False, description="Test conexión"),
):
    if isTest:
        return {
            "status": "success",
        }

    if end_year < start_year:
        raise HTTPException(
            status_code=400,
            detail="end_year no puede ser anterior a start_year",
        )

    country_list = [c.strip().upper() for c in countries.split(",")]
    for c in country_list:
        if c not in LATAM_COUNTRIES:
            raise HTTPException(status_code=400, detail=f"País no válido: {c}")

    indicator_list = [i.strip() for i in indicators.split(",")]

    countries_str = ";".join(country_list)
    indicators_str = ";".join(indicator_list)
    date_range = f"{start_year}:{end_year}"
    endpoint = f"/country/{countries_str}/indicator/{indicators_str}"
    query_params = urlencode({"date": date_range, "format": "json", "per_page": 20000, "source": 2})
    base_url = f"{BASE_WB_URL}{endpoint}?{query_params}"

    try:
        raw_data = fetch_all_world_bank_data(base_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error consultando WB: {e}")

    if not raw_data:
        raise HTTPException(status_code=404, detail="No se encontraron datos")

    df = pd.json_normalize(raw_data)

    df.rename(
        columns={
            "countryiso3code": "country_code",
            "country.value": "country_name",
            "indicator.id": "indicator_code",
            "indicator.value": "indicator_name",
        },
        inplace=True,
    )

    df["date"] = pd.to_numeric(df["date"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df.sort_values(["country_code", "indicator_code", "date"], inplace=True)

    uploaded_files = []

    for country in country_list:
        country_df = df[df["country_code"] == country]
        if country_df.empty:
            continue

        for year in country_df["date"].unique():
            year_df = country_df[country_df["date"] == year]

            buf = io.BytesIO()
            year_df.to_parquet(buf, index=False, compression="snappy", engine="pyarrow")
            buf.seek(0)

            file_key = f"{S3_BRONZE_PREFIX}/world_bank/country={country}/year={year}/world_bank.parquet"

            s3_path = upload_to_s3(
                buf.getvalue(),
                file_key,
                content_type="application/octet-stream",
                metadata={
                    "country": country,
                    "source": "world-bank",
                    "layer": "bronze",
                },
            )
            uploaded_files.append(s3_path)

    records = df.to_dict(orient="records")
    wb_sanitized_records = wb_sanitize(records)

    return {
        "status": "success",
        "records": len(wb_sanitized_records),
        "countries": countries.split(","),
        "indicators": indicators.split(","),
        "years": list(range(start_year, end_year + 1)),
        "files_uploaded": uploaded_files,
        "total_files": len(uploaded_files),
        "sample": wb_sanitized_records,
    }


@app.get("/ingest/open-meteo-simple")
def ingest_open_meteo_simple(
    country: LatamCountry = Query(..., description="Código ISO3 del país LATAM"),
    latitude: float = Query(..., ge=-90, le=90, description="Latitud (-90 a 90)"),
    longitude: float = Query(..., ge=-180, le=180, description="Longitud (-180 a 180)"),
    start_date: date = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Fecha fin (YYYY-MM-DD)"),
    timezone_utc: bool = Query(False, description="Timezone (por defecto UTC)"),
    isTest: bool = Query(False, description="Test conexión"),
):
    if isTest:
        return {
            "status": "success",
        }

    if end_date < start_date:
        raise HTTPException(
            status_code=400,
            detail="end_date no puede ser anterior a start_date",
        )

    query_params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum,et0_fao_evapotranspiration",
        # "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,et0_fao_evapotranspiration",
        "timezone": ("UTC" if timezone_utc else COUNTRY_TIMEZONE.get(country, "UTC")),
    }

    country = country.value
    try:
        response = requests.get(BASE_OM_URL, params=query_params)
        response.raise_for_status()

        data = response.json()
        df = om_to_dataframe(data, country)

        df["date"] = pd.to_datetime(df["date"])
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

        uploaded_files = []

        for (year, month), group_df in df.groupby(["year", "month"]):
            buf = io.BytesIO()
            group_df.drop(columns=["year", "month"], inplace=True)
            group_df.to_parquet(buf, index=False, compression="snappy")

            file_key = f"{S3_BRONZE_PREFIX}/open_meteo/{country}/year={year}/month={month}/weather.parquet"

            s3_path = upload_to_s3(
                buf.getvalue(),
                file_key,
                content_type="application/octet-stream",
                metadata={
                    "country": country,
                    "source": "open-meteo",
                    "layer": "bronze",
                },
            )
            uploaded_files.append(s3_path)

    except requests.exceptions.HTTPError as e:
        raise HTTPException(status_code=response.status_code, detail=str(e))
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error de conexión: {e}")
    except (ValueError, KeyError) as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error procesando la respuesta de Open-Meteo: {e}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Ocurrió un error inesperado",
            detail=f"Error: {e}",
        )

    return {
        "status": "success",
        "country": country,
        "rows": len(df),
        "data": df.to_dict(orient="records"),
    }


semaphore = asyncio.Semaphore(5)


async def fetch_province(client, prov_name, lat, lon, start_date, end_date, timezone):
    async with semaphore:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,et0_fao_evapotranspiration",
            "timezone": timezone,
        }
        try:
            resp = await client.get(BASE_OM_URL, params=params, timeout=20)
            if resp.status_code == 200:
                return prov_name, resp.json()
            else:
                return prov_name, {"error": resp.text}
        except Exception as e:
            return prov_name, {"error": str(e)}


def weather_response_to_dataframe(response: dict) -> pd.DataFrame:
    rows = []

    for province, pdata in response["data"].items():
        daily = pdata["daily"]
        dates = daily["time"]

        for i, dt in enumerate(dates):
            rows.append(
                {
                    "province": province,
                    "date": dt,
                    "temperature_2m_max": daily["temperature_2m_max"][i],
                    "temperature_2m_min": daily["temperature_2m_min"][i],
                    "precipitation_sum": daily["precipitation_sum"][i],
                    "et0_fao_evapotranspiration": daily["et0_fao_evapotranspiration"][i],
                }
            )

    return pd.DataFrame(rows)


@app.get("/ingest/open-meteo")
async def ingest_open_meteo(
    country: LatamCountry = Query(..., description="Código ISO3 del país LATAM"),
    start_date: date = Query(..., description="Fecha inicio, formato YYYY-MM-DD"),
    end_date: date = Query(..., description="Fecha fin, formato YYYY-MM-DD"),
    timezone_utc: bool = Query(False, description="Timezone (por defecto UTC)"),
    isTest: bool = Query(False, description="Test conexión"),
):
    if isTest:
        return {
            "status": "success",
        }

    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_province(
                client,
                prov,
                info["latitude"],
                info["longitude"],
                start_date,
                end_date,
                ("UTC" if timezone_utc else COUNTRY_TIMEZONE.get(country, "UTC")),
            )
            for prov, info in COUNTRY_PROVINCES_MAPPING[country.value].items()
        ]
        results = await asyncio.gather(*tasks)

    df = weather_response_to_dataframe({"data": dict(results)})

    if df.empty:
        raise HTTPException(status_code=404, detail="No se encontraron datos de Open-Meteo")

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    uploaded_files = []

    for prov in df["province"].unique():
        prov_df = df[df["province"] == prov]
        for y in prov_df["year"].unique():
            for m in prov_df[prov_df["year"] == y]["month"].unique():
                partition_df = prov_df[(prov_df["year"] == y) & (prov_df["month"] == m)].copy()

                buf = io.BytesIO()
                partition_df.to_parquet(buf, index=False, compression="snappy")

                file_key = (
                    f"{S3_BRONZE_PREFIX}/open_meteo/{country.value}/province={prov}/year={y}/month={m}/data.parquet"
                )

                s3_path = upload_to_s3(
                    buf.getvalue(),
                    file_key,
                    content_type="application/octet-stream",
                    metadata={
                        "country": country.value,
                        "province": prov.encode("ascii", errors="ignore").decode(),
                        "source": "open-meteo",
                        "layer": "bronze",
                    },
                )
                uploaded_files.append(s3_path)

    return {
        "status": "success",
        "country": country.value,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "rows_ingested": len(df),
        "uploaded_files": uploaded_files,
    }


@app.get("/ingest/jmp")
def ingest_jmp_data(
    csv_path: str = Query(
        "data/jmp_latam_2019_2024.csv",
        description="ruta local del archivo JMP csv",
    ),
    isTest: bool = Query(False, description="Test conexión"),
):
    if isTest:
        return {
            "status": "success",
        }

    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail=f"Archivo CSV no encontrado: {csv_path}")

    try:
        df = pd.read_csv(csv_path)

        required_cols = [
            "ISO3",
            "Country",
            "Residence Type",
            "Service Type",
            "Service level",
            "Year",
            "Coverage",
            "Population",
        ]

        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"El dataset JMP no contiene las columnas requeridas: {missing}",
            )

        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
        df["Coverage"] = pd.to_numeric(df["Coverage"], errors="coerce")
        df["Population"] = pd.to_numeric(df["Population"], errors="coerce")

        uploaded_files = []

        for iso3 in df["ISO3"].unique():
            country_df = df[df["ISO3"] == iso3]

            for year in country_df["Year"].unique():
                partition_df = country_df[country_df["Year"] == year].copy()

                if partition_df.empty:
                    continue

                buf = io.BytesIO()
                partition_df.to_parquet(buf, index=False, compression="snappy")
                buf.seek(0)

                file_key = f"{S3_BRONZE_PREFIX}/jmp/" f"country={iso3}/year={int(year)}/jmp.parquet"

                s3_path = upload_to_s3(
                    buf.getvalue(),
                    key=file_key,
                    content_type="application/octet-stream",
                    metadata={
                        "source": "jmp",
                        "layer": "bronze",
                        "country": iso3,
                    },
                )

                uploaded_files.append(s3_path)

        return {
            "status": "success",
            "rows": len(df),
            "countries_detected": df["ISO3"].unique().tolist(),
            "years_detected": sorted(df["Year"].dropna().unique().tolist()),
            "total_files_uploaded": len(uploaded_files),
            "files": uploaded_files,
            "sample": df.head(5).to_dict(orient="records"),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando JMP: {str(e)}")


@app.get("/health", tags=["Health"])
def health_check():
    service_status = "ok"

    return {
        "status": service_status,
        "service": "Agua Latam API",
        "version": os.getenv("API_VERSION", "1.0.0"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

import io
import os
import asyncio
from datetime import date
from urllib.parse import urlencode

import pandas as pd
import requests
import httpx

from fastapi import HTTPException

from utils.om import om_to_dataframe
from utils.wb import fetch_all_world_bank_data, wb_sanitize
from utils.s3_utils import upload_to_s3
from utils.consts import (
    LATAM_COUNTRIES,
    COUNTRY_TIMEZONE,
    COUNTRY_PROVINCES_MAPPING,
    BASE_WB_URL,
    BASE_OM_URL,
    S3_BRONZE_PREFIX,
)

def ingest_world_bank_service(
    countries: str,
    indicators: str,
    start_year: int,
    end_year: int,
    isTest: bool
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
    query_params = urlencode(
        {"date": date_range, "format": "json", "per_page": 20000, "source": 2}
    )
    base_url = f"{BASE_WB_URL}{endpoint}?{query_params}"

    try:
        raw_data = fetch_all_world_bank_data(base_url)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error consultando WB: {e}"
        )

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
            year_df.to_parquet(
                buf, index=False, compression="snappy", engine="pyarrow"
            )
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


# ============================================================
# 2. OPEN METEO SIMPLE SERVICE
# ============================================================

def ingest_open_meteo_simple_service(
    country,
    latitude: float,
    longitude: float,
    start_date: date,
    end_date: date,
    timezone_utc: bool,
    isTest: bool
):

    if isTest:
        return {"status": "success"}

    if end_date < start_date:
        raise HTTPException(400, "end_date no puede ser anterior a start_date")

    timezone = "UTC" if timezone_utc else COUNTRY_TIMEZONE.get(country.value, "UTC")

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "precipitation_sum,et0_fao_evapotranspiration",
        "timezone": timezone,
    }

    try:
        r = requests.get(BASE_OM_URL, params=params)
        r.raise_for_status()
    except Exception as e:
        raise HTTPException(500, f"Error consultando Open-Meteo: {e}")

    df = om_to_dataframe(r.json(), country.value)

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    uploaded_files = []

    for (year, month), group in df.groupby(["year", "month"]):
        buf = io.BytesIO()
        group.drop(columns=["year", "month"], inplace=True)
        group.to_parquet(buf, index=False, compression="snappy")

        key = f"{S3_BRONZE_PREFIX}/open_meteo/{country.value}/year={year}/month={month}/weather.parquet"
        s3_path = upload_to_s3(buf.getvalue(), key, metadata={"source": "open-meteo"})

        uploaded_files.append(s3_path)

    return {
        "status": "success",
        "country": country.value,
        "rows": len(df),
        "uploaded_files": uploaded_files,
    }


# ============================================================
# 3. OPEN METEO PROVINCES SERVICE
# ============================================================

semaphore = asyncio.Semaphore(5)

async def _fetch_province(client, prov, lat, lon, start_date, end_date, timezone):
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
            r = await client.get(BASE_OM_URL, params=params)
            return prov, r.json()
        except Exception as e:
            return prov, {"error": str(e)}


def _response_to_df(response: dict) -> pd.DataFrame:
    rows = []
    for prov, pdata in response.items():
        daily = pdata["daily"]
        for i, dt in enumerate(daily["time"]):
            rows.append({
                "province": prov,
                "date": dt,
                "temperature_2m_max": daily["temperature_2m_max"][i],
                "temperature_2m_min": daily["temperature_2m_min"][i],
                "precipitation_sum": daily["precipitation_sum"][i],
                "et0_fao_evapotranspiration": daily["et0_fao_evapotranspiration"][i],
            })
    return pd.DataFrame(rows)


async def ingest_open_meteo_service(
    country,
    start_date: date,
    end_date: date,
    timezone_utc: bool,
    isTest: bool
):

    if isTest:
        return {"status": "success"}

    timezone = "UTC" if timezone_utc else COUNTRY_TIMEZONE.get(country.value, "UTC")

    provinces = COUNTRY_PROVINCES_MAPPING[country.value]

    async with httpx.AsyncClient() as client:
        tasks = [
            _fetch_province(
                client,
                prov,
                info["latitude"],
                info["longitude"],
                start_date,
                end_date,
                timezone,
            )
            for prov, info in provinces.items()
        ]
        results = await asyncio.gather(*tasks)

    df = _response_to_df(dict(results))

    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    uploaded_files = []

    for prov in df["province"].unique():
        pdf = df[df["province"] == prov]
        for year in pdf["year"].unique():
            for month in pdf[pdf["year"] == year]["month"].unique():
                chunk = pdf[(pdf["year"] == year) & (pdf["month"] == month)]

                buf = io.BytesIO()
                chunk.to_parquet(buf, index=False, compression="snappy")

                key = f"{S3_BRONZE_PREFIX}/open_meteo/{country.value}/province={prov}/year={year}/month={month}/data.parquet"
                s3_path = upload_to_s3(buf.getvalue(), key, metadata={
                    "source": "open-meteo",
                    "country": country.value,
                    "province": prov,
                })
                uploaded_files.append(s3_path)

    return {
        "status": "success",
        "country": country.value,
        "start_date": start_date,
        "end_date": end_date,
        "rows_ingested": len(df),
        "uploaded_files": uploaded_files,
    }


# ============================================================
# 4. JMP SERVICE
# ============================================================

def ingest_jmp_service(csv_path: str, isTest: bool):

    if isTest:
        return {"status": "success"}

    if not os.path.exists(csv_path):
        raise HTTPException(404, f"Archivo no encontrado: {csv_path}")

    df = pd.read_csv(csv_path)

    required = [
        "ISO3", "Country", "Residence Type", "Service Type",
        "Service level", "Year", "Coverage", "Population",
    ]

    for col in required:
        if col not in df.columns:
            raise HTTPException(400, f"Falta columna requerida: {col}")

    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype("Int64")
    df["Coverage"] = pd.to_numeric(df["Coverage"], errors="coerce")
    df["Population"] = pd.to_numeric(df["Population"], errors="coerce")

    uploaded = []

    for iso3 in df["ISO3"].unique():
        sub = df[df["ISO3"] == iso3]
        for year in sub["Year"].unique():
            chunk = sub[sub["Year"] == year]

            buf = io.BytesIO()
            chunk.to_parquet(buf, index=False, compression="snappy")

            key = f"{S3_BRONZE_PREFIX}/jmp/country={iso3}/year={year}/jmp.parquet"
            s3path = upload_to_s3(buf.getvalue(), key, metadata={"source": "jmp"})
            uploaded.append(s3path)

    return {
        "status": "success",
        "rows": len(df),
        "countries_detected": df["ISO3"].unique().tolist(),
        "years_detected": sorted(df["Year"].dropna().unique().tolist()),
        "uploaded_files": uploaded,
        "sample": df.head(5).to_dict(orient="records"),
    }

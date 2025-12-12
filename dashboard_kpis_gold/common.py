# common.py
#
# Utilidades compartidas para los dashboards de KPIs de huella hídrica.

import streamlit as st
import pandas as pd

# =====================================================================
# Rutas en S3 para cada KPI
# =====================================================================
S3_PATH_KPI01 = "s3://henry-pf-g2-huella-hidrica/gold/model/kpi01_climate_water/"
S3_PATH_KPI02 = "s3://henry-pf-g2-huella-hidrica/gold/model/kpi02_water_mobility/"
S3_PATH_KPI03 = "s3://henry-pf-g2-huella-hidrica/gold/model/kpi03_critical_zones/"
S3_PATH_KPI04 = (
    "s3://henry-pf-g2-huella-hidrica/gold/model/kpi04_weighted_health_risk_index/"
)
S3_PATH_KPI05 = "s3://henry-pf-g2-huella-hidrica/gold/model/kpi05_urban_rural_gap_water/"
S3_PATH_KPI06 = "s3://henry-pf-g2-huella-hidrica/gold/model/kpi06_water_gdp_corr/"
S3_PATH_KPI07 = "s3://henry-pf-g2-huella-hidrica/gold/model/kpi07_water_sanitation_gap/"

# =====================================================================
# Coordenadas de países (para mapas a nivel país, KPI 1 y 2)
# =====================================================================
COUNTRY_COORDS = {
    "Mexico": (23.0, -102.0),
    "Argentina": (-34.0, -64.0),
    "Brazil": (-10.0, -55.0),
    "Chile": (-30.0, -71.0),
    "Peru": (-9.0, -75.0),
    "Colombia": (4.0, -74.0),
    "Ecuador": (-1.5, -78.0),
    "Bolivia (Plurinational State of)": (-16.5, -64.9),
    "Paraguay": (-23.0, -58.0),
    "Uruguay": (-32.5, -56.0),
    "Venezuela (Bolivarian Republic of)": (7.0, -66.0),
    "Guatemala": (15.5, -90.25),
    "Honduras": (15.0, -86.5),
    "El Salvador": (13.8, -88.9),
    "Nicaragua": (12.8, -85.0),
    "Costa Rica": (10.0, -84.0),
    "Panama": (8.5, -80.0),
    "Belize": (17.25, -88.77),
    "Cuba": (21.5, -80.0),
    "Haiti": (19.0, -72.5),
    "Dominican Republic": (19.0, -70.7),
    "Guyana": (5.0, -59.0),
    "Suriname": (4.0, -56.0),
    "Trinidad and Tobago": (10.69, -61.22),
    "Antigua and Barbuda": (17.05, -61.8),
    "Dominica": (15.4, -61.3),
    "Jamaica": (18.1, -77.3),
    "Saint Lucia": (13.9, -60.97),
    "Turks and Caicos Islands": (21.75, -71.58),
    "Falkland Islands (Malvinas)": (-51.8, -59.0),
    "Saint Martin (French Part)": (18.07, -63.08),
    "Cayman Islands": (19.31, -81.25),
    "Saint Barthélemy": (17.9, -62.83),
}

# =====================================================================
# Coordenadas de provincias/estados (para KPI 3)
# =====================================================================

# Estados de México
PROVINCE_COORDS_MX = {
    "Aguascalientes": (21.88, -102.30),
    "Baja California": (30.84, -115.28),
    "Baja California Sur": (25.65, -111.99),
    "Campeche": (19.00, -90.50),
    "Chiapas": (16.50, -93.00),
    "Chihuahua": (28.50, -106.00),
    "Ciudad de México": (19.43, -99.13),
    "Coahuila": (27.30, -101.00),
    "Colima": (19.10, -103.85),
    "Durango": (24.00, -104.60),
    "Guanajuato": (21.00, -101.25),
    "Guerrero": (17.50, -99.50),
    "Hidalgo": (20.50, -98.90),
    "Jalisco": (20.70, -103.30),
    "México": (19.30, -99.60),  # Estado de México
    "Michoacán": (19.50, -101.50),
    "Morelos": (18.77, -99.10),
    "Nayarit": (21.70, -105.20),
    "Nuevo León": (25.50, -99.80),
    "Oaxaca": (17.00, -96.50),
    "Puebla": (19.00, -98.20),
    "Querétaro": (20.80, -99.90),
    "Quintana Roo": (19.60, -88.30),
    "San Luis Potosí": (22.00, -100.90),
    "Sinaloa": (24.00, -107.50),
    "Sonora": (29.00, -110.00),
    "Tabasco": (17.80, -92.60),
    "Tamaulipas": (24.00, -98.50),
    "Tlaxcala": (19.40, -98.20),
    "Veracruz": (19.00, -96.50),
    "Yucatán": (20.70, -89.00),
    "Zacatecas": (23.00, -102.50),
}

# Provincias de Argentina
PROVINCE_COORDS_AR = {
    "Buenos Aires": (-36.50, -60.00),
    "Catamarca": (-27.20, -66.30),
    "Chaco": (-26.50, -60.60),
    "Chubut": (-43.50, -68.50),
    "Córdoba": (-31.40, -64.20),
    "Corrientes": (-28.70, -58.80),
    "Entre Ríos": (-32.00, -59.00),
    "Formosa": (-26.20, -58.20),
    "Jujuy": (-23.20, -65.30),
    "La Pampa": (-37.00, -65.00),
    "La Rioja": (-29.40, -67.00),
    "Mendoza": (-34.60, -68.30),
    "Misiones": (-27.30, -55.90),
    "Neuquén": (-38.90, -70.10),
    "Río Negro": (-40.80, -67.80),
    "Salta": (-24.80, -65.40),
    "San Juan": (-30.90, -68.80),
    "San Luis": (-33.30, -66.40),
    "Santa Cruz": (-48.80, -69.00),
    "Santa Fe": (-31.50, -60.70),
    "Santiago del Estero": (-27.80, -64.30),
    "Tierra del Fuego": (-54.80, -68.30),
    "Tucumán": (-26.90, -65.20),
}

# =====================================================================
# Colores de riesgo (semáforo) para pydeck
# =====================================================================
def risk_color_rgb(risk: str):
    """Colores semáforo (RGB) para usar en pydeck."""
    if risk == "green":
        return [39, 174, 96]
    if risk == "yellow":
        return [242, 201, 76]
    if risk == "red":
        return [229, 57, 53]
    return [180, 180, 180]


# =====================================================================
# Lectura de datos (modelos Gold)
# =====================================================================
@st.cache_data
def load_kpi01() -> pd.DataFrame:
    return pd.read_parquet(S3_PATH_KPI01)


@st.cache_data
def load_kpi02() -> pd.DataFrame:
    return pd.read_parquet(S3_PATH_KPI02)


@st.cache_data
def load_kpi03() -> pd.DataFrame:
    """Zonas críticas clima + saneamiento (provincias MX/AR)."""
    return pd.read_parquet(S3_PATH_KPI03)


@st.cache_data
def load_kpi04() -> pd.DataFrame:
    return pd.read_parquet(S3_PATH_KPI04)


@st.cache_data
def load_kpi05() -> pd.DataFrame:
    return pd.read_parquet(S3_PATH_KPI05)


@st.cache_data
def load_kpi06() -> pd.DataFrame:
    return pd.read_parquet(S3_PATH_KPI06)


@st.cache_data
def load_kpi07() -> pd.DataFrame:
    return pd.read_parquet(S3_PATH_KPI07)


# =====================================================================
# Filtros comunes
# =====================================================================
def year_slider(df: pd.DataFrame, label: str = "Rango de años") -> pd.DataFrame:
    """Devuelve el dataframe filtrado por un slider de años en la barra lateral."""
    years = sorted(df["year"].dropna().unique())
    year_min, year_max = int(min(years)), int(max(years))
    year_range = st.sidebar.slider(
        label,
        min_value=year_min,
        max_value=year_max,
        value=(year_min, year_max),
    )
    return df[(df["year"] >= year_range[0]) & (df["year"] <= year_range[1])]

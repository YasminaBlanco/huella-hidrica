LATAM_COUNTRIES = {
    "ARG", "BOL", "BRA", "CHL", "COL", "CRI", "CUB", "DOM", "ECU",
    "SLV", "GTM", "HND", "MEX", "NIC", "PAN", "PRY", "PER", "URY", "VEN",
}

COUNTRY_TIMEZONE = {
    "ARG": "America/Argentina/Buenos_Aires",
    "BRA": "America/Sao_Paulo",
    "CHL": "America/Santiago",
    "COL": "America/Bogota",
    "MEX": "America/Mexico_City",
    # completar si querés más
}

# Falso ejemplo basado en tu main, reemplazar por coordenadas reales
COUNTRY_PROVINCES_MAPPING = {
    "ARG": {
        "Buenos Aires": {"latitude": -34.6, "longitude": -58.45},
        "Cordoba": {"latitude": -31.4, "longitude": -64.19},
    },
    "BRA": {
        "Sao Paulo": {"latitude": -23.55, "longitude": -46.63},
    }
}

BASE_WB_URL = "https://api.worldbank.org/v2"
BASE_OM_URL = "https://api.open-meteo.com/v1/forecast"

S3_BRONZE_PREFIX = "bronze"

# Water-Ingest API

API construida con **FastAPI** para **ingestar datos** provenientes de distintas fuentes y dejarlos en la **capa bronce**

## Descripción

- API REST para ingestión de datos
- Fuentes de datos:
  - **World Bank**
  - **Open-Meteo**
- Sube los datos a un bucket en **S3**

## Estructura del proyecto

- `main.py` → endpoints 
- `utils/` → funciones utilitarias 
- `requirements.txt` → depdendencias 

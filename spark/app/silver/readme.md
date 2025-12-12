## Silver Layer (Curated) — Modelo relacional y metadatos

La **capa Silver** es la zona “curada” del Data Lake (Arquitectura Medallion) donde consolidamos datos de **clima**, **WASH (agua/saneamiento)** y **World Bank (socioeconómicos)** en un **modelo consistente** (dimensiones conformadas + tablas de hechos) listo para:
- servir como base estable para la **capa Gold (KPIs)**,
- simplificar joins (claves surrogate),
- asegurar tipos de dato, reglas de calidad y granularidades claras.

---

### Diagrama ER (Silver)

> 📌 Actualiza la ruta del archivo.

![ERD Silver](/docs/erd_silver.jpg)

---

## Convenciones de diseño (Silver)

**Formato de almacenamiento**
- Archivos: **Parquet** (columnar, compresión y lectura eficiente).
- Compresión: **Snappy**.
- Organización en S3:
  - Limpieza por fuente: `s3://<bucket>/silver/<fuente>/...`
  - Modelo dimensional: `s3://<bucket>/silver/model/<tabla>/...`

**Particionado (S3 Layout)**
- **Limpieza**
  - `silver/climate_monthly/`: `country_iso3`, `year`, `month`
  - `silver/jmp/`: `country_iso3`, `year`
  - `silver/socioeconomic/`: `country_iso3`, `year`
- **Modelo (`silver/model/`)**
  - **Facts**: particionadas por surrogate keys:
    - Anual (grano país): `country_key`, `date_key`
    - Mensual subnacional: `province_key`, `date_key`
  - **Dims**: no se particionan (se almacenan como Parquet único o pocos `part-*` por ser tablas pequeñas).

**Nombres**
- Tablas y columnas en `snake_case`.
- Claves surrogate: `*_key` (INT) en dimensiones.
- PK técnica de hechos: `*_id` (BIGINT) para unicidad técnica.

**Claves y relaciones**
- Dimensiones (verde): entidades descriptivas y relativamente estables (catálogos).
- Hechos (rojo): métricas numéricas con una **granularidad definida** (anual o mensual).
- Conexión por FKs desde hechos hacia dimensiones (modelo tipo estrella / copo de nieve ligero):
  - `province -> country` para habilitar análisis subnacional con contexto país.


---

## Diccionario de datos (Silver)

> Nota: en este proyecto distinguimos:
> - **Silver Limpieza (por fuente)**: `silver/<fuente>/...` (particionado por `country_iso3` y tiempo).
> - **Silver Model (dimensional)**: `silver/model/<tabla>/...` (facts particionadas por surrogate keys; dims sin particionar).

---

## Dimensiones (Silver Model)

<details>
<summary><strong>dim.country</strong> — Catálogo de países</summary>

**Propósito:** dimensión conformada para estandarizar país y región.  
**Ubicación:** `silver/model/country/` (sin partición)  
**PK:** `country_key` (INT)  
**Grano:** 1 fila por país

| Columna | Tipo | Descripción |
|---|---|---|
| country_key | INT | PK surrogate del país |
| country_iso3 | CHAR(3) | Código ISO3 del país |
| country_name | VARCHAR(100) | Nombre del país |
| region_name | VARCHAR(100) | Región (ej. “Latin America & Caribbean”) |

</details>

<details>
<summary><strong>dim.province</strong> — Catálogo de provincias/estados</summary>

**Propósito:** habilitar clima mensual a nivel subnacional.  
**Ubicación:** `silver/model/province/` (sin partición)  
**PK:** `province_key` (INT)  
**FK:** `country_key -> dim.country.country_key`  
**Grano:** 1 fila por provincia/estado

| Columna | Tipo | Descripción |
|---|---|---|
| province_key | INT | PK surrogate de provincia/estado |
| country_key | INT | FK al país |
| province_name | VARCHAR(150) | Nombre de provincia/estado |

</details>

<details>
<summary><strong>dim.date</strong> — Dimensión calendario</summary>

**Propósito:** unificar el tiempo para joins y agregaciones.  
**Ubicación:** `silver/model/date/` (sin partición)  
**PK:** `date_key` (INT)  
**Grano:** 1 fila por fecha

| Columna | Tipo | Descripción |
|---|---|---|
| date_key | INT | PK surrogate de fecha (ancla anual o fin de mes, según el caso) |
| date | DATE | Fecha calendario |
| year | INT | Año |
| month | INT | Mes (1–12) |
| month_name | VARCHAR(20) | Nombre del mes |
| quarter | INT | Trimestre (1–4) |

</details>

<details>
<summary><strong>dim.residence_type</strong> — Urbano/Rural</summary>

**Propósito:** dimensionar cobertura WASH por tipo de residencia.  
**Ubicación:** `silver/model/residence_type/` (sin partición)  
**PK:** `residence_type_key` (INT)  
**Grano:** 1 fila por tipo (p.ej. Urban/Rural)

| Columna | Tipo | Descripción |
|---|---|---|
| residence_type_key | INT | PK surrogate |
| residence_type_code | VARCHAR(10) | Código (ej. `URB`, `RUR`) |
| residence_type_desc | VARCHAR(50) | Descripción |

</details>

<details>
<summary><strong>dim.service_type</strong> — Tipo de servicio WASH</summary>

**Propósito:** separar agua / saneamiento / higuiene  
**Ubicación:** `silver/model/service_type/` (sin partición)  
**PK:** `service_type_key` (INT)  
**Grano:** 1 fila por tipo de servicio

| Columna | Tipo | Descripción |
|---|---|---|
| service_type_key | INT | PK surrogate |
| service_type_desc | VARCHAR(100) | Descripción del tipo de servicio |

</details>

<details>
<summary><strong>dim.service_level</strong> — Nivel de servicio WASH</summary>

**Propósito:** clasificar nivel (ej. basic, safely managed, etc.).  
**Ubicación:** `silver/model/service_level/` (sin partición)  
**PK:** `service_level_key` (INT)  
**Grano:** 1 fila por nivel

| Columna | Tipo | Descripción |
|---|---|---|
| service_level_key | INT | PK surrogate |
| service_level_desc | VARCHAR(150) | Descripción del nivel |

</details>

<details>
<summary><strong>dim.indicator</strong> — Indicadores socioeconómicos</summary>

**Propósito:** catálogo conformado de indicadores (World Bank).  
**Ubicación:** `silver/model/indicator/` (sin partición)  
**PK:** `indicator_key` (INT)  
**Grano:** 1 fila por indicador

| Columna | Tipo | Descripción |
|---|---|---|
| indicator_key | INT | PK surrogate |
| indicator_code | VARCHAR(30) | Código del indicador |
| indicator_name | VARCHAR(255) | Nombre del indicador |

</details>

---

## Hechos (Silver Model)

<details>
<summary><strong>fact.climate_annual</strong> — Clima anual (país-año)</summary>

**Ubicación:** `silver/model/climate_annual/`  
**Partición:** `country_key`, `date_key`  
**Grano:** 1 fila por **(country_key, date_key)** (anual)  
**PK técnica:** `climate_annual_id` (BIGINT)  
**FKs:**  
- `country_key -> dim.country.country_key`  
- `date_key -> dim.date.date_key`

| Columna | Tipo | Descripción |
|---|---|---|
| climate_annual_id | BIGINT | PK surrogate del hecho |
| country_key | INT | FK país |
| date_key | INT | FK fecha (ancla anual, p.ej. `YYYY1231`) |
| precip_total_mm_year | DECIMAL(10,2) | Precipitación total anual (mm) |
| precip_avg_mm_year | DECIMAL(10,2) | Precipitación promedio anual (mm) |
| temp_max_avg_year | DECIMAL(5,2) | Temp. máxima promedio anual (°C) |
| temp_min_avg_year | DECIMAL(5,2) | Temp. mínima promedio anual (°C) |
| et0_total_mm_year | DECIMAL(8,2) | ET0 total anual (mm) |
| et0_avg_mm_year | DECIMAL(8,2) | ET0 promedio anual (mm) |
| dry_months | DECIMAL(10,2) | Meses secos  |
| heavy_rain_months | DECIMAL(10,2) | Meses con lluvia intensa  |
| drought_index | DECIMAL(6,3) | Índice de sequía  |
| heavy_rain_index | DECIMAL(6,3) | Índice de lluvia intensa  |

</details>

<details>
<summary><strong>fact.climate_monthly</strong> — Clima mensual (provincia-mes)</summary>

**Ubicación:** `silver/model/climate_monthly/`  
**Partición:** `province_key`, `date_key`  
**Grano:** 1 fila por **(province_key, date_key)** (mensual)  
**PK técnica:** `climate_monthly_id` (BIGINT)  
**FKs:**  
- `province_key -> dim.province.province_key`  
- `date_key -> dim.date.date_key`

| Columna | Tipo | Descripción |
|---|---|---|
| climate_monthly_id | BIGINT | PK surrogate del hecho |
| province_key | INT | FK provincia/estado |
| date_key | INT | FK fecha (ancla mensual, p.ej. fin de mes `YYYYMMDD`) |
| precip_total_mm | DECIMAL(8,2) | Precipitación total mensual (mm) |
| temp_max_avg_c | DECIMAL(5,2) | Temp. máxima promedio mensual (°C) |
| temp_min_avg_c | DECIMAL(5,2) | Temp. mínima promedio mensual (°C) |
| et0_total_mm | DECIMAL(8,2) | ET0 total mensual (mm) |
| et0_avg_mm | DECIMAL(8,2) | ET0 promedio mensual (mm) |
| dry_month_flag | BOOLEAN | Bandera: mes seco  |
| heavy_rain_month_flag | BOOLEAN | Bandera: lluvia intensa |

</details>

<details>
<summary><strong>fact.socioeconomic</strong> — Socioeconómico (país-año-indicador)</summary>

**Ubicación:** `silver/model/socioeconomic/`  
**Partición:** `country_key`, `date_key`  
**Grano:** 1 fila por **country_key, date_key** (anual)  
**PK técnica:** `socioeconomic_id` (BIGINT)  
**FKs:**  
- `country_key -> dim.country.country_key`  
- `date_key -> dim.date.date_key`  
- `indicator_key -> dim.indicator.indicator_key`

| Columna | Tipo | Descripción |
|---|---|---|
| socioeconomic_id | BIGINT | PK surrogate del hecho |
| country_key | INT | FK país |
| date_key | INT | FK fecha (p.ej. `YYYY1231`) |
| indicator_key | INT | FK indicador |
| indicator_value | DECIMAL(18,4) | Valor numérico del indicador |

</details>

<details>
<summary><strong>fact.wash_coverage</strong> — Cobertura WASH (país-año-residencia-servicio-nivel)</summary>

**Ubicación:** `silver/model/wash_coverage/`  
**Partición:** `country_key`, `date_key`  
**Grano:** 1 fila por **country_key, date_key** (anual)  
**PK técnica:** `wash_coverage_id` (BIGINT)  
**FKs:**  
- `country_key -> dim.country.country_key`  
- `date_key -> dim.date.date_key`  
- `residence_type_key -> dim.residence_type.residence_type_key`  
- `service_type_key -> dim.service_type.service_type_key`  
- `service_level_key -> dim.service_level.service_level_key`

| Columna | Tipo | Descripción |
|---|---|---|
| wash_coverage_id | BIGINT | PK surrogate del hecho |
| country_key | INT | FK país |
| date_key | INT | FK fecha (p.ej. `YYYY1231`) |
| residence_type_key | INT | FK tipo de residencia |
| service_type_key | INT | FK tipo de servicio |
| service_level_key | INT | FK nivel de servicio |
| population_total | BIGINT | Población total considerada |
| coverage_pct | DECIMAL(5,2) | Porcentaje de cobertura (0–100) |

</details>

---

## Tablas Silver “Limpieza” (por fuente)

> Estas tablas viven fuera del modelo dimensional y se usan como “curated raw” para auditoría, re-procesos y trazabilidad.

<details>
<summary><strong>silver/climate_monthly</strong> — Limpieza (por país/año/mes)</summary>

**Ubicación:** `silver/climate_monthly/`  
**Partición:** `country_iso3`, `year`, `month`  
**Descripción:** datos de clima limpios por fuente, antes de mapear a surrogate keys y construir facts en `silver/model/`.

</details>

<details>
<summary><strong>silver/jmp</strong> — Limpieza (por país/año)</summary>

**Ubicación:** `silver/jmp/`  
**Partición:** `country_iso3`, `year`  
**Descripción:** datos WASH (JMP) limpios por fuente, base para construir `fact.wash_coverage` en `silver/model/`.

</details>

<details>
<summary><strong>silver/socioeconomic</strong> — Limpieza (por país/año)</summary>

**Ubicación:** `silver/socioeconomic/`  
**Partición:** `country_iso3`, `year`  
**Descripción:** datos socioeconómicos limpios por fuente, base para construir `fact.socioeconomic` + `dim.indicator` en `silver/model/`.

</details>


## Reglas de calidad (Silver)

Estas validaciones se aplican durante los jobs Silver (**clean** y **model**) para asegurar consistencia y confiabilidad en los datos.

- **Integridad de claves**
  - `*_key` y `*_id` **no nulos**.
  - Las **FKs deben existir** en su dimensión correspondiente (`country`, `province`, `date`, etc.).

- **Rangos esperados**
  - `coverage_pct` ∈ **[0, 100]**
  - Métricas climáticas **≥ 0** donde aplique (ej. precipitación, ET0).
  - Flags: únicamente valores booleanos (`true/false`).

- **Unicidad (sin duplicados por grano)**
  - Hechos sin duplicados por su grano (en el modelo dimensional):
    - `fact.climate_annual`: **(country_key, date_key)**
    - `fact.climate_monthly`: **(province_key, date_key)**
    - `fact.socioeconomic`: **(country_key, date_key)**
    - `fact.wash_coverage`: **(country_key, date_key,)**

- **Consistencia temporal**
  - `dim.date.year`, `dim.date.month`, `dim.date.quarter` derivados correctamente desde `dim.date.date`.
  - `date_key` coherente con la frecuencia:
    - anual: ancla de año (ej. `YYYY1231`)
    - mensual: ancla de mes (ej. fin de mes `YYYYMMDD`)

- **Esquema y tipos**
  - Se valida que el esquema esperado (nombres y tipos de columnas) no cambie entre ejecuciones.
  - Columnas numéricas parseadas a tipo final (ej. `DECIMAL`) y fechas normalizadas.

---

## Metadatos y catálogo 

Para que Silver sea **auditable**, replicable y fácil de operar/mantener, documentamos:

- **Data Dictionary / Contract**
  - Este README: definición de tablas, grano, claves, particionado y tipos.

- **Estructura en S3 y particionado**
  - Limpieza por fuente:
    - `silver/climate_monthly/` particionado por `country_iso3`, `year`, `month`
    - `silver/jmp/` particionado por `country_iso3`, `year`
    - `silver/socioeconomic/` particionado por `country_iso3`, `year`
  - Modelo dimensional:
    - Facts particionadas por surrogate keys (`country_key/date_key` o `province_key/date_key`)
    - Dims sin particionar

- **Lineage (origen → Silver)**
  - **Clima**:
    - `silver/climate_monthly/` ← fuente clima (API) ya limpia
    - `silver/model/climate_monthly/` y `silver/model/climate_annual/` ← mapeo a keys + agregaciones + métricas derivadas
  - **WASH (JMP)**:
    - `silver/jmp/` ← fuente JMP limpia
    - `silver/model/wash_coverage/` ← normalización + mapeo a keys
  - **Socioeconómico**:
    - `silver/socioeconomic/` ← fuente socioeconómica limpia
    - `silver/model/socioeconomic/` + `silver/model/indicator/` ← catálogo + mapeo a keys

---

## Notas de mantenimiento

- **Dims “vigentes” (SCD Tipo 1):**  
  Las dimensiones se manejan como versión actual (se actualiza descripción/catálogo si cambia).

- **Operación y performance**
  - El particionado por `country_key/date_key` (y `province_key/date_key` en mensual) facilita:
    - re-procesos por ventana temporal,
    - lectura selectiva (menos costo),
    - y pipelines incrementales.

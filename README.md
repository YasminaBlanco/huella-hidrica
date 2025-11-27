# 🚀 Implementación de Pipeline ETL Serverless & CI/CD

**Rama:** `feature/configuracion-inicial-ale`
**Responsable:** Alejandro Nelson Herrera Soria
**Tickets Asociados:** `KAN-34` (AWS Glue), `KAN-33` (CI/CD)

-----

## 📑 Tabla de Contenidos

1.  [Resumen Ejecutivo](https://www.google.com/search?q=%23-resumen-ejecutivo)
2.  [Arquitectura y Decisiones de Diseño](https://www.google.com/search?q=%23-arquitectura-y-decisiones-de-dise%C3%B1o)
3.  [Detalle de Implementación: ETL con AWS Glue](https://www.google.com/search?q=%23-detalle-de-implementaci%C3%B3n-etl-con-aws-glue)
4.  [Detalle de Implementación: CI/CD Pipeline](https://www.google.com/search?q=%23-detalle-de-implementaci%C3%B3n-cicd-pipeline)
5.  [Desafíos Técnicos y Soluciones (Troubleshooting)](https://www.google.com/search?q=%23-desaf%C3%ADos-t%C3%A9cnicos-y-soluciones)
6.  [Estructura del Código](https://www.google.com/search?q=%23-estructura-del-c%C3%B3digo)

-----

## 📋 Resumen Ejecutivo

Esta rama consolida la infraestructura de procesamiento de datos y control de calidad del proyecto. Se ha implementado un **Pipeline ETL Serverless** utilizando **AWS Glue** para la transformación de datos crudos (Bronze) a datos limpios y optimizados (Silver), y se ha establecido un flujo de **Integración Continua (CI)** mediante **GitHub Actions** para garantizar la estandarización del código Python.

-----

## 🏗 Arquitectura y Decisiones de Diseño

### 1\. Procesamiento Serverless con AWS Glue (KAN-34)

Optamos por **AWS Glue** (sobre soluciones basadas en EC2 como Airflow workers) basándonos en tres pilares:

  * **Escalabilidad Horizontal:** Glue gestiona automáticamente los recursos (Workers/Executors) de Spark. Si el volumen de datos crece de Gigabytes a Terabytes, el Job escala sin intervención manual.
  * **Optimización de Costos:** Modelo de pago por uso (Serverless). Solo incurrimos en costos durante los minutos de ejecución del ETL, evitando el gasto de servidores EC2 ociosos las 24 horas.
  * **Mantenibilidad:** Se elimina la carga operativa de parchear sistemas operativos, gestionar memoria RAM o configurar clústeres de Spark manualmente.

### 2\. Almacenamiento Optimizado (Silver Layer)

  * **Formato:** **Parquet**. Elegido por su naturaleza columnar, ideal para consultas analíticas (OLAP), reduciendo drásticamente el tiempo de I/O comparado con CSV o JSON.
  * **Compresión:** **Snappy**. Ofrece el mejor balance entre ratio de compresión y velocidad de descompresión para ecosistemas Hadoop/Spark.
  * **Particionamiento:** Datos organizados por `year` o `province` para habilitar el *Partition Pruning* en consultas futuras (Athena/PowerBI), minimizando costos de escaneo.

### 3\. Calidad de Código Automatizada (KAN-33)

Implementamos un **Quality Gate** en el repositorio para prevenir deuda técnica:

  * **Linter:** `flake8` para detección temprana de errores de sintaxis y bugs potenciales.
  * **Formatter:** `black` para asegurar un estilo de código consistente y legible (PEP 8).
  * **Import Sorter:** `isort` para organizar dependencias.

-----

## 🛠 Detalle de Implementación: ETL con AWS Glue

Los scripts de ETL (`src/glue/`) realizan la transición **Bronze → Silver** aplicando las siguientes reglas de negocio y limpieza técnica:

1.  **Esquema Dictatorial (Schema Enforcement):**

      * Se definen esquemas manuales (`StructType`) para cada fuente. Esto blinda al pipeline contra el *Schema Drift* (cambios inesperados en los tipos de datos de la fuente).
      * Se ignoran columnas técnicas irrelevantes o corruptas del origen.

2.  **Normalización de Tipos:**

      * Casteo explícito de campos numéricos (`Double`, `Long`) y fechas.
      * Manejo de inconsistencias en la ingesta (ej. fechas guardadas como `INT64`).

3.  **Estandarización de Nombres:**

      * Conversión de columnas a `snake_case` (ej. `Country Name` → `country_name`) para facilitar el uso en SQL.

**Jobs Desarrollados:**

  * `job_bronze_to_silver_world_bank.py`: Procesa indicadores socioeconómicos.
  * `job_bronze_to_silver_jmp.py`: Procesa datos de Agua y Saneamiento (WHO/UNICEF).
  * `job_bronze_to_silver_weather.py`: Procesa datos climáticos históricos (Open-Meteo).

-----

## 🔄 Detalle de Implementación: CI/CD Pipeline

Se configuró un Workflow de GitHub Actions (`.github/workflows/ci.yml`) que se dispara automáticamente en cada `Push` o `Pull Request` hacia la rama `main`.

**Pasos del Pipeline:**

1.  Levanta un contenedor Ubuntu con Python 3.10.
2.  Instala dependencias de calidad: `black`, `flake8`, `isort`.
3.  Ejecuta formateo y linting sobre el código fuente en `src/`.
4.  **Bloqueo:** Si se detectan errores críticos, el Pipeline falla, alertando al equipo antes de fusionar el código defectuoso.

-----

## 💥 Desafíos Técnicos y Soluciones

Durante el desarrollo nos enfrentamos a inconsistencias críticas en la capa Bronze (Ingesta). A continuación se documentan las soluciones aplicadas:

| Desafío / Error | Causa Raíz | Solución Implementada |
| :--- | :--- | :--- |
| **Schema Drift / Merge Failure**<br>`[CANNOT_MERGE_SCHEMAS]` | Los archivos Parquet en Bronze tenían tipos de datos mixtos (ej. columna `scale` a veces era `INT`, a veces `STRING` o `NULL`) debido a la inferencia dinámica de Pandas en la ingesta. | Implementación de **Lectura con Esquema Manual** (`spark.read.schema(...)`). Esto fuerza a Spark a ignorar la inferencia y adherirse estrictamente al tipo de dato esperado. |
| **Conflicto de Particiones**<br>`COLUMN_ALREADY_EXISTS` | Existencia de columnas en el archivo (ej. `country`) con el mismo nombre que las carpetas de partición (`country=ARG`), generando ambigüedad en el Catálogo de Datos. | Uso de **`recursiveFileLookup`** y lectura directa desde S3 (bypasseando el Catálogo de Glue) para ignorar la estructura de carpetas y leer solo el contenido de los archivos. |
| **Formatos Híbridos**<br>`Not a Parquet file` | Se detectó que algunos datasets en la carpeta Bronze eran archivos CSV planos, a pesar de estar en una estructura de Data Lake. | Adaptación dinámica del lector de Spark (`.csv` con headers) dentro del Job de JMP, manteniendo la salida estandarizada en Parquet para la capa Silver. |
| **Inferencia de Fechas**<br>`INT64 vs Timestamp` | Las fechas fueron guardadas como enteros (microsegundos) sin metadatos de tiempo. | Lectura inicial como `LongType` y transformación matemática (`col/1000000`) a `Timestamp` dentro del ETL. |

-----

## 📂 Estructura del Código

```text
huella-hidrica/
├── .github/
│   └── workflows/
│       └── ci.yml          # Definición del Pipeline de Calidad (GitHub Actions)
├── src/
│   └── glue/               # Scripts PySpark (ETL Jobs)
│       ├── job_bronze_to_silver_world_bank.py
│       ├── job_bronze_to_silver_jmp.py
│       └── job_bronze_to_silver_weather.py
├── .gitignore              # Exclusiones (venv, terraform state, etc.)
└── README.md               # Esta documentación
```

-----

*Documentación generada para el Sprint 1 del Proyecto Final - Data Engineering.*

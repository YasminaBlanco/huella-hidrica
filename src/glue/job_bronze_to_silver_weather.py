import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Importamos tipos
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- 1. ESQUEMA MANUAL (INT64 para la fecha) ---
weather_schema = StructType(
    [
        StructField("province", StringType(), True),
        StructField("date", LongType(), True),
        StructField("temperature_2m_max", DoubleType(), True),
        StructField("temperature_2m_min", DoubleType(), True),
        StructField("precipitation_sum", DoubleType(), True),
        StructField("et0_fao_evapotranspiration", DoubleType(), True),
    ]
)

# --- 2. LEER DIRECTO (Sin particiones para evitar conflictos) ---
s3_source_path = "s3://henry-pf-g2-huella-hidrica/bronze/open_meteo/"
print(f"Leyendo Clima desde: {s3_source_path}")

df_spark = (
    spark.read.schema(weather_schema)
    .option("recursiveFileLookup", "true")
    .parquet(s3_source_path)
)

# --- 3. LIMPIEZA Y CONVERSIÓN ---
# Dividimos por 1000000 asumiendo microsegundos (estándar Parquet/Arrow) a segundos para Spark
df_final = df_spark.select(
    col("province"),
    (col("date") / 1000000).cast("timestamp").alias("date"),
    col("temperature_2m_max"),
    col("temperature_2m_min"),
    col("precipitation_sum"),
    col("et0_fao_evapotranspiration"),
)

# --- 4. ESCRIBIR A SILVER (EN UNA SOLA LÍNEA PARA EVITAR ERROR DE SINTAXIS) ---
s3_target_path = "s3://henry-pf-g2-huella-hidrica/silver/weather_final/"
print(f"Escribiendo en: {s3_target_path}")

# Esta línea es larga pero segura:
df_final.write.mode("overwrite").option("compression", "snappy").partitionBy(
    "province"
).parquet(s3_target_path)

job.commit()

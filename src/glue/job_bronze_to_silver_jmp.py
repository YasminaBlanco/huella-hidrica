import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --- 1. DEFINIR ESQUEMA MANUAL (Basado en el CSV original) ---
# Usamos los nombres exactos que Yasmina mostró en su notebook
jmp_schema = StructType(
    [
        StructField("ISO3", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Residence Type", StringType(), True),
        StructField("Service Type", StringType(), True),
        StructField("Year", LongType(), True),
        StructField("Coverage", DoubleType(), True),
        StructField("Population", DoubleType(), True),
        StructField("Service level", StringType(), True),
    ]
)

# --- 2. LECTURA CORRECTA (Formato CSV) ---
# Apuntamos a la carpeta base.
# 'recursiveFileLookup' busca el CSV aunque esté escondido en subcarpetas.
s3_source_path = "s3://henry-pf-g2-huella-hidrica/bronze/jmp/"
print(f"Leyendo JMP (CSV) desde: {s3_source_path}")

df_spark = (
    spark.read.option("header", "true")
    .option("recursiveFileLookup", "true")
    .schema(jmp_schema)
    .csv(s3_source_path)
)  # <--- AQUÍ ESTÁ EL CAMBIO CLAVE

# --- 3. LIMPIEZA Y NORMALIZACIÓN ---
# Renombramos a snake_case para estandarizar con el resto de tablas
df_final = df_spark.select(
    col("ISO3").alias("country_iso3"),
    col("Country").alias("country_name"),
    col("Residence Type").alias("residence_type"),
    col("Service Type").alias("service_type"),
    col("Service level").alias("service_level"),
    col("Year").alias("year"),
    col("Coverage").alias("coverage_pct"),
    col("Population").alias("population"),
)

# --- 4. ESCRIBIR A SILVER (Ahora sí en Parquet limpio) ---
s3_target_path = "s3://henry-pf-g2-huella-hidrica/silver/jmp_final/"
print(f"Escribiendo en: {s3_target_path}")

df_final.write.mode("overwrite").option("compression", "snappy").partitionBy(
    "year"
).parquet(s3_target_path)

job.commit()

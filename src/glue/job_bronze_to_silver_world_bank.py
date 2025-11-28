import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col

# Importamos los tipos para definir el esquema manualmente
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ---------------------------------------------------------------------------
# 1. DEFINIR ESQUEMA MANUAL ("La Solución Blindada")
# ---------------------------------------------------------------------------
# Esto evita que Spark lea los metadatos "sucios" de los archivos.
# Definimos SOLO las columnas útiles. Excluimos 'scale' y 'country' (duplicado).
clean_schema = StructType(
    [
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True),
        StructField("indicator_code", StringType(), True),
        StructField("indicator_name", StringType(), True),
        StructField("date", LongType(), True),  # Carlos lo guarda como Int64 -> Long
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("obs_status", StringType(), True),
        StructField("decimal", LongType(), True),
    ]
)

# ---------------------------------------------------------------------------
# 2. LEER FORZANDO EL ESQUEMA (Bypass del Catálogo y de MergeSchema)
# ---------------------------------------------------------------------------
# Apuntamos directo al bucket. Spark leerá el contenido con 'clean_schema'
# y agregará automáticamente las particiones (year, country) al final.
s3_source_path = "s3://henry-pf-g2-huella-hidrica/bronze/world_bank/"

print(f"Leyendo con esquema forzado desde: {s3_source_path}")

df_spark = spark.read.schema(clean_schema).parquet(s3_source_path)

# ---------------------------------------------------------------------------
# 3. LIMPIEZA FINAL Y ESCRITURA
# ---------------------------------------------------------------------------
# Seleccionamos y casteamos para asegurar que Silver quede perfecto
df_final = df_spark.select(
    col("country_code"),
    col("indicator_code"),
    col("indicator_name"),
    col("date").alias("year"),  # Renombramos 'date' a 'year' si es necesario
    col("value"),
    col("unit"),
    col("obs_status"),
    col("decimal"),
)

s3_target_path = "s3://henry-pf-g2-huella-hidrica/silver/world_bank_final/"

df_final.write.mode("overwrite").option("compression", "snappy").partitionBy(
    "year"
).parquet(s3_target_path)

job.commit()

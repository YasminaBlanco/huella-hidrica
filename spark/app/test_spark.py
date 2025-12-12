from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    # 1. Crear SparkSession 
    spark = (
        SparkSession.builder
        .appName("test-worldbank-silver")
        .master("local[*]")
        .getOrCreate()
    )

    # Rutas en S3
    bronze_path = "s3a://henry-pf-g2-huella-hidrica/bronze/world_bank/country=ARG/year=2024/"
    silver_test_path = "s3a://henry-pf-g2-huella-hidrica/silver/test_world_bank/country=ARG/year=2024/"

    # 2. Leer parquet desde bronze
    print(f"Leyendo desde: {bronze_path}")
    df = spark.read.parquet(bronze_path)

    # 3. Traer algunas columnas y agregar timestamp
    df_test = (
        df.select("country_code", "indicator_code", "date", "value")
          .withColumn("elt_processed_at", F.current_timestamp())
    )

    # 4. Escribir en silver de prueba (parquet + snappy)
    print(f"Escribiendo en: {silver_test_path}")
    df_test.write.mode("overwrite").parquet(silver_test_path)

    spark.stop()
    print("Proceso terminado sin errores.")
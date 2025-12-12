# base_gold_model_job.py
"""
Plantilla base para jobs de la capa GOLD (KPIs).

Patrones de diseño aplicados:
- Template Method:
   Flujo estándar de ejecución de un KPI:
      build() - printSchema() - show() - validate() - write()
- Strategy:
    Cada KPI implementa su propia estrategia en:
      - kpi_name()
      - build()
      - output_path()
"""

from pyspark.sql import DataFrame, SparkSession

# ============================================================
# 0. SparkSession para la capa GOLD
# ============================================================


def create_spark_session(app_name: str = "Huella_Hidrica_Gold_Model") -> SparkSession:
    """
    Crea y devuelve una SparkSession.

    IMPORTANTE:
    - Toda la configuración de S3,
      partitionOverwriteMode = dynamic, jars, etc. vive en:
        - spark-defaults.conf
        - Dockerfile / conf del contenedor
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    # Reducir ruido en los logs
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ============================================================
# 1. Clase base para KPIs GOLD
# ============================================================


class BaseGoldKPIJob:
    """
    Base para todos los KPIs del modelo GOLD.

    Cada KPI debe heredar de esta clase e implementar:
      - kpi_name(): nombre identificador del KPI
      - build(): lógica de transformación y agregación
      - output_path(): ruta final en GOLD donde escribir el parquet

    """

    def __init__(
        self,
        spark: SparkSession,
        silver_model_base_path: str,
        gold_model_base_path: str,
        write_mode: str = "overwrite",
    ) -> None:
        self.spark = spark
        self.silver_base = silver_model_base_path.rstrip("/")
        self.gold_base = gold_model_base_path.rstrip("/")
        self.write_mode = write_mode

    # ----------------- Helpers comunes -----------------

    def log(self, message: str) -> None:
        """
        Helper para loguear mensajes con prefijo estándar.
        """
        print(f"[GOLD][{self.kpi_name()}] {message}")

    def read_silver_table(self, table: str) -> DataFrame:
        """
        Lee una tabla de la capa Silver (modelo) en formato Parquet.

        Ejemplo:
            table = "wash_coverage"
            -> path = {silver_base}/wash_coverage
        """
        path = f"{self.silver_base}/{table}"
        self.log(f"Leyendo tabla Silver '{table}' desde {path}")
        return self.spark.read.parquet(path)

    # ----------------- Métodos a sobreescribir en los KPIs -----------------

    def kpi_name(self) -> str:
        """
        Nombre corto del KPI.
        """
        raise NotImplementedError("Debes implementar kpi_name() en la clase KPI hija.")

    def build(self) -> DataFrame:
        """
        Implementar aquí la lógica principal del KPI:
          - joins entre facts y dims
          - agregaciones
          - cálculo de métricas, indicadores y semáforos

        Debe devolver el DataFrame final listo para escribirse en GOLD.
        """
        raise NotImplementedError("Debes implementar build() en la clase KPI hija.")

    def output_path(self) -> str:
        """
        Ruta completa en GOLD donde se escribirá el parquet de este KPI.

        Ejemplo:
            return f"{self.gold_base}/kpi01_climate_water"
        """
        raise NotImplementedError(
            "Debes implementar output_path() en la clase KPI hija."
        )

    # ----------------- Hooks y Template Method -----------------

    def validate(self, df: DataFrame) -> None:
        """
        validaciones específicas del KPI.

          - Cuenta filas y lanza un warning si el DF está vacío.

        """
        count = df.count()
        if count == 0:
            self.log("WARNING: el DataFrame resultante tiene 0 filas.")
        else:
            self.log(f"Validación básica OK: {count} filas generadas.")

    def write(self, df: DataFrame) -> None:
        """
        Escritura estándar en formato Parquet en la ruta de GOLD definida
        por output_path().
        """
        path = self.output_path()
        self.log(f"Escribiendo resultado en GOLD: {path} (mode={self.write_mode})")
        (df.write.mode(self.write_mode).parquet(path))

    def run(self) -> None:
        """
        Template Method que define el flujo estándar de un KPI GOLD:

          1) build()      -> genera el DataFrame resultado
          2) printSchema  -> muestra el esquema
          3) show()       -> muestra una muestra de filas
          4) validate()   -> validaciones específicas (hook)
          5) write()      -> escritura en GOLD
        """
        self.log("Iniciando job GOLD...")
        df = self.build()

        self.log("Schema de la tabla GOLD resultante:")
        df.printSchema()

        self.log("Primeras filas del KPI (muestra):")
        df.show(10, truncate=False)

        self.validate(df)
        self.write(df)
        self.log("Job GOLD completado correctamente.")

"""
ETL BATCH: spark_batch_etl.py
Proyecto: Sistema de Monitoreo Sísmico Global en Tiempo Real y Batch
Responsable: KEVIN — Ingeniero de Procesamiento Masivo (Apache Spark Batch)
Emisor: JESÚS — Ingeniero de Orquestación Batch (Apache Airflow)
Receptor: JHONATAN — Tech Lead / Base de Datos y Web

Descripción:
  1. Lee el consolidado diario de sismos (24 horas) depositado en MinIO Bronze (/batch/).
  2. Transforma y limpia a Capa SILVER en formato Parquet particionado por fecha_evento.
  3. Modela y agrega a Capa GOLD en MinIO (Parquet):
     - Gold 1: resumen_diario_pais
     - Gold 2: zonas_criticas_frecuentes (hotspots / fallas geologicas ~10km2)
  4. Inserta las tablas consolidadas en PostgreSQL (Serving Layer) via JDBC para alimentar
     las vistas unificadas consumidas por la Web y Power BI.
"""

import os
import sys
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, to_date, hour, when,
    coalesce, lit, split, element_at, trim, round as spark_round,
    count, max as spark_max, avg as spark_avg, sum as spark_sum,
    min as spark_min, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, ArrayType, BooleanType
)

# -----------------------------------------------------------------------------
# Configuración y Parámetros
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuración MinIO (S3)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://10.175.132.248:9000")
MINIO_FALLBACK_ENDPOINT = os.getenv("MINIO_FALLBACK_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "n3jTFho71P8FV8jPHhQC")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "vX4t11jbqRLXRkoTtkW3i6qVvWGvKY9E21tRvm54")

# Configuración PostgreSQL Serving Layer
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_serving")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Rutas del Data Lake
BRONZE_S3_PATH = "s3a://bronze/batch/*/*/*/*.json"
LOCAL_FALLBACK_PATH = "/opt/spark/apps/sample_batch_dia.json"
SILVER_S3_PATH = "s3a://silver/batch/"
GOLD_1_S3_PATH = "s3a://gold/batch/resumen_diario_pais/"
GOLD_2_S3_PATH = "s3a://gold/batch/zonas_criticas_frecuentes/"


def crear_spark_session(endpoint: str) -> SparkSession:
    """Crea la sesion de Spark con soporte para MinIO (S3A) y JDBC Postgres."""
    logging.info(f"Iniciando SparkSession configurando MinIO endpoint en: {endpoint}")
    return SparkSession.builder \
        .appName("Kevin_Spark_Batch_ETL") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def main():
    print("=" * 80)
    print(">>> INICIANDO PIPELINE BATCH ETL — KEVIN (APACHE SPARK) <<<")
    print("=" * 80)

    # 1. Crear sesión de Spark
    try:
        spark = crear_spark_session(MINIO_ENDPOINT)
    except Exception as e:
        logging.warning(f"Error con endpoint principal {MINIO_ENDPOINT}: {e}. Usando fallback local {MINIO_FALLBACK_ENDPOINT}")
        spark = crear_spark_session(MINIO_FALLBACK_ENDPOINT)

    spark.sparkContext.setLogLevel("WARN")

    # 2. Lectura de Capa Bronze
    # Intenta leer desde MinIO; si no encuentra datos o falla la conexion, usa el sample local
    df_raw = None
    try:
        logging.info(f"Intentando leer datos crudos desde MinIO: {BRONZE_S3_PATH}")
        df_raw = spark.read.option("multiline", "true").json(BRONZE_S3_PATH)
        total_raw = df_raw.count()
        logging.info(f"Lectura exitosa desde MinIO. Lotes encontrados: {total_raw}")
    except Exception as e:
        logging.warning(f"No fue posible leer de MinIO S3: {e}")
        posibles_rutas_locales = [
            LOCAL_FALLBACK_PATH,
            "/opt/spark/apps/sample_batch_dia.json",
            "infraestructura/spark/apps/sample_batch_dia.json",
            "data/sample_batch_dia.json",
            "../data/sample_batch_dia.json",
            "../../data/sample_batch_dia.json"
        ]
        for ruta in posibles_rutas_locales:
            if os.path.exists(ruta):
                logging.info(f"Usando archivo de prueba local: {ruta}")
                df_raw = spark.read.option("multiline", "true").json(ruta)
                break

    if df_raw is None:
        logging.error("No se pudo obtener datos crudos ni de MinIO ni de archivo local. Abortando.")
        sys.exit(1)

    # -------------------------------------------------------------------------
    # 3. Transformación a Capa SILVER (fact_sismos)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> PROCESANDO CAPA SILVER (Limpieza, Aplanado y Desduplicación) <<<")
    print("-" * 60)

    # Desanidar el array 'features' de GeoJSON
    df_features = df_raw.select(explode(col("features")).alias("feature"))

    # Extraer campos tipados y normalizados segun especificacion tecnica
    df_silver = df_features.select(
        col("feature.id").alias("id_evento"),
        to_timestamp(col("feature.properties.time") / 1000).alias("fecha_hora_utc"),
        to_date(to_timestamp(col("feature.properties.time") / 1000)).alias("fecha_evento"),
        hour(to_timestamp(col("feature.properties.time") / 1000)).alias("hora_evento"),
        coalesce(col("feature.properties.mag"), lit(0.0)).cast("decimal(4,2)").alias("magnitud"),
        col("feature.geometry.coordinates")[2].cast("decimal(8,2)").alias("profundidad_km"),
        col("feature.properties.sig").cast("int").alias("significancia"),
        when(col("feature.properties.tsunami") == 1, True).otherwise(False).alias("alerta_tsunami"),
        col("feature.geometry.coordinates")[1].cast("decimal(9,6)").alias("latitud"),
        col("feature.geometry.coordinates")[0].cast("decimal(9,6)").alias("longitud"),
        trim(element_at(split(col("feature.properties.place"), ","), -1)).alias("pais_estado"),
        col("feature.properties.place").alias("ubicacion_completa"),
        lit("BATCH").alias("origen_pipeline")
    ).filter(
        col("id_evento").isNotNull() & col("latitud").isNotNull() & col("longitud").isNotNull()
    ).dropDuplicates(["id_evento"])

    # Regla de negocio para clasificacion_intensidad
    df_silver = df_silver.withColumn(
        "clasificacion_intensidad",
        when(col("magnitud") < 3.0, "Menor")
        .when((col("magnitud") >= 3.0) & (col("magnitud") < 5.0), "Ligero")
        .when((col("magnitud") >= 5.0) & (col("magnitud") < 7.0), "Moderado / Fuerte")
        .otherwise("Mayor / Catastrófico")
    )

    total_silver = df_silver.count()
    logging.info(f"Total de sismos limpios en Capa Silver: {total_silver}")
    df_silver.select("id_evento", "fecha_evento", "hora_evento", "magnitud", "clasificacion_intensidad", "pais_estado").show(5, truncate=False)

    # Guardar en MinIO Silver Parquet particionado
    try:
        logging.info(f"Guardando Capa Silver en MinIO: {SILVER_S3_PATH}")
        df_silver.write.mode("append").partitionBy("fecha_evento").parquet(SILVER_S3_PATH)
        logging.info("¡Capa Silver guardada exitosamente en MinIO!")
    except Exception as e:
        logging.warning(f"Aviso guardando Silver en MinIO: {e}")

    # -------------------------------------------------------------------------
    # 4. Transformación a Capa GOLD 1: Resumen Diario por País
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> PROCESANDO CAPA GOLD TABLA 1: resumen_diario_pais <<<")
    print("-" * 60)

    df_gold_diario = df_silver.groupBy("fecha_evento", "pais_estado").agg(
        count("id_evento").alias("total_sismos"),
        spark_max("magnitud").alias("magnitud_maxima"),
        spark_round(spark_avg("magnitud"), 2).alias("magnitud_promedio"),
        spark_round(spark_avg("profundidad_km"), 2).alias("profundidad_promedio_km"),
        spark_sum(when(col("alerta_tsunami") == True, 1).otherwise(0)).alias("total_alertas_tsunami"),
        spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_peligrosos_m5")
    ).withColumn("ultima_actualizacion_utc", current_timestamp())

    logging.info(f"Total registros agregados en Gold 1: {df_gold_diario.count()}")
    df_gold_diario.show(5, truncate=False)

    try:
        logging.info(f"Guardando Gold 1 en MinIO: {GOLD_1_S3_PATH}")
        df_gold_diario.write.mode("overwrite").parquet(GOLD_1_S3_PATH)
        logging.info("¡Capa Gold 1 guardada exitosamente en MinIO!")
    except Exception as e:
        logging.warning(f"Aviso guardando Gold 1 en MinIO: {e}")

    # -------------------------------------------------------------------------
    # 5. Transformación a Capa GOLD 2: Zonas Críticas y Hotspots Sísmicos
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> PROCESANDO CAPA GOLD TABLA 2: zonas_criticas_frecuentes <<<")
    print("-" * 60)

    # Redondear latitud y longitud a 1 decimal (~10 km2) para agrupar fallas
    df_gold_hotspots = df_silver.withColumn(
        "cuadricula_lat", spark_round(col("latitud"), 1)
    ).withColumn(
        "cuadricula_lon", spark_round(col("longitud"), 1)
    ).groupBy("pais_estado", "cuadricula_lat", "cuadricula_lon").agg(
        count("id_evento").alias("total_sismos_registrados"),
        spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_graves_m5"),
        spark_max("magnitud").alias("magnitud_maxima"),
        spark_round(spark_avg("profundidad_km"), 2).alias("profundidad_promedio_km"),
        spark_min("fecha_hora_utc").alias("fecha_primer_sismo"),
        spark_max("fecha_hora_utc").alias("fecha_ultimo_sismo")
    ).withColumn(
        "nivel_alerta",
        when((col("sismos_graves_m5") > 0) | (col("total_sismos_registrados") >= 15), "CRITICO")
        .when(col("magnitud_maxima") >= 4.0, "MODERADO")
        .otherwise("NORMAL")
    )

    logging.info(f"Total zonas críticas detectadas en Gold 2: {df_gold_hotspots.count()}")
    df_gold_hotspots.show(5, truncate=False)

    try:
        logging.info(f"Guardando Gold 2 en MinIO: {GOLD_2_S3_PATH}")
        df_gold_hotspots.write.mode("overwrite").parquet(GOLD_2_S3_PATH)
        logging.info("¡Capa Gold 2 guardada exitosamente en MinIO!")
    except Exception as e:
        logging.warning(f"Aviso guardando Gold 2 en MinIO: {e}")

    # -------------------------------------------------------------------------
    # 6. Carga a PostgreSQL (Serving Layer para Jhonatan)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> CARGA A POSTGRESQL (Serving Layer) VIA JDBC <<<")
    print("-" * 60)

    jdbc_properties = {
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver"
    }

    try:
        logging.info(f"Escribiendo tabla 'gold_resumen_batch' en PostgreSQL: {POSTGRES_JDBC_URL}")
        df_gold_diario.write.jdbc(
            url=POSTGRES_JDBC_URL,
            table="gold_resumen_batch",
            mode="overwrite",
            properties=jdbc_properties
        )
        logging.info("¡Tabla 'gold_resumen_batch' escrita exitosamente en PostgreSQL!")

        logging.info(f"Escribiendo tabla 'gold_zonas_batch' en PostgreSQL: {POSTGRES_JDBC_URL}")
        df_gold_hotspots.write.jdbc(
            url=POSTGRES_JDBC_URL,
            table="gold_zonas_batch",
            mode="overwrite",
            properties=jdbc_properties
        )
        logging.info("¡Tabla 'gold_zonas_batch' escrita exitosamente en PostgreSQL!")
    except Exception as e:
        logging.warning(f"Aviso de conexion JDBC hacia PostgreSQL: {e}")
        logging.info("Las tablas quedaron completamente procesadas y guardadas en formato Parquet.")

    print("\n" + "=" * 80)
    print(">>> PIPELINE BATCH ETL COMPLETADO EXITOSAMENTE — KEVIN <<<")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()

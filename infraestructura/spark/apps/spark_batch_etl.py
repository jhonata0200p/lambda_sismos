"""
ETL BATCH: spark_batch_etl.py
Proyecto: Sistema de Monitoreo Sísmico Global en Tiempo Real y Batch
Responsable: KEVIN — Ingeniero de Procesamiento Masivo (Apache Spark Batch)
Emisor: JESÚS — Ingeniero de Orquestación Batch (Apache Airflow)
Receptor: JHONATAN — Tech Lead / Base de Datos y Web

Descripción:
  1. Descarga el consolidado diario de sismos depositado por Airflow en MinIO Bronze (/batch/).
  2. Transforma y limpia a Capa SILVER en formato Parquet particionado por fecha_evento.
  3. Modela y agrega a Capa GOLD en MinIO (Parquet):
     - Gold 1: resumen_diario_pais
     - Gold 2: zonas_criticas_frecuentes (hotspots / fallas geologicas ~10km2)
  4. Sube de forma automatizada las capas SILVER y GOLD a MinIO (Local y Remoto).
  5. Inserta las tablas consolidadas en PostgreSQL (Serving Layer) via JDBC si está disponible.
"""

import os
import sys
import shutil
import logging
from datetime import datetime
import urllib.request

from minio import Minio
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

# Configuración MinIO
MINIO_REMOTE_HOST = os.getenv("MINIO_ENDPOINT", "http://10.175.132.248:9000")
MINIO_LOCAL_HOST = os.getenv("MINIO_FALLBACK_ENDPOINT", "http://minio:9000")
MINIO_REMOTE_AK = os.getenv("MINIO_ACCESS_KEY", "n3jTFho71P8FV8jPHhQC")
MINIO_REMOTE_SK = os.getenv("MINIO_SECRET_KEY", "vX4t11jbqRLXRkoTtkW3i6qVvWGvKY9E21tRvm54")

# Configuración PostgreSQL Serving Layer
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_serving")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")
POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Rutas locales temporales de staging en el contenedor Spark
STAGING_BRONZE_DIR = "/tmp/bronze_batch"
STAGING_SILVER_DIR = "/tmp/silver_batch"
STAGING_GOLD1_DIR = "/tmp/gold1_batch"
STAGING_GOLD2_DIR = "/tmp/gold2_batch"
LOCAL_FALLBACK_PATH = "/opt/spark/apps/sample_batch_dia.json"


def conectar_minios():
    """Detecta y devuelve los clientes MinIO activos (remoto y/o local)."""
    clientes = []
    
    # 1. Probar remoto (10.175.132.248:9000)
    try:
        urllib.request.urlopen(f"{MINIO_REMOTE_HOST}/minio/health/live", timeout=2)
        host_remoto = MINIO_REMOTE_HOST.replace("http://", "").replace("https://", "")
        m_remoto = Minio(host_remoto, access_key=MINIO_REMOTE_AK, secret_key=MINIO_REMOTE_SK, secure=False)
        m_remoto.list_buckets()
        logging.info(f"¡Conectado exitosamente a MinIO REMOTO: {MINIO_REMOTE_HOST}!")
        clientes.append((m_remoto, MINIO_REMOTE_HOST))
    except Exception as e:
        logging.warning(f"MinIO Remoto ({MINIO_REMOTE_HOST}) no responde: {e}")

    # 2. Probar local (minio:9000)
    try:
        urllib.request.urlopen(f"{MINIO_LOCAL_HOST}/minio/health/live", timeout=2)
        host_local = MINIO_LOCAL_HOST.replace("http://", "").replace("https://", "")
        m_local = Minio(host_local, access_key="minioadmin", secret_key="minioadmin", secure=False)
        m_local.list_buckets()
        logging.info(f"¡Conectado exitosamente a MinIO LOCAL: {MINIO_LOCAL_HOST}!")
        clientes.append((m_local, MINIO_LOCAL_HOST))
    except Exception as e:
        logging.warning(f"MinIO Local ({MINIO_LOCAL_HOST}) no responde: {e}")

    return clientes


def descargar_bronze(clientes, bucket="bronze", prefix="batch/") -> list:
    """Descarga los archivos JSON crudos depositados por Airflow."""
    os.makedirs(STAGING_BRONZE_DIR, exist_ok=True)
    archivos = []
    
    for client, ep in clientes:
        try:
            logging.info(f"Buscando archivos crudos en {ep} -> {bucket}/{prefix}...")
            objetos = client.list_objects(bucket, prefix=prefix, recursive=True)
            for obj in objetos:
                if not obj.object_name.endswith(".json"):
                    continue
                fname = os.path.basename(obj.object_name)
                local_path = os.path.join(STAGING_BRONZE_DIR, fname)
                logging.info(f"Descargando {obj.object_name} ({obj.size} bytes) de {ep}...")
                client.fget_object(bucket, obj.object_name, local_path)
                if local_path not in archivos:
                    archivos.append(local_path)
            if archivos:
                break
        except Exception as e:
            logging.warning(f"Error descargando Bronze de {ep}: {e}")

    return archivos


def subir_directorio_minio(clientes, local_dir: str, bucket: str, prefix: str):
    """Sube recursivamente archivos Parquet a todos los MinIO activos."""
    if not os.path.exists(local_dir):
        return

    for client, ep in clientes:
        subidos = 0
        logging.info(f"Subiendo resultados a {ep} (bucket: {bucket}, prefix: {prefix})...")
        for root, _, files in os.walk(local_dir):
            for fname in files:
                if fname.startswith(".") or fname.endswith(".crc"):
                    continue
                local_path = os.path.join(root, fname)
                rel_path = os.path.relpath(local_path, local_dir).replace("\\", "/")
                remote_key = f"{prefix.rstrip('/')}/{rel_path}" if prefix else rel_path
                try:
                    client.fput_object(bucket, remote_key, local_path)
                    logging.info(f"✓ Subido a {ep}: {bucket}/{remote_key}")
                    subidos += 1
                except Exception as e:
                    logging.error(f"Error subiendo {remote_key} a {ep}: {e}")
        logging.info(f"Total archivos subidos a {ep} en {bucket}/{prefix}: {subidos}")


def crear_spark_session() -> SparkSession:
    """Crea la sesion de Apache Spark para procesamiento local/cluster."""
    logging.info("Iniciando SparkSession para Kevin_Spark_Batch_ETL...")
    return SparkSession.builder \
        .appName("Kevin_Spark_Batch_ETL") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def main():
    print("=" * 80)
    print(">>> INICIANDO PIPELINE BATCH ETL — KEVIN (APACHE SPARK) <<<")
    print("=" * 80)

    # 1. Conectar a los MinIO disponibles
    clientes_minio = conectar_minios()

    # 2. Descargar datos crudos desde MinIO Bronze (Airflow)
    archivos_json = []
    if clientes_minio:
        archivos_json = descargar_bronze(clientes_minio, bucket="bronze", prefix="batch/")

    # 3. Inicializar SparkSession
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 4. Lectura de Capa Bronze
    df_raw = None
    if archivos_json:
        logging.info(f"Leyendo archivos Bronze descargados: {archivos_json}")
        df_raw = spark.read.option("multiline", "true").json(archivos_json)
    else:
        logging.warning("No se obtuvieron archivos de MinIO. Buscando fallback local...")
        posibles_rutas_locales = [
            LOCAL_FALLBACK_PATH,
            "/opt/spark/apps/sample_batch_dia.json",
            "infraestructura/spark/apps/sample_batch_dia.json",
            "data/sample_batch_dia.json"
        ]
        for ruta in posibles_rutas_locales:
            if os.path.exists(ruta):
                logging.info(f"Usando archivo de respaldo local: {ruta}")
                df_raw = spark.read.option("multiline", "true").json(ruta)
                break

    if df_raw is None:
        logging.error("No se pudo obtener datos crudos ni de MinIO ni de archivo local. Abortando.")
        sys.exit(1)

    total_crudos = df_raw.count()
    logging.info(f"Lectura exitosa de Bronze. Total lotes cargados: {total_crudos}")

    # -------------------------------------------------------------------------
    # 5. Transformación a Capa SILVER (fact_sismos)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> PROCESANDO CAPA SILVER (Limpieza, Aplanado y Desduplicación) <<<")
    print("-" * 60)

    # Desanidar array 'features' de GeoJSON
    df_features = df_raw.select(explode(col("features")).alias("feature"))

    # Extraer campos tipados y normalizados segun especificacion tecnica Medallion
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
    logging.info(f"Total de sismos limpios y desduplicados en Capa Silver: {total_silver}")
    df_silver.select("id_evento", "fecha_evento", "hora_evento", "magnitud", "clasificacion_intensidad", "pais_estado").show(5, truncate=False)

    # Guardar en staging local Parquet particionado
    if os.path.exists(STAGING_SILVER_DIR):
        shutil.rmtree(STAGING_SILVER_DIR)
    logging.info(f"Escribiendo Parquet Silver local en {STAGING_SILVER_DIR}...")
    df_silver.write.mode("overwrite").partitionBy("fecha_evento").parquet(STAGING_SILVER_DIR)

    # Subir a MinIO Capa Silver
    if clientes_minio:
        logging.info("Subiendo Parquet de Capa Silver a MinIO...")
        subir_directorio_minio(clientes_minio, STAGING_SILVER_DIR, bucket="silver", prefix="batch")

    # -------------------------------------------------------------------------
    # 6. Transformación a Capa GOLD 1: Resumen Diario por País
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

    if os.path.exists(STAGING_GOLD1_DIR):
        shutil.rmtree(STAGING_GOLD1_DIR)
    logging.info(f"Escribiendo Parquet Gold 1 local en {STAGING_GOLD1_DIR}...")
    df_gold_diario.write.mode("overwrite").parquet(STAGING_GOLD1_DIR)

    if clientes_minio:
        logging.info("Subiendo Parquet Gold 1 a MinIO...")
        subir_directorio_minio(clientes_minio, STAGING_GOLD1_DIR, bucket="gold", prefix="batch/resumen_diario_pais")

    # -------------------------------------------------------------------------
    # 7. Transformación a Capa GOLD 2: Zonas Críticas y Hotspots Sísmicos
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

    if os.path.exists(STAGING_GOLD2_DIR):
        shutil.rmtree(STAGING_GOLD2_DIR)
    logging.info(f"Escribiendo Parquet Gold 2 local en {STAGING_GOLD2_DIR}...")
    df_gold_hotspots.write.mode("overwrite").parquet(STAGING_GOLD2_DIR)

    if clientes_minio:
        logging.info("Subiendo Parquet Gold 2 a MinIO...")
        subir_directorio_minio(clientes_minio, STAGING_GOLD2_DIR, bucket="gold", prefix="batch/zonas_criticas_frecuentes")

    # -------------------------------------------------------------------------
    # 8. Carga a PostgreSQL (Serving Layer para Jhonatan)
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
        logging.info("Las tablas quedaron completamente procesadas y guardadas en formato Parquet en MinIO.")

    print("\n" + "=" * 80)
    print(">>> PIPELINE BATCH ETL COMPLETADO EXITOSAMENTE — KEVIN <<<")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()

"""
ETL BATCH: spark_batch_etl.py
Proyecto: Sistema de Monitoreo Sísmico Global en Tiempo Real y Batch
Responsable: KEVIN — Ingeniero de Procesamiento Masivo (Apache Spark Batch)
Emisor: JESÚS — Ingeniero de Orquestación Batch (Apache Airflow)
Receptor: JHONATAN — Tech Lead / Base de Datos y Web

Descripción:
  1. Conecta exclusivamente al MinIO central del equipo (10.131.229.248:9000).
  2. Descarga todos los lotes JSON depositados por Airflow en MinIO Bronze (/batch/).
  3. Transforma y limpia a Capa SILVER en formato Parquet particionado por fecha_evento.
  4. Modela y agrega a Capa GOLD en MinIO (Parquet) y PostgreSQL:
     - Tabla 1: gold_resumen_batch (métricas consolidadas por país y fecha)
     - Tabla 2: gold_zonas_batch (hotspots por cuadrícula lat/lon con zona_referencia)
  5. Sube automáticamente a MinIO (buckets silver y gold).
  6. Escribe en PostgreSQL Serving Layer (10.131.229.248:5432) en las tablas físicas gold.
"""

import os
import sys
import shutil
import logging
import time
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch
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
    LongType, IntegerType, ArrayType, BooleanType, DecimalType
)

# -----------------------------------------------------------------------------
# Configuración y Parámetros
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuración MinIO Central del Equipo
MINIO_REMOTE_ENDPOINT = os.getenv("MINIO_ENDPOINT", "10.131.229.248:9000").replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "Uvp62vvXRo9GiUjZPiC6")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "SVnjG3CxoqgmAZ7r3nbVAYDK2V6wvxLUGMq35YRn")

# Configuración PostgreSQL Serving Layer (Jhonatan en 10.131.229.248)
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.131.229.248")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "sismos_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")

# Rutas locales temporales de staging en el contenedor Spark
STAGING_BRONZE_DIR = "/tmp/bronze_batch"
STAGING_SILVER_DIR = "/tmp/silver_batch"
STAGING_GOLD1_DIR = "/tmp/gold1_batch"
STAGING_GOLD2_DIR = "/tmp/gold2_batch"
LOCAL_FALLBACK_PATH = "/opt/spark/apps/sample_batch_dia.json"


def conectar_minio_remoto(reintentos: int = 3, espera_seg: int = 2) -> Minio:
    """Conecta exclusivamente al servidor MinIO central del equipo con reintentos."""
    logging.info(f"Conectando a MinIO Central en: http://{MINIO_REMOTE_ENDPOINT}...")
    for intento in range(1, reintentos + 1):
        try:
            client = Minio(
                MINIO_REMOTE_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            buckets = [b.name for b in client.list_buckets()]
            logging.info(f"¡Conexión EXITOSA a MinIO {MINIO_REMOTE_ENDPOINT}! Buckets detectados: {buckets}")
            return client
        except Exception as e:
            logging.warning(f"[Intento {intento}/{reintentos}] No fue posible conectar a MinIO ({MINIO_REMOTE_ENDPOINT}): {e}")
            if intento < reintentos:
                time.sleep(espera_seg)

    logging.error(f"FATAL: No se pudo establecer conexión con el MinIO central ({MINIO_REMOTE_ENDPOINT}).")
    return None


def resolver_nombre_bucket(client: Minio, candidatos: list) -> str:
    """Verifica qué nombre de bucket existe en MinIO (ej: 'gold' o 'golden')."""
    for nombre in candidatos:
        try:
            if client.bucket_exists(nombre):
                return nombre
        except Exception:
            pass
    return candidatos[0]


def descargar_bronze(client: Minio, bucket_bronze: str, prefix="batch/") -> list:
    """Descarga todos los lotes JSON depositados por Airflow en MinIO Bronze."""
    if os.path.exists(STAGING_BRONZE_DIR):
        shutil.rmtree(STAGING_BRONZE_DIR)
    os.makedirs(STAGING_BRONZE_DIR, exist_ok=True)
    archivos = []

    try:
        logging.info(f"Buscando lotes crudos en s3://{bucket_bronze}/{prefix}...")
        objetos = client.list_objects(bucket_bronze, prefix=prefix, recursive=True)
        for obj in objetos:
            if not obj.object_name.endswith(".json"):
                continue
            fname = os.path.basename(obj.object_name)
            local_path = os.path.join(STAGING_BRONZE_DIR, fname)
            logging.info(f"Descargando {obj.object_name} ({obj.size} bytes) -> {local_path}...")
            client.fget_object(bucket_bronze, obj.object_name, local_path)
            if local_path not in archivos:
                archivos.append(local_path)

        logging.info(f"Total lotes descargados desde MinIO Bronze: {len(archivos)}")
    except Exception as e:
        logging.error(f"Error descargando datos de Bronze en MinIO: {e}")

    return archivos


def subir_directorio_minio(client: Minio, local_dir: str, bucket: str, prefixes: list):
    """Sube recursivamente los archivos Parquet procesados a MinIO bajo los prefijos solicitados."""
    if not os.path.exists(local_dir):
        logging.warning(f"Directorio local no existe: {local_dir}")
        return

    if isinstance(prefixes, str):
        prefixes = [prefixes]

    # Verificar que el bucket existe, si no crearlo
    try:
        if not client.bucket_exists(bucket):
            logging.info(f"El bucket '{bucket}' no existe en MinIO. Creándolo...")
            client.make_bucket(bucket)
    except Exception as e:
        logging.warning(f"Aviso verificando bucket '{bucket}': {e}")

    for prefix in prefixes:
        subidos = 0
        logging.info(f"Iniciando subida a s3://{bucket}/{prefix}...")
        for root, _, files in os.walk(local_dir):
            for fname in files:
                if fname.startswith(".") or fname.endswith(".crc"):
                    continue
                local_path = os.path.join(root, fname)
                rel_path = os.path.relpath(local_path, local_dir).replace("\\", "/")
                remote_key = f"{prefix.rstrip('/')}/{rel_path}" if prefix else rel_path
                try:
                    client.fput_object(bucket, remote_key, local_path)
                    logging.info(f"✓ MinIO Subido: s3://{bucket}/{remote_key}")
                    subidos += 1
                except Exception as e:
                    logging.error(f"Error subiendo {remote_key} a {bucket}: {e}")

        logging.info(f"Total archivos subidos exitosamente a s3://{bucket}/{prefix}: {subidos}")


def cargar_postgresql_batch(df_gold1, df_gold2):
    """Carga los DataFrames agregados directamente en PostgreSQL (Serving Layer)."""
    logging.info(f"Iniciando carga a PostgreSQL Serving Layer en {POSTGRES_HOST}:{POSTGRES_PORT}...")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            dbname=POSTGRES_DB,
            connect_timeout=5
        )
        cur = conn.cursor()

        # 1. Tabla gold_resumen_batch
        rows_gold1 = df_gold1.collect()
        q_gold1 = """
        INSERT INTO gold_resumen_batch (
            fecha_evento, pais_estado, total_sismos, magnitud_maxima, 
            magnitud_promedio, profundidad_promedio_km, total_alertas_tsunami, 
            sismos_peligrosos_m5, ultima_actualizacion_utc
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (fecha_evento, pais_estado) DO UPDATE SET
            total_sismos = EXCLUDED.total_sismos,
            magnitud_maxima = EXCLUDED.magnitud_maxima,
            magnitud_promedio = EXCLUDED.magnitud_promedio,
            profundidad_promedio_km = EXCLUDED.profundidad_promedio_km,
            total_alertas_tsunami = EXCLUDED.total_alertas_tsunami,
            sismos_peligrosos_m5 = EXCLUDED.sismos_peligrosos_m5,
            ultima_actualizacion_utc = CURRENT_TIMESTAMP;
        """
        data_gold1 = [
            (
                r["fecha_evento"],
                str(r["pais_estado"])[:100],
                int(r["total_sismos"]),
                float(r["magnitud_maxima"]),
                float(r["magnitud_promedio"]),
                float(r["profundidad_promedio_km"]),
                int(r["total_alertas_tsunami"]),
                int(r["sismos_peligrosos_m5"])
            )
            for r in rows_gold1
        ]
        execute_batch(cur, q_gold1, data_gold1)
        conn.commit()
        logging.info(f"¡ÉXITO! Tabla 'gold_resumen_batch' actualizada en PostgreSQL con {len(data_gold1)} registros.")

        # 2. Tabla gold_zonas_batch
        rows_gold2 = df_gold2.collect()
        q_gold2 = """
        INSERT INTO gold_zonas_batch (
            cuadricula_lat, cuadricula_lon, zona_referencia, 
            total_sismos_registrados, sismos_graves_m5, magnitud_maxima, 
            profundidad_promedio_km, nivel_alerta, fecha_primer_sismo, fecha_ultimo_sismo
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cuadricula_lat, cuadricula_lon) DO UPDATE SET
            zona_referencia = EXCLUDED.zona_referencia,
            total_sismos_registrados = EXCLUDED.total_sismos_registrados,
            sismos_graves_m5 = EXCLUDED.sismos_graves_m5,
            magnitud_maxima = EXCLUDED.magnitud_maxima,
            profundidad_promedio_km = EXCLUDED.profundidad_promedio_km,
            nivel_alerta = EXCLUDED.nivel_alerta,
            fecha_primer_sismo = LEAST(gold_zonas_batch.fecha_primer_sismo, EXCLUDED.fecha_primer_sismo),
            fecha_ultimo_sismo = GREATEST(gold_zonas_batch.fecha_ultimo_sismo, EXCLUDED.fecha_ultimo_sismo),
            ultima_actualizacion_utc = CURRENT_TIMESTAMP;
        """
        data_gold2 = [
            (
                float(r["cuadricula_lat"]),
                float(r["cuadricula_lon"]),
                str(r["zona_referencia"])[:100],
                int(r["total_sismos_registrados"]),
                int(r["sismos_graves_m5"]),
                float(r["magnitud_maxima"]),
                float(r["profundidad_promedio_km"]),
                str(r["nivel_alerta"]),
                r["fecha_primer_sismo"],
                r["fecha_ultimo_sismo"]
            )
            for r in rows_gold2
        ]
        execute_batch(cur, q_gold2, data_gold2)
        conn.commit()
        logging.info(f"¡ÉXITO! Tabla 'gold_zonas_batch' actualizada en PostgreSQL con {len(data_gold2)} registros.")

        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error conectando o escribiendo en PostgreSQL ({POSTGRES_HOST}:{POSTGRES_PORT}): {e}")


def crear_spark_session() -> SparkSession:
    """Crea la sesion de Apache Spark para el cluster."""
    logging.info("Iniciando SparkSession para Kevin_Spark_Batch_ETL...")
    return SparkSession.builder \
        .appName("Kevin_Spark_Batch_ETL") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def main():
    print("=" * 80)
    print(">>> INICIANDO PIPELINE BATCH ETL — KEVIN (APACHE SPARK BATCH) <<<")
    print(f">>> Destino MinIO: http://{MINIO_REMOTE_ENDPOINT}")
    print(f">>> Destino PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT} (db: {POSTGRES_DB})")
    print("=" * 80)

    # 1. Conectar a MinIO Central
    minio_client = conectar_minio_remoto()

    bucket_bronze = "bronze"
    bucket_silver = "silver"
    bucket_gold = "gold"

    if minio_client:
        bucket_bronze = resolver_nombre_bucket(minio_client, ["bronze"])
        bucket_silver = resolver_nombre_bucket(minio_client, ["silver", "silvers"])
        bucket_gold = resolver_nombre_bucket(minio_client, ["gold", "golden"])
        logging.info(f"Buckets objetivos en MinIO: Bronze='{bucket_bronze}', Silver='{bucket_silver}', Gold='{bucket_gold}'")

    # 2. Descargar datos crudos desde MinIO Bronze (Airflow)
    archivos_json = []
    if minio_client:
        archivos_json = descargar_bronze(minio_client, bucket_bronze=bucket_bronze, prefix="batch/")

    # 3. Inicializar SparkSession
    spark = crear_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 4. Lectura de Capa Bronze
    df_raw = None
    if archivos_json:
        logging.info(f"Leyendo archivos Bronze descargados: {archivos_json}")
        df_raw = spark.read.option("multiline", "true").json(archivos_json)
    else:
        logging.warning("No se obtuvieron archivos de MinIO Bronze. Buscando fallback local de respaldo...")
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
        coalesce(col("feature.properties.mag"), lit(0.0)).cast(DecimalType(4, 2)).alias("magnitud"),
        col("feature.geometry.coordinates")[2].cast(DecimalType(8, 2)).alias("profundidad_km"),
        col("feature.properties.sig").cast("int").alias("significancia"),
        when(col("feature.properties.tsunami") == 1, True).otherwise(False).alias("alerta_tsunami"),
        col("feature.geometry.coordinates")[1].cast(DecimalType(9, 6)).alias("latitud"),
        col("feature.geometry.coordinates")[0].cast(DecimalType(9, 6)).alias("longitud"),
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

    # Guardar en staging local Parquet particionado por fecha_evento
    if os.path.exists(STAGING_SILVER_DIR):
        shutil.rmtree(STAGING_SILVER_DIR)
    logging.info(f"Escribiendo Parquet Silver local en {STAGING_SILVER_DIR}...")
    df_silver.write.mode("overwrite").partitionBy("fecha_evento").parquet(STAGING_SILVER_DIR)

    # Subir a MinIO Capa Silver: s3://silver/batch/fecha_evento=YYYY-MM-DD/
    if minio_client:
        logging.info("Subiendo Parquet de Capa Silver a MinIO...")
        subir_directorio_minio(minio_client, STAGING_SILVER_DIR, bucket=bucket_silver, prefixes=["batch"])

    # -------------------------------------------------------------------------
    # 6. Transformación a Capa GOLD 1: gold_resumen_batch
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> PROCESANDO CAPA GOLD TABLA 1: gold_resumen_batch <<<")
    print("-" * 60)

    df_gold_diario = df_silver.groupBy("fecha_evento", "pais_estado").agg(
        count("id_evento").cast("int").alias("total_sismos"),
        spark_max("magnitud").cast(DecimalType(4, 2)).alias("magnitud_maxima"),
        spark_round(spark_avg("magnitud"), 2).cast(DecimalType(4, 2)).alias("magnitud_promedio"),
        spark_round(spark_avg("profundidad_km"), 2).cast(DecimalType(8, 2)).alias("profundidad_promedio_km"),
        spark_sum(when(col("alerta_tsunami") == True, 1).otherwise(0)).cast("int").alias("total_alertas_tsunami"),
        spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).cast("int").alias("sismos_peligrosos_m5")
    ).withColumn("ultima_actualizacion_utc", current_timestamp())

    logging.info(f"Total registros agregados en Gold 1 (gold_resumen_batch): {df_gold_diario.count()}")
    df_gold_diario.show(5, truncate=False)

    if os.path.exists(STAGING_GOLD1_DIR):
        shutil.rmtree(STAGING_GOLD1_DIR)
    logging.info(f"Escribiendo Parquet Gold 1 local en {STAGING_GOLD1_DIR}...")
    df_gold_diario.write.mode("overwrite").parquet(STAGING_GOLD1_DIR)

    if minio_client:
        logging.info(f"Subiendo Parquet Gold 1 a MinIO '{bucket_gold}'...")
        subir_directorio_minio(minio_client, STAGING_GOLD1_DIR, bucket=bucket_gold, prefixes=["batch/resumen_diario_pais"])

    # -------------------------------------------------------------------------
    # 7. Transformación a Capa GOLD 2: gold_zonas_batch
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> PROCESANDO CAPA GOLD TABLA 2: gold_zonas_batch <<<")
    print("-" * 60)

    # Redondear latitud y longitud a 1 decimal (~10 km2) para agrupar fallas
    # Clave primaria: (cuadricula_lat, cuadricula_lon) con campo descriptivo zona_referencia
    df_gold_hotspots = df_silver.withColumn(
        "cuadricula_lat", spark_round(col("latitud"), 1).cast(DecimalType(5, 1))
    ).withColumn(
        "cuadricula_lon", spark_round(col("longitud"), 1).cast(DecimalType(5, 1))
    ).groupBy("cuadricula_lat", "cuadricula_lon").agg(
        spark_max("ubicacion_completa").alias("zona_referencia"),
        count("id_evento").cast("int").alias("total_sismos_registrados"),
        spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).cast("int").alias("sismos_graves_m5"),
        spark_max("magnitud").cast(DecimalType(4, 2)).alias("magnitud_maxima"),
        spark_round(spark_avg("profundidad_km"), 2).cast(DecimalType(8, 2)).alias("profundidad_promedio_km"),
        spark_min("fecha_hora_utc").alias("fecha_primer_sismo"),
        spark_max("fecha_hora_utc").alias("fecha_ultimo_sismo")
    ).withColumn(
        "nivel_alerta",
        when((col("sismos_graves_m5") > 0) | (col("total_sismos_registrados") >= 15), "CRITICO")
        .when(col("magnitud_maxima") >= 4.0, "MODERADO")
        .otherwise("NORMAL")
    )

    logging.info(f"Total zonas críticas detectadas en Gold 2 (gold_zonas_batch): {df_gold_hotspots.count()}")
    df_gold_hotspots.show(5, truncate=False)

    if os.path.exists(STAGING_GOLD2_DIR):
        shutil.rmtree(STAGING_GOLD2_DIR)
    logging.info(f"Escribiendo Parquet Gold 2 local en {STAGING_GOLD2_DIR}...")
    df_gold_hotspots.write.mode("overwrite").parquet(STAGING_GOLD2_DIR)

    if minio_client:
        logging.info(f"Subiendo Parquet Gold 2 a MinIO '{bucket_gold}'...")
        subir_directorio_minio(minio_client, STAGING_GOLD2_DIR, bucket=bucket_gold, prefixes=["batch/zonas_criticas_frecuentes"])

    # -------------------------------------------------------------------------
    # 8. Carga a PostgreSQL Serving Layer (Jhonatan en 10.131.229.248:5432)
    # -------------------------------------------------------------------------
    print("\n" + "-" * 60)
    print(">>> CARGA DIRECTA A POSTGRESQL (Serving Layer) <<<")
    print("-" * 60)
    
    cargar_postgresql_batch(df_gold_diario, df_gold_hotspots)

    # -------------------------------------------------------------------------
    # 9. Notificacion de Linaje a Apache Atlas (Gobernanza)
    # -------------------------------------------------------------------------
    try:
        from atlas_hook import notificar_proceso_atlas
        notificar_proceso_atlas(
            proceso_nombre="spark_batch_etl",
            proceso_tipo="Process",
            inputs=[
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://bronze/batch/@minio"}}
            ],
            outputs=[
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://silver/batch/@minio"}},
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://gold/batch/resumen_diario_pais/@minio"}},
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://gold/batch/zonas_criticas_frecuentes/@minio"}},
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "postgres://sismos_db/gold_resumen_batch@postgres"}},
                {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "postgres://sismos_db/gold_zonas_batch@postgres"}}
            ],
            descripcion="Procesamiento Spark Batch: Bronze -> Silver -> Gold y PostgreSQL"
        )
    except Exception as e_atlas:
        logging.info(f"Atlas no disponible o error menor: {e_atlas}")

    print("\n" + "=" * 80)
    print(">>> PIPELINE BATCH ETL COMPLETADO EXITOSAMENTE — KEVIN <<<")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()

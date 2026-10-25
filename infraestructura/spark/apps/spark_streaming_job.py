import os
import json
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, to_date, from_unixtime,
    round as spark_round, count, max as spark_max, avg,
    when, coalesce, lit, split, trim, sum as spark_sum, min as spark_min, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, ArrayType
)

# ---------------------------------------------------------
# ---------------------------------------------------------
# 0. Parámetros de Conexión Distribuida (Dinámicos por Variables de Entorno)
# ---------------------------------------------------------
import os

def sanitizar_val(val: str, default: str) -> str:
    if not val:
        return default
    return val

def sanitizar_key(val: str, default: str) -> str:
    if not val:
        return default
    return val

# Kafka de Josue (Configurable por KAFKA_BOOTSTRAP_SERVERS)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "10.131.229.5:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sismos_crudos")

# MinIO Data Lake de Sayder (Configurable por MINIO_ENDPOINT)
raw_minio = os.getenv("MINIO_ENDPOINT", "10.131.229.248:9000")
if not raw_minio.startswith("http"):
    MINIO_ENDPOINT = f"http://{raw_minio}"
else:
    MINIO_ENDPOINT = raw_minio

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "Uvp62vvXRo9GiUjZPiC6")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "SVnjG3CxoqgmAZ7r3nbVAYDK2V6wvxLUGMq35YRn")

# PostgreSQL Serving DB de Sayder (Configurable por POSTGRES_HOST)
PG_HOST = os.getenv("POSTGRES_HOST", "10.131.229.248")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "sismos_db")
PG_USER = os.getenv("POSTGRES_USER", "postgres_user")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password123")

# Auto-verificación y creación de buckets Medallion en MinIO
try:
    from minio import Minio
    ep_minio_clean = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    _minio_check = Minio(ep_minio_clean, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    for _b in ["bronze", "silver", "gold"]:
        if not _minio_check.bucket_exists(_b):
            _minio_check.make_bucket(_b)
            print(f">>> [STREAMING-INIT] Bucket '{_b}' creado en MinIO ({ep_minio_clean}).", flush=True)
        else:
            print(f">>> [STREAMING-INIT] Bucket '{_b}' confirmado en MinIO.", flush=True)
except Exception as _e_bkt:
    print(f">>> [STREAMING-INIT] Aviso verificando buckets MinIO: {_e_bkt}", flush=True)

# 1. Sesión de Spark con soporte S3A (MinIO remoto)
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("SparkStreamingUSGS") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "5000") \
    .config("spark.hadoop.fs.s3a.attempts.maximum", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# ---------------------------------------------------------
# 2. Esquema individual de cada Feature (lo que envía tu compañero)
# ---------------------------------------------------------
schema_properties = StructType([
    StructField("mag", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("tsunami", IntegerType(), True)
])

schema_geometry = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", ArrayType(DoubleType()), True)
])

schema_single_feature = StructType([
    StructField("id", StringType(), True),
    StructField("properties", schema_properties, True),
    StructField("geometry", schema_geometry, True)
])

# ---------------------------------------------------------
# 3. Lectura continua desde Kafka (desde el inicio: earliest)
# ---------------------------------------------------------
print(f">>> [STREAMING-INIT] Conectando a Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topico: '{KAFKA_TOPIC}'...", flush=True)
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# ---------------------------------------------------------
# 4. Estructuración Silver y Deduplicación
# ---------------------------------------------------------
df_raw_str = df_raw.select(col("value").cast("string").alias("raw_json"))
df_parsed = df_raw_str.select(from_json(col("raw_json"), schema_single_feature).alias("feature"))

df_silver = df_parsed.select(
    col("feature.id").alias("id_sismo"),
    to_date(from_unixtime(col("feature.properties.time") / 1000)).alias("fecha_evento"),
    from_unixtime(col("feature.properties.time") / 1000).cast("timestamp").alias("timestamp_evento"),
    coalesce(col("feature.properties.mag"), lit(0.0)).alias("magnitud"),
    col("feature.properties.place").alias("lugar"),
    col("feature.geometry.coordinates").getItem(1).alias("latitud"),
    col("feature.geometry.coordinates").getItem(0).alias("longitud"),
    coalesce(col("feature.geometry.coordinates").getItem(2), lit(0.0)).alias("profundidad_km"),
    coalesce(col("feature.properties.tsunami"), lit(0)).alias("alerta_tsunami")
).filter(col("id_sismo").isNotNull())

# ---------------------------------------------------------
# 5. Función de procesamiento por micro-lote
# ---------------------------------------------------------
def procesar_microbatch(batch_df, batch_id):
    conteo = batch_df.count()
    if conteo == 0:
        print(f">>> [STREAMING] Micro-batch {batch_id}: Vacio (sin sismos nuevos en Kafka). Esperando...", flush=True)
        return

    print("=======================================================", flush=True)
    print(f">>> [STREAMING] Micro-batch {batch_id} RECIBIDO: {conteo} eventos sismicos", flush=True)
    print("=======================================================", flush=True)

    # DEDUPLICACIÓN: Evita procesar sismos duplicados dentro del micro-lote
    batch_df_unique = batch_df.dropDuplicates(["id_sismo"])
    batch_df_unique.persist()

    # --- AGREGACIÓN 1: Resumen Diario (Gold) ---
    df_resumen = batch_df_unique.withColumn(
        "pais_estado",
        when(
            col("lugar").contains(","),
            trim(split(col("lugar"), ",").getItem(1))
        ).otherwise(coalesce(col("lugar"), lit("Desconocido")))
    ).groupBy("fecha_evento", "pais_estado").agg(
        count("id_sismo").alias("total_sismos"),
        spark_max("magnitud").alias("magnitud_maxima"),
        avg("magnitud").alias("magnitud_promedio"),
        avg("profundidad_km").alias("profundidad_promedio_km"),
        spark_sum(when(col("alerta_tsunami") > 0, 1).otherwise(0)).alias("total_alertas_tsunami"),
        spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_peligrosos_m5")
    ).withColumn("ultima_actualizacion_utc", current_timestamp())

    # --- AGREGACIÓN 2: Zonas Críticas (Gold) ---
    df_zonas = batch_df_unique.withColumn("cuadricula_lat", spark_round(col("latitud"), 1)) \
                              .withColumn("cuadricula_lon", spark_round(col("longitud"), 1)) \
                              .groupBy("cuadricula_lat", "cuadricula_lon").agg(
                                  spark_max("lugar").alias("zona_referencia"),
                                  count("id_sismo").alias("total_sismos_registrados"),
                                  spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_graves_m5"),
                                  spark_max("magnitud").alias("magnitud_maxima"),
                                  avg("profundidad_km").alias("profundidad_promedio_km"),
                                  spark_min("timestamp_evento").alias("fecha_primer_sismo"),
                                  spark_max("timestamp_evento").alias("fecha_ultimo_sismo"),
                                  when((spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)) > 0) | (count("id_sismo") >= 15), "CRITICO")
                                  .when(spark_max("magnitud") >= 4.0, "MODERADO")
                                  .otherwise("NORMAL").alias("nivel_alerta")
                              )

    # --- PASO 1: ESCRITURA INMEDIATA EN POSTGRESQL (Serving Layer) ---
    # 1.1 gold_resumen_streaming
    rows_resumen = df_resumen.collect()
    if rows_resumen:
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            cur = conn.cursor()
            query_resumen = """
                INSERT INTO gold_resumen_streaming (
                    fecha_evento, pais_estado, total_sismos, magnitud_maxima, 
                    magnitud_promedio, profundidad_promedio_km, total_alertas_tsunami, 
                    sismos_peligrosos_m5, ultima_actualizacion_utc
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (fecha_evento, pais_estado) DO UPDATE SET
                    magnitud_promedio = (
                        (gold_resumen_streaming.magnitud_promedio * gold_resumen_streaming.total_sismos) + 
                        (EXCLUDED.magnitud_promedio * EXCLUDED.total_sismos)
                    ) / (gold_resumen_streaming.total_sismos + EXCLUDED.total_sismos),
                    profundidad_promedio_km = (
                        (gold_resumen_streaming.profundidad_promedio_km * gold_resumen_streaming.total_sismos) + 
                        (EXCLUDED.profundidad_promedio_km * EXCLUDED.total_sismos)
                    ) / (gold_resumen_streaming.total_sismos + EXCLUDED.total_sismos),
                    total_sismos = gold_resumen_streaming.total_sismos + EXCLUDED.total_sismos,
                    magnitud_maxima = GREATEST(gold_resumen_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima),
                    total_alertas_tsunami = gold_resumen_streaming.total_alertas_tsunami + EXCLUDED.total_alertas_tsunami,
                    sismos_peligrosos_m5 = gold_resumen_streaming.sismos_peligrosos_m5 + EXCLUDED.sismos_peligrosos_m5,
                    ultima_actualizacion_utc = CURRENT_TIMESTAMP;
            """
            data_resumen = [
                (
                    r["fecha_evento"],
                    str(r["pais_estado"])[:100],
                    int(r["total_sismos"] or 0),
                    float(r["magnitud_maxima"] or 0.0),
                    float(r["magnitud_promedio"] or 0.0),
                    float(r["profundidad_promedio_km"] or 0.0),
                    int(r["total_alertas_tsunami"] or 0),
                    int(r["sismos_peligrosos_m5"] or 0)
                )
                for r in rows_resumen
            ]
            execute_batch(cur, query_resumen, data_resumen)
            conn.commit()
            print(f">>> [STREAMING] {len(data_resumen)} filas insertadas/actualizadas en gold_resumen_streaming exitosamente.", flush=True)
        except Exception as e:
            print(f"Error escribiendo gold_resumen_streaming: {e}", flush=True)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    # 1.2 gold_zonas_streaming
    rows_zonas = df_zonas.collect()
    if rows_zonas:
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD
            )
            cur = conn.cursor()
            query_zonas = """
                INSERT INTO gold_zonas_streaming (
                    cuadricula_lat, cuadricula_lon, zona_referencia, 
                    total_sismos_registrados, sismos_graves_m5, magnitud_maxima, 
                    profundidad_promedio_km, nivel_alerta, fecha_primer_sismo, fecha_ultimo_sismo,
                    ultima_actualizacion_utc
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (cuadricula_lat, cuadricula_lon) DO UPDATE SET
                    zona_referencia = EXCLUDED.zona_referencia,
                    profundidad_promedio_km = (
                        (gold_zonas_streaming.profundidad_promedio_km * gold_zonas_streaming.total_sismos_registrados) + 
                        (EXCLUDED.profundidad_promedio_km * EXCLUDED.total_sismos_registrados)
                    ) / (gold_zonas_streaming.total_sismos_registrados + EXCLUDED.total_sismos_registrados),
                    total_sismos_registrados = gold_zonas_streaming.total_sismos_registrados + EXCLUDED.total_sismos_registrados,
                    sismos_graves_m5 = gold_zonas_streaming.sismos_graves_m5 + EXCLUDED.sismos_graves_m5,
                    magnitud_maxima = GREATEST(gold_zonas_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima),
                    nivel_alerta = CASE 
                        WHEN (gold_zonas_streaming.sismos_graves_m5 + EXCLUDED.sismos_graves_m5) > 0 
                             OR (gold_zonas_streaming.total_sismos_registrados + EXCLUDED.total_sismos_registrados) >= 15 THEN 'CRITICO'
                        WHEN GREATEST(gold_zonas_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima) >= 4.0 THEN 'MODERADO'
                        ELSE 'NORMAL'
                    END,
                    fecha_primer_sismo = LEAST(gold_zonas_streaming.fecha_primer_sismo, EXCLUDED.fecha_primer_sismo),
                    fecha_ultimo_sismo = GREATEST(gold_zonas_streaming.fecha_ultimo_sismo, EXCLUDED.fecha_ultimo_sismo),
                    ultima_actualizacion_utc = CURRENT_TIMESTAMP;
            """
            data_zonas = [
                (
                    float(r["cuadricula_lat"]),
                    float(r["cuadricula_lon"]),
                    str(r["zona_referencia"])[:100],
                    int(r["total_sismos_registrados"] or 0),
                    int(r["sismos_graves_m5"] or 0),
                    float(r["magnitud_maxima"] or 0.0),
                    float(r["profundidad_promedio_km"] or 0.0),
                    str(r["nivel_alerta"]),
                    r["fecha_primer_sismo"],
                    r["fecha_ultimo_sismo"]
                )
                for r in rows_zonas
            ]
            execute_batch(cur, query_zonas, data_zonas)
            conn.commit()
            print(f">>> [STREAMING] {len(data_zonas)} filas insertadas/actualizadas en gold_zonas_streaming exitosamente.", flush=True)
        except Exception as e:
            print(f"Error escribiendo gold_zonas_streaming: {e}", flush=True)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    # --- PASO 2: ESCRITURA EN MINIO REMOTO (Silver y Gold Parquet) ---
    try:
        batch_df_unique.write \
            .mode("append") \
            .partitionBy("fecha_evento") \
            .parquet("s3a://silver/streaming/")
        print(">>> [STREAMING] Parquet Silver guardado en MinIO s3a://silver/streaming/", flush=True)
    except Exception as e:
        print(f"Aviso escribiendo Silver en MinIO: {e}", flush=True)

    try:
        df_resumen.write \
            .mode("append") \
            .partitionBy("fecha_evento") \
            .parquet("s3a://gold/streaming/resumen_diario_pais/")
        print(">>> [STREAMING] Parquet Gold Resumen guardado en MinIO", flush=True)
    except Exception as e:
        print(f"Aviso escribiendo Gold Resumen en MinIO: {e}", flush=True)

    try:
        df_zonas.write \
            .mode("append") \
            .parquet("s3a://gold/streaming/zonas_criticas_frecuentes/")
        print(">>> [STREAMING] Parquet Gold Zonas guardado en MinIO", flush=True)
    except Exception as e:
        print(f"Aviso escribiendo Gold Zonas en MinIO: {e}", flush=True)

    batch_df_unique.unpersist()

# ---------------------------------------------------------
# 6. Lanzamiento del Streaming continuo
# ---------------------------------------------------------
query = df_silver.writeStream \
    .foreachBatch(procesar_microbatch) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/streaming_pipeline_v3") \
    .start()

# Notificar Linaje Streaming inicial a Apache Atlas
try:
    from atlas_hook import notificar_proceso_atlas
    notificar_proceso_atlas(
        proceso_nombre="spark_streaming_pipeline",
        proceso_tipo="Process",
        inputs=[
            {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "kafka://sismos_crudos@kafka"}}
        ],
        outputs=[
            {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://silver/streaming/@minio"}},
            {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://gold/streaming/resumen_diario_pais/@minio"}},
            {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "s3a://gold/streaming/zonas_criticas_frecuentes/@minio"}},
            {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "postgres://sismos_db/gold_resumen_streaming@postgres"}},
            {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "postgres://sismos_db/gold_zonas_streaming@postgres"}}
        ],
        descripcion="PySpark Structured Streaming: Kafka topic 'sismos_crudos' -> Silver Parquet -> Gold Analytics y PostgreSQL"
    )
except Exception as e_atlas:
    print(f"[Atlas Hook] Omitiendo reporte streaming: {e_atlas}")

query.awaitTermination()
import json
import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, to_date, from_unixtime,
    round as spark_round, count, max as spark_max, avg,
    when, coalesce, lit, split, trim, sum as spark_sum, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, IntegerType, ArrayType
)

# 1. Sesión de Spark
spark = SparkSession.builder \
    .appName("SparkStreamingUSGS") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Esquema para datos de USGS
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

schema_feature = StructType([
    StructField("id", StringType(), True),
    StructField("properties", schema_properties, True),
    StructField("geometry", schema_geometry, True)
])

schema_usgs = StructType([
    StructField("features", ArrayType(schema_feature), True)
])

# 3. Flujo desde Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:29092") \
    .option("subscribe", "sismos_crudos") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Transformación de capa Silver
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema_usgs).alias("data"))
df_exploded = df_parsed.select(explode(col("data.features")).alias("feature"))

df_silver = df_exploded.select(
    col("feature.id").alias("id_sismo"),
    to_date(from_unixtime(col("feature.properties.time") / 1000)).alias("fecha_evento"),
    from_unixtime(col("feature.properties.time") / 1000).cast("timestamp").alias("timestamp_evento"),
    col("feature.properties.mag").alias("magnitud"),
    col("feature.properties.place").alias("lugar"),
    col("feature.geometry.coordinates").getItem(1).alias("latitud"),
    col("feature.geometry.coordinates").getItem(0).alias("longitud"),
    col("feature.geometry.coordinates").getItem(2).alias("profundidad_km"),
    col("feature.properties.tsunami").alias("alerta_tsunami")
).filter(col("id_sismo").isNotNull())

# 5. Procesamiento por Micro-lote
def procesar_microbatch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    batch_df.persist()

    # --- ESCRITURA EN MINIO (SILVER) ---
    batch_df.write \
        .mode("append") \
        .partitionBy("fecha_evento") \
        .parquet("s3a://silver/streaming/")

    # --- AGREGACIÓN 1: Resumen Diario ---
    df_resumen = batch_df.withColumn(
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
    )

    df_resumen.write \
        .mode("append") \
        .parquet("s3a://gold/streaming/resumen_diario/")

    rows_resumen = df_resumen.collect()
    if rows_resumen:
        try:
            conn = psycopg2.connect(
                host="10.175.132.244",
                port=5432,
                database="sismos_db",
                user="postgres_user",
                password="password123"
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
                    total_sismos = gold_resumen_streaming.total_sismos + EXCLUDED.total_sismos,
                    magnitud_maxima = GREATEST(gold_resumen_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima),
                    magnitud_promedio = (gold_resumen_streaming.magnitud_promedio + EXCLUDED.magnitud_promedio) / 2,
                    profundidad_promedio_km = (gold_resumen_streaming.profundidad_promedio_km + EXCLUDED.profundidad_promedio_km) / 2,
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
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error escribiendo gold_resumen_streaming: {e}")

    # --- AGREGACIÓN 2: Zonas Críticas ---
    df_zonas = batch_df.withColumn("cuadricula_lat", spark_round(col("latitud"), 1)) \
                       .withColumn("cuadricula_lon", spark_round(col("longitud"), 1)) \
                       .groupBy("cuadricula_lat", "cuadricula_lon").agg(
                           spark_max("lugar").alias("zona_referencia"),
                           count("id_sismo").alias("total_sismos_registrados"),
                           spark_sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_graves_m5"),
                           spark_max("magnitud").alias("magnitud_maxima"),
                           avg("profundidad_km").alias("profundidad_promedio_km"),
                           spark_min("timestamp_evento").alias("fecha_primer_sismo"),
                           spark_max("timestamp_evento").alias("fecha_ultimo_sismo"),
                           when(spark_max("magnitud") >= 6.0, "ALTA")
                           .when(spark_max("magnitud") >= 4.5, "MEDIA")
                           .otherwise("BAJA").alias("nivel_alerta")
                       )

    df_zonas.write \
        .mode("append") \
        .parquet("s3a://gold/streaming/zonas_criticas/")

    rows_zonas = df_zonas.collect()
    if rows_zonas:
        try:
            conn = psycopg2.connect(
                host="10.175.132.244",
                port=5432,
                database="sismos_db",
                user="postgres_user",
                password="password123"
            )
            cur = conn.cursor()
            query_zonas = """
                INSERT INTO gold_zonas_streaming (
                    cuadricula_lat, cuadricula_lon, zona_referencia, 
                    total_sismos_registrados, sismos_graves_m5, magnitud_maxima, 
                    profundidad_promedio_km, nivel_alerta, fecha_primer_sismo, fecha_ultimo_sismo
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (cuadricula_lat, cuadricula_lon) DO UPDATE SET
                    zona_referencia = EXCLUDED.zona_referencia,
                    total_sismos_registrados = gold_zonas_streaming.total_sismos_registrados + EXCLUDED.total_sismos_registrados,
                    sismos_graves_m5 = gold_zonas_streaming.sismos_graves_m5 + EXCLUDED.sismos_graves_m5,
                    magnitud_maxima = GREATEST(gold_zonas_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima),
                    profundidad_promedio_km = (gold_zonas_streaming.profundidad_promedio_km + EXCLUDED.profundidad_promedio_km) / 2,
                    nivel_alerta = EXCLUDED.nivel_alerta,
                    fecha_primer_sismo = LEAST(gold_zonas_streaming.fecha_primer_sismo, EXCLUDED.fecha_primer_sismo),
                    fecha_ultimo_sismo = GREATEST(gold_zonas_streaming.fecha_ultimo_sismo, EXCLUDED.fecha_ultimo_sismo);
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
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error escribiendo gold_zonas_streaming: {e}")

    batch_df.unpersist()

# 6. Inicio de la consulta de Streaming
query = df_silver.writeStream \
    .foreachBatch(procesar_microbatch) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/streaming_pipeline") \
    .start()

query.awaitTermination()

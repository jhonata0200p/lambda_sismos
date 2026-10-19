import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, from_json, to_timestamp, to_date, hour, 
    when, coalesce, lit, split, element_at, trim, round, 
    count, max, avg, sum, min, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, IntegerType, ArrayType
)

# 1. Sesión Spark con soporte MinIO (S3A) y PostgreSQL
spark = (
    SparkSession.builder
    .appName("SparkStreamingUSGS")
    .master("spark://spark-master:7077")
    # Configuración MinIO / S3A
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Schema estricto del GeoJSON USGS
schema_geojson = StructType([
    StructField("type", StringType(), True),
    StructField("features", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("properties", StructType([
                StructField("mag", DoubleType(), True),
                StructField("place", StringType(), True),
                StructField("time", LongType(), True),
                StructField("tsunami", IntegerType(), True),
                StructField("sig", IntegerType(), True)
            ]), True),
            StructField("geometry", StructType([
                StructField("coordinates", ArrayType(DoubleType()), True)
            ]), True)
        ])
    ), True)
])

# 3. Lectura continua desde Kafka
df_kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "sismos_crudos")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. Parsear JSON y desanidar features
df_parsed = df_kafka_raw.select(
    from_json(col("value").cast("string"), schema_geojson).alias("data")
)

df_features = df_parsed.select(explode(col("data.features")).alias("feature"))

# 5. Capa Silver base
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
    col("feature.properties.place").alias("ubicacion_completa")
).filter(
    col("id_evento").isNotNull() & col("latitud").isNotNull() & col("longitud").isNotNull()
)

df_silver = df_silver.withColumn(
    "clasificacion_intensidad",
    when(col("magnitud") < 3.0, "Menor")
    .when((col("magnitud") >= 3.0) & (col("magnitud") < 5.0), "Ligero")
    .when((col("magnitud") >= 5.0) & (col("magnitud") < 7.0), "Moderado / Fuerte")
    .otherwise("Mayor / Catastrófico")
)

# 6. Procesamiento por micro-lote
def procesar_micro_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Desduplicar eventos repetidos dentro de la misma ventana
    batch_silver = batch_df.dropDuplicates(["id_evento"])

    # Columnas estrictas de la tabla fact_sismos_streaming_hoy
    cols_postgres = [
        "id_evento", "fecha_hora_utc", "fecha_evento", "hora_evento",
        "magnitud", "clasificacion_intensidad", "profundidad_km",
        "significancia", "alerta_tsunami", "latitud", "longitud",
        "pais_estado", "ubicacion_completa"
    ]
    df_pg = batch_silver.select(cols_postgres)

    # --- A. Escritura en PostgreSQL mediante Staging para evitar Primary Key duplicate error ---
    pg_url = "jdbc:postgresql://postgres_serving:5432/sismos_db"
    pg_properties = {
        "user": "postgres_user",
        "password": "password123",
        "driver": "org.postgresql.Driver"
    }

    try:
        staging_table = f"staging_sismos_{batch_id}"
        df_pg.write.jdbc(url=pg_url, table=staging_table, mode="overwrite", properties=pg_properties)

        # Merge de staging a fact_sismos_streaming_hoy con ON CONFLICT DO NOTHING
        import psycopg2
        conn = psycopg2.connect(
            host="postgres_serving",
            database="sismos_db",
            user="postgres_user",
            password="password123"
        )
        cursor = conn.cursor()
        upsert_sql = f"""
            INSERT INTO fact_sismos_streaming_hoy ({', '.join(cols_postgres)})
            SELECT {', '.join(cols_postgres)} FROM {staging_table}
            ON CONFLICT (id_evento) DO NOTHING;
            DROP TABLE IF EXISTS {staging_table};
        """
        cursor.execute(upsert_sql)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR POSTGRES] Lote {batch_id}: {e}")

    # --- B. Capa Silver en MinIO (Parquet) ---
    try:
        (
            batch_silver.withColumn("origen_pipeline", lit("STREAMING"))
            .write
            .mode("append")
            .partitionBy("fecha_evento")
            .parquet("s3a://silver/streaming/")
        )
    except Exception as e:
        print(f"[ERROR MINIO SILVER] Lote {batch_id}: {e}")

    # --- C. Capa Gold 1: Resumen Diario por País en MinIO ---
    try:
        df_gold_diario = batch_silver.groupBy("fecha_evento", "pais_estado").agg(
            count("id_evento").alias("total_sismos"),
            max("magnitud").alias("magnitud_maxima"),
            round(avg("magnitud"), 2).alias("magnitud_promedio"),
            round(avg("profundidad_km"), 2).alias("profundidad_promedio_km"),
            sum(when(col("alerta_tsunami") == True, 1).otherwise(0)).alias("total_alertas_tsunami"),
            sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_peligrosos_m5")
        ).withColumn("ultima_actualizacion_utc", current_timestamp())

        (
            df_gold_diario.write
            .mode("append")
            .parquet("s3a://gold/streaming/resumen_diario_pais/")
        )
    except Exception as e:
        print(f"[ERROR MINIO GOLD 1] Lote {batch_id}: {e}")

    # --- D. Capa Gold 2: Zonas Críticas en MinIO ---
    try:
        df_gold_hotspots = batch_silver.withColumn(
            "cuadricula_lat", round(col("latitud"), 1)
        ).withColumn(
            "cuadricula_lon", round(col("longitud"), 1)
        ).groupBy("pais_estado", "cuadricula_lat", "cuadricula_lon").agg(
            count("id_evento").alias("total_sismos_registrados"),
            sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_graves_m5"),
            max("magnitud").alias("magnitud_maxima"),
            round(avg("profundidad_km"), 2).alias("profundidad_promedio_km"),
            min("fecha_hora_utc").alias("fecha_primer_sismo"),
            max("fecha_hora_utc").alias("fecha_ultimo_sismo")
        ).withColumn(
            "nivel_alerta",
            when((col("sismos_graves_m5") > 0) | (col("total_sismos_registrados") >= 15), "CRITICO")
            .when(col("magnitud_maxima") >= 4.0, "MODERADO")
            .otherwise("NORMAL")
        )

        (
            df_gold_hotspots.write
            .mode("append")
            .parquet("s3a://gold/streaming/zonas_criticas_frecuentes/")
        )
    except Exception as e:
        print(f"[ERROR MINIO GOLD 2] Lote {batch_id}: {e}")

# 7. Ejecución continua
query = (
    df_silver.writeStream
    .foreachBatch(procesar_micro_batch)
    .option("checkpointLocation", "/tmp/spark_checkpoints/sismos_streaming")
    .start()
)

query.awaitTermination()
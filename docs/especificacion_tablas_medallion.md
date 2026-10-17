# ESPECIFICACIÓN TÉCNICA DE MODELADO DE DATOS (MEDALLION & LAMBDA)
**Proyecto:** Monitoreo Sísmico Global en Tiempo Real y Batch  
**Arquitectura:** Medallion (Bronze, Silver, Gold) sobre MinIO + PostgreSQL Serving Layer  
**Tecnología de Procesamiento:** Apache Spark (PySpark)

---

## 1. RESUMEN GENERAL DEL FLUJO DE DATOS

```
[API USGS GeoJSON]
       │
       ▼
[CAPA BRONZE] ────────► Archivos JSON crudos (Sin alterar)
       │
  (Spark ETL: Limpieza, aplanado de arrays, desduplicación, tipado)
       ▼
[CAPA SILVER] ────────► Tabla de Hechos Limpia en Parquet (1 fila = 1 sismo)
       │
  ┌────┴──────────────────────────┐
  │                               │
  ▼ (Spark: Agregaciones)          ▼ (Spark JDBC Write)
[CAPA GOLD]                     [POSTGRESQL SERVING LAYER]
  ├── Tabla 1: Resumen Diario     ├── Streaming: fact_sismos_streaming_hoy
  └── Tabla 2: Zonas Críticas     ├── Batch: fact_sismos_batch_historico
                                  └── Vista: vista_sismos_unificados (Web & BI)
```

---

## 2. CAPA BRONZE (MinIO) — Datos Crudos

En esta capa no se modifica nada. Se almacena el payload JSON original.

* **Estructura Raíz:** Objeto GeoJSON con array `features[]`.
* **Rutas en MinIO:**
  * **Streaming:** `s3a://bronze/streaming/año=YYYY/mes=MM/dia=DD/sismos_raw_<timestamp>.json`
  * **Batch:** `s3a://bronze/batch/año=YYYY/mes=MM/dia=DD/sismos_batch_YYYY-MM-DD.json`

---

## 3. CAPA SILVER (MinIO) — Tabla de Hechos Limpia

En Silver los datos están **aplanados, limpios, tipados estrictamente y sin duplicados**.  
Tanto Streaming como Batch producen **exactamente el mismo esquema de columnas**.

* **Formato:** Apache Parquet (Compresión `snappy`).
* **Particionado:** `partitionBy("fecha_evento")`.
* **Rutas en MinIO:**
  * **Streaming:** `s3a://silver/streaming/fecha_evento=YYYY-MM-DD/*.parquet`
  * **Batch:** `s3a://silver/batch/fecha_evento=YYYY-MM-DD/*.parquet`

### Diccionario de Datos: Tabla Silver (`fact_sismos`)

| Columna | Tipo Spark | Tipo SQL | Descripción / Origen |
| :--- | :--- | :--- | :--- |
| `id_evento` | `StringType` | `VARCHAR(50)` | Identificador único del sismo (de `feature.id`). Clave primaria. |
| `fecha_hora_utc` | `TimestampType` | `TIMESTAMP` | Fecha y hora UTC del sismo (de `properties.time / 1000`). |
| `fecha_evento` | `DateType` | `DATE` | Fecha del sismo en formato `YYYY-MM-DD` (Columna de partición). |
| `hora_evento` | `IntegerType` | `INT` | Hora del sismo (0 a 23). |
| `magnitud` | `DecimalType(4,2)`| `NUMERIC(4,2)` | Magnitud del sismo (`properties.mag`). Si es null, imputar `0.0`. |
| `clasificacion_intensidad` | `StringType` | `VARCHAR(20)` | Regla de negocio derivada según la magnitud. |
| `profundidad_km` | `DecimalType(8,2)`| `NUMERIC(8,2)` | Profundidad del hipocentro en km (`geometry.coordinates[2]`). |
| `significancia` | `IntegerType` | `INT` | Nivel de impacto (0 a 1000) calculado por USGS (`properties.sig`). |
| `alerta_tsunami` | `BooleanType` | `BOOLEAN` | `true` si `properties.tsunami == 1`, sino `false`. |
| `latitud` | `DecimalType(9,6)`| `NUMERIC(9,6)` | Coordenada Y (`geometry.coordinates[1]`). |
| `longitud` | `DecimalType(9,6)`| `NUMERIC(9,6)` | Coordenada X (`geometry.coordinates[0]`). |
| `pais_estado` | `StringType` | `VARCHAR(100)` | País o estado extraído tras la última coma de `properties.place`. |
| `ubicacion_completa` | `StringType` | `TEXT` | Texto original del lugar (`properties.place`). |
| `origen_pipeline` | `StringType` | `VARCHAR(10)` | Identificador del carril: `'STREAMING'` o `'BATCH'`. |

### Reglas de Negocio para `clasificacion_intensidad`:
* Magnitud $< 3.0 \rightarrow$ `'Menor'`
* Magnitud $\ge 3.0$ y $< 5.0 \rightarrow$ `'Ligero'`
* Magnitud $\ge 5.0$ y $< 7.0 \rightarrow$ `'Moderado / Fuerte'`
* Magnitud $\ge 7.0 \rightarrow$ `'Mayor / Catastrófico'`

---

## 4. CAPA GOLD (MinIO) — Tablas Analíticas / Data Marts

La capa Gold no contiene eventos sueltos, sino **inteligencia precalculada** para que la analítica y los dashboards respondan en milisegundos.

### TABLA GOLD 1: Resumen Diario por País y Región
* **Propósito:** Alimentar gráficos de tendencias temporales, conteos por país, métricas de severidad y alertas por fecha.
* **Formato:** Parquet.
* **Ruta MinIO:**
  * Batch: `s3a://gold/batch/resumen_diario_pais/*.parquet`
  * Streaming: `s3a://gold/streaming/resumen_diario_pais/*.parquet`

#### Diccionario de Datos: Tabla Gold 1 (`resumen_diario_pais`)

| Columna | Tipo Spark | Expresión / Cálculo en Spark |
| :--- | :--- | :--- |
| `fecha_evento` | `DateType` | Columna de agrupación (`groupBy`). |
| `pais_estado` | `StringType` | Columna de agrupación (`groupBy`). |
| `total_sismos` | `LongType` | `count("id_evento")` |
| `magnitud_maxima` | `DecimalType(4,2)`| `max("magnitud")` |
| `magnitud_promedio` | `DecimalType(4,2)`| `round(avg("magnitud"), 2)` |
| `profundidad_promedio_km` | `DecimalType(8,2)`| `round(avg("profundidad_km"), 2)` |
| `total_alertas_tsunami` | `LongType` | `sum(when(col("alerta_tsunami") == True, 1).otherwise(0))` |
| `sismos_peligrosos_m5` | `LongType` | `sum(when(col("magnitud") >= 5.0, 1).otherwise(0))` |
| `ultima_actualizacion_utc`| `TimestampType` | `current_timestamp()` |

---

### TABLA GOLD 2: Zonas Críticas y Hotspots Sísmicos
* **Propósito:** Detección de clústeres de alta sismicidad (fallas geológicas activas) para mapas de calor y sistemas de alerta temprana.
* **Técnica:** Se redondean la latitud y longitud a 1 decimal ($\sim 10\text{ km}^2$) para agrupar los epicentros en cuadrículas geográficas.
* **Formato:** Parquet.
* **Ruta MinIO:**
  * Batch: `s3a://gold/batch/zonas_criticas_frecuentes/*.parquet`
  * Streaming: `s3a://gold/streaming/zonas_criticas_frecuentes/*.parquet`

#### Diccionario de Datos: Tabla Gold 2 (`zonas_criticas_frecuentes`)

| Columna | Tipo Spark | Expresión / Cálculo en Spark |
| :--- | :--- | :--- |
| `zona_referencia` | `StringType` | Primer lugar de referencia detectado o `pais_estado`. |
| `cuadricula_lat` | `DecimalType(5,1)`| `round(col("latitud"), 1)` (Agrupación espacial). |
| `cuadricula_lon` | `DecimalType(5,1)`| `round(col("longitud"), 1)` (Agrupación espacial). |
| `total_sismos_registrados`| `LongType` | `count("id_evento")` |
| `sismos_graves_m5` | `LongType` | `sum(when(col("magnitud") >= 5.0, 1).otherwise(0))` |
| `magnitud_maxima` | `DecimalType(4,2)`| `max("magnitud")` |
| `profundidad_promedio_km` | `DecimalType(8,2)`| `round(avg("profundidad_km"), 2)` |
| `nivel_alerta` | `StringType` | Regla: Si `sismos_graves_m5 > 0` o `total > 15` $\rightarrow$ `'CRITICO'` / Si `magnitud_maxima >= 4.0` $\rightarrow$ `'MODERADO'` / Sino `'NORMAL'`. |
| `fecha_primer_sismo` | `TimestampType` | `min("fecha_hora_utc")` |
| `fecha_ultimo_sismo` | `TimestampType` | `max("fecha_hora_utc")` |

---

## 5. CAPA SERVING (PostgreSQL) — Consumo para la Web y Power BI

Spark toma los DataFrames limpios de Silver y los inserta vía JDBC en PostgreSQL:

* **Streaming (en vivo):** Escribe en `fact_sismos_streaming_hoy`.
* **Batch (histórico diario):** Escribe en `fact_sismos_batch_historico`.
* **Consumo (Power BI / Backend Web):** Consulta siempre la vista unificada:
  ```sql
  SELECT * FROM vista_sismos_unificados WHERE pais_estado = 'CA' ORDER BY fecha_hora_utc DESC;
  ```

---

## 6. CÓDIGO PYSPARK DE REFERENCIA PARA LOS COMPAÑEROS

Tus compañeros pueden copiar y utilizar este bloque exacto de código para construir los pipelines:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, to_timestamp, to_date, hour, when, 
    coalesce, lit, split, element_at, trim, round, count, 
    max, avg, sum, current_timestamp
)

# 1. Lectura del JSON crudo de Bronze
df_raw = spark.read.option("multiline", "true").json("s3a://bronze/batch/.../*.json")

# 2. Desanidar el array 'features'
df_features = df_raw.select(explode(col("features")).alias("feature"))

# 3. Transformación a Silver
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

# Clasificación de intensidad de negocio
df_silver = df_silver.withColumn(
    "clasificacion_intensidad",
    when(col("magnitud") < 3.0, "Menor")
    .when((col("magnitud") >= 3.0) & (col("magnitud") < 5.0), "Ligero")
    .when((col("magnitud") >= 5.0) & (col("magnitud") < 7.0), "Moderado / Fuerte")
    .otherwise("Mayor / Catastrófico")
)

# Guardar en Silver Parquet
df_silver.write.mode("append").partitionBy("fecha_evento").parquet("s3a://silver/batch/")

# 4. Generación de Gold Tabla 1: Resumen Diario por País
df_gold_diario = df_silver.groupBy("fecha_evento", "pais_estado").agg(
    count("id_evento").alias("total_sismos"),
    max("magnitud").alias("magnitud_maxima"),
    round(avg("magnitud"), 2).alias("magnitud_promedio"),
    round(avg("profundidad_km"), 2).alias("profundidad_promedio_km"),
    sum(when(col("alerta_tsunami") == True, 1).otherwise(0)).alias("total_alertas_tsunami"),
    sum(when(col("magnitud") >= 5.0, 1).otherwise(0)).alias("sismos_peligrosos_m5")
).withColumn("ultima_actualizacion_utc", current_timestamp())

df_gold_diario.write.mode("overwrite").parquet("s3a://gold/batch/resumen_diario_pais/")

# 5. Generación de Gold Tabla 2: Zonas Críticas / Hotspots
df_gold_hotspots = df_silver.withColumn(
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

df_gold_hotspots.write.mode("overwrite").parquet("s3a://gold/batch/zonas_criticas_frecuentes/")
```

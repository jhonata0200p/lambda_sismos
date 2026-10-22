# Guía Oficial de Estudio y Guión de Exposición: Clúster de Monitoreo Sísmico Global

Documento de preparación para la sustentación del proyecto de Big Data. Explica con claridad, precisión técnica y un lenguaje directo las decisiones de arquitectura, el flujo de datos y el papel de cada integrante ante los evaluadores y el Tech Lead.

---

## 1. Estructura General y Orden de Exposición (15 a 18 minutos)

| Turno | Expositor | Rol Técnico | Tema Central | Tiempo Estimado |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Jhonatan** | Tech Lead | Apertura, Arquitectura General Lambda + Medallion y Repartición de Hardware | 2 a 3 min |
| **2** | **Josué** | Data Streaming Engineer | Ingesta en Tiempo Real, Broker Apache Kafka y Conectores Kafka Connect | 3 a 4 min |
| **3** | **Sayder** | Storage & Database Engineer | Data Lake Medallion en MinIO y Serving Layer en PostgreSQL | 3 a 4 min |
| **4** | **Jesús** | Processing & Orchestration Engineer | Pipeline por Lotes con Airflow y Cómputo Distribuido con Apache Spark | 4 a 5 min |
| **5** | **Jhonatan** | Tech Lead | Gobernanza de Datos en Apache Atlas, Deduplicación Lambda y Demostración en Vivo de TerraPulse GIS | 4 a 5 min |

---

## 2. Guión Detallado por Integrante

---

### Turno 1: Jhonatan (Tech Lead) — Introducción y Visión General de Arquitectura

#### Qué debes decir:
> "Buenos días. Nuestro proyecto resuelve un problema fundamental en la ingeniería de datos moderna: **¿cómo procesar y alertar sismos en tiempo real sin perder la completitud y precisión del análisis histórico?**
>
> Para resolverlo, implementamos una **Arquitectura Lambda** distribuida en 4 computadores físicos, combinada con un **Data Lake Medallion (Bronze, Silver, Gold)**. 
> 
> En lugar de procesarlo todo en un solo servidor monolítico, desacoplamos el sistema en dos carriles:
> 1. Un **Speed Layer (Carril Streaming)** que captura sismos segundo a segundo desde la API de USGS para alertar a la población y a los servicios de rescate en milisegundos.
> 2. Un **Batch Layer (Carril por Lotes)** que corre todos los días a las 7:00 AM para reconciliar y asegurar que ningún dato se pierda por fallas de conexión o interrupciones de red.
>
> Ambos carriles convergen en un **Serving Layer en PostgreSQL**, donde creamos vistas SQL unificadas que resuelven la deduplicación en tiempo de consulta. Además, gobernamos todo el ciclo de vida del dato con **Apache Atlas** y presentamos los resultados en nuestro centro de mando táctico **TerraPulse GIS**."

#### Por qué tomamos esta decisión técnica (Para defenderla si te preguntan):
* **¿Por qué Arquitectura Lambda y no solo Streaming o solo Batch?**
  * Si usáramos solo Batch, las alertas de sismos llegarían con horas de retraso, lo cual es inútil para gestión del riesgo.
  * Si usáramos solo Streaming, ante caídas de red o fallas en la API pública se acumularían micro-huecos en los datos. La capa Batch repara y sella el histórico diariamente con datos oficiales consolidados.

---

### Turno 2: Josué — Ingesta en Tiempo Real con Apache Kafka

#### Qué debes decir:
> "Yo estuve a cargo de la ingesta continua y el desacoplamiento en el Speed Layer, utilizando **Apache Kafka** y **Kafka Connect** en mi máquina Linux.
>
> Nuestro flujo de tiempo real funciona así:
> 1. Configuramos un conector **HTTP Source Connector** en Kafka Connect que consulta la API de USGS cada 60 segundos buscando nuevos sismos en el mundo.
> 2. Cada sismo se publica de inmediato como un mensaje JSON independiente en el topic `sismos_crudos` de Kafka.
> 3. Al mismo tiempo, desplegamos un conector **S3 Sink Connector**, el cual toma esos mensajes del topic en tiempo real y los deposita automáticamente en el Data Lake MinIO en la Capa Bronze (`s3a://bronze/streaming/`), particionándolos por año, mes y día sin tocar una sola línea de código manual.
> 4. Kafka actúa como nuestro amortiguador elástico: si la base de datos o Spark tienen un pico de carga o se desconectan temporalmente, Kafka retiene los mensajes en disco con seguridad y nadie pierde información."

#### Por qué tomamos esta decisión técnica:
* **¿Por qué usar Kafka Connect en lugar de un script simple de Python con `requests` y un `while True`?**
  * Kafka Connect es un estándar empresarial que gestiona reconexiones, control de offsets, particionamiento en S3 y tolerancia a fallos automáticamente. Si el conector falla, se reanuda desde el último timestamp sin duplicar peticiones.
* **¿Por qué Kafka en modo KRaft (sin ZooKeeper)?**
  * Utilizamos Kafka 7.5 en modo KRaft para eliminar la sobrecarga de ZooKeeper, logrando menor consumo de memoria y una sincronización de metadatos más rápida en nuestro clúster.

---

### Turno 3: Sayder — Data Lake MinIO y PostgreSQL Serving Layer

#### Qué debes decir:
> "Yo gestiono la persistencia y almacenamiento distribuido en mi computador Linux (con 4 GB de RAM), albergando tanto el **Data Lake de MinIO** como la base de datos **PostgreSQL Serving Layer**.
>
> En el Data Lake organizamos los datos bajo el patrón **Medallion**:
> 1. **Capa Bronze:** Almacena los archivos JSON crudos e inmutables. Aquí se guardan dos rutas: `/streaming/` (lo que sube Kafka Connect en vivo) y `/batch/` (lo que deposita Airflow en la mañana). Nunca modificamos esta capa; es nuestra fuente de verdad ante cualquier auditoría.
> 2. **Capa Silver:** Almacena la tabla `fact_sismos` en formato Apache Parquet con compresión Snappy, particionada por `fecha_evento`. Aquí los datos ya están limpios, con tipos estrictos (decimales, timestamps) y deduplicados por el ID de USGS.
> 3. **Capa Gold:** Almacena nuestros dos data marts analíticos en Parquet: `resumen_diario_pais` (agregaciones por país) y `zonas_criticas_frecuentes` (hotspots agrupados por cuadrícula geográfica).
>
> En cuanto a **PostgreSQL**, actúa exclusivamente como Serving Layer: no guarda datos crudos. Solo recibe las tablas agregadas de Gold (`gold_resumen_streaming` y `gold_resumen_batch`) para que la aplicación web y Power BI consulten métricas en milisegundos sin sobrecargar el Data Lake."

#### Por qué tomamos esta decisión técnica:
* **¿Por qué MinIO y formato Parquet en lugar de guardar todo directo en PostgreSQL?**
  * MinIO ofrece almacenamiento de objetos compatible con AWS S3 a costo cero.
  * Formato Parquet es columnar y comprimido (Snappy), reduciendo hasta un 80% el espacio en disco comparado con JSON y permitiendo que Spark lea únicamente las columnas necesarias.
* **¿Por qué PostgreSQL para la capa de consumo y no consultar MinIO directamente desde la web?**
  * Consultar archivos Parquet en un Data Lake para peticiones web concurrentes genera latencia de segundos. PostgreSQL indexa los resultados Gold y responde a la interfaz en menos de 10 milisegundos.

---

### Turno 4: Jesús — Orquestación Batch (Airflow) y Cómputo Distribuido (Spark)

#### Qué debes decir:
> "En mi computador Windows tengo desplegados dos motores fundamentales: **Apache Airflow** para la orquestación y **Apache Spark** para el procesamiento masivo distribuido.
>
> Mi rol cubre dos pipelines principales:
>
> **1. Pipeline Batch Diario (Airflow + Spark Batch):**
> * A las 7:00 AM, un DAG en Airflow se activa automáticamente.
> * Tarea 1: Descarga el resumen oficial de 24 horas (`all_day.geojson`) de la API de USGS.
> * Tarea 2: Deposita el JSON crudo en MinIO Bronze (`s3a://bronze/batch/año=YYYY/mes=MM/dia=DD/`).
> * Tarea 3: Hace una petición HTTP hacia nuestro **Spark Trigger Server** en el puerto 5050 para lanzar `spark_batch_etl.py`.
> * Spark toma los datos de Bronze, limpia a Silver Parquet, calcula los modelos analíticos de Gold y carga las tablas `gold_resumen_batch` y `gold_zonas_batch` en PostgreSQL.
>
> **2. Pipeline Streaming en Tiempo Real (Spark Structured Streaming):**
> * En `spark_streaming_job.py`, Spark se suscribe continuamente al topic de Kafka `sismos_crudos`.
> * Procesa micro-lotes de eventos en vivo, deduplica por `id_evento`, escribe los Parquet en `silver/streaming/` y `gold/streaming/`, y ejecuta un `ON CONFLICT DO UPDATE` en PostgreSQL para que las estadísticas del día se actualicen al instante."

#### Por qué tomamos esta decisión técnica:
* **¿Por qué Apache Spark tanto para Batch como para Streaming?**
  * Spark ofrece la abstracción de **DataFrames unificados**. Usamos la misma lógica de negocio, tipado y limpieza tanto para el archivo de 24 horas como para los micro-lotes de Kafka, evitando duplicar código en diferentes tecnologías.
* **¿Por qué Airflow para orquestar y no un simple Cron de Linux o Windows Task Scheduler?**
  * Airflow maneja reintentos automáticos si la API externa está caída, guarda logs detallados de cada tarea, tiene dependencias explícitas (la Tarea 3 solo corre si la Tarea 2 subió el archivo con éxito) y provee una interfaz gráfica para monitorear el SLA de ejecución diaria.

---

### Turno 5: Jhonatan (Tech Lead) — Gobernanza (Atlas), Deduplicación Lambda y Demo Web

#### Qué debes decir:
> "Para cerrar el ciclo del dato, yo lidero la **Gobernanza con Apache Atlas**, el mecanismo de **Deduplicación Lambda en PostgreSQL** y la interfaz de mando en **TerraPulse GIS**.
>
> **1. Gobernanza de Datos y Linaje (Apache Atlas - Puerto 21000):**
> * En proyectos empresariales de Big Data, no basta con procesar datos; hay que saber de dónde vinieron, cómo se transformaron y quién los consume.
> * Desarrollé el script `inicializar_atlas.py` y el hook `atlas_hook.py`. Registramos 13 datasets y 6 procesos en Apache Atlas con etiquetas como `MEDALLION_BRONZE`, `MEDALLION_SILVER` y `MEDALLION_GOLD`.
> * Atlas nos permite auditar en un grafo visual exactamente cómo un sismo de la USGS pasa por Kafka, MinIO, Spark y PostgreSQL hasta llegar a la web.
>
> **2. Deduplicación Lambda en PostgreSQL:**
> * ¿Cómo evitamos que los datos de streaming y los de batch se pisen o se muestren dos veces?
> * En `vista_resumen_diario_pais`, usamos una consulta `UNION ALL` con cláusula `NOT EXISTS`: el histórico consolidado proviene de Batch, y para los sismos que acaban de ocurrir hoy y aún no han entrado a Batch, la vista toma la telemetría en vivo de Streaming con la etiqueta `[STREAMING]`.
>
> **3. Demostración en Vivo de TerraPulse GIS (Puerto 8000):**
> * [Aquí proyectas la pantalla de la web en http://localhost:8000]:
>   * *Mapa GIS Táctico:* Epicentros en vivo con ondas de choque animadas y fallas tectónicas oficiales.
>   * *Mapas base 100% libres:* Conmutador entre Gris Táctico, Océanos, Satelital y OSM sin requerir API keys.
>   * *Consola de Auditoría Lambda:* Abrir el modal y mostrar las filas con distintivos cian `[STREAMING]` y morados `[BATCH]`.
>   * *Analítica Gold (Chart.js):* Mostrar el gráfico de barras por países y dona por severidad.
>   * *Simular Streaming:* Hacer clic en el botón, mostrar cómo se inyecta un sismo en vivo y cómo la pantalla activa la Alerta Sísmica Temprana roja si es $\ge 6.0\text{ Mw}$.
>   * *Consultor GeoIA:* Hacerle una pregunta rápida al asistente de inteligencia artificial conectado por RAG a PostgreSQL."

---

## 3. Matriz de Preguntas Difíciles y Cómo Responder con Autoridad

### Pregunta 1: "¿Por qué tienen un Data Lake en MinIO y al mismo tiempo PostgreSQL? ¿No es redundante?"
* **Respuesta:**
  > "No, es una separación deliberada de responsabilidades recomendada en la Arquitectura Medallion:
  > * MinIO es nuestro **almacén analítico masivo de bajo costo (Data Lake)**. Guarda terabytes de datos históricos crudos en JSON y Parquet sin degradar su rendimiento.
  > * PostgreSQL es exclusivamente nuestro **Serving Layer transaccional de consumo**. No almacena petabytes de eventos crudos; únicamente almacena los modelos agregados de la capa Gold para responderle a la web y a los dashboards en milisegundos."

### Pregunta 2: "¿Qué pasa si la máquina de Kafka se apaga durante 15 minutos?"
* **Respuesta:**
  > "El carril Streaming se detendrá temporalmente durante esos 15 minutos. Sin embargo, no hay pérdida definitiva de datos:
  > 1. Al reactivarse Kafka Connect, consultará la API de USGS con su puntero de timestamp y recuperará los eventos no leídos.
  > 2. Pero incluso en el peor de los casos, a las 7:00 AM el DAG de Airflow descargará el consolidado de 24 horas completo y sellará la brecha en MinIO Bronze Batch y PostgreSQL. Es precisamente la garantía de auto-reconciliación de la Arquitectura Lambda."

### Pregunta 3: "¿Cómo garantizan la calidad del dato entre Bronze y Silver?"
* **Respuesta:**
  > "En el paso de Bronze a Silver aplicamos 4 reglas estrictas con Spark:
  > 1. **Deduplicación:** Filtramos duplicados basados en la clave natural del evento (`feature.id`).
  > 2. **Tipado Fuerte:** Cast de strings a `DecimalType(4,2)` en magnitud y `DecimalType(9,6)` en coordenadas geoespaciales.
  > 3. **Normalización:** Imputamos valores nulos a 0.0 y extraemos el país limpio tras la última coma del campo `place`.
  > 4. **Enriquecimiento:** Derivamos la columna `clasificacion_intensidad` basada en umbrales oficiales de sismología."

### Pregunta 4: "¿Por qué utilizaron Apache Atlas y no simplemente un catálogo en markdown o un Excel?"
* **Respuesta:**
  > "Apache Atlas provee **linaje activo automatizado a través de API REST v2**. Si un analista de datos ve un reporte en la web o una tabla en PostgreSQL, puede entrar a Atlas en el puerto 21000 y ver programáticamente de qué bucket de MinIO se calculó y qué job de Spark generó esa transformación. Es el estándar de la Apache Software Foundation para trazabilidad y auditoría de datos."

---

## 4. Checklist Rápido de Verificación Mañana Temprano

Antes de que empiece la clase, verificar en este orden estricto:

1. **Sayder:** Prender contenedores `minio` y `postgres_serving`. Verificar que respondan los puertos 9000 y 5432.
2. **Josué:** Prender `kafka`, `kafka_init_topic` y `kafka_connect`. Verificar en Kafdrop (`:9009`) que el topic `sismos_crudos` esté activo.
3. **Jesús:** Prender `spark_master`, `spark_worker` y `airflow`. Verificar que Spark Master responda en `:8080` y el Trigger Server en `:5050`.
4. **Jhonatan:** 
   * Prender `apache_atlas` (`:21000`).
   * Esperar 2 minutos y ejecutar en terminal: `py inicializar_atlas.py`.
   * Abrir la terminal web y ejecutar: `py app.py`.
   * Abrir en el navegador: `http://localhost:8000`. Probar el botón **Simular Streaming** para confirmar que la base de datos de Sayder esté recibiendo datos en vivo.

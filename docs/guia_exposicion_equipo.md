# Guia Oficial de Estudio y Guion de Sustentacion: Cluster de Monitoreo Sismico Global

Documento maestro de preparacion tecnica para la sustentacion del proyecto de Big Data y la evaluacion de desempeno del Tech Lead. Define con exactitud el rol asignado a cada uno de los 5 integrantes segun el trabajo desarrollado, los libretos de exposicion, las defensas tecnicas ante preguntas trampa del jurado y la justificacion de la arquitectura distribuida.

---

## 1. Estructura General y Orden de Exposicion (5 Integrantes - 18 a 20 minutos)

| Turno | Expositor | Rol Tecnico / Modulo Desarrollado | Tema Central de Exposicion | Tiempo Estimado |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Jhonatan** | Tech Lead & Data Architect | Apertura, Arquitectura General Lambda + Medallion y Separacion Hardware vs Modulos | 2 a 3 min |
| **2** | **Sayder** | Ingestion & Streaming Engineer | Apache Kafka en modo KRaft, Topico `sismos_crudos` y Conectores Kafka Connect (HTTP Source y S3 Sink) | 3 a 4 min |
| **3** | **Josue** | Real-Time Processing Engineer | Apache Spark Structured Streaming: Micro-lotes continuos, limpieza Silver y carga streaming en PostgreSQL | 3 a 4 min |
| **4** | **Jesus** | Orchestration & Workflow Engineer | Apache Airflow: Orquestacion Batch diaria a las 7:00 AM, DAG `dag_sismos_batch_usgs` y Trigger HTTP local | 3 a 4 min |
| **5** | **Kevin** | Batch Processing Engineer | PySpark Batch ETL: Computo masivo, transformacion Medallion Parquet Snappy, modelos Gold y Hook de Linaje | 3 a 4 min |
| **6** | **Jhonatan** | Tech Lead & Data Architect | Data Lake MinIO, PostgreSQL Serving Layer, Gobernanza (Atlas & OpenMetadata), Demo TerraPulse GIS y Cierre | 4 a 5 min |

---

## 2. Aclaracion Fundamental: Hardware vs Roles de Desarrollo

> **Nota para el Jurado:** La infraestructura de hardware se organizo segun la capacidad fisica de los equipos disponibles (RAM y procesador), lo cual es independiente del modulo que desarrollo cada integrante:
> - **Sayder:** Desarrollo la plataforma de **Apache Kafka**. Su computador (4 GB RAM) se destino como servidor de almacenamiento liviano de **MinIO y PostgreSQL**.
> - **Josue:** Desarrollo el pipeline de **Spark Streaming**. Su computador se utilizo para desplegar el broker de **Kafka y Connect**.
> - **Jesus:** Desarrollo la orquestacion de **Apache Airflow**. Su computador (alta potencia) centralizo tanto **Airflow** como **Spark Master y Workers** (corriendo el streaming de Josue y el batch de Kevin).
> - **Kevin:** Desarrollo el pipeline de **PySpark Batch ETL**.
> - **Jhonatan:** Diseno la **Arquitectura Lambda**, construyo el **Data Lake MinIO**, la **Serving Layer en PostgreSQL**, configuro **Apache Atlas**, **OpenMetadata** y desarrollo el cuadro de mando **TerraPulse GIS** con **GeoIA Copilot**.

---

## 3. Guion Detallado por Integrante

---

### Turno 1: Jhonatan (Tech Lead) - Apertura y Vision Arquitectonica

#### Que debes decir:
> "Buenos dias. Nuestro proyecto resuelve un reto critico de ingenieria de datos: **¿como capturar y alertar sismos mundiales en tiempo real con latencia de milisegundos, garantizando al mismo tiempo la reconciliacion matematica e historica completa de los datos?**
>
> Para solucionar esto, implementamos una **Arquitectura Lambda** combinada con un **Data Lake Medallion (Bronze, Silver, Gold)**. 
>
> Desacoplamos el sistema en dos carriles:
> 1. Un **Speed Layer (Carril Streaming)** de baja latencia que ingesta eventos segundo a segundo desde la API de USGS para emitir alertas tempranas.
> 2. Un **Batch Layer (Carril por Lotes)** que se ejecuta diariamente para auditar, consolidar y reparar historicos ante eventuales cortes de red.
>
> Ambos carriles convergen en una **Serving Layer en PostgreSQL**, gobernada por **Apache Atlas** y **OpenMetadata**, y visualizada en nuestra plataforma GIS interactiva **TerraPulse GIS**.
>
> A continuacion, cada especialista del equipo explicara a profundidad el modulo que desarrollo."

---

### Turno 2: Sayder - Ingesta y Mensajeria con Apache Kafka & Connect

#### Que debes decir:
> "En mi rol de Ingeniero de Ingesta, mi trabajo fue diseñar y poner en marcha la capa de mensajeria y captura en tiempo real con **Apache Kafka** y **Kafka Connect**.
>
> Implementamos Kafka en modo **KRaft (Kafka Raft)** eliminando ZooKeeper, lo que reduce la latencia de metadatos y optimiza el uso de memoria.
>
> Configure dos conectores empresariales en **Kafka Connect**:
> 1. **HTTP Source Connector (`http-source-usgs`):** Consulta periodicamente el feed GeoJSON de la API de USGS y deposita los eventos en el topico particionado `sismos_crudos`.
> 2. **S3 Sink Connector (`minio-sink-bronze-streaming`):** Consume continuamente de `sismos_crudos` y escribe el JSON crudo inalterable directamente en el Data Lake MinIO en la ruta `bronze/streaming/ano=YYYY/mes=MM/dia=DD/`.
>
> Gracias a Kafka, desacoplamos totalmente la API externa del procesamiento: si Spark o la red se detienen, los mensajes quedan retenidos de forma segura en las particiones del broker."

#### Preguntas trampa del jurado para Sayder:
* **¿Por que usaste Kafka Connect en vez de un bucle `while True` con un script de Python?**
  * *Respuesta:* "Kafka Connect es tolerante a fallos de nivel industrial. Si el contenedor se apaga, Connect registra el offset en `connect-offsets` y reanuda sin perder mensajes ni duplicar la ingesta. Ademas, gestiona reintentos y escalabilidad de forma nativa sin mantenimiento manual de codigo."
* **¿Que politica de particionamiento usaste en Kafka?**
  * *Respuesta:* "Para este volumen se configuro 1 particion con factor de replicacion 1, garantizando orden secuencial estricto en la entrega de sismos hacia el consumidor de Spark."

---

### Turno 3: Josue - Procesamiento en Tiempo Real con Spark Structured Streaming

#### Que debes decir:
> "Mi responsabilidad fue el desarrollo del pipeline de computo en tiempo real implementado en `spark_streaming_job.py` utilizando **Spark Structured Streaming**.
>
> Mi pipeline corre de manera continua las 24 horas y realiza las siguientes operaciones:
> 1. Se conecta mediante `readStream` al topico de Kafka `sismos_crudos` con configuracion `failOnDataLoss=false`.
> 2. Parsea el payload JSON de USGS, extrayendo y aplanando coordenadas (`latitud`, `longitud`, `profundidad`), magnitud y lugar.
> 3. Aplica logica de enriquecimiento: clasifica la severidad del sismo (`CRITICO` si $Mw \ge 6.0$, `MODERADO` si $5.0 \le Mw < 6.0$) y determina el tipo de profundidad (`SUPERFICIAL`, `INTERMEDIA`, `PROFUNDA`).
> 4. Escribe en MinIO en la capa **Silver** en formato columnar Parquet Snappy particionado por `fecha_evento`.
> 5. Ejecuta un `foreachBatch` para escribir en la Serving Layer de PostgreSQL mediante `upsert` (`ON CONFLICT (id_evento) DO UPDATE`) sobre la tabla `delta_streaming_hoy`. De este modo, la aplicacion web visualiza el sismo en cuestion de segundos despues de ocurrido."

#### Preguntas trampa del jurado para Josue:
* **¿Por que Spark Structured Streaming en lugar de Spark DStreams clasico?**
  * *Respuesta:* "Structured Streaming procesa los datos como una tabla relacional infinita construida sobre el motor optimizado Catalyst. Permite la misma sintaxis de DataFrames que Spark Batch y garantiza semantica de entrega *Exactly-Once* mediante checkpoints en disco."
* **¿Como evitas bloquear la base de datos PostgreSQL con el streaming continuo?**
  * *Respuesta:* "Utilizamos `foreachBatch` con micro-lotes y pools de conexiones, actualizando unicamente la tabla delta del dia (`delta_streaming_hoy`). Las consultas pesadas de historicos van contra las tablas batch, evitando contencion de bloqueos."

---

### Turno 4: Jesus - Orquestacion del Pipeline Batch con Apache Airflow

#### Que debes decir:
> "Mi labor consistio en diseñar, programar y automatizar el flujo de trabajo batch mediante **Apache Airflow**, materializado en el DAG `dag_sismos_batch_usgs.py`.
>
> El DAG esta programado para correr todos los dias a las 07:00 AM UTC (`schedule='0 7 * * *'`) y ejecuta una secuencia estricta de 3 tareas:
> 1. **`tarea_1_descargar_sismos_usgs`:** Descarga el consolidado oficial de 24 horas (`all_day.geojson`) de la API de USGS y valida la integridad del archivo.
> 2. **`tarea_2_guardar_minio_bronze`:** Se conecta con MinIO y deposita el archivo crudo inmutable en la ruta `bronze/batch/ano=YYYY/mes=MM/dia=DD/sismos_batch_YYYY-MM-DD.json`.
> 3. **`tarea_3_disparar_spark_batch`:** Realiza una invocacion POST HTTP a nuestro Trigger Server interno en el puerto 5050 (`http://spark-master:5050/trigger`).
>
> Al convivir Airflow y Spark en el mismo computador dentro de la red Docker compartida `red_bigdata`, este disparo es 100% local, seguro y no requiere trafico por la red externa."

#### Preguntas trampa del jurado para Jesus:
* **¿Que pasa si la conexion con MinIO falla durante la ejecucion de Airflow?**
  * *Respuesta:* "El DAG tiene configurada una politica de resiliencia con 2 reintentos automaticos cada 2 minutos (`retries=2, retry_delay=timedelta(minutes=2)`). Si hay una indisponibilidad temporal, Airflow reintenta sin intervencion humana y deja registro completo en los logs de auditoria."
* **¿Por que usar un Trigger HTTP en Spark en vez de `SparkSubmitOperator` directo?**
  * *Respuesta:* "Porque desacopla los entornos de ejecucion: Airflow no necesita cargar librerias pesadas de Spark ni JDK en su imagen principal; simplemente le envia la orden de ejecucion al contenedor especializado de Spark Master y recibe el codigo de salida y los logs detallados del job."

---

### Turno 5: Kevin - Computacion Masiva por Lotes con PySpark Batch ETL

#### Que debes decir:
> "Mi responsabilidad fue el desarrollo del motor de transformacion y analitica masiva en **PySpark Batch** (`spark_batch_etl.py`).
>
> Cuando Airflow dispara la ejecucion, mi pipeline realiza el procesamiento de gran escala a traves de las capas Medallion:
> 1. **Lectura Bronze:** Carga el JSON consolidado de 24 horas directamente desde MinIO S3 (`s3a://bronze/batch/`).
> 2. **Refinamiento a Silver (`fact_sismos`):** Limpia tipos de datos, castea campos numericos con precision decimal (`DecimalType(4,2)`), filtra sismos anomalos y escribe en MinIO Silver en formato Parquet comprimido con Snappy, particionado por fecha.
> 3. **Modelado Analitico Gold:**
>    - Genera el modelo **`resumen_diario_pais`**: calcula totales de eventos, sismos criticos ($Mw \ge 6.0$), magnitud maxima y profundidad promedio agrupados por pais y dia.
>    - Genera el modelo **`zonas_criticas_frecuentes`**: cuadrantes de 1 grado x 1 grado (`floor(lat)`, `floor(lon)`) para deteccion de enjambres sismicos y fallas activas.
> 4. **Carga en Serving Layer:** Escribe los resultados en las tablas fisicas `gold_resumen_batch` y `gold_zonas_batch` en PostgreSQL.
> 5. **Trazabilidad:** Al concluir, invoca a `atlas_hook.py` para notificar a Apache Atlas el linaje de datos de extremo a extremo."

#### Preguntas trampa del jurado para Kevin:
* **¿Por que escribir en formato Parquet con Snappy en lugar de CSV o JSON?**
  * *Respuesta:* "Parquet es un formato columnar que almacena estadisticas de min/max por bloque y soporta compresion Snappy, reduciendo el tamaño en disco hasta en un 75%. Ademas, permite *Column Pruning* y *Predicate Pushdown*, logrando que Spark lea unicamente las columnas consultadas y descarte bloques completos que no coincidan con el filtro de fecha."
* **¿Como manejaste los valores nulos en magnitudes o coordenadas?**
  * *Respuesta:* "En el ETL implementamos reglas de calidad con funciones `when` y `coalesce`: magnitudes nulas se imputan a 0.0 y registros con coordenadas corruptas son descartados para no generar ruido en los modelos analiticos de la capa Gold."

---

### Turno 6: Jhonatan (Tech Lead) - Data Lake, Serving Layer, Gobernanza, Demo Web y Cierre

#### Que debes decir:
> "Para consolidar todo el valor del cluster, diseñe e implemente las capas de Almacenamiento, Servicio, Gobernanza y Consumo:
>
> **1. Data Lake MinIO y PostgreSQL Serving Layer:**
> Diseñe la arquitectura Medallion en MinIO asegurando particionamiento temporal Hive (`ano/mes/dia`) para garantizar *Partition Pruning*. En PostgreSQL cree el DDL de tablas fisicas y la vista unificada `vista_resumen_diario_pais`. La vista aplica un `UNION ALL` donde los datos de streaming del dia en curso tienen prioridad absoluta sobre el historico batch, resolviendo la deduplicacion sin sobrecostos computacionales.
>
> **2. Gobernanza con Apache Atlas & OpenMetadata:**
> - En **Apache Atlas (`:21000`)** registramos un grafo de linaje de 13 datasets y procesos. Cualquier auditor puede ver la trazabilidad visual desde la API de USGS, pasando por Kafka, MinIO Bronze, Spark y PostgreSQL.
> - En **OpenMetadata (`:8585`)** centralizamos el catalogo de datos y glosario de terminos, monitoreando el estado de los esquemas y conexiones.
>
> **3. Demostracion en Vivo de TerraPulse GIS (`:8000`):**
> (Abres la aplicacion en el navegador):
> - **Marcador de sismo critico:** Muestra en vivo el evento mayor con anillos concentricos de ondas de choque y fallas geologicas.
> - **Buscador Inteligente:** Buscamos 'Chile' o 'Japon'; el mapa navega instantaneamente al epicentro con normalizacion fonetica y de tildes.
> - **Capas Cartograficas Dinamicas:** Alternamos entre Gris Tactico, Oceanos, Satelital y OpenStreetMap con confirmacion visual mediante toasts.
> - **Paneles Bento Colapsables:** Ocultamos las tarjetas tacticas para habilitar el modo pantalla limpia y visualizar fallas tectonicas completas.
> - **GeoIA Copilot:** Abrimos el asistente inteligente desde el boton del planeta Tierra con carita de bot y le preguntamos: *'¿Cual fue el sismo mas critico registrado hoy?'*. La IA consulta PostgreSQL mediante RAG y responde en lenguaje natural.
>
> Demostramos asi una solucion Big Data distribuida, resiliente, totalmente desacoplada por variables de entorno y gobernada segun estandares empresariales."

---

## 4. Hoja Exclusiva de Preparacion para la Prueba de Desempeño del Tech Lead

Respuestas a preguntas avanzadas de arquitectura exclusivas para la evaluacion del lider:

### 1. ¿Como garantizan la consistencia eventual entre el carril Batch y el carril Streaming?
* **Respuesta:** "Mediante el principio de inmutabilidad de la capa Bronze y la reconciliacion deterministica en la Serving Layer. El carril streaming calcula sobre una ventana en movimiento (estado transitorio), mientras que el carril batch computa la funcion $f(DatosCrudos) = VistaHistorica$. Dado que los datos crudos en Bronze son inmutables, el carril batch reescribe cualquier discrepancia ocurrida en streaming cada 24 horas, alcanzando consistencia eventual perfecta."

### 2. Si se cae el Wi-Fi o el celular cambia de subred, ¿que pasa con el cluster?
* **Respuesta:** "El cluster esta 100% desacoplado mediante variables de entorno en archivos `.env`. Ningun contenedor ni script tiene IPs fijas quemadas en codigo. Si la red cambia, solo se actualizan las 3 IPs remotas (`IP_SAYDER`, `IP_JOSUE`, `IP_JHONATAN`) en los archivos `.env` respectivos y los servicios se reconectan automaticamente sin compilar ni tocar una sola linea de codigo."

### 3. ¿Que patron de particionamiento fisico utilizaron en MinIO y por que?
* **Respuesta:** "Utilizamos particionamiento temporal estilo Hive: `ano=YYYY/mes=MM/dia=DD/` en Bronze y `fecha_evento=YYYY-MM-DD/` en Silver. Esto habilita la tecnica de **Partition Pruning** en Apache Spark: al consultar sismos de un rango de fechas, Spark no escanea todo el bucket, sino que va directamente a la carpeta fisica correspondiente, ahorrando hasta un 90% de I/O en disco y memoria."

### 4. ¿Por que eligieron KRaft para Kafka en vez de ZooKeeper?
* **Respuesta:** "ZooKeeper introducia un punto unico de falla y sobrecarga de sincronizacion de metadatos fuera del broker. Con KRaft (KIP-500), el consenso se maneja internamente mediante el protocolo Raft en el puerto 29093. Esto reduce el consumo de RAM en la laptop de Josue y permite recuperaciones ante fallos en menos de 3 segundos."

# Guia Oficial de Estudio y Guion de Exposicion: Cluster de Monitoreo Sismico Global

Documento maestro de preparacion tecnica para la sustentacion del proyecto de Big Data y la evaluacion de desempeno del Tech Lead. Diseñado para que cada integrante conozca su rol con precision, justifique las decisiones de diseño y responda con seguridad ante las preguntas del jurado evaluador.

---

## 1. Estructura General y Tiempos de Exposicion (15 a 18 minutos)

| Turno | Expositor | Rol Tecnico | Tema Central | Tiempo Estimado |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Jhonatan** | Tech Lead | Apertura, Arquitectura General Lambda + Medallion y Reparticion de Hardware | 2 a 3 min |
| **2** | **Josue** | Data Streaming Engineer | Ingesta en Tiempo Real, Broker Apache Kafka y Conectores Kafka Connect | 3 a 4 min |
| **3** | **Sayder** | Storage & Database Engineer | Data Lake Medallion en MinIO y Serving Layer en PostgreSQL | 3 a 4 min |
| **4** | **Jesus** | Processing & Orchestration Engineer | Pipeline por Lotes con Airflow y Computo Distribuido con Apache Spark | 4 a 5 min |
| **5** | **Jhonatan** | Tech Lead | Gobernanza (Atlas & OpenMetadata), Deduplicacion Lambda y Demostracion en Vivo de TerraPulse GIS | 4 a 5 min |

---

## 2. Guion Detallado por Integrante

---

### Turno 1: Jhonatan (Tech Lead) - Introduccion y Vision General de Arquitectura

#### Que debes decir:
> "Buenos dias. Nuestro proyecto resuelve un problema fundamental en la ingenieria de datos moderna: **¿como procesar y alertar sismos en tiempo real sin perder la completitud y precision del analisis historico?**
>
> Para resolverlo, diseñamos e implementamos una **Arquitectura Lambda** distribuida en 4 computadores fisicos, combinada con un **Data Lake Medallion (Bronze, Silver, Gold)**. 
> 
> En lugar de procesarlo todo en un solo servidor monolitico, desacoplamos el sistema en dos carriles:
> 1. Un **Speed Layer (Carril Streaming)** que captura sismos segundo a segundo desde la API de USGS para alertar en milisegundos.
> 2. Un **Batch Layer (Carril por Lotes)** que corre todos los dias a las 7:00 AM para reconciliar y asegurar que ningun dato se pierda por fallas de red o caidas temporales de la API.
>
> Ambos carriles convergen en una **Serving Layer en PostgreSQL**, donde creamos vistas SQL unificadas que resuelven la deduplicacion en tiempo de consulta. Ademas, gobernamos todo el ciclo de vida del dato con **Apache Atlas** y **OpenMetadata**, y presentamos la telemetria en nuestro centro de mando tactico **TerraPulse GIS**, que incluye un copiloto de inteligencia artificial geofisica.
>
> A continuacion, mis compañeros explicaran el funcionamiento tecnico de cada capa del cluster."

#### Preguntas trampa del jurado y como responderlas:
* **¿Por que Arquitectura Lambda y no solo Streaming (Kappa) o solo Batch?**
  * *Respuesta:* "Si usaramos solo Batch, las alertas sismicas llegarian con horas de retraso, lo cual es inaceptable para salvar vidas o activar protocolos de emergencia. Si usaramos solo Streaming (Kappa), ante desconexiones de red prolongadas se acumularian huecos en los datos que son muy costosos de reprocesar en vivo. Lambda nos da lo mejor de ambos mundos: inmediatez en vivo y consistencia matematica infalible en el historico."
* **¿Por que distribuyeron los servicios en 4 computadores de esa manera?**
  * *Respuesta:* "La distribucion responde a los recursos de hardware: Sayder (4 GB RAM) se encarga exclusivamente de almacenamiento y base de datos (MinIO y PostgreSQL). Josue gestiona la ingesta de streaming (Kafka y Connect). Jesus, que cuenta con una maquina potente, centraliza el motor de computo pesado (Spark Master/Workers) y la orquestacion de Airflow, comunicandolos localmente por Docker sin saturar el Wi-Fi. Y en mi maquina centralizamos la gobernanza (Atlas y OpenMetadata) y la aplicacion web de consumo."

---

### Turno 2: Josue - Ingesta y Mensajeria en Tiempo Real (Apache Kafka & Connect)

#### Que debes decir:
> "En mi rol de Ingeniero de Streaming, mi responsabilidad es garantizar que ningun evento sismico se pierda y que la ingesta ocurra de manera continua las 24 horas del dia.
>
> En mi maquina corre un cluster de **Apache Kafka en modo KRaft (Kafka Raft)**, lo que significa que eliminamos la dependencia de ZooKeeper, logrando mayor velocidad y menor consumo de memoria.
>
> Para la ingesta utilizamos **Kafka Connect**:
> 1. Un conector **HTTP Source** sondea continuamente el feed en vivo de la API de USGS y deposita los sismos en el topico `sismos_crudos`.
> 2. Un conector **S3 Sink** lee inmediatamente de ese topico y replica los datos crudos hacia el Data Lake MinIO en la laptop de Sayder, guardandolos en la ruta `bronze/streaming` particionados por año, mes y dia.
>
> De esta forma, el broker de Kafka actúa como un amortiguador (buffer desacoplador): si la red falla o Spark se reinicia, los datos quedan protegidos en las particiones del broker con retencion garantizada."

#### Preguntas trampa del jurado y como responderlas:
* **¿Por que usar Kafka Connect en vez de un script de Python con `requests` y `kafka-python`?**
  * *Respuesta:* "Kafka Connect es un estandar industrial tolerante a fallos. Si el proceso se cae, Connect guarda el estado del offset (`connect-offsets`) y reinicia exactamente donde quedo, evitando duplicados. Ademas, permite escalar horizontalmente agregando workers sin reescribir codigo."
* **¿Que garantia de entrega tienen los mensajes en Kafka?**
  * *Respuesta:* "Configuramos entrega *At-least-once* (al menos una vez). Si ocurre un corte de red entre Kafka y MinIO, el conector reintenta el envio. Los posibles duplicados no afectan al usuario final porque se resuelven de forma deterministica mediante deduplicacion por `id_evento` en las capas Silver y Serving."

---

### Turno 3: Sayder - Data Lake Medallion y Base de Datos Serving Layer (MinIO & PostgreSQL)

#### Que debes decir:
> "En mi computador administro el corazon del almacenamiento del cluster: el **Data Lake Medallion sobre MinIO S3** y la **Serving Layer sobre PostgreSQL**.
>
> Mi maquina cuenta con 4 GB de memoria RAM, por lo que optimizamos los servicios para que MinIO y PostgreSQL operen de manera eficiente sin sobrecargar la CPU.
>
> El Data Lake en MinIO esta organizado bajo la **Arquitectura Medallion**:
> 1. **Capa Bronze:** Contiene los datos crudos en formato JSON inmutable, tanto del carril streaming como del carril batch.
> 2. **Capa Silver:** Contiene los datos limpios y tipados en formato columnar **Snappy Parquet** (`fact_sismos`), particionados por fecha.
> 3. **Capa Gold:** Contiene los modelos analiticos agregados: `resumen_diario_pais` y `zonas_criticas_frecuentes`.
>
> En la **Serving Layer en PostgreSQL**, recibimos los resultados finales:
> - Spark Streaming hace `upsert` continuo en la tabla `delta_streaming_hoy`.
> - Spark Batch carga diariamente las tablas `gold_resumen_batch` y `gold_zonas_batch`.
> - Creamos la vista `vista_resumen_diario_pais`, la cual realiza una union inteligente: si la consulta es del dia de hoy, toma los datos en vivo de streaming; si es de dias anteriores, toma los datos consolidados de batch. Asi la aplicacion web obtiene respuestas en menos de 10 milisegundos sin duplicados."

#### Preguntas trampa del jurado y como responderlas:
* **¿Por que guardar en MinIO con Parquet en vez de guardar todo directo en PostgreSQL?**
  * *Respuesta:* "PostgreSQL es una base de datos relacional orientada a transacciones y consultas de serving (OLTP). Si le metemos millones de registros crudos en JSON, el almacenamiento colapsa y las consultas se vuelven lentas. MinIO almacena objetos masivos a bajo costo, y el formato Parquet con compresion Snappy reduce el tamaño en disco hasta en un 75% y acelera las lecturas analiticas al leer solo las columnas necesarias."
* **¿Como resuelven la deduplicacion en PostgreSQL?**
  * *Respuesta:* "Lo resolvemos en dos niveles: en la tabla `delta_streaming_hoy` usamos llaves unicas (`ON CONFLICT (id_evento) DO UPDATE`) para no duplicar sismos en vivo. Y en la vista SQL `vista_resumen_diario_pais` aplicamos un filtro donde el carril Streaming tiene precedencia absoluta sobre el dia en curso y Batch sobre el historico."

---

### Turno 4: Jesus - Procesamiento Masivo y Orquestacion (Apache Spark & Airflow)

#### Que debes decir:
> "En mi computador centralizo la capacidad de computo y orquestacion del cluster, donde conviven **Apache Airflow** y **Apache Spark (Master y Workers)** comunicados directamente a traves de una red interna de Docker (`red_bigdata`).
>
> Mi labor abarca los dos pipelines de procesamiento:
>
> **1. Pipeline de Streaming (Structured Streaming):**
> Corre de manera ininterrumpida ejecutando `spark_streaming_job.py`. Lee los micro-lotes desde el topico de Kafka de Josue, parsea el GeoJSON, limpia nulos, clasifica los sismos segun su profundidad y magnitud (Mw >= 6.0 como CRITICO), escribe en MinIO Silver/Gold en Parquet y actualiza PostgreSQL en Sayder.
>
> **2. Pipeline Batch (Lotes Diarios con Airflow):**
> Corre todos los dias a las 7:00 AM mediante el DAG `dag_sismos_batch_usgs.py`:
> - Tarea 1: Descarga el consolidado de 24 horas de la API de USGS.
> - Tarea 2: Deposita el JSON inmutable en MinIO Bronze en Sayder.
> - Tarea 3: Hace un llamado HTTP interno a nuestro Trigger Server en el puerto 5050 (`spark-master:5050/trigger`), disparando `spark_batch_etl.py`.
> - Spark Batch toma el lote diario, procesa las agregaciones a gran escala, escribe en MinIO Gold y vuelca el historico en PostgreSQL. Al finalizar, notifica el linaje hacia Apache Atlas en la laptop de Jhonatan."

#### Preguntas trampa del jurado y como responderlas:
* **¿Que pasa si la tarea de Spark Batch falla durante la ejecucion en Airflow?**
  * *Respuesta:* "El DAG tiene configurada una politica de resiliencia con 2 reintentos automaticos y un retraso de 2 minutos (`retries=2, retry_delay=timedelta(minutes=2)`). Si el trigger HTTP devuelve error 500, Airflow lo registra, reintenta y alerta el fallo sin detener el carril streaming, garantizando aislamiento de fallos."
* **¿Por que Airflow y Spark estan en la misma maquina?**
  * *Respuesta:* "Fue una decision de diseño para optimizar la red. Si Airflow estuviera en una maquina y Spark en otra, el disparo y la transferencia de logs saturarian la red Wi-Fi compartida. Al estar juntos en la red Docker `red_bigdata`, la comunicacion toma menos de 1 milisegundo y tiene cero dependencias externas."

---

### Turno 5: Jhonatan (Tech Lead) - Gobernanza, Consumo y Cierre

#### Que debes decir:
> "Para cerrar el ciclo del dato, en mi computador gestionamos la **Gobernanza**, la **Auditoria** y el **Consumo Final**:
>
> **1. Gobernanza de Datos con Apache Atlas y OpenMetadata:**
> No basta con mover datos; hay que saber de donde vienen y quien los consume.
> - Con **Apache Atlas (`:21000`)** monitoreamos el **Linaje del Dato**: cada vez que Spark procesa un lote, el script `atlas_hook.py` envia los metadatos de transformacion, permitiendo ver graficamente la ruta completa: USGS -> MinIO Bronze -> Spark ETL -> MinIO Silver/Gold -> PostgreSQL.
> - Con **OpenMetadata (`:8585`)** centralizamos el catalogo de datos, glosario de negocio y estado de conexion de Kafka, PostgreSQL y MinIO.
>
> **2. Demostracion en Vivo de TerraPulse GIS (`:8000`):**
> (Abres la pagina en el navegador y muestras):
> - **Marcador de choque y sismo critico:** Muestra en tiempo real el sismo mas fuerte registrado con ondas concentricas de choque.
> - **Buscador Inteligente:** Buscamos por ejemplo 'Chile' o 'Japon'; el mapa navega instantaneamente al epicentro con normalizacion de tildes.
> - **Capas Cartograficas Dinamicas:** Alternamos entre Gris Tactico, Satelital, Oceanos y OpenStreetMap con notificaciones toast en vivo.
> - **Modo Pantalla Limpia:** Ocultamos los paneles Bento colapsables para analisis visual de fallas tectonicas.
> - **GeoIA Copilot:** Abrimos el asistente inteligente desde el boton del planeta Tierra y le preguntamos en lenguaje natural: *'¿Cual fue el sismo mas critico registrado hoy?'*. La IA consulta la base de datos PostgreSQL via RAG y responde con precision milimetrica.
>
> Con esto demostramos una arquitectura Big Data empresarial: resiliente, desacoplada, gobernada y lista para produccion. Quedamos atentos a sus preguntas."

---

## 3. Hoja de Preparacion para la Prueba de Desempeño del Tech Lead

Como lider del equipo, estas son las preguntas avanzadas sobre decisiones de arquitectura que te pueden hacer individualmente:

### 1. ¿Como garantizan la consistencia eventual entre el carril Batch y el carril Streaming?
* **Respuesta maestra:** "Mediante el principio de inmutabilidad de la capa Bronze y la reconciliacion en la Serving Layer. El carril streaming calcula sobre una ventana en movimiento (estado transitorio), mientras que el carril batch computa la funcion $f(DatosCrudos) = VistaHistorica$. Dado que los datos crudos en Bronze son inmutables, el carril batch reescribe cualquier discrepancia ocurrida en streaming cada 24 horas, alcanzando consistencia eventual perfecta."

### 2. Si se cae el Wi-Fi o el celular cambia de IP, ¿que pasa con el cluster?
* **Respuesta maestra:** "El cluster esta 100% desacoplado mediante variables de entorno en archivos `.env`. Ningun script ni contenedor tiene IPs fijas quemadas en codigo. Si la red cambia de subred, solo se actualizan las 3 IPs remotas (`IP_SAYDER`, `IP_JOSUE`, `IP_JHONATAN`) en los archivos `.env` respectivos y los servicios se reconectan automaticamente sin compilar ni tocar una sola linea de codigo."

### 3. ¿Que patron de particionamiento fisico utilizaron en MinIO y por que?
* **Respuesta maestra:** "Utilizamos particionamiento temporal estilo Hive: `ano=YYYY/mes=MM/dia=DD/` en Bronze y `fecha_evento=YYYY-MM-DD/` en Silver. Esto permite la tecnica de **Partition Pruning** (poda de particiones) en Apache Spark: cuando consultamos los sismos de un mes o un dia, Spark no escanea todo el bucket, sino que va directamente a la carpeta fisica correspondiente, ahorrando hasta un 90% de I/O en disco y memoria."

### 4. ¿Por que eligieron KRaft para Kafka en vez de ZooKeeper?
* **Respuesta maestra:** "ZooKeeper introducia un punto unico de falla y sobrecarga de sincronizacion de metadatos fuera del broker. Con KRaft (KIP-500), el consenso se maneja internamente mediante el protocolo Raft en el puerto 29093. Esto reduce el consumo de RAM en la laptop de Josue y permite recuperaciones ante fallos en menos de 3 segundos."

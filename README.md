# Proyecto de Monitoreo Sísmico Global (Big Data)

Arquitectura **Lambda** combinada con **Medallion (Bronze, Silver, Gold)** sobre MinIO, Kafka, Apache Spark, Apache Airflow, PostgreSQL y Gobernanza de Datos.

---

## 🏛️ Diagrama de Arquitectura
Los diagramas visuales completos de la arquitectura se encuentran en la carpeta [`docs/img/`](docs/img/):
* **Diagrama General Lambda + Medallion:** `docs/img/arquitectura_lambda_medallion.jpg`
* **Diagrama Limpio por Carriles (Streaming vs Batch):** `docs/img/arquitectura_diagrama_limpio.jpg`

---

## 📁 Estructura del Repositorio

```text
Proyecto_sismos/
├── data/
│   ├── sample_streaming_hora.json     <- Muestra cruda de la API (última hora) para el pipeline Streaming
│   └── sample_batch_dia.json          <- Muestra cruda de la API (últimas 24 horas) para el pipeline Batch
├── docs/
│   ├── especificacion_tablas_medallion.md <- Especificación exacta de esquemas Silver y Gold
│   └── img/                           <- Diagramas de arquitectura en alta resolución
├── infraestructura/
│   ├── kafka/                         <- Kafka KRaft + Kafka Connect + HTTP Source Connector
│   ├── spark/                         <- Master y Worker de Apache Spark 3.5.0
│   ├── minIO/                         <- Object Storage S3 (Buckets Bronze, Silver, Gold)
│   ├── airflow/                       <- Airflow 3.3.2 CeleryExecutor (Batch 7:00 AM)
│   ├── postgres/                      <- Serving Layer (Tablas y Vista Unificada)
│   └── gobernanza/                    <- Apache Atlas & OpenMetadata
└── README.md
```

---

## 📊 Datos Crudos de Entrada (Capa Bronze)

En la carpeta [`data/`](data/) se encuentran dos archivos `.json` reales descargados de la API del USGS:

1. **`sample_streaming_hora.json`:** Representa lo que consume Kafka Connect en tiempo real (ventana corta de la última hora).
2. **`sample_batch_dia.json`:** Representa lo que descarga Airflow a las 7:00 AM (ventana acumulada de las últimas 24 horas).

> **Estructura idéntica:** Ambos archivos tienen **exactamente la misma estructura GeoJSON (`features[]`)**. La única diferencia es el volumen de sismos y la ventana de tiempo.

---

## 🛠️ Tareas y Especificación para el Equipo

Cada integrante debe programar sus respectivos componentes desde cero basándose en la documentación técnica:

* **Especificación de Esquemas:** Consultar [`docs/especificacion_tablas_medallion.md`](docs/especificacion_tablas_medallion.md) para conocer las columnas exactas, tipos de datos, particionado y reglas de negocio para:
  * **Capa Silver:** Tabla de hechos limpia y desduplicada (1 fila = 1 sismo).
  * **Capa Gold Tabla 1:** Resumen diario por país (`resumen_diario_pais`).
  * **Capa Gold Tabla 2:** Zonas críticas y hotspots sísmicos (`zonas_criticas_frecuentes`).

---

## 👥 Repartición de Responsabilidades

1. **Tech Lead / Serving Layer & Web App:** Configuración global, PostgreSQL, Vista Unificada y Frontend/Consumo.
2. **Ingeniero Streaming (Kafka & Kafka Connect):** Pipeline en tiempo real, HTTP Source Connector y S3 Sink a MinIO Bronze.
3. **Ingeniero Batch (Apache Airflow):** DAG diario de las 7:00 AM, extracción masiva a MinIO Bronze y trigger de Spark.
4. **Ingeniero de Datos (Apache Spark - PySpark):** Scripts ETL de limpieza Bronze -> Silver y agregaciones Silver -> Gold.
5. **Ingeniero de Almacenamiento y Gobernanza:** Gestión de MinIO (Data Lake) y catálogo de datos/linaje con Atlas/OpenMetadata.

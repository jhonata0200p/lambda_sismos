import os
import sys
import time
import requests
import json
import logging

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [ATLAS-INIT] %(levelname)s: %(message)s")

ATLAS_HOST = os.getenv("ATLAS_HOST", "localhost")
ATLAS_PORT = os.getenv("ATLAS_PORT", "21000")
ATLAS_USER = os.getenv("ATLAS_USER", "admin")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD", "admin")

BASE_URL = f"http://{ATLAS_HOST}:{ATLAS_PORT}/api/atlas/v2"
AUTH = (ATLAS_USER, ATLAS_PASSWORD)
HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

def esperar_atlas(max_reintentos=45, pausa_seg=5):
    """Espera hasta que Apache Atlas esté completamente listo y responda."""
    logging.info(f"Verificando disponibilidad de Apache Atlas en {BASE_URL}...")
    for i in range(1, max_reintentos + 1):
        try:
            r = requests.get(f"{BASE_URL}/types/typedefs/headers", auth=AUTH, headers=HEADERS, timeout=4)
            if r.status_code == 200:
                logging.info("Apache Atlas está EN LÍNEA y listo para recibir metadatos.")
                return True
        except Exception:
            pass
        logging.info(f"[{i}/{max_reintentos}] Atlas inicializando HBase/Solr... esperando {pausa_seg}s...")
        time.sleep(pausa_seg)
    logging.error("No fue posible conectar con Apache Atlas dentro del tiempo límite.")
    return False

def registrar_clasificaciones():
    """Registra etiquetas de gobernanza (Classification Definitions)."""
    logging.info("Registrando clasificaciones de gobernanza en Atlas...")
    classifications = [
        {"name": "MEDALLION_BRONZE", "description": "Capa Bronze: Datos Crudos JSON (Ingesta directa de USGS)"},
        {"name": "MEDALLION_SILVER", "description": "Capa Silver: Datos Limpios Parquet Particionados"},
        {"name": "MEDALLION_GOLD", "description": "Capa Gold: Modelos Analíticos y Agregaciones Geofísicas"},
        {"name": "CRITICO_M5", "description": "Sismos Graves con Magnitud >= 5.0 (Peligro Elevado)"},
        {"name": "ALERTA_TSUNAMI", "description": "Protocolo de Alerta de Tsunami Costero Activado"}
    ]
    payload = {"classificationDefs": classifications}
    try:
        r = requests.post(f"{BASE_URL}/types/typedefs", json=payload, auth=AUTH, headers=HEADERS, timeout=10)
        if r.status_code in [200, 201]:
            logging.info("Clasificaciones de gobernanza creadas exitosamente.")
        else:
            logging.info(f"Clasificaciones ya existentes o código {r.status_code}.")
    except Exception as e:
        logging.warning(f"Aviso registrando clasificaciones: {e}")

def registrar_entidades_y_linaje():
    """Registra todo el catálogo de datasets y procesos conectando el grafo de linaje."""
    logging.info("Construyendo árbol de linaje Medallion y Serving Layer...")

    # 1. Definición de Datasets (Almacenamiento)
    datasets = [
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "usgs_api_raw@usgs",
                "name": "API Externa USGS Earthquake Hazards",
                "description": "Feed público REST JSON en vivo de la USGS (Earthquake Hazards Program)"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "kafka://streaming@kafka",
                "name": "Topic Kafka: streaming",
                "description": "Topic primario de ingesta streaming 24/7 en Apache Kafka"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "kafka://sismos_crudos@kafka",
                "name": "Topic Kafka: sismos_crudos",
                "description": "Topic sincronizado en tiempo real para consumo del cluster Spark Streaming"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://bronze/streaming/@minio",
                "name": "MinIO Bronze Streaming (/streaming)",
                "description": "Data Lake MinIO (Laptop Sayder 10.175.132.248): JSONs crudos particionados año/mes/día"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://bronze/batch/@minio",
                "name": "MinIO Bronze Batch (/batch)",
                "description": "Data Lake MinIO: Extracciones diarias consolidadas a las 07:00 AM ejecutadas por Airflow"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://silver/streaming/fact_sismos/@minio",
                "name": "MinIO Silver Streaming (/silver/streaming)",
                "description": "Parquet enriquecido y tipado: sismos deduplicados y limpios en tiempo real"
            },
            "classifications": [{"typeName": "MEDALLION_SILVER"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://silver/batch/fact_sismos/@minio",
                "name": "MinIO Silver Batch (/silver/batch)",
                "description": "Parquet limpio diario procesado por Spark Batch ETL"
            },
            "classifications": [{"typeName": "MEDALLION_SILVER"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/streaming/resumen_diario_pais/@minio",
                "name": "MinIO Gold Streaming: Resumen Diario",
                "description": "Agregación analítica por país/estado en vivo"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/streaming/zonas_criticas_frecuentes/@minio",
                "name": "MinIO Gold Streaming: Cuadrículas Críticas",
                "description": "Matriz de coordenadas y cuadrículas activas en vivo"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/batch/resumen_diario_pais/@minio",
                "name": "MinIO Gold Batch: Resumen Diario País",
                "description": "Parquet analítico Gold consolidado diario"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/batch/zonas_criticas_frecuentes/@minio",
                "name": "MinIO Gold Batch: Zonas Críticas",
                "description": "Historial de fallas críticas acumuladas"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_resumen_streaming@postgres",
                "name": "PostgreSQL: gold_resumen_streaming",
                "description": "Tabla física en PostgreSQL Serving Layer (Laptop Sayder 10.175.132.248:5432)"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_resumen_batch@postgres",
                "name": "PostgreSQL: gold_resumen_batch",
                "description": "Tabla física en PostgreSQL con datos históricos del lote diario"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_zonas_streaming@postgres",
                "name": "PostgreSQL: gold_zonas_streaming",
                "description": "Tabla física con cuadrículas críticas actualizadas en streaming"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}, {"typeName": "CRITICO_M5"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_zonas_batch@postgres",
                "name": "PostgreSQL: gold_zonas_batch",
                "description": "Tabla física con historial consolidado de zonas sísmicas"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/vista_resumen_diario_pais@postgres",
                "name": "PostgreSQL Vista SQL: vista_resumen_diario_pais",
                "description": "Vista unificada sin duplicados de la Arquitectura Lambda (Streaming + Batch)"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/vista_zonas_criticas_frecuentes@postgres",
                "name": "PostgreSQL Vista SQL: vista_zonas_criticas_frecuentes",
                "description": "Vista unificada de fallas geológicas para monitoreo de riesgo"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}, {"typeName": "ALERTA_TSUNAMI"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "dashboard_web_serving@fastapi",
                "name": "Centro de Mando Sísmico Web (FastAPI)",
                "description": "Consola operativa de visualización táctica y Asistente GeoCopilot Gemini AI"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        }
    ]

    # 2. Definición de Procesos (Flechas de Linaje)
    def ref(qn):
        return {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": qn}}

    processes = [
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "kafka_connect_http_source@kafka",
                "name": "Kafka Connect: HTTP Source Ingestion",
                "description": "Consulta periódica en tiempo real hacia la API de USGS",
                "inputs": [ref("usgs_api_raw@usgs")],
                "outputs": [ref("kafka://streaming@kafka"), ref("kafka://sismos_crudos@kafka")]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "kafka_connect_s3_sink@kafka",
                "name": "Kafka Connect: MinIO S3 Sink Bronze",
                "description": "Persistencia directa 24/7 de eventos crudos en MinIO Bronze Streaming",
                "inputs": [ref("kafka://streaming@kafka")],
                "outputs": [ref("s3a://bronze/streaming/@minio")]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "airflow_batch_extractor@airflow",
                "name": "Apache Airflow: Extracción Batch Diaria (07:00 AM)",
                "description": "DAG programado de extracción consolidada hacia Bronze Batch",
                "inputs": [ref("usgs_api_raw@usgs")],
                "outputs": [ref("s3a://bronze/batch/@minio")]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "spark_streaming_pipeline@spark",
                "name": "Apache Spark Streaming Job",
                "description": "Micro-batches de limpieza y agregación en tiempo real (Laptop Jesús)",
                "inputs": [ref("kafka://sismos_crudos@kafka")],
                "outputs": [
                    ref("s3a://silver/streaming/fact_sismos/@minio"),
                    ref("s3a://gold/streaming/resumen_diario_pais/@minio"),
                    ref("s3a://gold/streaming/zonas_criticas_frecuentes/@minio"),
                    ref("postgres://sismos_db/gold_resumen_streaming@postgres"),
                    ref("postgres://sismos_db/gold_zonas_streaming@postgres")
                ]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "spark_batch_etl@spark",
                "name": "Apache Spark Batch ETL Job",
                "description": "ETL pesado de normalización y modelado analítico batch",
                "inputs": [ref("s3a://bronze/batch/@minio")],
                "outputs": [
                    ref("s3a://silver/batch/fact_sismos/@minio"),
                    ref("s3a://gold/batch/resumen_diario_pais/@minio"),
                    ref("s3a://gold/batch/zonas_criticas_frecuentes/@minio"),
                    ref("postgres://sismos_db/gold_resumen_batch@postgres"),
                    ref("postgres://sismos_db/gold_zonas_batch@postgres")
                ]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "postgres_view_consolidation@postgres",
                "name": "PostgreSQL: Consolidación SQL Lambda",
                "description": "Vistas SQL que unifican el carril Streaming y Batch sin duplicados",
                "inputs": [
                    ref("postgres://sismos_db/gold_resumen_streaming@postgres"),
                    ref("postgres://sismos_db/gold_resumen_batch@postgres"),
                    ref("postgres://sismos_db/gold_zonas_streaming@postgres"),
                    ref("postgres://sismos_db/gold_zonas_batch@postgres")
                ],
                "outputs": [
                    ref("postgres://sismos_db/vista_resumen_diario_pais@postgres"),
                    ref("postgres://sismos_db/vista_zonas_criticas_frecuentes@postgres")
                ]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "serving_web_layer@fastapi",
                "name": "Consumo Serving Layer: Web Dashboard FastAPI",
                "description": "Exposición de KPIs, sismógrafo y RAG GeoCopilot hacia la interfaz",
                "inputs": [
                    ref("postgres://sismos_db/vista_resumen_diario_pais@postgres"),
                    ref("postgres://sismos_db/vista_zonas_criticas_frecuentes@postgres")
                ],
                "outputs": [ref("dashboard_web_serving@fastapi")]
            }
        }
    ]

    payload = {"entities": datasets + processes}

    try:
        r = requests.post(f"{BASE_URL}/entity/bulk", json=payload, auth=AUTH, headers=HEADERS, timeout=20)
        if r.status_code in [200, 201]:
            logging.info("¡Todas las entidades y relaciones de linaje fueron cargadas exitosamente en Apache Atlas!")
            logging.info("Puedes verificar el linaje en: http://localhost:21000")
            return True
        else:
            logging.error(f"Error cargando entidades: {r.status_code} - {r.text[:300]}")
            return False
    except Exception as e:
        logging.error(f"Falla conectando con Atlas en el bulk: {e}")
        return False

if __name__ == "__main__":
    logging.info("=== INICIALIZADOR DEL ÁRBOL DE GOBERNANZA APACHE ATLAS ===")
    if esperar_atlas():
        registrar_clasificaciones()
        registrar_entidades_y_linaje()
        logging.info("Proceso completado. Linaje listo para sustentación.")
    else:
        sys.exit(1)

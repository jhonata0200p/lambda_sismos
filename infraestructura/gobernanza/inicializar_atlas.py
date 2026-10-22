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
                logging.info("Apache Atlas esta EN LINEA y listo para recibir metadatos.")
                return True
        except Exception:
            pass
        logging.info(f"[{i}/{max_reintentos}] Atlas inicializando HBase/Solr... esperando {pausa_seg}s...")
        time.sleep(pausa_seg)
    logging.error("No fue posible conectar con Apache Atlas dentro del tiempo limite.")
    return False

def registrar_clasificaciones():
    """Registra etiquetas de gobernanza (Classification Definitions)."""
    logging.info("Registrando clasificaciones de gobernanza en Atlas...")
    classifications = [
        {"name": "MEDALLION_BRONZE", "description": "Capa Bronze: Datos Crudos JSON (Ingesta directa de USGS)"},
        {"name": "MEDALLION_SILVER", "description": "Capa Silver: Datos Limpios Parquet Particionados"},
        {"name": "MEDALLION_GOLD", "description": "Capa Gold: Modelos Analiticos y Agregaciones Geofisicas"},
        {"name": "CRITICO_M5", "description": "Sismos Graves con Magnitud >= 5.0 (Peligro Elevado)"},
        {"name": "ALERTA_TSUNAMI", "description": "Protocolo de Alerta de Tsunami Costero Activado"}
    ]
    payload = {"classificationDefs": classifications}
    try:
        r = requests.post(f"{BASE_URL}/types/typedefs", json=payload, auth=AUTH, headers=HEADERS, timeout=10)
        if r.status_code in [200, 201]:
            logging.info("Clasificaciones de gobernanza creadas exitosamente.")
        else:
            logging.info(f"Clasificaciones ya existentes o codigo {r.status_code}.")
    except Exception as e:
        logging.warning(f"Aviso registrando clasificaciones: {e}")

def registrar_glosario():
    """Registra el Glosario de Negocio de Gobernanza y sus terminos tecnicos."""
    logging.info("Registrando Glosario de Terminos de Negocio en Atlas...")
    glossary_payload = {
        "name": "Glosario_Sismico_Global",
        "shortDescription": "Glosario oficial de terminos tecnicos y gobernanza de datos sismicos",
        "longDescription": "Estandar corporativo que define las metricas geofisicas, niveles de alerta y capas Medallion"
    }

    glossary_guid = None
    try:
        # 1. Verificar si ya existe el glosario
        r_list = requests.get(f"{BASE_URL}/glossary", auth=AUTH, headers=HEADERS, timeout=10)
        if r_list.status_code == 200:
            for g in r_list.json():
                if g.get("name") == "Glosario_Sismico_Global":
                    glossary_guid = g.get("guid")
                    logging.info(f"Glosario existente detectado (GUID: {glossary_guid}).")
                    break

        # 2. Si no existe, crearlo
        if not glossary_guid:
            r_create = requests.post(f"{BASE_URL}/glossary", json=glossary_payload, auth=AUTH, headers=HEADERS, timeout=10)
            if r_create.status_code in [200, 201]:
                glossary_guid = r_create.json().get("guid")
                logging.info(f"Glosario de Negocio creado exitosamente (GUID: {glossary_guid}).")

        # 3. Registrar los terminos del glosario
        if glossary_guid:
            terminos = [
                {
                    "name": "Sismo_Critico_M5",
                    "shortDescription": "Sismo grave con magnitud Richter >= 5.0",
                    "longDescription": "Clasificacion tecnica de sismos con alto potencial destructivo que detonan alertas prioritarias"
                },
                {
                    "name": "Alerta_Tsunami",
                    "shortDescription": "Aviso oceanografico por sismo costero/submarino >= 6.0",
                    "longDescription": "Protocolo de prevencion activado por el modelo analitico ante eventos sismicos de alta energia en zonas costeras"
                },
                {
                    "name": "Cuadricula_Hotspot",
                    "shortDescription": "Celda espacial 0.1 grados (~10 km2) de agrupacion de fallas",
                    "longDescription": "Malla analitica de agregacion de coordenadas para detectar enjambres sismicos y recurrencia geologica"
                },
                {
                    "name": "Medallion_Bronze",
                    "shortDescription": "Datos crudos inmutables en formato GeoJSON de USGS",
                    "longDescription": "Capa base de ingesta sin transformacion depositada en tiempo real (Kafka Connect) y batch (Airflow)"
                },
                {
                    "name": "Medallion_Silver",
                    "shortDescription": "Datos limpios, desanidados y deduplicados en Parquet",
                    "longDescription": "Capa intermedia optimizada columnarmente con particionamiento diario y tipos de datos normalizados"
                },
                {
                    "name": "Medallion_Gold",
                    "shortDescription": "Modelos analiticos agregados y matriz de riesgo sismico",
                    "longDescription": "Capa de kpis consolidados por fecha/pais y hotspots geograficos lista para consumo de analistas"
                },
                {
                    "name": "Serving_Layer_Lambda",
                    "shortDescription": "Capa de servicio PostgreSQL con vistas unificadas",
                    "longDescription": "Unificacion de datos historicos batch con el flujo streaming en vivo sin duplicados para la Web y BI"
                }
            ]

            for t in terminos:
                term_payload = {
                    "name": t["name"],
                    "shortDescription": t["shortDescription"],
                    "longDescription": t["longDescription"],
                    "anchor": {"glossaryGuid": glossary_guid}
                }
                r_term = requests.post(f"{BASE_URL}/glossary/term", json=term_payload, auth=AUTH, headers=HEADERS, timeout=10)
                if r_term.status_code in [200, 201]:
                    logging.info(f"Termino de glosario registrado: {t['name']}")
                else:
                    logging.info(f"Termino {t['name']} ya existente o verificado ({r_term.status_code}).")
    except Exception as e:
        logging.warning(f"Aviso registrando glosario de negocio: {e}")

def registrar_entidades_y_linaje():
    """Registra todo el catalogo de datasets y procesos conectando el grafo de linaje."""
    logging.info("Construyendo arbol de linaje Medallion y Serving Layer...")

    # 1. Definicion de Datasets (Almacenamiento)
    datasets = [
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "usgs_api_raw@usgs",
                "name": "API Externa USGS Earthquake Hazards",
                "description": "Feed publico REST JSON en vivo de la USGS (Earthquake Hazards Program)"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "kafka://sismos_crudos@kafka",
                "name": "Topic Kafka: sismos_crudos",
                "description": "Topic oficial primario de ingesta streaming 24/7 en Apache Kafka"
            },
            "classifications": [{"typeName": "MEDALLION_BRONZE"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://bronze/streaming/@minio",
                "name": "MinIO Bronze Streaming (/streaming)",
                "description": "Data Lake MinIO: JSONs crudos particionados ano/mes/dia persistidos en tiempo real"
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
                "qualifiedName": "s3a://silver/streaming/@minio",
                "name": "MinIO Silver Streaming (/silver/streaming)",
                "description": "Parquet enriquecido y tipado: sismos deduplicados y limpios en tiempo real"
            },
            "classifications": [{"typeName": "MEDALLION_SILVER"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://silver/batch/@minio",
                "name": "MinIO Silver Batch (/silver/batch)",
                "description": "Parquet limpio diario procesado por Spark Batch ETL"
            },
            "classifications": [{"typeName": "MEDALLION_SILVER"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/streaming/resumen_diario_pais/@minio",
                "name": "MinIO Gold Streaming: Resumen Diario Pais",
                "description": "Agregacion analitica por pais/estado en tiempo real"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/streaming/zonas_criticas_frecuentes/@minio",
                "name": "MinIO Gold Streaming: Zonas Criticas",
                "description": "Matriz de coordenadas y cuadriculas activas en tiempo real"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/batch/resumen_diario_pais/@minio",
                "name": "MinIO Gold Batch: Resumen Diario Pais",
                "description": "Parquet analitico Gold consolidado diario de resumen sismico"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "s3a://gold/batch/zonas_criticas_frecuentes/@minio",
                "name": "MinIO Gold Batch: Zonas Criticas",
                "description": "Historial de fallas criticas acumuladas en el carril batch"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_resumen_streaming@postgres",
                "name": "PostgreSQL: gold_resumen_streaming",
                "description": "Tabla fisica en PostgreSQL Serving Layer receptora de agregaciones en streaming"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_resumen_batch@postgres",
                "name": "PostgreSQL: gold_resumen_batch",
                "description": "Tabla fisica en PostgreSQL receptora de datos historicos consolidados diarios"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_zonas_streaming@postgres",
                "name": "PostgreSQL: gold_zonas_streaming",
                "description": "Tabla fisica con cuadriculas criticas actualizadas en streaming"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}, {"typeName": "CRITICO_M5"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "postgres://sismos_db/gold_zonas_batch@postgres",
                "name": "PostgreSQL: gold_zonas_batch",
                "description": "Tabla fisica con historial consolidado de zonas sismicas batch"
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
                "description": "Vista unificada de fallas geologicas para evaluacion de riesgo geoespacial"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}, {"typeName": "ALERTA_TSUNAMI"}]
        },
        {
            "typeName": "DataSet",
            "attributes": {
                "qualifiedName": "dashboard_web_serving@fastapi",
                "name": "Web Serving Layer: GeoCopilot Dashboard (FastAPI)",
                "description": "Capa de presentacion con FastAPI, sismografo interactivo y Asistente GeoCopilot AI"
            },
            "classifications": [{"typeName": "MEDALLION_GOLD"}]
        }
    ]

    # 2. Definicion de Procesos (Flechas de Linaje)
    def ref(qn):
        return {"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": qn}}

    processes = [
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "kafka_connect_http_source@kafka",
                "name": "Kafka Connect: HTTP Source Ingestion",
                "description": "Consulta periodica cada 60s hacia la API publica de USGS (Responsable: Josue)",
                "inputs": [ref("usgs_api_raw@usgs")],
                "outputs": [ref("kafka://sismos_crudos@kafka")]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "kafka_connect_s3_sink@kafka",
                "name": "Kafka Connect: MinIO S3 Sink Bronze",
                "description": "Persistencia directa 24/7 de eventos crudos en MinIO Bronze Streaming (Josue y Sayder)",
                "inputs": [ref("kafka://sismos_crudos@kafka")],
                "outputs": [ref("s3a://bronze/streaming/@minio")]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "airflow_batch_extractor@airflow",
                "name": "Apache Airflow: Extraccion Batch Diaria (07:00 AM)",
                "description": "DAG programado de extraccion consolidada de 24h hacia Bronze Batch (Responsable: Jesus)",
                "inputs": [ref("usgs_api_raw@usgs")],
                "outputs": [ref("s3a://bronze/batch/@minio")]
            }
        },
        {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": "spark_streaming_pipeline@spark",
                "name": "Apache Spark Streaming Pipeline",
                "description": "Micro-batches de limpieza a Silver Parquet y agregacion analitica a Gold y Postgres (Responsable: Josue)",
                "inputs": [ref("kafka://sismos_crudos@kafka")],
                "outputs": [
                    ref("s3a://silver/streaming/@minio"),
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
                "description": "ETL de normalizacion pesada, limpieza Parquet y modelado analitico diario (Responsable: Kevin)",
                "inputs": [ref("s3a://bronze/batch/@minio")],
                "outputs": [
                    ref("s3a://silver/batch/@minio"),
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
                "name": "PostgreSQL: Consolidacion SQL Lambda",
                "description": "Vistas SQL que unifican el carril Streaming y Batch sin duplicados (Sayder y Jhonatan)",
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
                "description": "Exposicion de KPIs, sismografo y Asistente GeoCopilot hacia la interfaz web (Responsable: Jhonatan)",
                "inputs": [
                    ref("postgres://sismos_db/vista_resumen_diario_pais@postgres"),
                    ref("postgres://sismos_db/vista_zonas_criticas_frecuentes@postgres")
                ],
                "outputs": [ref("dashboard_web_serving@fastapi")]
            }
        }
    ]

    try:
        # 1. Registrar primero los Datasets
        r1 = requests.post(f"{BASE_URL}/entity/bulk", json={"entities": datasets}, auth=AUTH, headers=HEADERS, timeout=20)
        if r1.status_code in [200, 201]:
            logging.info("Datasets registrados exitosamente en Atlas.")
        else:
            logging.error(f"Error cargando datasets: {r1.status_code} - {r1.text[:300]}")
            return False

        # 2. Registrar los Procesos conectando el grafo de linaje
        r2 = requests.post(f"{BASE_URL}/entity/bulk", json={"entities": processes}, auth=AUTH, headers=HEADERS, timeout=20)
        if r2.status_code in [200, 201]:
            logging.info("Todas las entidades y relaciones de linaje fueron cargadas exitosamente en Apache Atlas.")
            logging.info("Puedes verificar el linaje en: http://localhost:21000")
            return True
        else:
            logging.error(f"Error cargando procesos de linaje: {r2.status_code} - {r2.text[:300]}")
            return False
    except Exception as e:
        logging.error(f"Falla conectando con Atlas en el bulk: {e}")
        return False

if __name__ == "__main__":
    logging.info("=== INICIALIZADOR DEL ARBOL DE GOBERNANZA APACHE ATLAS ===")
    if esperar_atlas():
        registrar_clasificaciones()
        registrar_glosario()
        registrar_entidades_y_linaje()
        logging.info("Proceso completado. Linaje y Glosario listos para sustentacion.")
    else:
        sys.exit(1)

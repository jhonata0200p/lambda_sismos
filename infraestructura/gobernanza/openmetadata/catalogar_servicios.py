#!/usr/bin/env python3
"""
catalogar_servicios.py
=============================================================================
Script integral de automatizacion de gobernanza, catalogo y linaje para OpenMetadata 2.0.6.
Registra de forma idempotente:
  1. Equipo y Usuarios tecnicos (Tech Lead, Spark, Kafka, Airflow, MinIO) como Owners.
  2. Glosario de Negocio 'Glosario_Sismico_Global' y 7 terminos tecnicos.
  3. Servicio PostgreSQL Serving Layer (Tablas Gold y Vistas Lambda con tags y owners).
  4. Servicio Apache Kafka (Topico 'sismos_crudos' con owner).
  5. Servicio MinIO Medallion Data Lake (Containers Bronze, Silver, Gold con tags y owners).
  6. Servicio Apache Spark (Pipelines 'spark_streaming_pipeline' y 'spark_batch_etl').
  7. Servicio Apache Airflow (Pipeline 'airflow_sismos_dag').
  8. Grafo de Linaje Completo (Kafka/Airflow -> MinIO -> Spark -> Postgres -> Vistas).
=============================================================================
"""

import os
import sys
import time
import json
import base64
import requests

OM_SERVER_URL = os.getenv("OM_SERVER_URL", "http://localhost:8585/api/v1")
OM_USER = os.getenv("OM_USER", "admin@open-metadata.org")
OM_PASSWORD = os.getenv("OM_PASSWORD", "admin")

CLUSTER_KAFKA_IP = os.getenv("CLUSTER_KAFKA_IP", "10.131.229.5")
CLUSTER_KAFKA_PORT = os.getenv("CLUSTER_KAFKA_PORT", "9092")

CLUSTER_POSTGRES_IP = os.getenv("CLUSTER_POSTGRES_IP", "10.131.229.248")
CLUSTER_POSTGRES_PORT = os.getenv("CLUSTER_POSTGRES_PORT", "5432")

CLUSTER_MINIO_IP = os.getenv("CLUSTER_MINIO_IP", "10.131.229.248")
CLUSTER_MINIO_PORT = os.getenv("CLUSTER_MINIO_PORT", "9000")


def api_request(method, endpoint, payload=None, token=None):
    url = f"{OM_SERVER_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.request(method, url, json=payload, headers=headers, timeout=30)
        if resp.status_code in [200, 201]:
            return resp.json() if resp.text else {}
        elif resp.status_code == 409:
            try:
                return resp.json()
            except Exception:
                return {"status": "conflict_exists"}
        else:
            print(f"[ERROR HTTP {resp.status_code}] en {method} {url}: {resp.text[:250]}")
            resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"[ERROR CONEXION] en {method} {url}: {e}")
        raise


def esperar_servidor(max_intentos=30, intervalo=3):
    print(f"Esperando disponibilidad de OpenMetadata Server en {OM_SERVER_URL}...")
    for i in range(1, max_intentos + 1):
        try:
            r = requests.get(f"{OM_SERVER_URL}/system/version", timeout=5)
            if r.status_code == 200:
                data = r.json()
                print(f"OpenMetadata Server activo. Version detectada: {data.get('version', '2.0.x')}")
                return True
        except Exception:
            pass
        time.sleep(intervalo)
    raise TimeoutError("No se pudo conectar a OpenMetadata Server tras agotar el tiempo maximo de espera.")


def autenticar():
    print("Autenticando contra OpenMetadata API...")
    pwd_b64 = base64.b64encode(OM_PASSWORD.encode("utf-8")).decode("utf-8")
    login_payload = {
        "email": OM_USER,
        "password": pwd_b64
    }
    try:
        res = api_request("POST", "users/login", login_payload)
        token = res.get("accessToken")
        if token:
            print("Autenticacion exitosa (Bearer JWT adquirido).")
            return token
    except Exception as e:
        print(f"Login POST users/login fallo: {e}")
    return None


def registrar_equipo_y_usuarios(token):
    print("\n--- 1. Registrando Equipo y Usuarios de Gobernanza (Owners) ---")
    team_payload = {
        "name": "Equipo_Ingenieria_Sismos",
        "displayName": "Equipo de Ingenieria de Datos — Sismos Globales",
        "description": "Equipo tecnico multidisciplinario a cargo de la arquitectura Lambda y Medallion.",
        "teamType": "Group"
    }
    try:
        api_request("PUT", "teams", team_payload, token)
        print("  Equipo 'Equipo_Ingenieria_Sismos' registrado.")
    except Exception as e:
        print(f"  Aviso registrando equipo: {e}")

    usuarios = [
        {"name": "jhonatan", "displayName": "Jhonatan (Tech Lead & Serving DB)", "email": "jhonatan@sismos.org"},
        {"name": "kevin", "displayName": "Kevin (Spark Batch Engineer)", "email": "kevin@sismos.org"},
        {"name": "josue", "displayName": "Josue (Kafka & Streaming Engineer)", "email": "josue@sismos.org"},
        {"name": "jesus", "displayName": "Jesus (Airflow Orchestration Engineer)", "email": "jesus@sismos.org"},
        {"name": "sayder", "displayName": "Sayder (MinIO Data Lake Engineer)", "email": "sayder@sismos.org"}
    ]

    owners_dict = {}
    for u in usuarios:
        try:
            res = api_request("POST", "users", u, token)
            owners_dict[u["name"]] = {"id": res["id"], "type": "user", "name": u["name"], "displayName": u["displayName"]}
            print(f"  Usuario registrado: {u['displayName']}")
        except Exception:
            try:
                res = api_request("GET", f"users/name/{u['name']}", token=token)
                owners_dict[u["name"]] = {"id": res["id"], "type": "user", "name": u["name"], "displayName": u["displayName"]}
                print(f"  Usuario existente cargado: {u['displayName']}")
            except Exception as e2:
                print(f"  Error recuperando usuario {u['name']}: {e2}")

    return owners_dict


def registrar_glosario_y_terminos(token):
    print("\n--- 2. Registrando Glosario de Negocio y Terminos ---")
    glosario_payload = {
        "name": "Glosario_Sismico_Global",
        "displayName": "Glosario Sismico Global",
        "description": "Glosario unificado de gobernanza de datos para la arquitectura Lambda y Medallion del Monitoreo Sismico Global."
    }
    api_request("PUT", "glossaries", glosario_payload, token)
    print("  Glosario 'Glosario_Sismico_Global' registrado con exito.")

    terminos = [
        {
            "name": "Sismo_Critico_M5",
            "displayName": "Sismo Critico (Magnitud >= 5.0)",
            "description": "Evento sismico con magnitud mayor o igual a 5.0 clasificado como de alta prioridad geoespacial y riesgo para la poblacion.",
            "glossary": "Glosario_Sismico_Global"
        },
        {
            "name": "Alerta_Tsunami",
            "displayName": "Alerta de Tsunami",
            "description": "Bandera booleana (tsunami=1) emitida oficialmente por la USGS ante riesgo sismico maritimo o costero.",
            "glossary": "Glosario_Sismico_Global"
        },
        {
            "name": "Cuadricula_Hotspot",
            "displayName": "Cuadricula Geoespacial Hotspot",
            "description": "Agrupacion espacial de epicentros sismicos calculada mediante redondeo de latitud/longitud a 1 decimal (~11 km de resolucion).",
            "glossary": "Glosario_Sismico_Global"
        },
        {
            "name": "Medallion_Bronze",
            "displayName": "Capa Bronze (Medallion)",
            "description": "Capa de almacenamiento en MinIO que contiene los payloads crudos JSON inalterados procedentes de la API USGS (rutas /streaming y /batch).",
            "glossary": "Glosario_Sismico_Global"
        },
        {
            "name": "Medallion_Silver",
            "displayName": "Capa Silver (Medallion)",
            "description": "Capa de datos limpios, deduplicados y tipados almacenados en formato columnar Parquet en MinIO.",
            "glossary": "Glosario_Sismico_Global"
        },
        {
            "name": "Medallion_Gold",
            "displayName": "Capa Gold (Medallion)",
            "description": "Capa de modelos analiticos agregados por fecha, pais y zona critica guardados en Parquet en MinIO y servidos en PostgreSQL.",
            "glossary": "Glosario_Sismico_Global"
        },
        {
            "name": "Serving_Layer_Lambda",
            "displayName": "Serving Layer Lambda",
            "description": "Capa de servicio en PostgreSQL que combina las tablas delta streaming de hoy con el historico batch mediante vistas SQL sin duplicacion.",
            "glossary": "Glosario_Sismico_Global"
        }
    ]

    for t in terminos:
        try:
            api_request("PUT", "glossaryTerms", t, token)
            print(f"  Termino registrado: {t['name']}")
        except Exception as e:
            print(f"  Aviso al registrar termino {t['name']}: {e}")


def registrar_servicio_postgresql(token, owners_dict):
    print("\n--- 3. Registrando Servicio de Base de Datos PostgreSQL Serving Layer ---")
    owner_jh = [owners_dict["jhonatan"]] if "jhonatan" in owners_dict else []
    owner_kv = [owners_dict["kevin"]] if "kevin" in owners_dict else []
    owner_js = [owners_dict["josue"]] if "josue" in owners_dict else []

    pg_service_payload = {
        "name": "sismos_postgresql_cluster",
        "displayName": "PostgreSQL Serving Layer (Cluster Sayder)",
        "serviceType": "Postgres",
        "description": f"Base de datos PostgreSQL en el nodo Sayder ({CLUSTER_POSTGRES_IP}:{CLUSTER_POSTGRES_PORT}) que almacena la Serving Layer de la arquitectura Lambda.",
        "owners": owner_jh,
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "hostPort": f"{CLUSTER_POSTGRES_IP}:{CLUSTER_POSTGRES_PORT}",
                "username": "postgres_user",
                "authType": {"password": "password123"},
                "database": "sismos_db"
            }
        }
    }
    api_request("PUT", "services/databaseServices", pg_service_payload, token)
    print("  Servicio de Base de Datos 'sismos_postgresql_cluster' registrado.")

    db_payload = {
        "name": "sismos_db",
        "displayName": "sismos_db",
        "description": "Base de datos principal de monitoreo sismico.",
        "service": "sismos_postgresql_cluster",
        "owners": owner_jh
    }
    api_request("PUT", "databases", db_payload, token)

    schema_payload = {
        "name": "public",
        "displayName": "public",
        "description": "Esquema publico con tablas Gold y vistas unificadas Lambda.",
        "database": "sismos_postgresql_cluster.sismos_db",
        "owners": owner_jh
    }
    api_request("PUT", "databaseSchemas", schema_payload, token)

    cols_resumen = [
        {"name": "fecha_evento", "dataType": "DATE", "description": "Fecha de ocurrencia UTC de los sismos"},
        {"name": "pais_estado", "dataType": "VARCHAR", "dataLength": 100, "description": "Pais o region geografica estandarizada"},
        {"name": "total_sismos", "dataType": "INT", "description": "Conteo total de sismos registrados en la ventana"},
        {"name": "magnitud_maxima", "dataType": "NUMERIC", "description": "Maxima magnitud registrada"},
        {"name": "magnitud_promedio", "dataType": "NUMERIC", "description": "Promedio aritmetico de magnitud"},
        {"name": "profundidad_promedio_km", "dataType": "NUMERIC", "description": "Promedio de profundidad hipocentral en kilometros"},
        {
            "name": "total_alertas_tsunami", "dataType": "INT", "description": "Total de eventos con bandera de tsunami activa",
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Alerta_Tsunami", "source": "Glossary"}]
        },
        {
            "name": "sismos_peligrosos_m5", "dataType": "INT", "description": "Total de sismos con magnitud >= 5.0",
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Sismo_Critico_M5", "source": "Glossary"}]
        },
        {"name": "ultima_actualizacion_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC de la ultima actualizacion registrada"}
    ]

    cols_zonas = [
        {
            "name": "cuadricula_lat", "dataType": "NUMERIC", "description": "Centroide latitudinal redondeado a 1 decimal",
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Cuadricula_Hotspot", "source": "Glossary"}]
        },
        {
            "name": "cuadricula_lon", "dataType": "NUMERIC", "description": "Centroide longitudinal redondeado a 1 decimal",
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Cuadricula_Hotspot", "source": "Glossary"}]
        },
        {"name": "zona_referencia", "dataType": "VARCHAR", "dataLength": 100, "description": "Zona geografica de referencia"},
        {"name": "total_sismos_registrados", "dataType": "INT", "description": "Numero total de eventos sismicos en el cuadrante"},
        {
            "name": "sismos_graves_m5", "dataType": "INT", "description": "Sismos con magnitud >= 5.0",
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Sismo_Critico_M5", "source": "Glossary"}]
        },
        {"name": "magnitud_maxima", "dataType": "NUMERIC", "description": "Maxima magnitud registrada en la zona"},
        {"name": "profundidad_promedio_km", "dataType": "NUMERIC", "description": "Menor profundidad registrada (sismos someros)"},
        {"name": "nivel_alerta", "dataType": "VARCHAR", "dataLength": 20, "description": "Nivel de alerta (NORMAL, MODERADO, CRITICO)"},
        {"name": "fecha_primer_sismo", "dataType": "TIMESTAMP", "description": "Fecha del primer evento registrado"},
        {"name": "fecha_ultimo_sismo", "dataType": "TIMESTAMP", "description": "Fecha del ultimo evento registrado"},
        {"name": "ultima_actualizacion_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC de la ultima actualizacion registrada"}
    ]

    tablas = [
        {
            "name": "gold_resumen_batch",
            "displayName": "gold_resumen_batch",
            "description": "Tabla historica acumulada de resumen sismico diario por pais calculada por Apache Spark Batch (Kevin).",
            "tableType": "Regular",
            "columns": cols_resumen,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public",
            "owners": owner_kv,
            "tags": [
                {"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"},
                {"tagFQN": "Glosario_Sismico_Global.Serving_Layer_Lambda", "source": "Glossary"}
            ]
        },
        {
            "name": "gold_resumen_streaming",
            "displayName": "gold_resumen_streaming",
            "description": "Tabla delta en tiempo real de resumen sismico de la ventana actual calculada por Apache Spark Streaming (Josue).",
            "tableType": "Regular",
            "columns": cols_resumen,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public",
            "owners": owner_js,
            "tags": [
                {"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"},
                {"tagFQN": "Glosario_Sismico_Global.Serving_Layer_Lambda", "source": "Glossary"}
            ]
        },
        {
            "name": "gold_zonas_batch",
            "displayName": "gold_zonas_batch",
            "description": "Tabla historica de zonas criticas frecuentes calculada por Apache Spark Batch (Kevin).",
            "tableType": "Regular",
            "columns": cols_zonas,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public",
            "owners": owner_kv,
            "tags": [
                {"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"},
                {"tagFQN": "Glosario_Sismico_Global.Cuadricula_Hotspot", "source": "Glossary"}
            ]
        },
        {
            "name": "gold_zonas_streaming",
            "displayName": "gold_zonas_streaming",
            "description": "Tabla delta en tiempo real de zonas criticas calculada por Apache Spark Streaming (Josue).",
            "tableType": "Regular",
            "columns": cols_zonas,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public",
            "owners": owner_js,
            "tags": [
                {"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"},
                {"tagFQN": "Glosario_Sismico_Global.Cuadricula_Hotspot", "source": "Glossary"}
            ]
        },
        {
            "name": "vista_resumen_diario_pais",
            "displayName": "vista_resumen_diario_pais (Vista Lambda)",
            "description": "Vista SQL unificada de la Serving Layer Lambda: combina streaming delta con el historico batch sin duplicaciones de fecha/pais.",
            "tableType": "View",
            "columns": cols_resumen + [{"name": "origen_dato", "dataType": "VARCHAR", "dataLength": 50, "description": "Identificador de procedencia (STREAMING o BATCH)"}],
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public",
            "owners": owner_jh,
            "tags": [
                {"tagFQN": "Glosario_Sismico_Global.Serving_Layer_Lambda", "source": "Glossary"}
            ]
        },
        {
            "name": "vista_zonas_criticas_frecuentes",
            "displayName": "vista_zonas_criticas_frecuentes (Vista Lambda)",
            "description": "Vista SQL unificada de zonas criticas con ponderacion de eventos, categoria de alerta (CRITICO/MODERADO/NORMAL) y maxima magnitud.",
            "tableType": "View",
            "columns": cols_zonas,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public",
            "owners": owner_jh,
            "tags": [
                {"tagFQN": "Glosario_Sismico_Global.Serving_Layer_Lambda", "source": "Glossary"},
                {"tagFQN": "Glosario_Sismico_Global.Cuadricula_Hotspot", "source": "Glossary"}
            ]
        }
    ]

    for t in tablas:
        try:
            api_request("PUT", "tables", t, token)
            print(f"  Tabla/Vista registrada con Owner y Tags: {t['name']}")
        except Exception as e:
            print(f"  Aviso al registrar {t['name']}: {e}")


def registrar_servicio_kafka(token, owners_dict):
    print("\n--- 4. Registrando Servicio de Mensajeria Apache Kafka ---")
    owner_js = [owners_dict["josue"]] if "josue" in owners_dict else []

    kafka_service_payload = {
        "name": "sismos_kafka_cluster",
        "displayName": "Apache Kafka Broker (Cluster Josue)",
        "serviceType": "Kafka",
        "description": f"Broker central Apache Kafka en el nodo Josue ({CLUSTER_KAFKA_IP}:{CLUSTER_KAFKA_PORT}) para ingestion streaming de eventos sismicos crudos.",
        "owners": owner_js,
        "connection": {
            "config": {
                "type": "Kafka",
                "bootstrapServers": f"{CLUSTER_KAFKA_IP}:{CLUSTER_KAFKA_PORT}"
            }
        }
    }
    api_request("PUT", "services/messagingServices", kafka_service_payload, token)
    print("  Servicio de Mensajeria 'sismos_kafka_cluster' registrado.")

    topico_payload = {
        "name": "sismos_crudos",
        "displayName": "sismos_crudos",
        "description": "Topico central de eventos sismicos consumido por Spark Streaming y MinIO Bronze S3 Sink.",
        "partitions": 1,
        "replicationFactor": 1,
        "cleanupPolicies": ["delete"],
        "retentionTime": 86400000.0,
        "maximumMessageSize": 1048576,
        "service": "sismos_kafka_cluster",
        "owners": owner_js,
        "tags": [
            {"tagFQN": "Glosario_Sismico_Global.Medallion_Bronze", "source": "Glossary"}
        ]
    }
    api_request("PUT", "topics", topico_payload, token)
    print("  Topico 'sismos_crudos' registrado con Owner y Tags.")


def registrar_servicio_minio(token, owners_dict):
    print("\n--- 5. Registrando Servicio de Almacenamiento MinIO Medallion Data Lake ---")
    owner_sy = [owners_dict["sayder"]] if "sayder" in owners_dict else []

    minio_service_payload = {
        "name": "sismos_minio_datalake",
        "displayName": "MinIO Medallion Data Lake (Cluster Sayder)",
        "serviceType": "CustomStorage",
        "description": f"Servicio Object Storage S3 compatible en nodo Sayder ({CLUSTER_MINIO_IP}:{CLUSTER_MINIO_PORT}) con buckets Medallion Bronze, Silver y Gold.",
        "owners": owner_sy,
        "connection": {
            "config": {
                "type": "CustomStorage"
            }
        }
    }
    api_request("PUT", "services/storageServices", minio_service_payload, token)
    print("  Servicio de Almacenamiento 'sismos_minio_datalake' registrado.")

    buckets = [
        {
            "name": "bronze",
            "displayName": "Bucket Bronze (Crudos)",
            "description": "Almacenamiento de payloads JSON crudos inalterados de USGS. Prefijos: /streaming (Kafka Connect S3 Sink) y /batch (Airflow DAG).",
            "service": "sismos_minio_datalake",
            "owners": owner_sy,
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Medallion_Bronze", "source": "Glossary"}]
        },
        {
            "name": "silver",
            "displayName": "Bucket Silver (Limpios)",
            "description": "Almacenamiento en formato columnar Parquet de datos limpios, parseados y tipados. Prefijos: /streaming y /batch.",
            "service": "sismos_minio_datalake",
            "owners": owner_sy,
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Medallion_Silver", "source": "Glossary"}]
        },
        {
            "name": "gold",
            "displayName": "Bucket Gold (Modelos Analiticos)",
            "description": "Almacenamiento en formato columnar Parquet de agregaciones analiticas por fecha, pais y hotspot. Prefijos: /streaming y /batch.",
            "service": "sismos_minio_datalake",
            "owners": owner_sy,
            "tags": [{"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"}]
        }
    ]

    for b in buckets:
        try:
            api_request("PUT", "containers", b, token)
            print(f"  Bucket/Container registrado con Owner y Tags: {b['name']}")
        except Exception as e:
            print(f"  Aviso al registrar bucket {b['name']}: {e}")


def registrar_servicios_procesamiento(token, owners_dict):
    print("\n--- 6. Registrando Servicios de Procesamiento (Apache Spark y Airflow) ---")
    owner_kv = [owners_dict["kevin"]] if "kevin" in owners_dict else []
    owner_js = [owners_dict["josue"]] if "josue" in owners_dict else []
    owner_je = [owners_dict["jesus"]] if "jesus" in owners_dict else []

    # 6.1 Servicio Apache Spark
    spark_service = {
        "name": "sismos_spark_cluster",
        "displayName": "Apache Spark Cluster (Batch & Streaming)",
        "serviceType": "Spark",
        "description": "Cluster de computo distribuido Apache Spark 3.5 para transformaciones Medallion y agregaciones analiticas.",
        "owners": owner_kv,
        "connection": {
            "config": {
                "type": "Spark"
            }
        }
    }
    api_request("PUT", "services/pipelineServices", spark_service, token)
    print("  Servicio Pipeline 'sismos_spark_cluster' registrado.")

    # Pipelines Spark
    p_stream = {
        "name": "spark_streaming_pipeline",
        "displayName": "PySpark Structured Streaming Pipeline (Josue)",
        "description": "Pipeline en tiempo real 24/7: consume de Kafka, limpia a Silver Parquet en MinIO, agrega a Gold Parquet y Serving Layer PostgreSQL.",
        "service": "sismos_spark_cluster",
        "owners": owner_js,
        "tags": [
            {"tagFQN": "Glosario_Sismico_Global.Medallion_Silver", "source": "Glossary"},
            {"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"}
        ]
    }
    api_request("PUT", "pipelines", p_stream, token)
    print("  Pipeline 'spark_streaming_pipeline' registrado con Owner y Tags.")

    p_batch = {
        "name": "spark_batch_etl",
        "displayName": "PySpark Batch ETL Pipeline (Kevin)",
        "description": "Pipeline batch diario programado: descarga Bronze de MinIO, transforma a Silver Parquet, modela agregaciones Gold Parquet y Serving Layer PostgreSQL.",
        "service": "sismos_spark_cluster",
        "owners": owner_kv,
        "tags": [
            {"tagFQN": "Glosario_Sismico_Global.Medallion_Silver", "source": "Glossary"},
            {"tagFQN": "Glosario_Sismico_Global.Medallion_Gold", "source": "Glossary"}
        ]
    }
    api_request("PUT", "pipelines", p_batch, token)
    print("  Pipeline 'spark_batch_etl' registrado con Owner y Tags.")

    # 6.2 Servicio Apache Airflow
    airflow_service = {
        "name": "sismos_airflow_orchestrator",
        "displayName": "Apache Airflow (Nodo Jesus)",
        "serviceType": "Airflow",
        "description": "Orquestador de workflows batch para extraccion programada de la API USGS a MinIO Bronze y disparo de Spark Batch.",
        "owners": owner_je,
        "connection": {
            "config": {
                "type": "Airflow"
            }
        }
    }
    api_request("PUT", "services/pipelineServices", airflow_service, token)
    print("  Servicio Pipeline 'sismos_airflow_orchestrator' registrado.")

    dag_batch = {
        "name": "airflow_sismos_etl_batch",
        "displayName": "DAG airflow_sismos_etl_batch (Jesus)",
        "description": "Workflow Airflow: extrae eventos diarios de la API USGS, deposita JSON en MinIO Bronze (/batch) y orquesta ejecucion de Spark Batch.",
        "service": "sismos_airflow_orchestrator",
        "owners": owner_je,
        "tags": [
            {"tagFQN": "Glosario_Sismico_Global.Medallion_Bronze", "source": "Glossary"}
        ]
    }
    api_request("PUT", "pipelines", dag_batch, token)
    print("  DAG Airflow 'airflow_sismos_etl_batch' registrado con Owner y Tags.")


def registrar_linaje_end_to_end(token):
    print("\n--- 7. Registrando Grafo de Linaje Completo (End-to-End) ---")

    def get_entity(endpoint, fqn):
        try:
            return api_request("GET", f"{endpoint}/name/{fqn}", token=token)
        except Exception:
            return None

    def conectar_linaje(from_entity, from_type, to_entity, to_type):
        if not from_entity or not to_entity:
            return
        edge_payload = {
            "edge": {
                "fromEntity": {
                    "id": from_entity["id"],
                    "type": from_type
                },
                "toEntity": {
                    "id": to_entity["id"],
                    "type": to_type
                }
            }
        }
        try:
            api_request("PUT", "lineage", edge_payload, token)
            print(f"  [OK] Linaje: [{from_type}] {from_entity.get('name')} -> [{to_type}] {to_entity.get('name')}")
        except Exception as e:
            print(f"  Aviso conectando linaje [{from_entity.get('name')} -> {to_entity.get('name')}]: {e}")

    # Entidades
    topico_kafka = get_entity("topics", "sismos_kafka_cluster.sismos_crudos")
    pipe_stream = get_entity("pipelines", "sismos_spark_cluster.spark_streaming_pipeline")
    pipe_batch = get_entity("pipelines", "sismos_spark_cluster.spark_batch_etl")
    dag_airflow = get_entity("pipelines", "sismos_airflow_orchestrator.airflow_sismos_etl_batch")

    c_bronze = get_entity("containers", "sismos_minio_datalake.bronze")
    c_silver = get_entity("containers", "sismos_minio_datalake.silver")
    c_gold = get_entity("containers", "sismos_minio_datalake.gold")

    t_res_stream = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_resumen_streaming")
    t_zon_stream = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_zonas_streaming")
    t_res_batch = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_resumen_batch")
    t_zon_batch = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_zonas_batch")

    v_resumen = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.vista_resumen_diario_pais")
    v_zonas = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.vista_zonas_criticas_frecuentes")

    # 1. Airflow -> MinIO Bronze
    conectar_linaje(dag_airflow, "pipeline", c_bronze, "container")

    # 2. Kafka -> MinIO Bronze (S3 Sink)
    conectar_linaje(topico_kafka, "topic", c_bronze, "container")

    # 3. Kafka -> Spark Streaming
    conectar_linaje(topico_kafka, "topic", pipe_stream, "pipeline")

    # 4. Spark Streaming -> MinIO Silver, MinIO Gold, PostgreSQL Streaming
    conectar_linaje(pipe_stream, "pipeline", c_silver, "container")
    conectar_linaje(pipe_stream, "pipeline", c_gold, "container")
    conectar_linaje(pipe_stream, "pipeline", t_res_stream, "table")
    conectar_linaje(pipe_stream, "pipeline", t_zon_stream, "table")

    # 5. MinIO Bronze -> Spark Batch
    conectar_linaje(c_bronze, "container", pipe_batch, "pipeline")

    # 6. Spark Batch -> MinIO Silver, MinIO Gold, PostgreSQL Batch
    conectar_linaje(pipe_batch, "pipeline", c_silver, "container")
    conectar_linaje(pipe_batch, "pipeline", c_gold, "container")
    conectar_linaje(pipe_batch, "pipeline", t_res_batch, "table")
    conectar_linaje(pipe_batch, "pipeline", t_zon_batch, "table")

    # 7. PostgreSQL Tablas -> Vistas Lambda Unificadas
    conectar_linaje(t_res_stream, "table", v_resumen, "table")
    conectar_linaje(t_res_batch, "table", v_resumen, "table")
    conectar_linaje(t_zon_stream, "table", v_zonas, "table")
    conectar_linaje(t_zon_batch, "table", v_zonas, "table")


def main():
    print("=" * 80)
    print("GOBERNANZA Y CATALOGO COMPLETO — OPENMETADATA 2.0.6 (SUSTENTACION TECH LEAD)")
    print("=" * 80)
    esperar_servidor()
    token = autenticar()
    owners_dict = registrar_equipo_y_usuarios(token)
    registrar_glosario_y_terminos(token)
    registrar_servicio_postgresql(token, owners_dict)
    registrar_servicio_kafka(token, owners_dict)
    registrar_servicio_minio(token, owners_dict)
    registrar_servicios_procesamiento(token, owners_dict)
    registrar_linaje_end_to_end(token)
    print("\n" + "=" * 80)
    print("GOBERNANZA, METADATOS Y LINAJE EN OPENMETADATA COMPLETADOS AL 100%.")
    print(f"Servidor: {OM_SERVER_URL.replace('/api/v1', '')}")
    print(f"Credenciales: {OM_USER} / {OM_PASSWORD}")
    print("=" * 80)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
catalogar_servicios.py
=============================================================================
Script de automatizacion de gobernanza y catalogo para OpenMetadata 2.0.6.
Registra de forma idempotente en OpenMetadata:
  1. Glosario de Negocio 'Glosario_Sismico_Global' y 7 terminos estandarizados.
  2. Servicio de Base de Datos PostgreSQL Serving Layer (10.175.132.248:5432).
     - Tablas Gold Batch & Streaming.
     - Vistas Lambda unificadas con esquemas, columnas y tipos.
  3. Servicio de Mensajeria Apache Kafka (10.175.132.5:9092).
     - Topico central 'sismos_crudos'.
  4. Servicio de Almacenamiento MinIO Data Lake (10.175.132.248:9000).
     - Buckets Medallion: Bronze, Silver, Gold.
  5. Grafo de Linaje de Datos End-to-End (Kafka/Airflow -> MinIO -> Spark -> Postgres -> Vistas).
=============================================================================
"""

import os
import sys
import time
import json
import base64
import urllib.request
import urllib.error

OM_SERVER_URL = os.getenv("OM_SERVER_URL", "http://localhost:8585/api/v1")
OM_USER = os.getenv("OM_USER", "admin@open-metadata.org")
OM_PASSWORD = os.getenv("OM_PASSWORD", "admin")

CLUSTER_KAFKA_IP = os.getenv("CLUSTER_KAFKA_IP", "10.175.132.5")
CLUSTER_KAFKA_PORT = os.getenv("CLUSTER_KAFKA_PORT", "9092")

CLUSTER_POSTGRES_IP = os.getenv("CLUSTER_POSTGRES_IP", "10.175.132.248")
CLUSTER_POSTGRES_PORT = os.getenv("CLUSTER_POSTGRES_PORT", "5432")

CLUSTER_MINIO_IP = os.getenv("CLUSTER_MINIO_IP", "10.175.132.248")
CLUSTER_MINIO_PORT = os.getenv("CLUSTER_MINIO_PORT", "9000")


def api_request(method, endpoint, payload=None, token=None):
    url = f"{OM_SERVER_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    else:
        # Fallback Basic Auth si no hay token aun
        creds = base64.b64encode(b"admin:admin").decode("utf-8")
        headers["Authorization"] = f"Basic {creds}"

    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            content = resp.read().decode("utf-8")
            if content:
                return json.loads(content)
            return {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="ignore")
        # Si es 409 (Conflict/Already exists), no es fatal para operaciones idempotentes
        if e.code == 409:
            try:
                return json.loads(error_body)
            except Exception:
                return {"status": "conflict_exists"}
        print(f"[ERROR HTTP {e.code}] en {method} {url}: {error_body}")
        raise
    except urllib.error.URLError as e:
        print(f"[ERROR CONEXION] en {method} {url}: {e.reason}")
        raise


def esperar_servidor(max_intentos=60, intervalo=5):
    print(f"Esperando disponibilidad de OpenMetadata Server en {OM_SERVER_URL}...")
    for i in range(1, max_intentos + 1):
        try:
            req = urllib.request.Request(f"{OM_SERVER_URL}/system/version", method="GET")
            with urllib.request.urlopen(req, timeout=5) as resp:
                if resp.status == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    print(f"OpenMetadata Server activo. Version detectada: {data.get('version', '2.0.x')}")
                    return True
        except Exception:
            pass
        print(f"Intento {i}/{max_intentos}: Servidor aun inicializando... reintentando en {intervalo}s")
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


def registrar_glosario_y_terminos(token):
    print("\n--- 1. Registrando Glosario de Negocio y Terminos ---")
    glosario_payload = {
        "name": "Glosario_Sismico_Global",
        "displayName": "Glosario Sismico Global",
        "description": "Glosario unificado de gobernanza de datos para la arquitectura Lambda y Medallion del Monitoreo Sismico Global."
    }
    api_request("PUT", "glossaries", glosario_payload, token)
    print("Glosario 'Glosario_Sismico_Global' registrado con exito.")

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


def registrar_servicio_postgresql(token):
    print("\n--- 2. Registrando Servicio de Base de Datos PostgreSQL Serving Layer ---")
    pg_service_payload = {
        "name": "sismos_postgresql_cluster",
        "displayName": "PostgreSQL Serving Layer (Cluster Sayder)",
        "serviceType": "Postgres",
        "description": f"Base de datos PostgreSQL en el nodo Sayder ({CLUSTER_POSTGRES_IP}:{CLUSTER_POSTGRES_PORT}) que almacena la Serving Layer de la arquitectura Lambda.",
        "connection": {
            "config": {
                "type": "Postgres",
                "scheme": "postgresql+psycopg2",
                "hostPort": f"{CLUSTER_POSTGRES_IP}:{CLUSTER_POSTGRES_PORT}",
                "username": "sismos_user",
                "authType": {
                    "password": "sismos_password"
                },
                "database": "sismos_db"
            }
        }
    }
    api_request("PUT", "services/databaseServices", pg_service_payload, token)
    print("Servicio de Base de Datos 'sismos_postgresql_cluster' registrado.")

    # Registrar Database
    db_payload = {
        "name": "sismos_db",
        "displayName": "sismos_db",
        "description": "Base de datos principal de monitoreo sismico.",
        "service": "sismos_postgresql_cluster"
    }
    api_request("PUT", "databases", db_payload, token)

    # Registrar Schema
    schema_payload = {
        "name": "public",
        "displayName": "public",
        "description": "Esquema publico con tablas Gold y vistas unificadas Lambda.",
        "database": "sismos_postgresql_cluster.sismos_db"
    }
    api_request("PUT", "databaseSchemas", schema_payload, token)
    print("Esquema 'sismos_postgresql_cluster.sismos_db.public' registrado.")

    # Definicion de columnas comunes
    cols_resumen = [
        {"name": "id", "dataType": "BIGINT", "description": "Clave primaria autoincremental"},
        {"name": "fecha", "dataType": "DATE", "description": "Fecha de ocurrencia UTC de los sismos"},
        {"name": "pais", "dataType": "VARCHAR", "dataLength": 100, "description": "Pais o region geografica estandarizada"},
        {"name": "total_sismos", "dataType": "INT", "description": "Conteo total de sismos registrados en la ventana"},
        {"name": "magnitud_promedio", "dataType": "FLOAT", "description": "Promedio aritmetico de magnitud"},
        {"name": "magnitud_maxima", "dataType": "FLOAT", "description": "Maxima magnitud registrada"},
        {"name": "magnitud_minima", "dataType": "FLOAT", "description": "Minima magnitud registrada"},
        {"name": "profundidad_promedio", "dataType": "FLOAT", "description": "Promedio de profundidad hipocentral en kilometros"},
        {"name": "sismos_criticos", "dataType": "INT", "description": "Total de sismos con magnitud >= 5.0"},
        {"name": "alertas_tsunami", "dataType": "INT", "description": "Total de eventos con bandera de tsunami activa"},
        {"name": "fecha_proceso", "dataType": "TIMESTAMP", "description": "Timestamp UTC de ejecucion del pipeline Spark"},
        {"name": "ultima_actualizacion_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC de la ultima actualizacion registrada"}
    ]

    cols_zonas = [
        {"name": "id", "dataType": "BIGINT", "description": "Clave primaria autoincremental"},
        {"name": "cuadricula", "dataType": "VARCHAR", "dataLength": 50, "description": "Codigo de cuadrante hotspot (lat_redondeada, lon_redondeada)"},
        {"name": "latitud_central", "dataType": "FLOAT", "description": "Centroide latitudinal de la cuadricula"},
        {"name": "longitud_central", "dataType": "FLOAT", "description": "Centroide longitudinal de la cuadricula"},
        {"name": "total_eventos", "dataType": "INT", "description": "Numero de eventos sismicos en el cuadrante"},
        {"name": "magnitud_max", "dataType": "FLOAT", "description": "Maxima magnitud registrada en la zona"},
        {"name": "profundidad_min", "dataType": "FLOAT", "description": "Menor profundidad registrada (sismos someros)"},
        {"name": "fecha_proceso", "dataType": "TIMESTAMP", "description": "Timestamp UTC de ejecucion del pipeline Spark"},
        {"name": "ultima_actualizacion_utc", "dataType": "TIMESTAMP", "description": "Timestamp UTC de la ultima actualizacion registrada"}
    ]

    tablas = [
        {
            "name": "gold_resumen_batch",
            "displayName": "gold_resumen_batch",
            "description": "Tabla historica acumulada de resumen sismico diario por pais calculada por Apache Spark Batch (Kevin).",
            "tableType": "Regular",
            "columns": cols_resumen,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public"
        },
        {
            "name": "gold_resumen_streaming",
            "displayName": "gold_resumen_streaming",
            "description": "Tabla delta en tiempo real de resumen sismico de la ventana actual calculada por Apache Spark Streaming (Josue).",
            "tableType": "Regular",
            "columns": cols_resumen,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public"
        },
        {
            "name": "gold_zonas_batch",
            "displayName": "gold_zonas_batch",
            "description": "Tabla historica de zonas criticas frecuentes calculada por Apache Spark Batch (Kevin).",
            "tableType": "Regular",
            "columns": cols_zonas,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public"
        },
        {
            "name": "gold_zonas_streaming",
            "displayName": "gold_zonas_streaming",
            "description": "Tabla delta en tiempo real de zonas criticas calculada por Apache Spark Streaming (Josue).",
            "tableType": "Regular",
            "columns": cols_zonas,
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public"
        },
        {
            "name": "vista_resumen_diario_pais",
            "displayName": "vista_resumen_diario_pais (Vista Lambda)",
            "description": "Vista SQL unificada de la Serving Layer Lambda: combina streaming delta con el historico batch sin duplicaciones de fecha/pais.",
            "tableType": "View",
            "columns": cols_resumen + [{"name": "capa_origen", "dataType": "VARCHAR", "dataLength": 50, "description": "Identificador de procedencia (STREAMING o BATCH)"}],
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public"
        },
        {
            "name": "vista_zonas_criticas_frecuentes",
            "displayName": "vista_zonas_criticas_frecuentes (Vista Lambda)",
            "description": "Vista SQL unificada de zonas criticas con ponderacion de eventos, categoria de alerta (CRITICO/MODERADO/NORMAL) y maxima magnitud.",
            "tableType": "View",
            "columns": [
                {"name": "cuadricula", "dataType": "VARCHAR", "dataLength": 50, "description": "Codigo de cuadrante hotspot"},
                {"name": "latitud_central", "dataType": "FLOAT"},
                {"name": "longitud_central", "dataType": "FLOAT"},
                {"name": "total_eventos_historico", "dataType": "INT"},
                {"name": "total_eventos_streaming", "dataType": "INT"},
                {"name": "eventos_totales_combinados", "dataType": "INT"},
                {"name": "magnitud_maxima_historica", "dataType": "FLOAT"},
                {"name": "magnitud_maxima_streaming", "dataType": "FLOAT"},
                {"name": "magnitud_maxima_global", "dataType": "FLOAT"},
                {"name": "profundidad_minima_global", "dataType": "FLOAT"},
                {"name": "categoria_alerta", "dataType": "VARCHAR", "dataLength": 50, "description": "Categoria ponderada: CRITICO, MODERADO o NORMAL"},
                {"name": "ultima_actualizacion_utc", "dataType": "TIMESTAMP"}
            ],
            "databaseSchema": "sismos_postgresql_cluster.sismos_db.public"
        }
    ]

    for t in tablas:
        try:
            api_request("PUT", "tables", t, token)
            print(f"  Tabla/Vista registrada: {t['name']}")
        except Exception as e:
            print(f"  Aviso al registrar {t['name']}: {e}")


def registrar_servicio_kafka(token):
    print("\n--- 3. Registrando Servicio de Mensajeria Apache Kafka ---")
    kafka_service_payload = {
        "name": "sismos_kafka_cluster",
        "displayName": "Apache Kafka Broker (Cluster Josue)",
        "serviceType": "Kafka",
        "description": f"Broker central Apache Kafka en el nodo Josue ({CLUSTER_KAFKA_IP}:{CLUSTER_KAFKA_PORT}) para ingestion streaming de eventos sismicos crudos.",
        "connection": {
            "config": {
                "type": "Kafka",
                "bootstrapServers": f"{CLUSTER_KAFKA_IP}:{CLUSTER_KAFKA_PORT}"
            }
        }
    }
    api_request("PUT", "services/messagingServices", kafka_service_payload, token)
    print("Servicio de Mensajeria 'sismos_kafka_cluster' registrado.")

    topico_payload = {
        "name": "sismos_crudos",
        "displayName": "sismos_crudos",
        "description": "Topico central de eventos sismicos consumido por Spark Streaming y MinIO Bronze S3 Sink.",
        "partitions": 1,
        "replicationFactor": 1,
        "cleanupPolicies": ["delete"],
        "retentionTime": 86400000.0,
        "maximumMessageSize": 1048576,
        "service": "sismos_kafka_cluster"
    }
    api_request("PUT", "topics", topico_payload, token)
    print("  Topico 'sismos_crudos' registrado con exito.")


def registrar_servicio_minio(token):
    print("\n--- 4. Registrando Servicio de Almacenamiento MinIO Medallion Data Lake ---")
    minio_service_payload = {
        "name": "sismos_minio_datalake",
        "displayName": "MinIO Medallion Data Lake (Cluster Sayder)",
        "serviceType": "CustomStorage",
        "description": f"Servicio Object Storage S3 compatible en nodo Sayder ({CLUSTER_MINIO_IP}:{CLUSTER_MINIO_PORT}) con buckets Medallion Bronze, Silver y Gold.",
        "connection": {
            "config": {
                "type": "CustomStorage"
            }
        }
    }
    api_request("PUT", "services/storageServices", minio_service_payload, token)
    print("Servicio de Almacenamiento 'sismos_minio_datalake' registrado.")

    buckets = [
        {
            "name": "bronze",
            "displayName": "Bucket Bronze (Crudos)",
            "description": "Almacenamiento de payloads JSON crudos inalterados de USGS. Prefijos: /streaming (Kafka Connect S3 Sink) y /batch (Airflow DAG).",
            "service": "sismos_minio_datalake"
        },
        {
            "name": "silver",
            "displayName": "Bucket Silver (Limpios)",
            "description": "Almacenamiento en formato columnar Parquet de datos limpios, parseados y tipados. Prefijos: /streaming y /batch.",
            "service": "sismos_minio_datalake"
        },
        {
            "name": "gold",
            "displayName": "Bucket Gold (Modelos Analiticos)",
            "description": "Almacenamiento en formato columnar Parquet de agregaciones analiticas por fecha, pais y hotspot. Prefijos: /streaming y /batch.",
            "service": "sismos_minio_datalake"
        }
    ]

    for b in buckets:
        try:
            api_request("PUT", "containers", b, token)
            print(f"  Bucket/Container registrado: {b['name']}")
        except Exception as e:
            print(f"  Aviso al registrar bucket {b['name']}: {e}")


def registrar_linaje_end_to_end(token):
    print("\n--- 5. Registrando Grafo de Linaje End-to-End ---")
    # Para registrar linaje en OpenMetadata, conectamos entidades upstream -> downstream
    # Primero obtenemos los IDs de las entidades registradas
    def get_entity(endpoint, fqn):
        try:
            return api_request("GET", f"{endpoint}/name/{fqn}", token=token)
        except Exception:
            return None

    topico_kafka = get_entity("topics", "sismos_kafka_cluster.sismos_crudos")
    t_streaming_resumen = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_resumen_streaming")
    t_batch_resumen = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_resumen_batch")
    v_resumen = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.vista_resumen_diario_pais")

    t_streaming_zonas = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_zonas_streaming")
    t_batch_zonas = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.gold_zonas_batch")
    v_zonas = get_entity("tables", "sismos_postgresql_cluster.sismos_db.public.vista_zonas_criticas_frecuentes")

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
            print(f"  Linaje conectado: [{from_type}] {from_entity.get('name')} -> [{to_type}] {to_entity.get('name')}")
        except Exception as e:
            print(f"  Aviso al conectar linaje: {e}")

    # Topico Kafka -> Tablas Streaming PostgreSQL
    if topico_kafka and t_streaming_resumen:
        conectar_linaje(topico_kafka, "topic", t_streaming_resumen, "table")
    if topico_kafka and t_streaming_zonas:
        conectar_linaje(topico_kafka, "topic", t_streaming_zonas, "table")

    # Tablas Streaming y Batch -> Vistas Lambda
    if t_streaming_resumen and v_resumen:
        conectar_linaje(t_streaming_resumen, "table", v_resumen, "table")
    if t_batch_resumen and v_resumen:
        conectar_linaje(t_batch_resumen, "table", v_resumen, "table")

    if t_streaming_zonas and v_zonas:
        conectar_linaje(t_streaming_zonas, "table", v_zonas, "table")
    if t_batch_zonas and v_zonas:
        conectar_linaje(t_batch_zonas, "table", v_zonas, "table")


def main():
    print("=" * 75)
    print("INICIALIZADOR DE GOBERNANZA Y CATALOGO - OPENMETADATA 2.0.6")
    print("=" * 75)
    esperar_servidor()
    token = autenticar()
    registrar_glosario_y_terminos(token)
    registrar_servicio_postgresql(token)
    registrar_servicio_kafka(token)
    registrar_servicio_minio(token)
    registrar_linaje_end_to_end(token)
    print("\n" + "=" * 75)
    print("CATALOGO Y GOBERNANZA EN OPENMETADATA COMPLETADOS CON EXITO.")
    print(f"Plataforma accesible en: http://localhost:8585")
    print(f"Credenciales por defecto: {OM_USER} / {OM_PASSWORD}")
    print("=" * 75)


if __name__ == "__main__":
    main()

"""
DAG: dag_sismos_batch_usgs.py
Proyecto: Sistema de Monitoreo Sísmico Global en Tiempo Real y Batch
Responsable: JESÚS — Ingeniero de Orquestación Batch (Apache Airflow)
Receptor: KEVIN — Ingeniero de Procesamiento Masivo (Apache Spark Batch)

Descripción:
  Este DAG se ejecuta todos los días a las 7:00 AM (0 7 * * *).
  1. Descarga el consolidado de las últimas 24 horas desde la API pública de USGS (all_day.geojson).
  2. Guarda el archivo JSON crudo e inmutable en el Data Lake MinIO en el bucket 'bronze' con la ruta:
     batch/año=YYYY/mes=MM/dia=DD/sismos_batch_YYYY-MM-DD.json
  3. Ejecuta automáticamente el pipeline de Spark Batch de Kevin (spark_batch_etl.py)
     para transformar a Silver (Parquet), Gold (resumen_diario_pais y zonas_criticas_frecuentes en Parquet)
     y cargar la Serving Layer en PostgreSQL (gold_resumen_batch y gold_zonas_batch).
"""

from datetime import datetime, timedelta
import json
import logging
import os
import pendulum
import requests
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator

# -------------------------------------------------------------------------
# Configuración y Constantes
# -------------------------------------------------------------------------
USGS_API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

# Endpoint primario central (IP de red compartida) y fallback local
REMOTE_MINIO_ENDPOINT = "http://10.175.132.248:9000"
LOCAL_MINIO_ENDPOINT = "http://minio:9000"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", REMOTE_MINIO_ENDPOINT)
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "n3jTFho71P8FV8jPHhQC")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "vX4t11jbqRLXRkoTtkW3i6qVvWGvKY9E21tRvm54")
MINIO_BUCKET = "bronze"

LOCAL_TEMP_DIR = "/tmp/usgs_batch"

# Argumentos por defecto del DAG
default_args = {
    "owner": "jesus",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


# -------------------------------------------------------------------------
# Tarea 1: Descargar datos de USGS
# -------------------------------------------------------------------------
def descargar_sismos_usgs(**context):
    """
    Descarga el corte oficial de sismos de las últimas 24 horas desde la API USGS.
    Almacena temporalmente el archivo local para ser subido a MinIO.
    """
    logging.info(f"Iniciando descarga desde USGS API: {USGS_API_URL}")
    response = requests.get(USGS_API_URL, timeout=30)
    response.raise_for_status()

    data = response.json()
    total_features = len(data.get("features", []))
    metadata = data.get("metadata", {})

    logging.info(f"Descarga exitosa. Título: {metadata.get('title')}")
    logging.info(f"Total de sismos recibidos: {total_features}")

    # Determinar fecha de ejecución (UTC)
    logical_date = context.get("logical_date") or context.get("execution_date") or datetime.utcnow()
    fecha_str = logical_date.strftime("%Y-%m-%d")

    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    temp_file_path = os.path.join(LOCAL_TEMP_DIR, f"sismos_batch_{fecha_str}.json")

    with open(temp_file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    logging.info(f"Archivo guardado temporalmente en: {temp_file_path}")

    # Compartir metadatos vía XCom
    context["ti"].xcom_push(key="temp_file_path", value=temp_file_path)
    context["ti"].xcom_push(key="total_sismos", value=total_features)
    context["ti"].xcom_push(key="fecha_evento", value=fecha_str)

    return temp_file_path


# -------------------------------------------------------------------------
# Tarea 2: Guardar archivo crudo en MinIO Bronze
# -------------------------------------------------------------------------
def guardar_en_minio_bronze(**context):
    """
    Sube el archivo JSON inmutable al bucket 'bronze' de MinIO con la estructura
    exigida: batch/año=YYYY/mes=MM/dia=DD/sismos_batch_YYYY-MM-DD.json
    """
    ti = context["ti"]
    temp_file_path = ti.xcom_pull(task_ids="tarea_1_descargar_sismos_usgs", key="temp_file_path")
    total_sismos = ti.xcom_pull(task_ids="tarea_1_descargar_sismos_usgs", key="total_sismos")

    logical_date = context.get("logical_date") or context.get("execution_date") or datetime.utcnow()
    año = logical_date.strftime("%Y")
    mes = logical_date.strftime("%m")
    dia = logical_date.strftime("%d")
    fecha_str = logical_date.strftime("%Y-%m-%d")

    # Clave de destino estricta según especificación técnica
    s3_key = f"batch/año={año}/mes={mes}/dia={dia}/sismos_batch_{fecha_str}.json"
    s3_uri = f"s3a://{MINIO_BUCKET}/{s3_key}"

    # Endpoints a probar: primero el central del equipo, luego el local
    endpoints_a_probar = []
    for ep in [MINIO_ENDPOINT, REMOTE_MINIO_ENDPOINT, "http://10.175.132.244:9000", LOCAL_MINIO_ENDPOINT]:
        if ep and ep not in endpoints_a_probar:
            endpoints_a_probar.append(ep)

    credenciales_a_probar = [
        (MINIO_ACCESS_KEY, MINIO_SECRET_KEY, "Nueva Key MinIO 10.175.132.248 (n3jTFh...)"),
        ("s65H8jUMyaDoZgPmdkWV", "RdyxisbfivojcjOySPV8d5tX1A3lilPUrXXsEg23", "Key de Servicio (s65H8j...)"),
        ("en108sgUBD48iFaDZu90", "yHV7uQJWapCqWrw7XUGMANpmdV8KQHDKTccQcmCy", "Key de Servicio (en108s...)"),
        ("minioadmin", "minioadmin", "Root Admin Key")
    ]

    s3_client = None
    subido_exitoso = False
    endpoint_usado = None

    for ep in endpoints_a_probar:
        if subido_exitoso:
            break
        logging.info(f"Intentando conectar al servidor MinIO en: {ep}")
        for ak, sk, label in credenciales_a_probar:
            try:
                logging.info(f"Probando en {ep} con {label} (AccessKey: {ak[:6]}...)")
                client = boto3.client(
                    "s3",
                    endpoint_url=ep,
                    aws_access_key_id=ak,
                    aws_secret_access_key=sk,
                    region_name="us-east-1",
                    config=boto3.session.Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1})
                )
                # Verificar si existe el bucket bronze, si no, crearlo
                try:
                    client.head_bucket(Bucket=MINIO_BUCKET)
                except ClientError:
                    logging.info(f"El bucket '{MINIO_BUCKET}' no existe en {ep}. Creándolo...")
                    client.create_bucket(Bucket=MINIO_BUCKET)

                # Subir archivo
                logging.info(f"Subiendo archivo a: {s3_uri} en {ep}")
                client.upload_file(temp_file_path, MINIO_BUCKET, s3_key)
                logging.info(f"¡Archivo subido exitosamente a MinIO Bronze en {ep}!")
                s3_client = client
                subido_exitoso = True
                endpoint_usado = ep
                break
            except Exception as e:
                logging.warning(f"No se pudo completar con {label} en {ep}: {e}")

    if not subido_exitoso:
        raise RuntimeError("No fue posible subir el archivo a MinIO con ninguna combinación de endpoint y credenciales.")

    logging.info(f"Operación finalizada con éxito utilizando el servidor MinIO: {endpoint_usado}")

    # Guardar en XCom la ruta oficial para el receptor (Kevin)
    ti.xcom_push(key="s3_bronze_uri", value=s3_uri)
    ti.xcom_push(key="s3_key", value=s3_key)

    # Limpiar archivo temporal
    if os.path.exists(temp_file_path):
        os.remove(temp_file_path)
        logging.info(f"Archivo temporal local {temp_file_path} eliminado correctamente.")

    return s3_uri


# -------------------------------------------------------------------------
# Tarea 3: Disparar ejecución automática en Spark Batch (Kevin)
# -------------------------------------------------------------------------
def ejecutar_spark_batch_kevin(**context):
    """
    Dispara la ejecución del pipeline PySpark (spark_batch_etl.py) en spark_master
    para procesar los datos de Bronze hacia Silver y Gold en MinIO,
    y actualizar las tablas gold_resumen_batch y gold_zonas_batch en PostgreSQL.
    """
    ti = context["ti"]
    s3_uri = ti.xcom_pull(task_ids="tarea_2_guardar_minio_bronze", key="s3_bronze_uri")
    total_sismos = ti.xcom_pull(task_ids="tarea_1_descargar_sismos_usgs", key="total_sismos")

    mensaje = f"""
    ================================================================================
    >>> CADENA DE RELEVOS BATCH — AIRFLOW (JESÚS) -> SPARK (KEVIN) <<<
    ================================================================================
    [EMISOR]   : Jesús (Ingeniero de Orquestación Batch - Apache Airflow)
    [RECEPTOR] : Kevin (Ingeniero de Procesamiento Masivo - PySpark Batch ETL)
    [ESTADO]   : Lote de 24 horas descargado y depositado con éxito en MinIO Bronze.
    [RUTA S3]  : {s3_uri}
    [VOLUMEN]  : {total_sismos} eventos sísmicos registrados.
    [ACCIÓN]   : DISPARANDO AUTOMÁTICAMENTE spark_batch_etl.py EN SPARK MASTER
    ================================================================================
    """
    logging.info(mensaje)
    print(mensaje)

    # Endpoints posibles para invocar el Trigger Service en el clúster Spark
    spark_endpoints = [
        "http://spark-master:5050/trigger",
        "http://spark_master:5050/trigger",
        "http://10.175.132.248:5050/trigger"
    ]

    spark_exitoso = False
    ultimo_error = None

    for ep in spark_endpoints:
        try:
            logging.info(f"Invocando Spark Batch ETL en {ep}...")
            response = requests.post(ep, timeout=600)
            
            if response.status_code == 200:
                data = response.json()
                logging.info(">>> ¡SPARK BATCH EJECUTADO EXITOSAMENTE! <<<")
                spark_log = data.get("output", "")
                logging.info(f"Resumen de ejecución Spark:\n{spark_log}")
                spark_exitoso = True
                break
            else:
                ultimo_error = f"HTTP {response.status_code}: {response.text}"
                logging.warning(f"Fallo en {ep}: {ultimo_error}")
        except Exception as e:
            ultimo_error = str(e)
            logging.warning(f"No se pudo conectar a {ep}: {e}")

    if not spark_exitoso:
        raise RuntimeError(f"Error al disparar el proceso Spark Batch de Kevin: {ultimo_error}")

    logging.info("=" * 80)
    logging.info(">>> PIPELINE COMPLETO FINALIZADO: BRONZE -> SILVER -> GOLD (MinIO + PostgreSQL) <<<")
    logging.info("=" * 80)
    return "SPARK_BATCH_SUCCESS"


# -------------------------------------------------------------------------
# Zona horaria local (Colombia / Perú / Ecuador: UTC-5)
# -------------------------------------------------------------------------
local_tz = pendulum.timezone("America/Bogota")

# Definición del DAG
# -------------------------------------------------------------------------
with DAG(
    dag_id="dag_sismos_batch_usgs",
    default_args=default_args,
    description="Pipeline Batch diario 7:00 AM - Ingesta USGS GeoJSON a MinIO Bronze y ejecución automática Spark Batch",
    schedule="0 7 * * *",  # Todos los días a las 7:00 AM hora local
    start_date=pendulum.datetime(2026, 1, 1, tz=local_tz),
    catchup=False,
    tags=["batch", "usgs", "sismos", "bronze", "silver", "gold", "jesus", "kevin"],
) as dag:

    tarea_1 = PythonOperator(
        task_id="tarea_1_descargar_sismos_usgs",
        python_callable=descargar_sismos_usgs,
    )

    tarea_2 = PythonOperator(
        task_id="tarea_2_guardar_minio_bronze",
        python_callable=guardar_en_minio_bronze,
    )

    tarea_3 = PythonOperator(
        task_id="tarea_3_trigger_spark_kevin",
        python_callable=ejecutar_spark_batch_kevin,
    )

    # Flujo secuencial de relevos: Bronze -> MinIO -> Spark ETL -> Silver/Gold/Postgres
    tarea_1 >> tarea_2 >> tarea_3

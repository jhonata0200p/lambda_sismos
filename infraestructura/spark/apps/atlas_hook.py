import os
import logging
import requests
from datetime import datetime

# Configuración de Logging
logger = logging.getLogger("AtlasHook")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(asctime)s] [ATLAS-HOOK] %(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

def sanitizar_val(val: str, default: str) -> str:
    if not val:
        return default
    return val

# Configuracion de Conexion a Apache Atlas (Laptop Jhonatan - Desde Variables de Entorno)
ATLAS_HOST = os.getenv("ATLAS_HOST", "10.131.229.244")
ATLAS_PORT = os.getenv("ATLAS_PORT", "21000")
ATLAS_USER = os.getenv("ATLAS_USER", "admin")
ATLAS_PASSWORD = os.getenv("ATLAS_PASSWORD", "admin")
ATLAS_TIMEOUT = float(os.getenv("ATLAS_TIMEOUT", "10.0")) # Timeout prudente para Wi-Fi sin frenar Spark

ATLAS_BASE_URL = f"http://{ATLAS_HOST}:{ATLAS_PORT}/api/atlas/v2"

def notificar_proceso_atlas(
    proceso_nombre: str,
    proceso_tipo: str,
    inputs: list,
    outputs: list,
    descripcion: str = "",
    detalles: dict = None
) -> bool:
    """
    Registra o actualiza la ejecución de un proceso y su linaje en Apache Atlas.
    Es 100% no-bloqueante: si Atlas no está encendido o no hay red, Spark/Airflow continúan sin fallar.
    
    :param proceso_nombre: Nombre único del proceso (ej. 'spark_batch_etl_run')
    :param proceso_tipo: Tipo de entidad Atlas (ej. 'Process', 'spark_process')
    :param inputs: Lista de diccionarios [{'typeName': '...', 'uniqueAttributes': {'qualifiedName': '...'}}]
    :param outputs: Lista de diccionarios [{'typeName': '...', 'uniqueAttributes': {'qualifiedName': '...'}}]
    :param descripcion: Descripción técnica del procesamiento
    :param detalles: Diccionario con métricas adicionales (registros, tiempo, etc.)
    :return: True si se registró exitosamente, False si falló de forma segura.
    """
    url = f"{ATLAS_BASE_URL}/entity"
    auth = (ATLAS_USER, ATLAS_PASSWORD)
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    now_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    qualified_name = f"{proceso_nombre}@spark"

    payload = {
        "entity": {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": proceso_nombre,
                "description": f"{descripcion} - Última ejecución: {now_iso}",
                "inputs": inputs,
                "outputs": outputs
            }
        }
    }

    try:
        response = requests.post(url, json=payload, auth=auth, headers=headers, timeout=ATLAS_TIMEOUT)
        if response.status_code in [200, 201]:
            logger.info(f"Linaje registrado en Atlas exitosamente: '{proceso_nombre}' ({len(inputs)} inputs -> {len(outputs)} outputs)")
            return True
        else:
            logger.warning(f"Atlas respondió con código {response.status_code}: {response.text[:150]}")
            return False
    except requests.exceptions.Timeout:
        logger.info(f"Atlas en {ATLAS_HOST}:{ATLAS_PORT} no respondió dentro de {ATLAS_TIMEOUT}s. Omitiendo reporte (ejecución intacta).")
        return False
    except requests.exceptions.ConnectionError:
        logger.info(f"Atlas en {ATLAS_HOST}:{ATLAS_PORT} no alcanzable por red. Omitiendo reporte (ejecución intacta).")
        return False
    except Exception as e:
        logger.info(f"Aviso menor de Atlas ({e}). Omitiendo reporte (ejecución intacta).")
        return False

# Pruebas directas si se ejecuta como script
if __name__ == "__main__":
    print(f"Probando conexión hacia Atlas en: {ATLAS_BASE_URL}")
    test_inputs = [{"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "usgs_api_raw@usgs"}}]
    test_outputs = [{"typeName": "DataSet", "uniqueAttributes": {"qualifiedName": "minio_bronze_streaming@minio"}}]
    exito = notificar_proceso_atlas("test_connection_job", "Process", test_inputs, test_outputs, "Prueba de conectividad")
    print(f"Resultado: {'Éxito' if exito else 'Atlas fuera de línea o inalcanzable (comportamiento esperado)'}")

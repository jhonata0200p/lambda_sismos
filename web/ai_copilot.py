import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from database import get_db

# Cargar variables de entorno desde .env
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

def obtener_contexto_sismico_actual():
    """
    RAG (Retrieval-Augmented Generation):
    Extrae la verdad del terreno directamente desde la capa Serving de PostgreSQL (Sayder).
    """
    contexto = {
        "timestamp_consulta_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        "kpis_globales": {},
        "origen_datos": {},
        "top_paises_afectados": [],
        "zonas_criticas_activas": []
    }
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # 1. Totales y magnitudes
                cur.execute("""
                    SELECT 
                        COALESCE(SUM(total_sismos), 0) AS total_sismos,
                        COALESCE(MAX(magnitud_maxima), 0.0) AS max_magnitud,
                        COALESCE(SUM(total_alertas_tsunami), 0) AS total_tsunamis,
                        COALESCE(SUM(sismos_peligrosos_m5), 0) AS sismos_peligrosos_m5
                    FROM vista_resumen_diario_pais;
                """)
                row = cur.fetchone()
                if row:
                    contexto["kpis_globales"] = {
                        "total_sismos": int(row["total_sismos"]),
                        "magnitud_maxima": float(row["max_magnitud"]),
                        "total_alertas_tsunami": int(row["total_tsunamis"]),
                        "sismos_peligrosos_m5": int(row["sismos_peligrosos_m5"])
                    }

                # 2. Desglose Streaming vs Batch
                cur.execute("""
                    SELECT origen_dato, COALESCE(SUM(total_sismos), 0) AS total
                    FROM vista_resumen_diario_pais
                    GROUP BY origen_dato;
                """)
                for r in cur.fetchall():
                    contexto["origen_datos"][r["origen_dato"]] = int(r["total"])

                # 3. Top países / estados
                cur.execute("""
                    SELECT pais_estado, SUM(total_sismos) AS total_sismos, MAX(magnitud_maxima) AS max_mag
                    FROM vista_resumen_diario_pais
                    GROUP BY pais_estado
                    ORDER BY total_sismos DESC
                    LIMIT 8;
                """)
                contexto["top_paises_afectados"] = [
                    {"pais_estado": r["pais_estado"], "total_sismos": int(r["total_sismos"]), "max_magnitud": float(r["max_mag"])}
                    for r in cur.fetchall()
                ]

                # 4. Zonas sísmicas agregadas por Spark en Gold
                cur.execute("""
                    SELECT zona_referencia, cuadricula_lat, cuadricula_lon, total_sismos_registrados, 
                           magnitud_maxima, profundidad_promedio_km, nivel_alerta
                    FROM vista_zonas_criticas_frecuentes
                    ORDER BY magnitud_maxima DESC
                    LIMIT 10;
                """)
                contexto["zonas_criticas_activas"] = [
                    {
                        "zona": r["zona_referencia"],
                        "latitud": float(r["cuadricula_lat"]),
                        "longitud": float(r["cuadricula_lon"]),
                        "sismos_en_zona": int(r["total_sismos_registrados"]),
                        "magnitud_pico": float(r["magnitud_maxima"]),
                        "profundidad_km": float(r["profundidad_promedio_km"]),
                        "nivel_alerta": r["nivel_alerta"]
                    }
                    for r in cur.fetchall()
                ]
    except Exception as e:
        contexto["error_base_datos"] = str(e)

    return contexto

def consultar_gemini(pregunta: str, contexto: dict) -> dict:
    """
    Envía la pregunta y el contexto de PostgreSQL a Google Gemini.
    Itera sobre la familia de modelos Flash de Gemini para asegurar alta disponibilidad.
    """
    api_key = os.getenv("GEMINI_API_KEY", GEMINI_API_KEY).strip()

    if not api_key:
        return {
            "respuesta": """⚠️ **Google Gemini no está configurado aún.**

Para usar el chat con Inteligencia Artificial real, necesitas agregar tu clave en el archivo `web/.env`:
```env
GEMINI_API_KEY="AQ.Ab8RN6..."
```
Reinicia FastAPI y ¡listo!""",
            "motor": "Google Gemini (Requiere API Key)",
            "status": "missing_key"
        }

    system_instruction = f"""Eres GeoCopilot, un asistente sismológico de élite para un sistema Big Data de monitoreo sísmico (Arquitectura Lambda + Medallion).
Tienes acceso en tiempo real a los datos consolidados en la base de datos PostgreSQL Serving Layer.

DATOS REALES EN VIVO DESDE POSTGRESQL:
{json.dumps(contexto, indent=2, ensure_ascii=False)}

REGLAS DE RESPUESTA:
1. Responde siempre en español, de forma concisa, profesional y con rigor científico sismológico.
2. Utiliza los números, magnitudes y coordenadas EXACTAS del JSON de PostgreSQL para fundamentar tus respuestas. Nunca inventes sismos que no estén en los datos.
3. Si el usuario pide un boletín o reporte oficial, estructúralo de manera formal con dictamen técnico y recomendaciones de prevención.
4. Si preguntan por la arquitectura, explica con propiedad técnica el flujo de Kafka (Streaming), Airflow (Batch), Spark y MinIO.
5. Usa viñetas y formato Markdown limpio con emojis técnicos (🚨, 📊, ⚡, 📍, 🟢)."""

    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": f"{system_instruction}\n\nPregunta del usuario: {pregunta}"}
                ]
            }
        ],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": 1000
        }
    }

    # Modelos oficiales Gemini Flash con fallback automático en caso de demanda alta
    modelos_a_probar = [
        "gemini-3.6-flash",
        "gemini-3-flash-preview",
        "gemini-3.5-flash",
        "gemini-3.8-flash"
    ]

    ultimo_error = None
    for modelo in modelos_a_probar:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{modelo}:generateContent?key={api_key}"
        try:
            response = requests.post(url, json=payload, timeout=12)
            if response.status_code == 200:
                data = response.json()
                texto_gemini = data["candidates"][0]["content"]["parts"][0]["text"]
                return {
                    "respuesta": texto_gemini,
                    "motor": f"Google Gemini ({modelo})",
                    "status": "success"
                }
            elif response.status_code in [429, 503]:
                # Modelo ocupado por demanda pico, pasar al siguiente modelo
                ultimo_error = f"{modelo} (HTTP {response.status_code})"
                continue
            else:
                ultimo_error = f"{modelo} error {response.status_code}: {response.text[:200]}"
        except requests.exceptions.Timeout:
            ultimo_error = f"Timeout en {modelo}."
            continue
        except Exception as e:
            ultimo_error = str(e)
            continue

    return {
        "respuesta": f"⚠️ **Error al consultar Gemini:** {ultimo_error}\nPor favor intenta enviar tu pregunta de nuevo.",
        "motor": "Google Gemini (Error)",
        "status": "api_error"
    }

def consultar_geocopilot(pregunta: str) -> dict:
    """Función consumida por el endpoint POST /api/chat-ia de FastAPI."""
    contexto = obtener_contexto_sismico_actual()
    return consultar_gemini(pregunta, contexto)

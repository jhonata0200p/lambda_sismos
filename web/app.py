import os
from decimal import Decimal
from datetime import date, datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from database import get_db
from pydantic import BaseModel
from ai_copilot import consultar_geocopilot

class ChatRequest(BaseModel):
    message: str

app = FastAPI(
    title="Centro de Monitoreo Sísmico API",
    description="API FastAPI conectada al Serving Layer de PostgreSQL (Arquitectura Lambda + Medallion)",
    version="1.0.0"
)

# Permitir CORS para peticiones desde cualquier origen local
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def serialize_row(row):
    """Convierte tipos Decimal y datetime de Postgres a tipos JSON serializables"""
    clean = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            clean[k] = float(v)
        elif isinstance(v, (datetime, date)):
            clean[k] = v.isoformat()
        else:
            clean[k] = v
    return clean

@app.get("/api/health")
def health_check():
    """Verifica la conectividad en vivo hacia PostgreSQL Serving Layer"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                return {
                    "status": "online",
                    "serving_layer": "PostgreSQL conectado exitosamente",
                    "timestamp": datetime.utcnow().isoformat()
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error conectando a PostgreSQL: {str(e)}")

@app.get("/api/kpis")
def get_kpis():
    """Retorna los 4 KPIs principales calculados desde las vistas unificadas"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # 1. Totales y máximos de la vista resumen
                cur.execute("""
                    SELECT 
                        COALESCE(SUM(total_sismos), 0) AS total_sismos,
                        COALESCE(MAX(magnitud_maxima), 0.0) AS max_magnitud,
                        COALESCE(SUM(total_alertas_tsunami), 0) AS total_tsunamis,
                        COALESCE(SUM(sismos_peligrosos_m5), 0) AS sismos_peligrosos_m5
                    FROM vista_resumen_diario_pais;
                """)
                resumen = cur.fetchone()

                # 2. Desglose Streaming vs Batch
                cur.execute("""
                    SELECT 
                        origen_dato, 
                        COALESCE(SUM(total_sismos), 0) AS total 
                    FROM vista_resumen_diario_pais 
                    GROUP BY origen_dato;
                """)
                desglose = {r["origen_dato"]: int(r["total"]) for r in cur.fetchall()}

                # 3. Conteo de zonas críticas activas
                cur.execute("""
                    SELECT COUNT(*) AS total_criticas 
                    FROM vista_zonas_criticas_frecuentes 
                    WHERE nivel_alerta = 'CRITICO';
                """)
                criticas = cur.fetchone()

                return {
                    "total_sismos": int(resumen["total_sismos"]),
                    "max_magnitud": float(resumen["max_magnitud"]),
                    "total_tsunamis": int(resumen["total_tsunamis"]),
                    "sismos_peligrosos_m5": int(resumen["sismos_peligrosos_m5"]),
                    "total_criticas": int(criticas["total_criticas"]),
                    "desglose_origen": {
                        "streaming": desglose.get("STREAMING", 0),
                        "batch": desglose.get("BATCH", 0)
                    }
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/resumen-paises")
def get_resumen_paises():
    """Retorna el Top 10 de países/estados con mayor sismicidad (Gold 1)"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        pais_estado,
                        SUM(total_sismos) AS total_sismos,
                        MAX(magnitud_maxima) AS magnitud_maxima,
                        ROUND(AVG(magnitud_promedio), 2) AS magnitud_promedio,
                        SUM(total_alertas_tsunami) AS total_tsunamis,
                        SUM(sismos_peligrosos_m5) AS sismos_peligrosos_m5
                    FROM vista_resumen_diario_pais
                    GROUP BY pais_estado
                    ORDER BY total_sismos DESC
                    LIMIT 10;
                """)
                rows = [serialize_row(r) for r in cur.fetchall()]
                return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/vista-unificada")
def get_vista_unificada():
    """Retorna los registros de vista_resumen_diario_pais con su origen STREAMING vs BATCH"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        fecha_evento,
                        pais_estado,
                        total_sismos,
                        magnitud_maxima,
                        magnitud_promedio,
                        profundidad_promedio_km,
                        total_alertas_tsunami,
                        sismos_peligrosos_m5,
                        ultima_actualizacion_utc,
                        origen_dato
                    FROM vista_resumen_diario_pais
                    ORDER BY ultima_actualizacion_utc DESC
                    LIMIT 50;
                """)
                rows = [serialize_row(r) for r in cur.fetchall()]
                return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/zonas-criticas")
def get_zonas_criticas():
    """Retorna los cuadrantes geográficos y fallas críticas para el mapa Leaflet y tabla (Gold 2)"""
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        zona_referencia,
                        cuadricula_lat,
                        cuadricula_lon,
                        total_sismos_registrados,
                        sismos_graves_m5,
                        magnitud_maxima,
                        profundidad_promedio_km,
                        nivel_alerta,
                        fecha_primer_sismo,
                        fecha_ultimo_sismo
                    FROM vista_zonas_criticas_frecuentes
                    ORDER BY 
                        CASE nivel_alerta 
                            WHEN 'CRITICO' THEN 1 
                            WHEN 'MODERADO' THEN 2 
                            ELSE 3 
                        END,
                        total_sismos_registrados DESC;
                """)
                rows = [serialize_row(r) for r in cur.fetchall()]
                return rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# CATÁLOGO DE REGIONES SÍSMICAS ACTIVAS PARA SIMULACIÓN MULTI-REGIÓN
REGIONES_SISMICAS_MUNDIALES = [
    {"pais": "Japón", "zona": "Fosa de Japón - Cinturón de Tokio", "lat": 35.7, "lon": 139.8, "min_m": 5.8, "max_m": 6.8, "prof_base": 44.5},
    {"pais": "Chile", "zona": "Zona de Subducción - Valparaíso, Chile", "lat": -33.1, "lon": -71.5, "min_m": 5.9, "max_m": 7.1, "prof_base": 33.0},
    {"pais": "Indonesia", "zona": "Arco de Sonda - Estrecho de la Sonda", "lat": -5.9, "lon": 105.7, "min_m": 5.7, "max_m": 6.7, "prof_base": 28.0},
    {"pais": "California", "zona": "Falla de San Andrés - The Geysers, CA", "lat": 38.8, "lon": -122.8, "min_m": 4.2, "max_m": 5.5, "prof_base": 7.5},
    {"pais": "Alaska", "zona": "Fosa de las Aleutianas - Anchorage, AK", "lat": 61.2, "lon": -149.9, "min_m": 5.1, "max_m": 6.3, "prof_base": 22.0},
    {"pais": "México", "zona": "Fosa Mesoamericana - Costa de Guerrero", "lat": 16.8, "lon": -99.9, "min_m": 5.5, "max_m": 6.5, "prof_base": 21.0},
    {"pais": "Nueva Zelanda", "zona": "Fosa de Kermadec - Norte de N. Zelanda", "lat": -30.5, "lon": -178.5, "min_m": 5.8, "max_m": 6.9, "prof_base": 48.0},
    {"pais": "Hawaii", "zona": "Pāhala - Zona Volcánica Kilauea", "lat": 19.3, "lon": -155.4, "min_m": 3.8, "max_m": 5.2, "prof_base": 32.0},
    {"pais": "Colombia", "zona": "Nido Sísmico de Los Santos, Santander", "lat": 6.8, "lon": -73.1, "min_m": 4.8, "max_m": 5.9, "prof_base": 145.0},
    {"pais": "Filipinas", "zona": "Fosa de Filipinas - Mindanao Oriental", "lat": 8.2, "lon": 126.3, "min_m": 5.6, "max_m": 6.8, "prof_base": 38.0}
]

@app.post("/api/simular-streaming")
def simular_streaming_insert():
    """Inyecta un sismo de prueba aleatorio en gold_resumen_streaming y gold_zonas_streaming"""
    import random
    region = random.choice(REGIONES_SISMICAS_MUNDIALES)
    magnitud = round(random.uniform(region["min_m"], region["max_m"]), 2)
    profundidad = round(region["prof_base"] + random.uniform(-4.0, 4.0), 1)
    es_grave = 1 if magnitud >= 5.0 else 0
    alerta_tsunami = 1 if magnitud >= 6.0 and region["pais"] not in ["California", "Colombia"] else 0
    nivel_alerta = "CRITICO" if (magnitud >= 5.0 or es_grave) else "MODERADO"

    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                # 1. Insertar o actualizar en gold_resumen_streaming (Gold 1)
                cur.execute("""
                    INSERT INTO gold_resumen_streaming (
                        fecha_evento, pais_estado, total_sismos, magnitud_maxima,
                        magnitud_promedio, profundidad_promedio_km, total_alertas_tsunami,
                        sismos_peligrosos_m5, ultima_actualizacion_utc
                    ) VALUES (
                        CURRENT_DATE, %s, 1, %s, %s, %s, %s, %s, NOW()
                    )
                    ON CONFLICT (fecha_evento, pais_estado) DO UPDATE SET
                        total_sismos = gold_resumen_streaming.total_sismos + 1,
                        magnitud_maxima = GREATEST(gold_resumen_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima),
                        magnitud_promedio = ROUND(((gold_resumen_streaming.magnitud_promedio * gold_resumen_streaming.total_sismos) + EXCLUDED.magnitud_promedio) / (gold_resumen_streaming.total_sismos + 1), 2),
                        total_alertas_tsunami = gold_resumen_streaming.total_alertas_tsunami + EXCLUDED.total_alertas_tsunami,
                        sismos_peligrosos_m5 = gold_resumen_streaming.sismos_peligrosos_m5 + EXCLUDED.sismos_peligrosos_m5,
                        ultima_actualizacion_utc = NOW();
                """, (region["pais"], magnitud, magnitud, profundidad, alerta_tsunami, es_grave))

                # 2. Insertar o actualizar en gold_zonas_streaming (Gold 2)
                cur.execute("""
                    INSERT INTO gold_zonas_streaming (
                        cuadricula_lat, cuadricula_lon, zona_referencia, total_sismos_registrados,
                        sismos_graves_m5, magnitud_maxima, profundidad_promedio_km,
                        nivel_alerta, fecha_primer_sismo, fecha_ultimo_sismo
                    ) VALUES (
                        %s, %s, %s, 1, %s, %s, %s, %s, NOW(), NOW()
                    )
                    ON CONFLICT (cuadricula_lat, cuadricula_lon) DO UPDATE SET
                        total_sismos_registrados = gold_zonas_streaming.total_sismos_registrados + 1,
                        sismos_graves_m5 = gold_zonas_streaming.sismos_graves_m5 + EXCLUDED.sismos_graves_m5,
                        magnitud_maxima = GREATEST(gold_zonas_streaming.magnitud_maxima, EXCLUDED.magnitud_maxima),
                        nivel_alerta = EXCLUDED.nivel_alerta,
                        fecha_ultimo_sismo = NOW(),
                        ultima_actualizacion_utc = NOW();
                """, (region["lat"], region["lon"], region["zona"], es_grave, magnitud, profundidad, nivel_alerta))

                conn.commit()

                return {
                    "message": "Sismo inyectado en PostgreSQL exitosamente",
                    "sismo": {
                        "pais": region["pais"],
                        "zona": region["zona"],
                        "lat": region["lat"],
                        "lon": region["lon"],
                        "magnitud": magnitud,
                        "profundidad_km": profundidad,
                        "tsunami": alerta_tsunami,
                        "nivel_alerta": nivel_alerta
                    }
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/chat-ia")
def chat_ia(req: ChatRequest):
    """Endpoint de GeoCopilot IA: Asistente sismológico RAG sobre PostgreSQL"""
    if not req.message or not req.message.strip():
        raise HTTPException(status_code=400, detail="El mensaje no puede estar vacío")
    return consultar_geocopilot(req.message)

# Servir archivos estáticos del frontend
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

@app.get("/")
def serve_index():
    index_file = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return {"message": "Backend FastAPI listo. Esperando index.html en static/"}

if __name__ == "__main__":
    import uvicorn
    print("\n=======================================================")
    print(">>> Iniciando Servidor Web TerraPulse GIS en http://localhost:8000")
    print("=======================================================\n")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)

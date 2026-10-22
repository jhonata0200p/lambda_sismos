-- =====================================================================
-- PROYECTO BIG DATA: MONITOREO SISMICO GLOBAL
-- PostgreSQL Serving Layer - Exclusivamente Capa GOLD (Consumo)
-- Arquitectura Lambda: Tablas fisicas separadas (Batch y Streaming)
-- y Vistas SQL Unificadas para consumo en Web / Power BI
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. MODELO GOLD 1: RESUMEN DIARIO POR PAIS
-- ---------------------------------------------------------------------

-- Tabla fisica receptora de Streaming (Tiempo Real)
CREATE TABLE IF NOT EXISTS gold_resumen_streaming (
    fecha_evento DATE NOT NULL,
    pais_estado VARCHAR(100) NOT NULL,
    total_sismos INT NOT NULL DEFAULT 0,
    magnitud_maxima NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    magnitud_promedio NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    total_alertas_tsunami INT NOT NULL DEFAULT 0,
    sismos_peligrosos_m5 INT NOT NULL DEFAULT 0,
    ultima_actualizacion_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_evento, pais_estado)
);

-- Tabla fisica receptora de Batch (Historico Consolidado)
CREATE TABLE IF NOT EXISTS gold_resumen_batch (
    fecha_evento DATE NOT NULL,
    pais_estado VARCHAR(100) NOT NULL,
    total_sismos INT NOT NULL DEFAULT 0,
    magnitud_maxima NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    magnitud_promedio NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    total_alertas_tsunami INT NOT NULL DEFAULT 0,
    sismos_peligrosos_m5 INT NOT NULL DEFAULT 0,
    ultima_actualizacion_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_evento, pais_estado)
);

-- Vista Unificada Lambda: Resumen Diario Pais (Web / Power BI)
CREATE OR REPLACE VIEW vista_resumen_diario_pais AS
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
    'BATCH' AS origen_dato
FROM gold_resumen_batch
UNION ALL
SELECT 
    s.fecha_evento,
    s.pais_estado,
    s.total_sismos,
    s.magnitud_maxima,
    s.magnitud_promedio,
    s.profundidad_promedio_km,
    s.total_alertas_tsunami,
    s.sismos_peligrosos_m5,
    s.ultima_actualizacion_utc,
    'STREAMING' AS origen_dato
FROM gold_resumen_streaming s
WHERE NOT EXISTS (
    SELECT 1 FROM gold_resumen_batch b 
    WHERE b.fecha_evento = s.fecha_evento AND b.pais_estado = s.pais_estado
);

-- ---------------------------------------------------------------------
-- 2. MODELO GOLD 2: ZONAS CRITICAS Y HOTSPOTS (Fallas Geologicas)
-- ---------------------------------------------------------------------

-- Tabla fisica receptora de Streaming (Tiempo Real)
CREATE TABLE IF NOT EXISTS gold_zonas_streaming (
    cuadricula_lat NUMERIC(5, 1) NOT NULL,
    cuadricula_lon NUMERIC(5, 1) NOT NULL,
    zona_referencia VARCHAR(100),
    total_sismos_registrados INT NOT NULL DEFAULT 0,
    sismos_graves_m5 INT NOT NULL DEFAULT 0,
    magnitud_maxima NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    nivel_alerta VARCHAR(20) NOT NULL DEFAULT 'NORMAL',
    fecha_primer_sismo TIMESTAMP,
    fecha_ultimo_sismo TIMESTAMP,
    ultima_actualizacion_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cuadricula_lat, cuadricula_lon)
);

-- Tabla fisica receptora de Batch (Historico Consolidado)
CREATE TABLE IF NOT EXISTS gold_zonas_batch (
    cuadricula_lat NUMERIC(5, 1) NOT NULL,
    cuadricula_lon NUMERIC(5, 1) NOT NULL,
    zona_referencia VARCHAR(100),
    total_sismos_registrados INT NOT NULL DEFAULT 0,
    sismos_graves_m5 INT NOT NULL DEFAULT 0,
    magnitud_maxima NUMERIC(4, 2) NOT NULL DEFAULT 0.0,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL DEFAULT 0.0,
    nivel_alerta VARCHAR(20) NOT NULL DEFAULT 'NORMAL',
    fecha_primer_sismo TIMESTAMP,
    fecha_ultimo_sismo TIMESTAMP,
    ultima_actualizacion_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (cuadricula_lat, cuadricula_lon)
);

-- Vista Unificada Lambda: Zonas Criticas y Hotspots (Web / Power BI)
CREATE OR REPLACE VIEW vista_zonas_criticas_frecuentes AS
SELECT 
    COALESCE(s.cuadricula_lat, b.cuadricula_lat) AS cuadricula_lat,
    COALESCE(s.cuadricula_lon, b.cuadricula_lon) AS cuadricula_lon,
    COALESCE(s.zona_referencia, b.zona_referencia) AS zona_referencia,
    COALESCE(b.total_sismos_registrados, 0) + COALESCE(s.total_sismos_registrados, 0) AS total_sismos_registrados,
    COALESCE(b.sismos_graves_m5, 0) + COALESCE(s.sismos_graves_m5, 0) AS sismos_graves_m5,
    GREATEST(COALESCE(b.magnitud_maxima, 0), COALESCE(s.magnitud_maxima, 0)) AS magnitud_maxima,
    ROUND(
        (COALESCE(b.profundidad_promedio_km, 0) * COALESCE(b.total_sismos_registrados, 0) + 
         COALESCE(s.profundidad_promedio_km, 0) * COALESCE(s.total_sismos_registrados, 0)) / 
        NULLIF((COALESCE(b.total_sismos_registrados, 0) + COALESCE(s.total_sismos_registrados, 0)), 0), 2
    ) AS profundidad_promedio_km,
    CASE 
        WHEN (COALESCE(b.sismos_graves_m5, 0) + COALESCE(s.sismos_graves_m5, 0)) > 0 OR 
             (COALESCE(b.total_sismos_registrados, 0) + COALESCE(s.total_sismos_registrados, 0)) >= 15 THEN 'CRITICO'
        WHEN GREATEST(COALESCE(b.magnitud_maxima, 0), COALESCE(s.magnitud_maxima, 0)) >= 4.0 THEN 'MODERADO'
        ELSE 'NORMAL'
    END AS nivel_alerta,
    LEAST(b.fecha_primer_sismo, s.fecha_primer_sismo) AS fecha_primer_sismo,
    GREATEST(b.fecha_ultimo_sismo, s.fecha_ultimo_sismo) AS fecha_ultimo_sismo,
    GREATEST(b.ultima_actualizacion_utc, s.ultima_actualizacion_utc) AS ultima_actualizacion_utc
FROM gold_zonas_batch b
FULL OUTER JOIN gold_zonas_streaming s 
    ON b.cuadricula_lat = s.cuadricula_lat AND b.cuadricula_lon = s.cuadricula_lon;

-- ---------------------------------------------------------------------
-- 3. INDICES PARA OPTIMIZACION DE CONSULTAS EN SERVING LAYER
-- ---------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_resumen_batch_fecha ON gold_resumen_batch(fecha_evento);
CREATE INDEX IF NOT EXISTS idx_resumen_streaming_fecha ON gold_resumen_streaming(fecha_evento);
CREATE INDEX IF NOT EXISTS idx_resumen_batch_pais ON gold_resumen_batch(pais_estado);
CREATE INDEX IF NOT EXISTS idx_resumen_streaming_pais ON gold_resumen_streaming(pais_estado);
CREATE INDEX IF NOT EXISTS idx_zonas_batch_alerta ON gold_zonas_batch(nivel_alerta);
CREATE INDEX IF NOT EXISTS idx_zonas_streaming_alerta ON gold_zonas_streaming(nivel_alerta);

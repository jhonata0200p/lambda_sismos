-- =====================================================================
-- PROYECTO BIG DATA: MONITOREO SÍSMICO GLOBAL
-- PostgreSQL Serving Layer - Exclusivamente Capa GOLD (Consumo)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. MODELO GOLD 1: RESUMEN DIARIO POR PAÍS
-- ---------------------------------------------------------------------

-- Tabla física receptora de Streaming (Josué)
CREATE TABLE IF NOT EXISTS gold_resumen_streaming (
    fecha_evento DATE NOT NULL,
    pais_estado VARCHAR(100) NOT NULL,
    total_sismos INT NOT NULL,
    magnitud_maxima NUMERIC(4, 2) NOT NULL,
    magnitud_promedio NUMERIC(4, 2) NOT NULL,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL,
    total_alertas_tsunami INT NOT NULL,
    sismos_peligrosos_m5 INT NOT NULL,
    ultima_actualizacion_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_evento, pais_estado)
);

-- Tabla física receptora de Batch (Kevin)
CREATE TABLE IF NOT EXISTS gold_resumen_batch (
    fecha_evento DATE NOT NULL,
    pais_estado VARCHAR(100) NOT NULL,
    total_sismos INT NOT NULL,
    magnitud_maxima NUMERIC(4, 2) NOT NULL,
    magnitud_promedio NUMERIC(4, 2) NOT NULL,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL,
    total_alertas_tsunami INT NOT NULL,
    sismos_peligrosos_m5 INT NOT NULL,
    ultima_actualizacion_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_evento, pais_estado)
);

-- Vista Unificada Lambda: Resumen Diario País (Lista para consumir en Web / BI)
CREATE OR REPLACE VIEW vista_resumen_diario_pais AS
SELECT 
    fecha_evento, pais_estado, total_sismos, magnitud_maxima, 
    magnitud_promedio, profundidad_promedio_km, total_alertas_tsunami, 
    sismos_peligrosos_m5, ultima_actualizacion_utc, 'BATCH' AS origen_dato
FROM gold_resumen_batch
UNION ALL
SELECT 
    s.fecha_evento, s.pais_estado, s.total_sismos, s.magnitud_maxima, 
    s.magnitud_promedio, s.profundidad_promedio_km, s.total_alertas_tsunami, 
    s.sismos_peligrosos_m5, s.ultima_actualizacion_utc, 'STREAMING' AS origen_dato
FROM gold_resumen_streaming s
WHERE NOT EXISTS (
    SELECT 1 FROM gold_resumen_batch b 
    WHERE b.fecha_evento = s.fecha_evento AND b.pais_estado = s.pais_estado
);

-- ---------------------------------------------------------------------
-- 2. MODELO GOLD 2: ZONAS CRÍTICAS Y HOTSPOTS (Fallas Geológicas)
-- ---------------------------------------------------------------------

-- Tabla física receptora de Streaming (Josué)
CREATE TABLE IF NOT EXISTS gold_zonas_streaming (
    cuadricula_lat NUMERIC(5, 1) NOT NULL,
    cuadricula_lon NUMERIC(5, 1) NOT NULL,
    zona_referencia VARCHAR(100),
    total_sismos_registrados INT NOT NULL,
    sismos_graves_m5 INT NOT NULL,
    magnitud_maxima NUMERIC(4, 2) NOT NULL,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL,
    nivel_alerta VARCHAR(20) NOT NULL,
    fecha_primer_sismo TIMESTAMP,
    fecha_ultimo_sismo TIMESTAMP,
    PRIMARY KEY (cuadricula_lat, cuadricula_lon)
);

-- Tabla física receptora de Batch (Kevin)
CREATE TABLE IF NOT EXISTS gold_zonas_batch (
    cuadricula_lat NUMERIC(5, 1) NOT NULL,
    cuadricula_lon NUMERIC(5, 1) NOT NULL,
    zona_referencia VARCHAR(100),
    total_sismos_registrados INT NOT NULL,
    sismos_graves_m5 INT NOT NULL,
    magnitud_maxima NUMERIC(4, 2) NOT NULL,
    profundidad_promedio_km NUMERIC(8, 2) NOT NULL,
    nivel_alerta VARCHAR(20) NOT NULL,
    fecha_primer_sismo TIMESTAMP,
    fecha_ultimo_sismo TIMESTAMP,
    PRIMARY KEY (cuadricula_lat, cuadricula_lon)
);

-- Vista Unificada Lambda: Zonas Críticas y Hotspots (Lista para consumir en Web / BI)
CREATE OR REPLACE VIEW vista_zonas_criticas_frecuentes AS
SELECT 
    COALESCE(s.zona_referencia, b.zona_referencia) AS zona_referencia,
    COALESCE(s.cuadricula_lat, b.cuadricula_lat) AS cuadricula_lat,
    COALESCE(s.cuadricula_lon, b.cuadricula_lon) AS cuadricula_lon,
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
    GREATEST(b.fecha_ultimo_sismo, s.fecha_ultimo_sismo) AS fecha_ultimo_sismo
FROM gold_zonas_batch b
FULL OUTER JOIN gold_zonas_streaming s 
    ON b.cuadricula_lat = s.cuadricula_lat AND b.cuadricula_lon = s.cuadricula_lon;

-- ---------------------------------------------------------------------
-- 3. DATOS DE PRUEBA (MOCK DATA) PARA VALIDAR Y DESARROLLAR
-- ---------------------------------------------------------------------

-- Resumen Diario Batch (Histórico)
INSERT INTO gold_resumen_batch (fecha_evento, pais_estado, total_sismos, magnitud_maxima, magnitud_promedio, profundidad_promedio_km, total_alertas_tsunami, sismos_peligrosos_m5)
VALUES 
    (CURRENT_DATE - INTERVAL '1 day', 'California', 45, 3.85, 1.72, 7.80, 0, 0),
    (CURRENT_DATE - INTERVAL '1 day', 'Chile', 14, 5.60, 4.10, 32.40, 1, 1),
    (CURRENT_DATE - INTERVAL '1 day', 'Japan', 22, 6.20, 4.80, 45.10, 1, 2),
    (CURRENT_DATE - INTERVAL '2 days', 'Alaska', 58, 4.10, 2.05, 14.20, 0, 0),
    (CURRENT_DATE - INTERVAL '2 days', 'Indonesia', 19, 5.90, 4.50, 28.90, 0, 1)
ON CONFLICT DO NOTHING;

-- Resumen Diario Streaming (Lo de hoy en vivo)
INSERT INTO gold_resumen_streaming (fecha_evento, pais_estado, total_sismos, magnitud_maxima, magnitud_promedio, profundidad_promedio_km, total_alertas_tsunami, sismos_peligrosos_m5)
VALUES 
    (CURRENT_DATE, 'California', 12, 4.10, 1.95, 8.20, 0, 0),
    (CURRENT_DATE, 'Chile', 3, 5.40, 4.20, 29.50, 0, 1),
    (CURRENT_DATE, 'Hawaii', 8, 3.20, 2.10, 5.10, 0, 0)
ON CONFLICT DO NOTHING;

-- Zonas Críticas Batch (Histórico)
INSERT INTO gold_zonas_batch (cuadricula_lat, cuadricula_lon, zona_referencia, total_sismos_registrados, sismos_graves_m5, magnitud_maxima, profundidad_promedio_km, nivel_alerta, fecha_primer_sismo, fecha_ultimo_sismo)
VALUES 
    (-33.1, -71.5, 'Costa Central de Chile', 21, 2, 6.20, 31.80, 'CRITICO', CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '1 day'),
    (38.8, -122.8, 'Falla San Andrés - The Geysers, CA', 78, 0, 4.10, 6.50, 'MODERADO', CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '1 day'),
    (35.7, 139.8, 'Cinturón de Tokio, Japón', 28, 2, 6.40, 44.50, 'CRITICO', CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '1 day'),
    (61.2, -149.9, 'Sur de Alaska', 95, 0, 3.90, 15.00, 'NORMAL', CURRENT_TIMESTAMP - INTERVAL '30 days', CURRENT_TIMESTAMP - INTERVAL '1 day')
ON CONFLICT DO NOTHING;

-- Zonas Críticas Streaming (Lo detectado hoy)
INSERT INTO gold_zonas_streaming (cuadricula_lat, cuadricula_lon, zona_referencia, total_sismos_registrados, sismos_graves_m5, magnitud_maxima, profundidad_promedio_km, nivel_alerta, fecha_primer_sismo, fecha_ultimo_sismo)
VALUES 
    (-33.1, -71.5, 'Costa Central de Chile', 3, 1, 5.40, 30.00, 'CRITICO', CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_TIMESTAMP),
    (38.8, -122.8, 'Falla San Andrés - The Geysers, CA', 7, 0, 3.20, 6.20, 'NORMAL', CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;
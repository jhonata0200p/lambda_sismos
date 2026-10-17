-- 1. Crear tabla para Streaming (Datos en vivo de hoy)
CREATE TABLE IF NOT EXISTS fact_sismos_streaming_hoy (
    id_evento VARCHAR(50) PRIMARY KEY,
    fecha_hora_utc TIMESTAMP NOT NULL,
    fecha_evento DATE NOT NULL,
    hora_evento INT NOT NULL,
    magnitud NUMERIC(4, 2) NOT NULL,
    clasificacion_intensidad VARCHAR(20) NOT NULL,
    profundidad_km NUMERIC(8, 2) NOT NULL,
    significancia INT NOT NULL,
    alerta_tsunami BOOLEAN NOT NULL,
    latitud NUMERIC(9, 6) NOT NULL,
    longitud NUMERIC(9, 6) NOT NULL,
    pais_estado VARCHAR(100),
    ubicacion_completa TEXT,
    fecha_ingesta_sistema TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Crear tabla para Batch (Histórico consolidado hasta ayer)
CREATE TABLE IF NOT EXISTS fact_sismos_batch_historico (
    id_evento VARCHAR(50) PRIMARY KEY,
    fecha_hora_utc TIMESTAMP NOT NULL,
    fecha_evento DATE NOT NULL,
    hora_evento INT NOT NULL,
    magnitud NUMERIC(4, 2) NOT NULL,
    clasificacion_intensidad VARCHAR(20) NOT NULL,
    profundidad_km NUMERIC(8, 2) NOT NULL,
    significancia INT NOT NULL,
    alerta_tsunami BOOLEAN NOT NULL,
    latitud NUMERIC(9, 6) NOT NULL,
    longitud NUMERIC(9, 6) NOT NULL,
    pais_estado VARCHAR(100),
    ubicacion_completa TEXT,
    fecha_corte_batch DATE
);

-- 3. Crear índices para búsquedas rápidas
CREATE INDEX IF NOT EXISTS idx_streaming_fecha ON fact_sismos_streaming_hoy(fecha_evento);
CREATE INDEX IF NOT EXISTS idx_batch_fecha ON fact_sismos_batch_historico(fecha_evento);

-- 4. Crear la Vista Unificada para Power BI y la Web (Cero duplicados)
CREATE OR REPLACE VIEW vista_sismos_unificados AS
SELECT 
    id_evento, fecha_hora_utc, fecha_evento, hora_evento, magnitud, 
    clasificacion_intensidad, profundidad_km, significancia, alerta_tsunami, 
    latitud, longitud, pais_estado, ubicacion_completa, 'BATCH' AS origen_dato
FROM fact_sismos_batch_historico

UNION ALL

SELECT 
    s.id_evento, s.fecha_hora_utc, s.fecha_evento, s.hora_evento, s.magnitud, 
    s.clasificacion_intensidad, s.profundidad_km, s.significancia, s.alerta_tsunami, 
    s.latitud, s.longitud, s.pais_estado, s.ubicacion_completa, 'STREAMING' AS origen_dato
FROM fact_sismos_streaming_hoy s
WHERE NOT EXISTS (
    SELECT 1 
    FROM fact_sismos_batch_historico b 
    WHERE b.id_evento = s.id_evento
);
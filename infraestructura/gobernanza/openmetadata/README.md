# Gobernanza y Catalogo Empresarial - OpenMetadata 2.0.6

## 1. Arquitectura de Gobernanza Dual (Atlas + OpenMetadata)

En este proyecto de monitoreo sismico global se implementa una estrategia de **Gobernanza Dual Complementaria**:

| Caracteristica | Apache Atlas 2.3.0 | OpenMetadata 2.0.6 |
| :--- | :--- | :--- |
| **Enfoque Principal** | Gobernanza tecnica y linaje automatico de runtime | Catalogo de datos empresarial y gobernanza colaborativa |
| **Integracion** | Hook Spark directo (`atlas_hook.py`) | API REST v1, Ingestion y Glosario centralizado |
| **Visualizacion** | Grafo de dependencias de procesos y datasets | Catalogo de activos, busqueda semantica, contratos y calidad |
| **Glosario** | Terminos tecnicos (`Glosario_Sismico_Global`) | Glosario de negocio enriquecido con terminos y tags |
| **Puerto Web** | `http://localhost:21000` | `http://localhost:8585` |

---

## 2. Servicios del Stack OpenMetadata

El stack de OpenMetadata esta completamente optimizado para operar con límites estrictos de memoria JVM y contenedores Docker:

- **openmetadata_postgresql (5433:5432):** Base de datos relacional de metadatos (Collate PostgreSQL 2.0.6).
- **openmetadata_elasticsearch (9200):** Motor de indexacion y busqueda de activos (Elasticsearch 9.3.0).
- **openmetadata_execute_migrate_all:** Contenedor efimero de migraciones iniciales Flyway y definicion de indices.
- **openmetadata_server (8585 / 8586):** Servidor central Java Dropwizard con interfaz web React y API REST v1.
- **openmetadata_ingestion (8587:8080):** Motor de ingesta Airflow (opcional bajo perfil `ingestion`).

---

## 3. Despliegue y Arranque

### Paso 1: Configurar Variables de Entorno
```bash
cp .env.example .env
```

### Paso 2: Iniciar Stack Base
```bash
docker compose up -d
```
El contenedor `execute-migrate-all` ejecutara las migraciones y finalizara con codigo 0, tras lo cual `openmetadata-server` pasara al estado `(healthy)`.

### Paso 3: Ejecutar Catalogo Automatico
```bash
python catalogar_servicios.py
```

---

## 4. Activos Catalogados en el Cluster (Red 10.175.132.X)

El script `catalogar_servicios.py` registra de forma idempotente:

1. **Glosario de Negocio (`Glosario_Sismico_Global`):**
   - `Sismo_Critico_M5` (Magnitud >= 5.0)
   - `Alerta_Tsunami` (tsunami = 1)
   - `Cuadricula_Hotspot` (Resolucion ~11 km)
   - `Medallion_Bronze` (Datos crudos JSON)
   - `Medallion_Silver` (Datos limpios Parquet)
   - `Medallion_Gold` (Modelos analiticos)
   - `Serving_Layer_Lambda` (Capa de servicio sin duplicados)

2. **Servicio PostgreSQL Serving Layer (`10.175.132.248:5432/sismos_db`):**
   - `gold_resumen_batch` (Historico Spark Batch - Kevin)
   - `gold_resumen_streaming` (Delta Spark Streaming - Josue)
   - `gold_zonas_batch` (Hotspots historicos)
   - `gold_zonas_streaming` (Hotspots tiempo real)
   - `vista_resumen_diario_pais` (Vista Lambda unificada)
   - `vista_zonas_criticas_frecuentes` (Vista Lambda ponderada)

3. **Servicio Apache Kafka (`10.175.132.5:9092`):**
   - Topico central: `sismos_crudos` (1 particion, factor de replicacion 1)

4. **Servicio MinIO Data Lake (`10.175.132.248:9000`):**
   - Buckets Medallion: `bronze`, `silver`, `gold`

5. **Linaje End-to-End Conectado:**
   - `sismos_crudos` -> `gold_resumen_streaming` -> `vista_resumen_diario_pais`
   - `gold_resumen_batch` -> `vista_resumen_diario_pais`
   - `sismos_crudos` -> `gold_zonas_streaming` -> `vista_zonas_criticas_frecuentes`
   - `gold_zonas_batch` -> `vista_zonas_criticas_frecuentes`

---

## 5. Acceso a la Plataforma

- **URL:** [http://localhost:8585](http://localhost:8585)
- **Usuario:** `admin@open-metadata.org`
- **Contrasena:** `admin`

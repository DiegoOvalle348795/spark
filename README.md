# Proyecto Final 4.28 — Análisis de Patrones de Movilidad Urbana con Apache Spark

Este proyecto implementa un pipeline de Big Data para analizar patrones de movilidad urbana usando:

- Apache Spark en cluster Docker con 3 nodos mínimos: 1 master + 2 workers.
- Apache Airflow para orquestar el pipeline.
- MongoDB para almacenar tablas analíticas.
- Streamlit para visualizar resultados.

## Arquitectura

```text
Dataset CSV
   ↓
Airflow DAG
   ↓
Spark cluster Docker: master + worker 1 + worker 2
   ↓
Limpieza, transformación, joins con zonas y agregaciones
   ↓
Archivos JSON procesados
   ↓
Carga a MongoDB
   ↓
Dashboard Streamlit
```

## Estructura

```text
proyecto_movilidad_spark/
├── airflow/dags/mobility_pipeline_dag.py
├── data/raw/trips.csv
├── data/raw/zones.csv
├── data/processed/
├── docs/reporte_borrador.md
├── jobs/mobility_spark_job.py
├── scripts/generate_sample_data.py
├── streamlit/app.py
├── Dockerfile.airflow
├── docker-compose.yml
└── requirements.txt
```

## Requisitos

- Docker Desktop instalado y abierto.
- Docker Compose.
- Python local opcional para generar datos de prueba.

## Paso 1: Generar dataset de prueba

Desde la carpeta del proyecto:

```bash
python3 scripts/generate_sample_data.py
```

Esto crea:

- `data/raw/trips.csv`
- `data/raw/zones.csv`

Si usas el dataset real de NYC Taxi & Limousine, coloca tu archivo en `data/raw/trips.csv` con las columnas esperadas por el proyecto.

## Paso 2: Levantar el cluster y servicios

```bash
docker compose up --build
```

Servicios importantes:

- Spark Master: http://localhost:8080
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082
- Airflow: http://localhost:8088
- Mongo Express: http://localhost:8085
- Streamlit: http://localhost:8501

Usuario de Airflow:

```text
usuario: admin
contraseña: admin
```

## Paso 3: Ejecutar pipeline en Airflow

1. Entra a `http://localhost:8088`.
2. Activa el DAG `urban_mobility_spark_pipeline`.
3. Dale click en `Trigger DAG`.
4. Espera a que terminen las tareas:
   - `run_spark_analysis`
   - `load_results_to_mongodb`

## Paso 4: Ver resultados

Entra a Streamlit:

```text
http://localhost:8501
```

Ahí verás:

- Viajes limpios.
- Duración promedio.
- Distancia promedio.
- Ingreso total.
- Demanda por hora.
- Demanda por día.
- Top zonas de origen y destino.
- Ingresos por tipo de pago.
- Rutas origen-destino más frecuentes.

## Cómo comprobar que el cluster tiene al menos 3 nodos

Entra a:

```text
http://localhost:8080
```

Deberías ver:

- `spark-master`
- `spark-worker-1`
- `spark-worker-2`

Esto cumple con la condición de presentar el proyecto en un cluster de nodos en Docker.

## Colecciones creadas en MongoDB

Base de datos: `urban_mobility`

Colecciones:

- `summary`
- `demand_by_hour`
- `demand_by_day`
- `top_pickup_zones`
- `top_dropoff_zones`
- `revenue_by_payment`
- `route_patterns`

## Detener servicios

```bash
docker compose down
```

Para borrar también volúmenes:

```bash
docker compose down -v
```

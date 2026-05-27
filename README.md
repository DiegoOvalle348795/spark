# Proyecto Final 4.28 — Análisis de Patrones de Movilidad Urbana con Apache Spark

Pipeline de Big Data para analizar **patrones de movilidad urbana en Nueva York** usando el dataset oficial de **NYC Taxi & Limousine Commission (TLC)**.

## Tecnologías

- **Apache Spark** — cluster Docker (1 master + 2 workers)
- **Apache Airflow** — orquestación del pipeline
- **MongoDB** — almacenamiento de tablas analíticas
- **Streamlit** — dashboard de visualización

## Dataset oficial TLC

| Archivo | URL |
|---------|-----|
| Yellow Taxi Trip Records (enero 2025) | [yellow_tripdata_2025-01.parquet](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet) |
| Taxi Zone Lookup Table | [taxi_zone_lookup.csv](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) |

Página oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Los archivos se guardan en `data/raw/tlc/`. **No hace falta descargarlos a mano**: el DAG de Airflow los descarga automáticamente si no existen.

## Arquitectura del pipeline

```text
NYC TLC (Parquet + CSV de zonas)
        ↓
  download_tlc_dataset   (Airflow)
        ↓
  run_spark_analysis     (Spark cluster: limpieza, joins, agregaciones)
        ↓
  data/processed/        (CSV agregados)
        ↓
  load_results_to_mongodb
        ↓
  Dashboard Streamlit
```

## Estructura del proyecto

```text
spark/
├── airflow/dags/mobility_pipeline_dag.py
├── data/
│   ├── raw/tlc/                    # Dataset oficial (descargado por el DAG)
│   └── processed/                  # Resultados de Spark (generados)
├── jobs/mobility_spark_job.py
├── scripts/
│   ├── download_tlc_data.py
│   └── load_results_to_mongodb.py
├── streamlit/app.py
├── Dockerfile.airflow
├── docker-compose.yml
└── README.md
```

## Requisitos

- Docker Desktop instalado y en ejecución
- Docker Compose v2
- Conexión a internet (para la primera descarga del Parquet TLC, ~50–100 MB)

## Paso 1: Levantar el cluster y servicios

```bash
docker compose up --build
```

Espera a que todos los contenedores estén en estado `Up` (especialmente `airflow-webserver`).

### URLs de acceso

| Servicio | URL |
|----------|-----|
| **Airflow** | http://localhost:8088 |
| **Streamlit** | http://localhost:8501 |
| **Spark Master UI** | http://localhost:8080 |
| Spark Worker 1 | http://localhost:8081 |
| Spark Worker 2 | http://localhost:8082 |
| Mongo Express | http://localhost:8085 |

### Credenciales Airflow

```text
Usuario:    admin
Contraseña: admin
```

## Paso 2: Ejecutar el pipeline en Airflow

1. Abre http://localhost:8088 e inicia sesión.
2. Activa el DAG **`urban_mobility_spark_pipeline`**.
3. Haz clic en **Trigger DAG** (▶).
4. Espera a que las tres tareas terminen en verde:
   - `download_tlc_dataset` — descarga Parquet y CSV de zonas (omite si ya existen)
   - `run_spark_analysis` — procesamiento Spark en el cluster
   - `load_results_to_mongodb` — carga resultados a MongoDB

> La primera ejecución puede tardar varios minutos por la descarga del Parquet y el volumen de datos de enero 2025.

### Descarga manual opcional (sin Airflow)

```bash
python3 scripts/download_tlc_data.py
```

## Paso 3: Ver resultados en Streamlit

Abre http://localhost:8501

El dashboard muestra:

- Demanda por hora del día
- Demanda por día de la semana
- Top 10 zonas de origen y destino
- Ingresos por tipo de pago
- Análisis de viajes al aeropuerto

## Cluster Spark (3 nodos)

En http://localhost:8080 deberías ver:

- `spark-master`
- `spark-worker-1`
- `spark-worker-2`

## Tablas analíticas generadas

Spark escribe CSV en `data/processed/` y MongoDB los expone en la base **`urban_mobility`**:

| Colección | Contenido |
|-----------|-----------|
| `demand_by_hour` | Viajes, duración, distancia, tarifa e ingresos por hora |
| `demand_by_day` | Métricas por día de la semana |
| `top_pickup_zones` | Top 10 zonas de origen |
| `top_dropoff_zones` | Top 10 zonas de destino |
| `payment_analysis` | Análisis por tipo de pago |
| `airport_analysis` | Viajes con zonas que contienen "Airport" |

## Limpieza de datos aplicada en Spark

Se eliminan registros con:

- Fechas de pickup/dropoff nulas
- `trip_distance` ≤ 0
- `fare_amount` < 0 o `total_amount` < 0
- `trip_duration_min` ≤ 0 o > 240 minutos
- `passenger_count` ≤ 0

## Detener servicios

```bash
docker compose down
```

Para eliminar también volúmenes (MongoDB y Postgres de Airflow):

```bash
docker compose down -v
```

## Referencias

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

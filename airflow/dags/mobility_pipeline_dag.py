"""
DAG de Airflow: pipeline de movilidad urbana con dataset oficial NYC TLC.

Etapas:
  1. download_tlc_dataset  -> descarga Parquet + CSV de zonas (si no existen)
  2. run_spark_analysis    -> procesamiento distribuido en cluster Spark
  3. load_results_to_mongodb -> carga tablas agregadas a MongoDB
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Rutas dentro del contenedor Airflow
SCRIPTS_DIR = Path("/opt/airflow/scripts")
DATA_RAW_TLC = Path("/opt/airflow/data/raw/tlc")


def download_tlc_dataset() -> None:
    """
    Descarga yellow_tripdata_2025-01.parquet y taxi_zone_lookup.csv
    solo si aún no están en data/raw/tlc/.
    """
    script = SCRIPTS_DIR / "download_tlc_data.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    parquet = DATA_RAW_TLC / "yellow_tripdata_2025-01.parquet"
    zones = DATA_RAW_TLC / "taxi_zone_lookup.csv"
    if not parquet.exists() or not zones.exists():
        raise FileNotFoundError(
            "No se encontraron los archivos TLC en data/raw/tlc/. "
            "Revisa la conexión a internet y vuelve a ejecutar el DAG."
        )


def load_results_to_mongodb() -> None:
    """Carga los CSV procesados por Spark hacia MongoDB."""
    script = SCRIPTS_DIR / "load_results_to_mongodb.py"
    result = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


with DAG(
    dag_id="urban_mobility_spark_pipeline",
    description=(
        "Pipeline NYC TLC: descarga oficial -> Spark cluster -> MongoDB -> Streamlit"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "big-data", "mobility", "tlc", "mongodb"],
) as dag:

    download_tlc_dataset_task = PythonOperator(
        task_id="download_tlc_dataset",
        python_callable=download_tlc_dataset,
    )

    run_spark_analysis = BashOperator(
        task_id="run_spark_analysis",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.executor.memory=2g "
            "--conf spark.driver.memory=2g "
            "--conf spark.sql.shuffle.partitions=8 "
            "/opt/airflow/jobs/mobility_spark_job.py"
        ),
    )

    load_to_mongo = PythonOperator(
        task_id="load_results_to_mongodb",
        python_callable=load_results_to_mongodb,
    )

    download_tlc_dataset_task >> run_spark_analysis >> load_to_mongo

from __future__ import annotations

import glob
import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

OUTPUT_BASE = "/opt/airflow/data/processed"
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "urban_mobility"

COLLECTIONS = [
    "demand_by_hour",
    "demand_by_day",
    "top_pickup_zones",
    "top_dropoff_zones",
    "revenue_by_payment",
    "route_patterns",
    "summary",
]


def load_json_folder_to_mongo(collection_name: str):
    folder = os.path.join(OUTPUT_BASE, collection_name)
    files = glob.glob(os.path.join(folder, "part-*.json"))

    docs = []
    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as file:
            for line in file:
                line = line.strip()
                if line:
                    docs.append(json.loads(line))

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[collection_name]
    collection.delete_many({})

    if docs:
        collection.insert_many(docs)

    client.close()
    print(f"Colección {collection_name}: {len(docs)} documentos cargados en MongoDB")


def load_all_results_to_mongo():
    for collection_name in COLLECTIONS:
        load_json_folder_to_mongo(collection_name)


with DAG(
    dag_id="urban_mobility_spark_pipeline",
    description="Pipeline de movilidad urbana: Spark cluster + MongoDB + Streamlit",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "big-data", "mobility", "mongodb"],
) as dag:

    run_spark_analysis = BashOperator(
        task_id="run_spark_analysis",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.executor.memory=512m "
            "--conf spark.driver.memory=512m "
            "/opt/airflow/jobs/mobility_spark_job.py"
        ),
    )

    load_to_mongo = PythonOperator(
        task_id="load_results_to_mongodb",
        python_callable=load_all_results_to_mongo,
    )

    run_spark_analysis >> load_to_mongo

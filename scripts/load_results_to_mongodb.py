#!/usr/bin/env python3
"""
Carga las tablas analíticas generadas por Spark (CSV en data/processed/)
hacia MongoDB para el dashboard de Streamlit.
"""

from __future__ import annotations

import csv
import glob
import os
import sys
from typing import Any

from pymongo import MongoClient

OUTPUT_BASE = os.getenv("PROCESSED_DATA_DIR", "/opt/airflow/data/processed")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "urban_mobility")

# Colecciones esperadas por el dashboard
COLLECTIONS = [
    "demand_by_hour",
    "demand_by_day",
    "top_pickup_zones",
    "top_dropoff_zones",
    "payment_analysis",
    "airport_analysis",
]


def _cast_value(value: str) -> Any:
    """Convierte strings numéricos a int/float cuando aplica."""
    if value == "" or value is None:
        return None
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def read_csv_folder(folder: str) -> list[dict[str, Any]]:
    """Lee archivos part-*.csv exportados por Spark."""
    pattern = os.path.join(folder, "part-*.csv")
    files = glob.glob(pattern)
    if not files:
        return []

    docs: list[dict[str, Any]] = []
    for file_path in files:
        with open(file_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                docs.append({k: _cast_value(v) for k, v in row.items()})
    return docs


def load_collection(db, collection_name: str) -> int:
    folder = os.path.join(OUTPUT_BASE, collection_name)
    docs = read_csv_folder(folder)

    collection = db[collection_name]
    collection.delete_many({})
    if docs:
        collection.insert_many(docs)

    print(f"Colección '{collection_name}': {len(docs)} documentos")
    return len(docs)


def load_all_results() -> None:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    total = 0
    for name in COLLECTIONS:
        total += load_collection(db, name)

    client.close()
    print(f"\nCarga completada: {total} documentos en total.")


def main() -> int:
    load_all_results()
    return 0


if __name__ == "__main__":
    sys.exit(main())

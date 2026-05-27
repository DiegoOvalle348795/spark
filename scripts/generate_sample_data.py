#!/usr/bin/env python3
"""
AVISO: Este script generaba datos simulados para pruebas locales.

El proyecto ahora usa el dataset oficial NYC TLC, descargado automáticamente por:
  - El DAG de Airflow (tarea download_tlc_dataset), o
  - scripts/download_tlc_data.py

Ejecuta uno de esos métodos en lugar de este script.
"""

import sys

print(
    "Este proyecto ya no usa datos simulados.\n\n"
    "Usa el dataset oficial TLC:\n"
    "  python3 scripts/download_tlc_data.py\n\n"
    "O ejecuta el DAG 'urban_mobility_spark_pipeline' en Airflow.\n"
)
sys.exit(0)

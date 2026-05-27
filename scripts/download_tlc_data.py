#!/usr/bin/env python3
"""
Descarga el dataset oficial NYC TLC Trip Record Data si aún no existe localmente.

Fuentes:
- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

# Ruta base dentro de Docker (/opt/airflow/data) o en local (./data)
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_TLC_DIR = BASE_DIR / "data" / "raw" / "tlc"

TLC_FILES = {
    "yellow_tripdata_2025-01.parquet": (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
    ),
    "taxi_zone_lookup.csv": (
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    ),
}


def download_file(url: str, destination: Path) -> None:
    """Descarga un archivo solo si no existe o está vacío."""
    if destination.exists() and destination.stat().st_size > 0:
        print(f"[OK] Ya existe: {destination} ({destination.stat().st_size:,} bytes)")
        return

    print(f"[DESCARGA] {url}")
    print(f"           -> {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)

    def progress(block_num: int, block_size: int, total_size: int) -> None:
        if total_size > 0 and block_num % 50 == 0:
            pct = min(block_num * block_size / total_size * 100, 100)
            print(f"           Progreso: {pct:.1f}%", end="\r")

    urllib.request.urlretrieve(url, destination, reporthook=progress)
    print(f"\n[OK] Descargado: {destination} ({destination.stat().st_size:,} bytes)")


def main() -> int:
    RAW_TLC_DIR.mkdir(parents=True, exist_ok=True)

    for filename, url in TLC_FILES.items():
        download_file(url, RAW_TLC_DIR / filename)

    print("\nDataset TLC listo en:", RAW_TLC_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())

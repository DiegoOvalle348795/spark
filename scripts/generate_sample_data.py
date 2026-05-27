import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
RAW_DIR = BASE / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

zones = [
    {"zone_id": 1, "zone_name": "Centro", "min_longitude": -74.015, "max_longitude": -73.990, "min_latitude": 40.700, "max_latitude": 40.725},
    {"zone_id": 2, "zone_name": "Norte", "min_longitude": -73.990, "max_longitude": -73.950, "min_latitude": 40.760, "max_latitude": 40.810},
    {"zone_id": 3, "zone_name": "Aeropuerto", "min_longitude": -73.810, "max_longitude": -73.750, "min_latitude": 40.630, "max_latitude": 40.670},
    {"zone_id": 4, "zone_name": "Sur", "min_longitude": -74.020, "max_longitude": -73.960, "min_latitude": 40.620, "max_latitude": 40.690},
    {"zone_id": 5, "zone_name": "Zona Universitaria", "min_longitude": -73.970, "max_longitude": -73.930, "min_latitude": 40.725, "max_latitude": 40.760},
    {"zone_id": 6, "zone_name": "Distrito Financiero", "min_longitude": -74.020, "max_longitude": -73.990, "min_latitude": 40.690, "max_latitude": 40.710},
]

payment_types = ["efectivo", "tarjeta", "app"]

def point_in_zone(zone):
    lon = random.uniform(zone["min_longitude"], zone["max_longitude"])
    lat = random.uniform(zone["min_latitude"], zone["max_latitude"])
    return round(lon, 6), round(lat, 6)

with open(RAW_DIR / "zones.csv", "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=list(zones[0].keys()))
    writer.writeheader()
    writer.writerows(zones)

random.seed(222)
start = datetime(2026, 4, 1, 0, 0, 0)
rows = []

for i in range(1, 5001):
    # Más viajes en horas pico para que el análisis se vea claro
    hour = random.choices(
        population=list(range(24)),
        weights=[1,1,1,1,1,2,5,8,10,6,4,4,5,5,5,6,8,11,10,7,5,3,2,1],
        k=1,
    )[0]
    day_offset = random.randint(0, 29)
    minute = random.randint(0, 59)
    pickup_dt = start + timedelta(days=day_offset, hours=hour, minutes=minute)

    pickup_zone = random.choices(zones, weights=[8, 5, 4, 3, 5, 7], k=1)[0]
    dropoff_zone = random.choices(zones, weights=[6, 5, 5, 3, 5, 6], k=1)[0]

    p_lon, p_lat = point_in_zone(pickup_zone)
    d_lon, d_lat = point_in_zone(dropoff_zone)

    base_distance = random.uniform(1.2, 9.5)
    if pickup_zone["zone_name"] == "Aeropuerto" or dropoff_zone["zone_name"] == "Aeropuerto":
        base_distance += random.uniform(6, 18)

    duration = base_distance * random.uniform(3.0, 5.5) + random.uniform(2, 12)
    dropoff_dt = pickup_dt + timedelta(minutes=duration)
    fare = 20 + base_distance * random.uniform(7.5, 12.0) + duration * random.uniform(0.8, 1.6)

    rows.append({
        "trip_id": i,
        "pickup_datetime": pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_datetime": dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "pickup_longitude": p_lon,
        "pickup_latitude": p_lat,
        "dropoff_longitude": d_lon,
        "dropoff_latitude": d_lat,
        "passenger_count": random.randint(1, 4),
        "trip_distance_km": round(base_distance, 2),
        "fare_amount": round(fare, 2),
        "payment_type": random.choices(payment_types, weights=[3, 5, 4], k=1)[0],
        "driver_id": random.randint(100, 180),
    })

with open(RAW_DIR / "trips.csv", "w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

print(f"Listo: {len(rows)} viajes generados en {RAW_DIR / 'trips.csv'}")
print(f"Listo: {len(zones)} zonas generadas en {RAW_DIR / 'zones.csv'}")

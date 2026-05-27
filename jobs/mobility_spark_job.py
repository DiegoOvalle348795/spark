"""
Job PySpark: análisis de movilidad urbana con dataset oficial NYC TLC.

Dataset:
- Yellow Taxi Trip Records (Parquet): yellow_tripdata_2025-01.parquet
- Taxi Zone Lookup (CSV): taxi_zone_lookup.csv

Ejecución en cluster:
  spark-submit --master spark://spark-master:7077 /opt/airflow/jobs/mobility_spark_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    dayofweek,
    desc,
    hour,
    lit,
    monotonically_increasing_id,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    unix_timestamp,
    when,
)

# Rutas dentro del contenedor (volumen compartido ./data -> /opt/airflow/data)
TRIPS_PARQUET = "/opt/airflow/data/raw/tlc/yellow_tripdata_2025-01.parquet"
ZONES_CSV = "/opt/airflow/data/raw/tlc/taxi_zone_lookup.csv"
OUTPUT_BASE = "/opt/airflow/data/processed"


def write_csv(df, name: str) -> None:
    """Exporta una tabla agregada a CSV (un solo archivo part-*)."""
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(f"{OUTPUT_BASE}/{name}")
    )


def payment_type_label():
    """Etiquetas legibles para el código payment_type del TLC."""
    return (
        when(col("payment_type") == 1, lit("Tarjeta de crédito"))
        .when(col("payment_type") == 2, lit("Efectivo"))
        .when(col("payment_type") == 3, lit("Sin cargo"))
        .when(col("payment_type") == 4, lit("Disputa"))
        .when(col("payment_type") == 5, lit("Desconocido"))
        .when(col("payment_type") == 6, lit("Viaje anulado"))
        .otherwise(lit("Otro"))
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("NYC TLC Urban Mobility Analysis").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # --- Lectura del dataset oficial TLC ---
    trips_raw = spark.read.parquet(TRIPS_PARQUET)

    zones_raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(ZONES_CSV)
    )

    # Catálogo de zonas para origen y destino
    pickup_zones = zones_raw.select(
        col("LocationID").cast("int").alias("pickup_zone_id"),
        col("Zone").alias("pickup_zone"),
        col("Borough").alias("pickup_borough"),
        col("service_zone").alias("pickup_service_zone"),
    )

    dropoff_zones = zones_raw.select(
        col("LocationID").cast("int").alias("dropoff_zone_id"),
        col("Zone").alias("dropoff_zone"),
        col("Borough").alias("dropoff_borough"),
        col("service_zone").alias("dropoff_service_zone"),
    )

    # --- Columnas derivadas y limpieza ---
    trips = (
        trips_raw.withColumn("trip_id", monotonically_increasing_id())
        .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
        .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
        .withColumn(
            "trip_duration_min",
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime"))
            / 60.0,
        )
        .withColumn("hour_of_day", hour(col("pickup_datetime")))
        .withColumn("day_of_week", date_format(col("pickup_datetime"), "EEEE"))
        .withColumn("day_number", dayofweek(col("pickup_datetime")))
        .withColumn(
            "trip_distance_km", spark_round(col("trip_distance") * 1.60934, 4)
        )
        .withColumn("pickup_zone_id", col("PULocationID").cast("int"))
        .withColumn("dropoff_zone_id", col("DOLocationID").cast("int"))
        .withColumn("fare_amount", col("fare_amount").cast("double"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("payment_type_label", payment_type_label())
        # Filtros de calidad de datos
        .filter(col("pickup_datetime").isNotNull())
        .filter(col("dropoff_datetime").isNotNull())
        .filter(col("trip_distance") > 0)
        .filter(col("fare_amount") >= 0)
        .filter(col("total_amount") >= 0)
        .filter(col("trip_duration_min") > 0)
        .filter(col("trip_duration_min") <= 240)
        .filter(col("passenger_count") > 0)
    )

    # --- Join con catálogo de zonas TLC ---
    trips_zoned = (
        trips.join(pickup_zones, on="pickup_zone_id", how="left")
        .join(dropoff_zones, on="dropoff_zone_id", how="left")
        .fillna(
            {
                "pickup_zone": "Zona desconocida",
                "pickup_borough": "Desconocido",
                "pickup_service_zone": "Desconocido",
                "dropoff_zone": "Zona desconocida",
                "dropoff_borough": "Desconocido",
                "dropoff_service_zone": "Desconocido",
            }
        )
    )

    # --- Tablas analíticas ---

    # A) Demanda por hora del día
    demand_by_hour = (
        trips_zoned.groupBy("hour_of_day")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
            spark_round(avg("trip_distance_km"), 2).alias("avg_distance_km"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("hour_of_day")
    )

    # B) Demanda por día de la semana
    demand_by_day = (
        trips_zoned.groupBy("day_number", "day_of_week")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
            spark_round(avg("trip_distance_km"), 2).alias("avg_distance_km"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("day_number")
        .drop("day_number")
    )

    # C) Top zonas de origen (más viajes)
    top_pickup_zones = (
        trips_zoned.groupBy("pickup_borough", "pickup_zone")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy(desc("total_trips"))
        .limit(10)
    )

    # D) Top zonas de destino
    top_dropoff_zones = (
        trips_zoned.groupBy("dropoff_borough", "dropoff_zone")
        .agg(count("trip_id").alias("total_trips"))
        .orderBy(desc("total_trips"))
        .limit(10)
    )

    # E) Análisis por tipo de pago
    payment_analysis = (
        trips_zoned.groupBy("payment_type", "payment_type_label")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .withColumnRenamed("payment_type_label", "payment_type_name")
        .orderBy(desc("total_revenue"))
    )

    # F) Viajes relacionados con aeropuertos (zonas que contienen "Airport")
    airport_trips = trips_zoned.filter(
        col("pickup_zone").contains("Airport") | col("dropoff_zone").contains("Airport")
    )

    airport_analysis = airport_trips.agg(
        count("trip_id").alias("total_airport_trips"),
        spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
        spark_round(avg("trip_distance_km"), 2).alias("avg_distance_km"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
    )

    # --- Escritura de resultados para MongoDB / Streamlit ---
    write_csv(demand_by_hour, "demand_by_hour")
    write_csv(demand_by_day, "demand_by_day")
    write_csv(top_pickup_zones, "top_pickup_zones")
    write_csv(top_dropoff_zones, "top_dropoff_zones")
    write_csv(payment_analysis, "payment_analysis")
    write_csv(airport_analysis, "airport_analysis")

    total_trips = trips_zoned.count()
    print("=== PROCESAMIENTO TLC COMPLETADO ===")
    print(f"Viajes válidos después de limpieza: {total_trips:,}")
    print("\n--- Demanda por hora (muestra) ---")
    demand_by_hour.show(24, truncate=False)
    print("\n--- Top 10 zonas de origen ---")
    top_pickup_zones.show(10, truncate=False)
    print("\n--- Análisis aeropuerto ---")
    airport_analysis.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

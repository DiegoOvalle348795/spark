from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, unix_timestamp, hour, date_format, dayofweek,
    when, count, avg, sum as spark_sum, round as spark_round, desc, lit
)

TRIPS_PATH = "/opt/airflow/data/raw/trips.csv"
ZONES_PATH = "/opt/airflow/data/raw/zones.csv"
OUTPUT_BASE = "/opt/airflow/data/processed"


def write_json(df, name):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .json(f"{OUTPUT_BASE}/{name}")
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Urban Mobility Patterns")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    trips_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(TRIPS_PATH)
    )

    zones = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ZONES_PATH)
    )

    # 1) Limpieza y transformación
    trips = (
        trips_raw
        .withColumn("pickup_datetime", to_timestamp(col("pickup_datetime")))
        .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))
        .withColumn("trip_distance_km", col("trip_distance_km").cast("double"))
        .withColumn("fare_amount", col("fare_amount").cast("double"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .dropna(subset=["trip_id", "pickup_datetime", "dropoff_datetime", "trip_distance_km", "fare_amount"])
        .withColumn(
            "trip_duration_min",
            (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0
        )
        .withColumn("hour_of_day", hour(col("pickup_datetime")))
        .withColumn("day_number", dayofweek(col("pickup_datetime")))
        .withColumn("day_of_week", date_format(col("pickup_datetime"), "EEEE"))
        .withColumn(
            "day_type",
            when(col("day_number").isin(1, 7), lit("Fin de semana")).otherwise(lit("Laboral"))
        )
        .filter(col("trip_distance_km") > 0)
        .filter(col("fare_amount") > 0)
        .filter((col("trip_duration_min") > 1) & (col("trip_duration_min") <= 180))
        .filter(col("passenger_count") >= 1)
    )

    # 2) Enriquecimiento por zona de origen y destino usando límites geográficos
    pz = zones.select(
        col("zone_id").alias("pickup_zone_id"),
        col("zone_name").alias("pickup_zone"),
        col("min_longitude").alias("p_min_lon"),
        col("max_longitude").alias("p_max_lon"),
        col("min_latitude").alias("p_min_lat"),
        col("max_latitude").alias("p_max_lat"),
    )

    dz = zones.select(
        col("zone_id").alias("dropoff_zone_id"),
        col("zone_name").alias("dropoff_zone"),
        col("min_longitude").alias("d_min_lon"),
        col("max_longitude").alias("d_max_lon"),
        col("min_latitude").alias("d_min_lat"),
        col("max_latitude").alias("d_max_lat"),
    )

    trips_zoned = (
        trips
        .join(
            pz,
            (col("pickup_longitude").between(col("p_min_lon"), col("p_max_lon"))) &
            (col("pickup_latitude").between(col("p_min_lat"), col("p_max_lat"))),
            "left"
        )
        .join(
            dz,
            (col("dropoff_longitude").between(col("d_min_lon"), col("d_max_lon"))) &
            (col("dropoff_latitude").between(col("d_min_lat"), col("d_max_lat"))),
            "left"
        )
        .fillna({"pickup_zone": "Zona desconocida", "dropoff_zone": "Zona desconocida"})
        .drop("p_min_lon", "p_max_lon", "p_min_lat", "p_max_lat", "d_min_lon", "d_max_lon", "d_min_lat", "d_max_lat")
    )

    # 3) Tablas analíticas
    demand_by_hour = (
        trips_zoned.groupBy("hour_of_day")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
            spark_round(avg("trip_distance_km"), 2).alias("avg_distance_km"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_revenue")
        )
        .orderBy("hour_of_day")
    )

    demand_by_day = (
        trips_zoned.groupBy("day_number", "day_of_week", "day_type")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_revenue"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare")
        )
        .orderBy("day_number")
    )

    top_pickup_zones = (
        trips_zoned.groupBy("pickup_zone")
        .agg(
            count("trip_id").alias("total_pickups"),
            spark_round(spark_sum("fare_amount"), 2).alias("revenue_from_pickups"),
            spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min")
        )
        .orderBy(desc("total_pickups"))
        .limit(10)
    )

    top_dropoff_zones = (
        trips_zoned.groupBy("dropoff_zone")
        .agg(count("trip_id").alias("total_dropoffs"))
        .orderBy(desc("total_dropoffs"))
        .limit(10)
    )

    revenue_by_payment = (
        trips_zoned.groupBy("payment_type")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_revenue"),
            spark_round(avg("fare_amount"), 2).alias("avg_revenue_per_trip")
        )
        .orderBy(desc("total_revenue"))
    )

    route_patterns = (
        trips_zoned.groupBy("pickup_zone", "dropoff_zone")
        .agg(
            count("trip_id").alias("total_trips"),
            spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
            spark_round(avg("trip_distance_km"), 2).alias("avg_distance_km"),
            spark_round(spark_sum("fare_amount"), 2).alias("total_revenue")
        )
        .orderBy(desc("total_trips"))
        .limit(20)
    )

    summary = (
        trips_zoned.agg(
            count("trip_id").alias("total_clean_trips"),
            spark_round(avg("trip_duration_min"), 2).alias("global_avg_duration_min"),
            spark_round(avg("trip_distance_km"), 2).alias("global_avg_distance_km"),
            spark_round(spark_sum("fare_amount"), 2).alias("global_total_revenue")
        )
    )

    # 4) Escritura a disco para que Airflow cargue en MongoDB
    write_json(demand_by_hour, "demand_by_hour")
    write_json(demand_by_day, "demand_by_day")
    write_json(top_pickup_zones, "top_pickup_zones")
    write_json(top_dropoff_zones, "top_dropoff_zones")
    write_json(revenue_by_payment, "revenue_by_payment")
    write_json(route_patterns, "route_patterns")
    write_json(summary, "summary")

    print("=== RESUMEN GENERAL ===")
    summary.show(truncate=False)
    print("=== DEMANDA POR HORA ===")
    demand_by_hour.show(24, truncate=False)
    print("=== TOP ZONAS DE ORIGEN ===")
    top_pickup_zones.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

"""
Dashboard Streamlit: visualización de patrones de movilidad urbana NYC TLC.

Lee las colecciones agregadas en MongoDB (base urban_mobility) generadas por el pipeline Spark.
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "urban_mobility")

st.set_page_config(
    page_title="Movilidad Urbana NYC TLC",
    page_icon="🚕",
    layout="wide",
)

PAYMENT_LABELS = {
    1: "Tarjeta de crédito",
    2: "Efectivo",
    3: "Sin cargo",
    4: "Disputa",
    5: "Desconocido",
    6: "Viaje anulado",
}


@st.cache_resource
def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def read_collection(name: str) -> pd.DataFrame:
    db = get_db()
    data = list(db[name].find({}, {"_id": 0}))
    return pd.DataFrame(data)


def format_payment_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "payment_type_name" in df.columns:
        df["payment_type_display"] = df["payment_type_name"]
    elif "payment_type" in df.columns:
        df["payment_type_display"] = df["payment_type"].map(PAYMENT_LABELS).fillna(
            df["payment_type"].astype(str)
        )
    return df


st.title("Análisis de Patrones de Movilidad Urbana — NYC TLC")
st.caption(
    "Dataset oficial: Yellow Taxi Trip Records (enero 2025) · "
    "Apache Spark · Airflow · MongoDB · Streamlit"
)

hour_df = read_collection("demand_by_hour")

if hour_df.empty:
    st.warning(
        "No hay datos en MongoDB. Ejecuta el DAG `urban_mobility_spark_pipeline` en Airflow "
        "(http://localhost:8088) y espera a que terminen las tres tareas."
    )
    st.stop()

day_df = read_collection("demand_by_day")
pickup_df = read_collection("top_pickup_zones")
dropoff_df = read_collection("top_dropoff_zones")
payment_df = format_payment_df(read_collection("payment_analysis"))
airport_df = read_collection("airport_analysis")

# --- Resumen rápido ---
total_trips_hour = int(hour_df["total_trips"].sum()) if "total_trips" in hour_df else 0
total_revenue = float(hour_df["total_revenue"].sum()) if "total_revenue" in hour_df else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Viajes analizados (suma por hora)", f"{total_trips_hour:,}")
col2.metric("Ingresos totales (USD)", f"${total_revenue:,.2f}")

if not airport_df.empty:
    airport = airport_df.iloc[0]
    col3.metric("Viajes aeropuerto", f"{int(airport.get('total_airport_trips', 0)):,}")
    col4.metric("Ticket promedio aeropuerto", f"${airport.get('avg_total_amount', 0):,.2f}")
else:
    col3.metric("Viajes aeropuerto", "—")
    col4.metric("Ticket promedio aeropuerto", "—")

st.divider()

tab1, tab2, tab3, tab4 = st.tabs(
    ["Demanda temporal", "Zonas", "Pagos", "Aeropuerto"]
)

with tab1:
    st.subheader("Demanda por hora del día")
    fig_hour = px.line(
        hour_df.sort_values("hour_of_day"),
        x="hour_of_day",
        y="total_trips",
        markers=True,
        labels={"hour_of_day": "Hora", "total_trips": "Total de viajes"},
        title="Viajes por hora (pickup)",
    )
    st.plotly_chart(fig_hour, use_container_width=True)

    if "total_revenue" in hour_df.columns:
        fig_rev_hour = px.bar(
            hour_df.sort_values("hour_of_day"),
            x="hour_of_day",
            y="total_revenue",
            labels={"hour_of_day": "Hora", "total_revenue": "Ingresos (USD)"},
            title="Ingresos por hora",
        )
        st.plotly_chart(fig_rev_hour, use_container_width=True)

    st.subheader("Demanda por día de la semana")
    if not day_df.empty:
        day_order = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday",
        ]
        day_df["day_of_week"] = pd.Categorical(
            day_df["day_of_week"], categories=day_order, ordered=True
        )
        fig_day = px.bar(
            day_df.sort_values("day_of_week"),
            x="day_of_week",
            y="total_trips",
            labels={"day_of_week": "Día", "total_trips": "Total de viajes"},
            title="Viajes por día de la semana",
        )
        st.plotly_chart(fig_day, use_container_width=True)
        st.dataframe(day_df, use_container_width=True)

with tab2:
    left, right = st.columns(2)
    with left:
        st.subheader("Top 10 zonas de origen")
        if not pickup_df.empty:
            pickup_df["etiqueta"] = (
                pickup_df["pickup_borough"] + " · " + pickup_df["pickup_zone"]
            )
            fig_pickup = px.bar(
                pickup_df,
                x="total_trips",
                y="etiqueta",
                orientation="h",
                title="Zonas con más viajes de origen",
            )
            st.plotly_chart(fig_pickup, use_container_width=True)
        st.dataframe(pickup_df, use_container_width=True)

    with right:
        st.subheader("Top 10 zonas de destino")
        if not dropoff_df.empty:
            dropoff_df["etiqueta"] = (
                dropoff_df["dropoff_borough"] + " · " + dropoff_df["dropoff_zone"]
            )
            fig_dropoff = px.bar(
                dropoff_df,
                x="total_trips",
                y="etiqueta",
                orientation="h",
                title="Zonas con más viajes de destino",
            )
            st.plotly_chart(fig_dropoff, use_container_width=True)
        st.dataframe(dropoff_df, use_container_width=True)

with tab3:
    st.subheader("Ingresos por tipo de pago")
    if not payment_df.empty:
        label_col = (
            "payment_type_display"
            if "payment_type_display" in payment_df.columns
            else "payment_type"
        )
        fig_payment = px.pie(
            payment_df,
            values="total_revenue",
            names=label_col,
            title="Distribución de ingresos por forma de pago",
        )
        st.plotly_chart(fig_payment, use_container_width=True)

        fig_payment_bar = px.bar(
            payment_df,
            x=label_col,
            y="total_trips",
            color="total_revenue",
            labels={label_col: "Tipo de pago", "total_trips": "Viajes"},
            title="Viajes e ingresos por tipo de pago",
        )
        st.plotly_chart(fig_payment_bar, use_container_width=True)
    st.dataframe(payment_df, use_container_width=True)

with tab4:
    st.subheader("Análisis de viajes al aeropuerto")
    st.caption(
        "Incluye viajes cuya zona de origen o destino contiene la palabra 'Airport' "
        "en el catálogo oficial TLC."
    )
    if not airport_df.empty:
        airport = airport_df.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total viajes aeropuerto", f"{int(airport.get('total_airport_trips', 0)):,}")
        c2.metric("Duración promedio", f"{airport.get('avg_duration_min', 0)} min")
        c3.metric("Distancia promedio", f"{airport.get('avg_distance_km', 0)} km")
        c4.metric("Monto promedio", f"${airport.get('avg_total_amount', 0):,.2f}")
        st.dataframe(airport_df, use_container_width=True)
    else:
        st.info("No hay datos de aeropuerto. Ejecuta el pipeline completo en Airflow.")

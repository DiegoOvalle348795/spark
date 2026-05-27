import os

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "urban_mobility")

st.set_page_config(
    page_title="Movilidad Urbana con Spark",
    page_icon="🚕",
    layout="wide",
)

@st.cache_resource
def get_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


def read_collection(name: str) -> pd.DataFrame:
    db = get_db()
    data = list(db[name].find({}, {"_id": 0}))
    return pd.DataFrame(data)

st.title("Análisis de Patrones de Movilidad Urbana")
st.caption("Apache Spark + Docker Cluster + Airflow + MongoDB + Streamlit")

summary_df = read_collection("summary")

if summary_df.empty:
    st.warning("Todavía no hay datos en MongoDB. Ejecuta el DAG `urban_mobility_spark_pipeline` desde Airflow.")
    st.stop()

summary = summary_df.iloc[0].to_dict()
col1, col2, col3, col4 = st.columns(4)
col1.metric("Viajes limpios", f"{int(summary.get('total_clean_trips', 0)):,}")
col2.metric("Duración promedio", f"{summary.get('global_avg_duration_min', 0)} min")
col3.metric("Distancia promedio", f"{summary.get('global_avg_distance_km', 0)} km")
col4.metric("Ingreso total", f"${summary.get('global_total_revenue', 0):,.2f}")

st.divider()

hour_df = read_collection("demand_by_hour")
day_df = read_collection("demand_by_day")
pickup_df = read_collection("top_pickup_zones")
dropoff_df = read_collection("top_dropoff_zones")
payment_df = read_collection("revenue_by_payment")
routes_df = read_collection("route_patterns")

tab1, tab2, tab3, tab4 = st.tabs(["Demanda", "Zonas", "Ingresos", "Rutas"])

with tab1:
    st.subheader("Demanda por hora del día")
    fig_hour = px.line(hour_df, x="hour_of_day", y="total_trips", markers=True, title="Viajes por hora")
    st.plotly_chart(fig_hour, use_container_width=True)

    st.subheader("Demanda por día de la semana")
    fig_day = px.bar(day_df, x="day_of_week", y="total_trips", color="day_type", title="Viajes por día")
    st.plotly_chart(fig_day, use_container_width=True)

with tab2:
    left, right = st.columns(2)
    with left:
        st.subheader("Top zonas de origen")
        fig_pickup = px.bar(pickup_df, x="pickup_zone", y="total_pickups", title="Zonas que generan más viajes")
        st.plotly_chart(fig_pickup, use_container_width=True)
        st.dataframe(pickup_df, use_container_width=True)
    with right:
        st.subheader("Top zonas de destino")
        fig_dropoff = px.bar(dropoff_df, x="dropoff_zone", y="total_dropoffs", title="Zonas que reciben más viajes")
        st.plotly_chart(fig_dropoff, use_container_width=True)
        st.dataframe(dropoff_df, use_container_width=True)

with tab3:
    st.subheader("Ingresos por forma de pago")
    fig_payment = px.pie(payment_df, values="total_revenue", names="payment_type", title="Distribución de ingresos")
    st.plotly_chart(fig_payment, use_container_width=True)
    st.dataframe(payment_df, use_container_width=True)

with tab4:
    st.subheader("Rutas con mayor frecuencia")
    if not routes_df.empty:
        routes_df["route"] = routes_df["pickup_zone"] + " → " + routes_df["dropoff_zone"]
        fig_routes = px.bar(routes_df, x="route", y="total_trips", title="Top rutas origen-destino")
        st.plotly_chart(fig_routes, use_container_width=True)
    st.dataframe(routes_df, use_container_width=True)

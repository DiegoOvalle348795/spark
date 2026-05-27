# Análisis de Patrones de Movilidad Urbana con Apache Spark

## 1. Objetivos

El objetivo general del proyecto es aplicar Apache Spark para el procesamiento y análisis de datos de movilidad urbana dentro de un entorno Big Data. El sistema busca identificar patrones relevantes como horarios de mayor demanda, zonas con mayor actividad, duración promedio de viajes, rutas frecuentes e ingresos por forma de pago.

### Objetivos específicos

- Construir un pipeline de datos usando Apache Spark y PySpark.
- Ejecutar el procesamiento en un cluster de Docker con al menos tres nodos.
- Orquestar el flujo de trabajo con Apache Airflow.
- Limpiar, transformar y enriquecer datos de viajes urbanos.
- Generar tablas analíticas para apoyar la toma de decisiones.
- Almacenar resultados en MongoDB.
- Visualizar los indicadores principales mediante Streamlit.

## 2. Descripción del caso

Una empresa de transporte urbano tipo taxi necesita comprender mejor el comportamiento de sus viajes. Para ello se analizan datos de movilidad que incluyen fecha y hora de inicio y fin, coordenadas geográficas, cantidad de pasajeros, distancia recorrida, tarifa, forma de pago y conductor.

El análisis permite responder preguntas como:

- ¿En qué horarios hay mayor demanda de viajes?
- ¿Qué zonas generan y reciben más viajes?
- ¿Cuál es la duración promedio de los viajes por hora?
- ¿Qué forma de pago genera más ingresos?
- ¿Cuáles son las rutas más frecuentes?

## 3. Arquitectura del proyecto

La arquitectura está compuesta por una fuente de datos en archivos CSV, un cluster de Apache Spark en Docker, Airflow como orquestador, MongoDB como base de datos de resultados y Streamlit como capa de visualización.

El flujo general es:

```text
CSV → Airflow → Spark Cluster → JSON procesado → MongoDB → Streamlit
```

El cluster Spark incluye un nodo master y dos nodos worker, cumpliendo con el requisito mínimo de tres nodos.

## 4. Decisiones de limpieza y modelado

Durante la limpieza se eliminaron registros con valores nulos en campos clave como fechas, distancia y tarifa. También se filtraron viajes con distancia igual a cero, tarifas no válidas, duración menor a un minuto o mayor a tres horas y registros sin pasajeros.

Se crearon columnas derivadas:

- `trip_duration_min`: duración del viaje en minutos.
- `hour_of_day`: hora del día en la que inició el viaje.
- `day_of_week`: día de la semana.
- `day_type`: clasificación entre día laboral y fin de semana.

Para el enriquecimiento espacial se usó un archivo de zonas con límites de latitud y longitud. Con esto se asignaron campos de zona de origen y zona de destino a cada viaje.

## 5. Desarrollo del pipeline

El pipeline se ejecuta desde Airflow mediante un DAG llamado `urban_mobility_spark_pipeline`. Este DAG tiene dos tareas principales:

1. Ejecutar el análisis con Spark mediante `spark-submit`.
2. Cargar las tablas resultantes a MongoDB.

Spark procesa el dataset, genera agregaciones y guarda los resultados en formato JSON. Posteriormente, Airflow carga esos archivos JSON a colecciones de MongoDB.

## 6. Resultados principales

Las principales salidas del análisis son:

- Demanda por hora del día.
- Demanda por día de la semana.
- Top 10 zonas de origen.
- Top 10 zonas de destino.
- Ingresos por tipo de pago.
- Rutas origen-destino más frecuentes.
- Resumen general de viajes, duración promedio, distancia promedio e ingresos totales.

Estas tablas permiten detectar horas pico, zonas estratégicas y comportamientos de pago de los usuarios.

## 7. Almacenamiento y visualización

Los resultados se almacenan en MongoDB dentro de la base de datos `urban_mobility`. Cada tabla analítica se guarda como una colección independiente.

La visualización se realiza con Streamlit. El dashboard muestra métricas generales, gráficas de demanda, barras comparativas por zona, distribución de ingresos por forma de pago y rutas más frecuentes.

## 8. Conclusiones

El proyecto demuestra cómo Apache Spark puede utilizarse para analizar grandes volúmenes de datos de movilidad urbana. La arquitectura permite separar claramente las etapas de ingesta, limpieza, procesamiento, almacenamiento y visualización.

Los resultados obtenidos pueden ayudar a una empresa de transporte a mejorar la asignación de conductores, identificar zonas de alta demanda, ajustar estrategias de precios y tomar decisiones basadas en datos.

El uso de Docker facilita la presentación del sistema en un ambiente reproducible, mientras que Airflow permite controlar el pipeline de forma ordenada y automatizada.

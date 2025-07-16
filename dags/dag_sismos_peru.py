from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os

# Directorio de salida
BASE_DIR = "/opt/airflow/dags/data"
os.makedirs(BASE_DIR, exist_ok=True)
OUTPUT_CSV = os.path.join(BASE_DIR, "sismos_peru.csv")

# Lat/lon de Perú: centro aproximado
LATITUDE = -9.19
LONGITUDE = -75.0152
RADIUS_KM = 700  # cubre gran parte del país

def fetch_and_filter_sismos():
    end = datetime.utcnow()
    start = end - timedelta(days=1)
    url = (
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
        f"?format=geojson&starttime={start.strftime('%Y-%m-%d')}"
        f"&endtime={end.strftime('%Y-%m-%d')}"
        f"&latitude={LATITUDE}&longitude={LONGITUDE}"
        f"&maxradiuskm={RADIUS_KM}&minmagnitude=4"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for feat in data.get("features", []):
        prop = feat["properties"]
        geom = feat["geometry"]["coordinates"]
        rows.append({
            "time": datetime.utcfromtimestamp(prop["time"] / 1000).isoformat(),
            "magnitude": prop["mag"],
            "place": prop["place"],
            "longitude": geom[0],
            "latitude": geom[1],
            "depth_km": geom[2]
        })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Guardados {len(df)} sismos con magnitud ≥ 4.0 en {OUTPUT_CSV}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dag_sismos_peru",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["sismos", "api"],
) as dag:

    tarea = PythonOperator(
        task_id="fetch_and_filter_sismos",
        python_callable=fetch_and_filter_sismos,
    )

    tarea

# pip install apache-airflow pandas requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd

# Ruta donde se guardará el CSV
CSV_PATH = "/tmp/usuarios.csv"

# Tarea 1: Descargar datos
def descargar_usuarios():
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()  # Lanza error si falla
    usuarios = response.json()

    # Guardar en archivo
    df = pd.DataFrame(usuarios)
    df.to_csv(CSV_PATH, index=False)
    print(f"Se guardaron {len(df)} usuarios en {CSV_PATH}")

# Tarea 2: Leer CSV y contar usuarios
def contar_usuarios():
    df = pd.read_csv(CSV_PATH)
    print(f"El archivo contiene {len(df)} usuarios.")
    print(df[["id", "name"]])

# Definir el DAG
with DAG(
    dag_id="dag_descarga_y_procesamiento",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["api", "csv", "pandas"]
) as dag:

    tarea_descargar = PythonOperator(
        task_id="descargar_usuarios_api",
        python_callable=descargar_usuarios
    )

    tarea_procesar = PythonOperator(
        task_id="contar_usuarios_csv",
        python_callable=contar_usuarios
    )

    tarea_descargar >> tarea_procesar  # Dependencia: primero descargar, luego procesar

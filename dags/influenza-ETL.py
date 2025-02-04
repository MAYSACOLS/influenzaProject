import requests
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine

def fetch_grippe_data():
    url = "https://www.sentiweb.fr/api/v1/datasets/rest/incidence?indicator=3&geo=RDD&span=all"
    response = requests.get(url)
    data = response.json()
    data = data.get("data", [])
    df = pd.DataFrame(data)
    return df


def transform_data(df):
    # Nettoyage et transformation optionnels
    df['date'] = pd.to_datetime(df['date'])
    df_cleaned = df.dropna(axis=0)
    return df_cleaned

def load_to_postgres(**kwargs):
     # 🔹 Récupération des identifiants via les Variables Airflow
    host = Variable.get("koyeb_postgres_host")
    login = Variable.get("koyeb_postgres_user")
    password = Variable.get("koyeb_postgres_password")  # ⚠️ Correction ici, utiliser la même variable
    port = Variable.get("koyeb_postgres_port")
    db = Variable.get("koyeb_postgres_db")

     # 🔹 Construction de la connexion PostgreSQL
    connection_string = f"postgresql://{login}:{password}@{host}:{port}/{db}"
    engine = create_engine(connection_string)
    
    # Reste du code identique
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_data')
    
    df.to_sql(
        name='grippe_incidence', 
        con=engine, 
        if_exists='append', 
        index=False
    )

with DAG(
    'grippe_etl_pipeline',
    start_date=datetime(2024, 2, 4),
    schedule_interval='@daily'
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_grippe_data',
        python_callable=fetch_grippe_data,
        provide_context=True
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )
    
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True
    )

    fetch_task >> transform_task >> load_task
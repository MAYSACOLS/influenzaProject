from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import os
from sqlalchemy import create_engine

# Configuration
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

API_URLS = {
    "vaccins": "https://www.data.gouv.fr/fr/datasets/r/848e3e48-4971-4dc5-97c7-d856cdfde2f6",
    "casgrippe": "https://www.sentiweb.fr/api/v1/datasets/rest/incidence?indicator=3&geo=PAY&span=all"
}

def get_file_path(data_type):
    """Génère le nom du fichier avec un timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DATA_DIR, f"{data_type}_{timestamp}.csv")

def extract_data(data_type, **context):
    """Extrait les données d'une API et les stocke en CSV"""
    try:
        url = API_URLS.get(data_type)
        if not url:
            raise ValueError(f"Type de données inconnu: {data_type}")
        
        response = requests.get(url)
        response.raise_for_status()

        df = pd.read_csv(io.StringIO(response.text))
        file_path = get_file_path(data_type)
        df.to_csv(file_path, index=False)
        
        context['task_instance'].xcom_push(key=f'file_path_{data_type}', value=file_path)
        print(f"✅ {data_type.upper()} - Fichier CSV sauvegardé : {file_path}")
    
    except Exception as e:
        raise Exception(f"❌ Erreur lors de l'extraction des données {data_type} : {e}")

def load_data_to_koyeb(data_type, table_name, **context):
    """Charge un fichier CSV dans PostgreSQL"""
    try:
        # Connexion PostgreSQL
        host = Variable.get("koyeb_postgres_host")
        login = Variable.get("koyeb_postgres_user")
        password = Variable.get("koyeb_postgres_password")
        port = Variable.get("koyeb_postgres_port")
        db = Variable.get("koyeb_postgres_db")

        endpoint_id = host.split('.')[0]
        connection_string = f"postgresql://{login}:{password}@{host}:{port}/{db}?options=endpoint%3D{endpoint_id}&sslmode=require"
        engine = create_engine(connection_string)

        # Récupération du fichier depuis XCom
        file_path = context['task_instance'].xcom_pull(task_ids=f'extract_{data_type}', key=f'file_path_{data_type}')
        if not file_path or not os.path.exists(file_path):
            raise Exception(f"❌ Fichier {data_type} introuvable : {file_path}")

        df = pd.read_csv(file_path)
        df_cleaned = df.dropna()

        df_cleaned.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )

        print(f"✅ Chargement réussi dans la table {table_name} ({len(df_cleaned)} lignes)")

    except Exception as e:
        raise Exception(f"❌ Erreur lors du chargement des données {data_type} : {e}")

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'grippe_ETL_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 2, 4, 9, 0, 0),
    schedule_interval='@hourly',
    max_active_runs=1,
    catchup=False
) as dag:

    # Tâches d'extraction
    extract_vaccins_task = PythonOperator(
        task_id='extract_vaccins',
        python_callable=extract_data,
        op_kwargs={'data_type': 'vaccins'},
        provide_context=True
    )

    extract_casgrippe_task = PythonOperator(
        task_id='extract_casgrippe',
        python_callable=extract_data,
        op_kwargs={'data_type': 'casgrippe'},
        provide_context=True
    )

    # Tâches de chargement en base
    load_vaccins_task = PythonOperator(
        task_id='load_vaccins',
        python_callable=load_data_to_koyeb,
        op_kwargs={'data_type': 'vaccins', 'table_name': 'grippe_vaccins'},
        provide_context=True
    )

    load_casgrippe_task = PythonOperator(
        task_id='load_casgrippe',
        python_callable=load_data_to_koyeb,
        op_kwargs={'data_type': 'casgrippe', 'table_name': 'grippe_cas'},
        provide_context=True
    )

    # Définition du workflow
    extract_vaccins_task >> load_vaccins_task
    extract_casgrippe_task >> load_casgrippe_task
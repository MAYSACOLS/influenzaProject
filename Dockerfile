# Utiliser l'image officielle d'Airflow 2.3.2
FROM apache/airflow:2.3.2

# Copier ton fichier requirements.txt dans l'image Docker
COPY requirements.txt .

# Installer les dépendances à partir de requirements.txt
RUN pip install -r requirements.txt
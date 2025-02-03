import requests
import pandas as pd
from datetime import datetime

# URL de l'API pour les données d'incidence de la grippe
url = "https://www.sentiweb.fr/api/v1/datasets/rest/incidence?indicator=3&geo=RDD&span=all"

# Faire la requête GET pour récupérer les données
response = requests.get(url)

# Vérifier si la requête est réussie
if response.status_code == 200:
    # Charger les données JSON
    data = response.json()

    # Extraire les données sous la clé "data"
    data = data.get("data", [])
    
    # Créer un DataFrame à partir des données
    df = pd.DataFrame(data)

    # Filtrer les données pour novembre 2024, décembre 2024, et janvier 2025
    start_date = 201801
    end_date = 202504

    # Filtrer les données entre les deux dates spécifiées
    filtered_df = df[(df['week'] >= start_date) & (df['week'] <= end_date)]

    # Afficher les résultats filtrés
    print(filtered_df)

    # Exporter le DataFrame filtré dans un fichier CSV
    filtered_df.to_csv('incidence_grippe_week_201801_to_week_202504.csv', index=False)
    print("Les données ont été exportées dans 'incidence_grippe_nov_2024_to_jan_2025.csv'.")
else:
    print(f"Erreur lors de la récupération des données : {response.status_code}")

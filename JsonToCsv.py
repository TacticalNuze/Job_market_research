import json
import csv

# Charger les données JSON
with open('processed_jobs.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Accéder à la liste selon la clé réelle
data = data["results"]  # ← adapte ce nom selon la structure réelle


# S'assurer que les données sont une liste
if not isinstance(data, list):
    print("Erreur : le fichier JSON ne contient pas une liste d'offres d'emploi.")
    exit()

# Définir les champs à écrire dans le CSV (dans l'ordre souhaité)
fieldnames = [
    "job_url", "title", "company", "description",
    "niveau_etudes", "niveau_experience", "contrat",
    "region", "competences", "publication_date",
    "via", "titre_homogene", "secteur", "niveau_qualification"
]

# Écrire dans le fichier CSV
with open('processed_jobs.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for item in data:
        # Si un champ est manquant, mettre une valeur vide
        row = {key: item.get(key, "") for key in fieldnames}
        writer.writerow(row)

print("Conversion terminée : processed_jobs.csv créé avec succès.")

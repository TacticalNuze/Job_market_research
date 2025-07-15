import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("traitement.log"), logging.StreamHandler()],
)

# Chargement des variables d'environnement
load_dotenv()
API_KEY = os.getenv("GROQ_API_KEY")
if not API_KEY or not API_KEY.startswith("gsk_"):
    logging.error("Clé API Groq non configurée - Vérifiez .env")
    exit(1)


def load_json(file_path: str) -> List[Dict[str, Any]]:
    """Charge le fichier JSON d'entrée"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Erreur de lecture {file_path} : {str(e)}")
        exit(1)


def prepare_offer(offer: Dict[str, Any]) -> Dict[str, Any]:
    """Prépare les données pour l'API en conservant les champs importants"""
    return {
        "title": (offer.get("title", "") or "")[:200],
        "description": (offer.get("description", "") or "")[:1000],
        "competences": [
            c.strip()
            for c in (offer.get("competences", "") or "").split("-")
            if c.strip()
        ],
    }


def clean_response(response_text: str) -> List[Dict[str, Any]]:
    """Extrait et nettoie la réponse JSON depuis la réponse de l'API"""
    json_pattern = r"```(?:json)?\s*([\s\S]*?)\s*```|(\[.*\])"
    matches = re.findall(json_pattern, response_text, re.IGNORECASE | re.DOTALL)

    for match in matches:
        json_segment = match[0] or match[1]
        try:
            data = json.loads(json_segment)
            for entry in data:
                # Conversion sécurisée de niveau_qualification en entier si possible
                try:
                    entry["niveau_qualification"] = int(
                        entry.get("niveau_qualification", 0)
                    )
                except (ValueError, TypeError):
                    entry["niveau_qualification"] = entry.get("niveau_qualification", 0)
                if "competences" in entry and isinstance(entry["competences"], list):
                    entry["competences"] = [
                        c.strip() for c in entry["competences"] if c.strip()
                    ]
            return data
        except json.JSONDecodeError:
            logging.error(
                f"JSONDecodeError pour le segment (limité à 500 caractères) : {json_segment[:500]}"
            )
            continue

    logging.error("Format de réponse non détecté")
    return []


def process_with_groq(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Envoie une requête à l'API Groq pour homogénéiser et classifier les titres"""
    GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
    max_retries = 3
    base_retry_delay = 2

    system_prompt = """CLASSIFICATION, NORMALISATION ET ENRICHISSEMENT DES TITRES D'OFFRES D'EMPLOI
Tu es un expert en RH et en analyse d'offres d'emploi. Tu reçois une liste d'offres provenant d'un fichier JSON comprenant les champs 'title', 'description', et 'competences'.
Ta mission est de :

Traiter et homogénéiser le titre de chaque offre en générant un champ 'titre_homogene' qui regroupe des intitulés similaires sous un libellé standard (par exemple, transformer 'Développeur Senior React/Next.js - Casablanca' en 'Développeur Frontend ').

Déduire le 'secteur' d'activité à partir du titre et de la description (par exemple, 'Informatique', 'Data', 'Cloud', etc.).

Déterminer le 'niveau_qualification' sous forme d'un entier de 1 à 5 (où 1 correspond à un niveau Bac et 5 à un niveau Doctorat), en se basant sur les informations du niveau d'études et de l'expérience indiqués dans les offres.

Ajouter, si nécessaire, d'autres colonnes pertinentes pour garantir que les données finales soient propres, claires et cohérentes, tout en conservant la structure de base des données.

Réponds uniquement avec un JSON valide, qui est une liste d'objets contenant obligatoirement les champs : 'title', 'titre_homogene', 'secteur', 'niveau_qualification'.

Exemple de format de sortie :
[
{
"title": "Customer Service Specialist (Spanish and Portuguese) - Rabat",
"titre_homogene": "Spécialiste Service Client Bilingue",
"secteur": "Service Client",
"niveau_qualification": 1
}
]"""

    for attempt in range(max_retries):
        try:
            response = requests.post(
                GROQ_API_URL,
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama3-8b-8192",
                    "temperature": 0.1,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {
                            "role": "user",
                            "content": json.dumps(batch, ensure_ascii=False),
                        },
                    ],
                },
                timeout=60,
            )
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            return clean_response(content)

        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < max_retries - 1:
                retry_after = response.headers.get("Retry-After")
                wait = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else base_retry_delay * (2**attempt)
                )
                logging.warning(
                    f"Trop de requêtes (429) - Retry dans {wait} secondes (essai {attempt + 1})"
                )
                time.sleep(wait)
                continue
            else:
                logging.error(f"Erreur API : {str(e)}")
                return []
        except Exception as e:
            logging.error(f"Erreur lors de l'appel API : {str(e)}")
            return []


def main():
    """Exécute le pipeline complet de traitement des offres d'emploi.

    Charge les offres d'un fichier JSON, les traite par lots via l'API Groq, et sauvegarde les résultats enrichis.
    Sauvegarde les résultats dans un fichier JSON avec métadonnées.
    """

    input_file = "merged_jobs.json"
    output_file = "processed_jobs.json"
    batch_size = 2  # Nombre d'offres par lot
    cooldown_after_batches = 10  # Nombre de lots avant une pause longue
    cooldown_delay = 60  # Délai en secondes pour la pause longue

    data = load_json(input_file)

    # Diviser les données en lots
    batches = [data[i : i + batch_size] for i in range(0, len(data), batch_size)]
    total_batches = len(batches)

    results = []
    for i, batch in enumerate(batches, 1):
        logging.info(f"Traitement lot {i}/{total_batches}")
        prepared_batch = [prepare_offer(offer) for offer in batch]
        processed = process_with_groq(prepared_batch)

        if processed:
            # Fusionner les résultats avec les données originales
            for original, updated in zip(batch, processed):
                original.update(updated)
            results.extend(batch)
        else:
            logging.error(f"Lot {i} n'a pas pu être traité correctement.")

        # Pause courte entre chaque lot
        time.sleep(5)
        # Appliquer une pause longue après un certain nombre de lots
        if i % cooldown_after_batches == 0:
            logging.info(
                f"Cooldown : Pause de {cooldown_delay} secondes après {cooldown_after_batches} lots"
            )
            time.sleep(cooldown_delay)

    # Générer le dictionnaire des titres homogénéisés
    dictionnaire_titres = {}
    for offer in results:
        titre_norm = offer.get("titre_homogene", "").strip()
        if titre_norm:
            dictionnaire_titres[titre_norm] = dictionnaire_titres.get(titre_norm, 0) + 1

    output = {
        "metadata": {
            "processed_at": datetime.now().isoformat(),
            "total_processed": len(results),
            "model": "llama3-8b-8192",
        },
        "results": results,
        "dictionnaire_titres": dictionnaire_titres,
    }

    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        logging.info(f"✅ Traitement terminé : {output_file}")
    except Exception as e:
        logging.error(f"Erreur d'écriture : {str(e)}")


if __name__ == "__main__":
    main()

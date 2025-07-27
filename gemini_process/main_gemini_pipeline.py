import os
import sys
import json
import time
from datetime import datetime

from dotenv import load_dotenv


from utils__init__ import *
from _init_gemini import normalize_date, normalize_text, parse_location, call_gemini  # module Gemini fourni

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()

# --- Configs
BATCH_SIZE = 5
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def preprocess_offer(offer: dict) -> dict:
    offer = offer.copy()
    offer["publication_date"] = normalize_date(offer.get("publication_date"))
    offer["location"] = parse_location(offer.get("lieu", ""))
    offer["titre"] = normalize_text(offer.get("titre", ""))
    offer["contrat"] = normalize_text(offer.get("contrat", ""))
    offer["type_travail"] = normalize_text(offer.get("type_travail", ""))
    offer["via"] = normalize_text(offer.get("via", ""))
    return offer


def main():
    logging.info("📦 Chargement des offres depuis MinIO bucket 'webscraping'")
    all_offers = read_all_from_bucket_memory(bucket_name="webscraping")
    logging.info(f"🔎 {len(all_offers)} offres chargées.")

    # Prétraitement
    preprocessed = []
    for i, offer in enumerate(all_offers):
        try:
            cleaned = preprocess_offer(offer)
            preprocessed.append(cleaned)
        except Exception as e:
            logging.warning(f"❌ Offre ignorée à l'index {i} à cause d'une erreur : {e}")

    logging.info(f"✅ {len(preprocessed)} offres prétraitées.")

    # Traitement avec Gemini
    enriched_profiles = []
    for i in range(0, len(preprocessed), BATCH_SIZE):
        batch = preprocessed[i:i + BATCH_SIZE]
        results = call_gemini(batch)

        for res in results:
            if isinstance(res, dict) and res.get("is_data_profile", False) is True:
                enriched_profiles.append(res)
            else:
                logging.info("⏭️ Offre non liée à la Data/IA ignorée.")

        time.sleep(1)  # Respecte le quota

    if not enriched_profiles:
        logging.warning("❗ Aucune offre Data/IA détectée. Fin du script.")
        return

    # Sauvegarde
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_DIR, f"filtered_data_profiles_{now}.json")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(enriched_profiles, f, ensure_ascii=False, indent=2)
    logging.info(f"✅ Résultat enregistré localement dans : {output_file}")

    # Envoi dans MinIO bucket "traitement"
    try:
        save_to_minio(file_path=output_file, bucket_name="traitement", content_type="application/json")
        logging.info("📤 Fichier uploadé dans MinIO bucket 'traitement'")
    except Exception as e:
        logging.error(f"❌ Erreur lors de l'upload MinIO : {e}")


if __name__ == "__main__":
    load_dotenv()
    main()

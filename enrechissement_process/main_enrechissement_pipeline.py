import json
import logging
import os
from datetime import datetime

from utils__init__ import read_and_normalize_all_offers, save_to_minio
from init_groq import process_all_offers

# 📌 Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# 📌 Constantes
BUCKET_INPUT = "webscraping"
BUCKET_OUTPUT = "traitement"
BATCH_SIZE = 10  # Ajustable selon capacité Groq
DATE_SUFFIX = datetime.now().strftime('%Y%m%d_%H%M%S')
FILENAME_OUTPUT = f"profils_data_enrichis_groq_{DATE_SUFFIX}.json"

def main():
    logging.info("🚀 Lancement du pipeline de traitement via Groq")

    # 1. Lecture et normalisation depuis MinIO
    all_offers = read_and_normalize_all_offers(bucket_name=BUCKET_INPUT)
    if not all_offers:
        logging.warning("❌ Aucune offre trouvée dans le bucket MinIO")
        return

    logging.info(f"📥 Offres à traiter : {len(all_offers)}")

    # 2. Traitement complet avec Groq
    enriched_profiles = process_all_offers(all_offers)

    # 3. Sauvegarde locale
    output_dir = "traitement"
    os.makedirs(output_dir, exist_ok=True)
    local_path = os.path.join(output_dir, FILENAME_OUTPUT)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(enriched_profiles, f, indent=2, ensure_ascii=False)
    logging.info(f"💾 Fichier local enregistré : {local_path}")

    # 4. Envoi vers MinIO
    # 📌 Chemin complet du fichier local JSON généré
    local_path = os.path.join("traitement", FILENAME_OUTPUT)
# 📤 Upload vers MinIO
    save_to_minio(local_path, bucket_name=BUCKET_OUTPUT)

    logging.info(f"☁️ Envoi vers MinIO bucket '{BUCKET_OUTPUT}' terminé")

if __name__ == "__main__":
    main()

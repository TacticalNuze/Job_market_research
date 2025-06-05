import os

from minio import Minio
from minio.error import S3Error

MINIO_URL = "http://127.0.0.1:9090/"
ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")
BUCKET_NAME = "webscraping"
FOLDER_PATH = "./Data_extraction/scraping_output"  # Dossier JSON du projet GitHub

# --- Connexion à MinIO ---
client = Minio(MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=True)

# --- Création du bucket s'il n'existe pas ---
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"✅ Bucket '{BUCKET_NAME}' créé.")
else:
    print(f"📦 Bucket '{BUCKET_NAME}' déjà existant.")

# --- Upload automatique de tous les fichiers .json ---
for filename in os.listdir(FOLDER_PATH):
    if filename.endswith(".json"):
        file_path = os.path.join(FOLDER_PATH, filename)
        object_name = f"scraping_output/{filename}"

        try:
            client.fput_object(
                BUCKET_NAME, object_name, file_path, content_type="application/json"
            )
            print(f"📤 Upload : {filename}")
        except S3Error as err:
            print(f"❌ Erreur : {filename} → {err}")

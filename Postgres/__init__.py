import json
import logging
import os

from minio import Minio, S3Error


def start_client(
    MINIO_URL=None,
    ACCESS_KEY=None,
    SECRET_KEY=None,
) -> Minio:
    # Valeurs par défaut si variables d'environnement manquantes
    if MINIO_URL is None:
        MINIO_URL = os.environ.get("MINIO_API", "localhost:9000")
    if ACCESS_KEY is None:
        ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    if SECRET_KEY is None:
        SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    if not MINIO_URL or not ACCESS_KEY or not SECRET_KEY:
        raise ValueError("Les variables d'environnement MINIO_API, MINIO_ROOT_USER et MINIO_ROOT_PASSWORD doivent être définies ou avoir des valeurs par défaut")

    client = Minio(
        MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False
    )
    return client

def make_buckets(bucket_list: list = ["webscraping", "traitement"]):
    client = start_client()
    for bucket_name in bucket_list:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' créé.")
        else:
            print(f"Bucket '{bucket_name}' déjà existant.")


def save_to_minio(
    file_path,
    bucket_name="webscraping",
    content_type="application/json",
):
    try:
        client = start_client()
        object_name = os.path.basename(file_path)
        client.fput_object(bucket_name, object_name, file_path, content_type)
        print(f" Uploaded the file : {object_name}")
    except S3Error as err:
        print(f" Erreur : {object_name} → {err}")


def read_from_minio(file_path, object_name, bucket_name="webscraping"):
    try:
        client = start_client()
        json_file = client.fget_object(bucket_name, object_name, file_path)
        return json_file
    except Exception as e:
        print(f"Can't download object from object storage: {e}")




def read_all_from_bucket(bucket_name="traitement"):
    try:
        client = start_client()
        all_data = []
        objects = client.list_objects(bucket_name)
        
        for obj in objects:
            object_name = obj.object_name
            if not object_name:
                logging.warning("Objet sans nom détecté, ignoré.")
                continue

            logging.info(f"Lecture de l'objet : {object_name}")
            response = client.get_object(bucket_name, object_name)
            data_bytes = response.read()
            response.close()
            response.release_conn()

            if not data_bytes:
                logging.warning(f"Objet {object_name} vide, ignoré.")
                continue

            data_str = data_bytes.decode("utf-8")

            try:
                data = json.loads(data_str)
            except json.JSONDecodeError as jde:
                logging.warning(f"Erreur JSON dans {object_name}: {jde}")
                continue
            
            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)
        
        return all_data

    except Exception as e:
        logging.error(f"Erreur lors de la lecture des objets dans MinIO: {e}")
        return []



def scraping_upload(scraping_dir="/app/data_extraction/scraping_output"):
    try:
        make_buckets()
    except Exception:
        print("Couldn't setup the initial buckets")
    try:
        scraping_files = os.listdir(scraping_dir)

        for file in scraping_files:
            file_path = os.path.join(scraping_dir, file)
            save_to_minio(file_path=file_path)

    except Exception as e:
        print(f"Couldn't list the files in the scraping folder:{e}")



def read_all_from_bucket_memory(bucket_name="webscraping") -> list:
    """
    Récupère tous les fichiers JSON du bucket MinIO en mémoire,
    parse leur contenu et retourne une liste contenant toutes les offres.

    Args:
        bucket_name (str): nom du bucket MinIO.

    Returns:
        list: liste des objets JSON extraits.
    """
    try:
        client = start_client()
    except Exception as e:
        logging.error(f"Couldn't start client connection to Minio: {e}")
        return []

    all_data = []

    try:
        objects = client.list_objects(bucket_name=bucket_name, recursive=True)
        for obj in objects:
            object_name = obj.object_name
            if not object_name:
                logging.warning("Objet sans nom, ignoré.")
                continue

            response = client.get_object(bucket_name, object_name)
            # Lire le flux en mémoire
            content = response.read()
            response.close()
            response.release_conn()

            # Parser le JSON
            data = json.loads(content.decode('utf-8'))

            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)

            logging.info(f"Chargé {object_name} depuis le bucket {bucket_name} en mémoire.")

        return all_data

    except Exception as e:
        logging.error(f"Erreur lors de la lecture des objets en mémoire dans MinIO: {e}")
        return []
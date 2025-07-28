import json
import logging
import os
from datetime import datetime
from minio import Minio, S3Error

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_client(MINIO_URL=None, ACCESS_KEY=None, SECRET_KEY=None) -> Minio:
    """
    Initialise une instance de client MinIO avec les paramètres fournis ou les variables d'environnement.
    
    Returns:
        Minio: Instance de client MinIO connectée.
    """
    MINIO_URL = MINIO_URL or os.environ.get("MINIO_API", "localhost:9000")
    ACCESS_KEY = ACCESS_KEY or os.environ.get("MINIO_ROOT_USER", "minioadmin")
    SECRET_KEY = SECRET_KEY or os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

    if not MINIO_URL or not ACCESS_KEY or not SECRET_KEY:
        raise ValueError("Les variables d'environnement MINIO_API, MINIO_ROOT_USER et MINIO_ROOT_PASSWORD doivent être définies")

    return Minio(MINIO_URL, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)

def make_buckets(bucket_list: list = ["webscraping", "traitement"]):
    """
    Crée les buckets MinIO s'ils n'existent pas déjà.
    
    Args:
        bucket_list (list): Liste des noms de buckets à créer.
    """
    client = start_client()
    for bucket_name in bucket_list:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' créé.")
        else:
            logging.info(f"Bucket '{bucket_name}' déjà existant.")

def save_to_minio(file_path, bucket_name="webscraping", content_type="application/json"):
    """
    Sauvegarde un fichier local dans un bucket MinIO.

    Args:
        file_path (str): Chemin du fichier local.
        bucket_name (str): Nom du bucket cible.
        content_type (str): Type MIME du fichier.
    """
    try:
        client = start_client()
        object_name = os.path.basename(file_path)
        client.fput_object(bucket_name, object_name, file_path, content_type)
        logging.info(f"✅ Uploaded the file : {object_name}")
    except S3Error as err:
        logging.error(f"❌ Erreur : {object_name} → {err}")

def read_from_minio(file_path, object_name, bucket_name="webscraping"):
    """
    Télécharge un objet MinIO et le sauvegarde localement.

    Returns:
        str: Chemin du fichier local téléchargé.
    """
    try:
        client = start_client()
        client.fget_object(bucket_name, object_name, file_path)
        return file_path
    except Exception as e:
        logging.error(f"❌ Can't download object from object storage: {e}")
        return None

def read_all_from_bucket(bucket_name="traitement"):
    """
    Lit tous les objets JSON d'un bucket MinIO et les agrège.

    Returns:
        list: Toutes les données JSON combinées.
    """
    try:
        client = start_client()
        all_data = []
        objects = client.list_objects(bucket_name, recursive=True)

        for obj in objects:
            object_name = obj.object_name
            if not object_name:
                logging.warning("Objet sans nom détecté, ignoré.")
                continue

            response = client.get_object(bucket_name, object_name)
            data_bytes = response.read()
            response.close()
            response.release_conn()

            if not data_bytes:
                logging.warning(f"Objet {object_name} vide, ignoré.")
                continue

            try:
                data = json.loads(data_bytes.decode("utf-8"))
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
            except json.JSONDecodeError as jde:
                logging.warning(f"Erreur JSON dans {object_name}: {jde}")

        return all_data

    except Exception as e:
        logging.error(f"Erreur lors de la lecture des objets dans MinIO: {e}")
        return []

def scraping_upload(scraping_dir="/app/data_extraction/scraping_output"):
    """
    Upload tous les fichiers du dossier local vers le bucket 'webscraping'.
    """
    try:
        make_buckets()
    except Exception:
        logging.error("❌ Couldn't setup the initial buckets")

    try:
        scraping_files = os.listdir(scraping_dir)
        for file in scraping_files:
            file_path = os.path.join(scraping_dir, file)
            save_to_minio(file_path=file_path)
    except Exception as e:
        logging.error(f"❌ Couldn't list the files in the scraping folder: {e}")

def read_all_from_bucket_memory(bucket_name: str = "webscraping") -> list:
    """
    Récupère tous les fichiers JSON du bucket MinIO en mémoire,
    parse leur contenu et retourne une liste contenant toutes les offres.

    Args:
        bucket_name (str): Nom du bucket MinIO à lire.

    Returns:
        list: Liste des objets JSON extraits de tous les fichiers du bucket.
    """
    try:
        client = start_client()
    except Exception as e:
        logging.error(f"❌ Échec de la connexion MinIO : {e}")
        return []

    all_data = []

    try:
        objects = client.list_objects(bucket_name=bucket_name, recursive=True)
        found = False

        for obj in objects:
            object_name = obj.object_name
            if not object_name:
                logging.warning("⚠️ Objet sans nom détecté, ignoré.")
                continue

            if not object_name.endswith(".json"):
                logging.info(f"📦 Fichier ignoré (non JSON) : {object_name}")
                continue

            found = True
            try:
                response = client.get_object(bucket_name, object_name)
                content = response.read()
                response.close()
                response.release_conn()

                data = json.loads(content.decode("utf-8"))
                if isinstance(data, list):
                    all_data.extend(data)
                    logging.info(f"✅ {len(data)} offres extraites de {object_name}")
                else:
                    all_data.append(data)
                    logging.info(f"✅ 1 offre extraite de {object_name}")

            except json.JSONDecodeError:
                logging.error(f"❌ Fichier {object_name} n'est pas un JSON valide.")
            except Exception as e:
                logging.error(f"❌ Erreur lors de la lecture de {object_name} : {e}")

        if not found:
            logging.warning(f"❗ Aucun fichier JSON trouvé dans le bucket '{bucket_name}'.")

        logging.info(f"📊 Total des offres collectées : {len(all_data)}")
        return all_data

    except S3Error as s3e:
        logging.error(f"❌ Erreur MinIO lors du listage des objets : {s3e}")
        return []
    except Exception as e:
        logging.error(f"❌ Erreur inattendue lors de la lecture du bucket MinIO : {e}")
        return []

def normalize_offer(raw_offer: dict) -> dict:
    """
    Normalise un dictionnaire d'offre selon un format commun.
    """
    pub_date = raw_offer.get("publication_date", "")
    pub_date_norm = pub_date
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d %b-%H:%M"):
        try:
            pub_date_norm = datetime.strptime(pub_date, fmt).strftime("%Y-%m-%d")
            break
        except Exception:
            continue

    normalized = {
        "titre": raw_offer.get("titre") or None,
        "publication_date": pub_date_norm,
        "competences": raw_offer.get("competences") or None,
        "companie": raw_offer.get("companie") or None,
        "description": raw_offer.get("description") or None,
        "secteur": raw_offer.get("secteur") or raw_offer.get("domaine") or None,
        "fonction": raw_offer.get("fonction") or None,
        "niveau_experience": raw_offer.get("niveau_experience") or None,
        "niveau_etudes": raw_offer.get("niveau_etudes") or None,
        "contrat": raw_offer.get("contrat") or None,
        "via": raw_offer.get("via") or None,
        "job_url": raw_offer.get("job_url") or None,
        "region": raw_offer.get("region") or raw_offer.get("ville") or None,
        "extra": raw_offer.get("extra") or None,
        "salaire": raw_offer.get("salaire") or None,
    }
    return normalized

def read_and_normalize_all_offers(bucket_name="webscraping") -> list:
    """
    Lit toutes les offres depuis un bucket MinIO et les renvoie sous forme normalisée.

    Returns:
        list: Liste des offres normalisées.
    """
    raw_offers = read_all_from_bucket_memory(bucket_name)
    normalized_offers = [normalize_offer(offer) for offer in raw_offers]
    logging.info(f"✅ Normalisé {len(normalized_offers)} offres.")
    return normalized_offers

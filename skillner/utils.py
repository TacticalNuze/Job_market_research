import os

from minio import Minio, S3Error
from minio.datatypes import Object


def start_client(
    MINIO_URL=os.environ.get("MINIO_API"),
    ACCESS_KEY=os.environ.get("MINIO_ROOT_USER"),
    SECRET_KEY=os.environ.get("MINIO_ROOT_PASSWORD"),
) -> Minio:
    """This function simply connects to the minio server and returns a client instance.

    It is supposed to be used in other modules to create and maintain connections to the Object sotrage server
    """

    # --- Connexion à MinIO ---
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


def read_all_from_bucket(
    dest_dir="data_extraction/scraping_output",
    bucket_name="webscraping",
) -> list[Object]:
    """Downloads all the objects found in the specified bucket to the destination folder for this function"""
    try:
        client = start_client()
    except Exception as e:
        print(f"Couldn't start client connection to Minio: {e}")
    try:
        file_names = client.list_objects(bucket_name=bucket_name)
        for file_name in file_names:
            file_path = os.path.join(dest_dir, file_name.object_name)

            client.fget_object(bucket_name, file_name.object_name, file_path)
            print(f"----Saved file: {file_name.object_name} to path: {file_path}----")
        return file_names
    except Exception as e:
        print(f"Couldn't list the objects in Minio: {e}")


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


def list_valid_json_objects(bucket_name):
    """
    Retourne les chemins valides des objets JSON présents dans le bucket MinIO 'webscraping'.
    Seuls les fichiers .json dont la taille > 10 octets sont conservés.
    """
    client = Minio(
        os.getenv("MINIO_API"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )
    objects = client.list_objects(bucket_name, recursive=True)
    valid_paths = [
        f"s3a://webscraping/{obj.object_name}"
        for obj in objects
        if obj.object_name.endswith(".json") and obj.size > 10
    ]
    return valid_paths


def read_all_json_from_minio():
    """
    Lit et fusionne tous les fichiers JSON valides depuis MinIO dans un DataFrame PySpark.
    """
    print("📥 Lecture filtrée des fichiers JSON valides depuis MinIO...")
    valid_files = list_valid_json_objects()

    if not valid_files:
        print("⚠️ Aucun fichier JSON valide trouvé dans le bucket.")
        return None

    print(f"🔍 Fichiers détectés : {len(valid_files)}")
    for path in valid_files:
        print(f"   → {path}")
        read_all_from_bucket(
            object_name=path, file_dir=os.getcwd(), bucket_name="webscraping"
        )
    return [valid_files]

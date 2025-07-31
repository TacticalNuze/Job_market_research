import os

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi


def send_to_mongo(data: list):
    """
    Envoie les données à MongoDB.
    """
    # --- Insertion dans MongoDB ---
    try:
        load_dotenv()
        uri = os.environ.get("MONGO_DB_URI")
        client = MongoClient(
            uri, tls=True, tlsAllowInvalidCertificates=False, server_api=ServerApi("1")
        )
        db = client["DXC"]
        collection = db["rekrute"]

        if data:
            result = collection.insert_many(data)
            print(f"{len(result.inserted_ids)} documents insérés dans MongoDB.")
        else:
            print("Aucune donnée à insérer dans MongoDB.")
    except Exception as e:
        print("Erreur lors de l'insertion dans MongoDB :", e)
    finally:
        client.close()

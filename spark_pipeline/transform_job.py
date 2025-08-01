import json
import os
import uuid
from datetime import datetime

from minio import Minio
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, split, trim, udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# -----------------------------------------------------------------------------------
# INITIALISATION
# -----------------------------------------------------------------------------------
# The schema used to read our json files
global_schema = StructType(
    [
        StructField("job_url", StringType(), True),
        StructField("publication_date", StringType(), True),
        StructField("via", StringType(), True),
        StructField("contrat", StringType(), True),
        StructField("titre", StringType(), True),
        StructField("description", StringType(), True),
        StructField("companie", StringType(), True),
        StructField("secteur", StringType(), True),
        StructField("niveau_etudes", StringType(), True),
        StructField("niveau_experience", StringType(), True),
        StructField(
            "skills",
            StructType(
                [
                    StructField("hard_skills", ArrayType(StringType()), nullable=True),
                    StructField("soft_skills", ArrayType(StringType()), nullable=True),
                ]
            ),
            nullable=True,
        ),
    ]
)


def create_spark_session():
    """
    Crée une SparkSession avec le package hadoop-aws pour accéder à MinIO via s3a://
    """
    print("🔥 Initialisation SparkSession...")
    return (
        SparkSession.builder.appName("JobCleaningPipeline")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .getOrCreate()
    )


def configure_minio(spark: SparkSession):
    """
    Configure l'accès à MinIO pour Spark via le protocole S3A.

    Nécessite les variables d'environnement :
    - MINIO_API : URL de MinIO (ex: http://minio:9000)
    - MINIO_ROOT_USER / MINIO_ROOT_PASSWORD : Identifiants d’accès
    """
    print("🔐 Configuration MinIO...")
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", os.getenv("MINIO_API", "http://minio:9000"))
    hadoop_conf.set("fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
    hadoop_conf.set("fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")


# -----------------------------------------------------------------------------------
# LECTURE DES FICHIERS JSON
# -----------------------------------------------------------------------------------


def list_valid_json_objects():
    """
    Retourne les chemins valides des objets JSON présents dans le bucket MinIO 'ner'.
    Seuls les fichiers .json dont la taille > 10 octets sont conservés.
    """
    client = Minio(
        os.getenv("MINIO_API"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )
    objects = client.list_objects("ner", recursive=True)
    valid_paths = [
        f"s3a://ner/{obj.object_name}"
        for obj in objects
        if obj.object_name.endswith(".json") and obj.size > 10
    ]
    return valid_paths


def read_all_json_from_minio(spark: SparkSession, schema: StructType = global_schema):
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

    df = spark.read.schema(global_schema).option("multiLine", True).json(valid_files)

    return df


# -----------------------------------------------------------------------------------
# NETTOYAGE DES DONNÉES
# -----------------------------------------------------------------------------------


def normalize_date(date: str):
    if date is None:
        return None
    formats = [
        "%Y-%m-%d",  # 2025-05-09
        "%d/%m/%Y",  # 20/05/2025
        "%d %b-%H:%M",  # 1 May-12:53 , %b is for partial month name (Jan)
        "%d %B-%H:%M",  # %B is for full month names
    ]
    for fmt in formats:
        try:
            # strptime parses the date
            parsed_date = datetime.strptime(date, fmt)

            if (
                parsed_date.year == 1900
            ):  # if no year is found in date it defaults to 1900
                parsed_date = parsed_date.replace(year=datetime.today().year)

            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def flatten_skills_format(skill_col: Row):
    """
    Convertit les skills de l'ancien format groupé en liste d'objets avec 'nom' et 'type_skill'.
    """
    skills = skill_col.asDict()
    print(f"-----------Column extracted {skills}")
    if skills:
        flat_skills = []
        for skill_type_key in skills.keys():
            print(f"-----------SKills types {skill_type_key}")

            # Normalize skill_type_key
            type_skill = "hard" if "hard" in skill_type_key else "soft"
            for nom in skills[skill_type_key]:
                if nom:
                    flat_skills.append({"nom": nom, "type_skill": type_skill})

        print(f"------------------------The parsed skills : {flat_skills}")
    return flat_skills


flatten_skills_udf = udf(
    flatten_skills_format,
    ArrayType(
        StructType(
            [
                StructField("nom", StringType(), True),
                StructField("type_skill", StringType(), True),
            ]
        )
    ),
)

normalize_date_udf = udf(normalize_date, StringType())


def clean_data(df: DataFrame):
    """
    Nettoie et transforme les données :
    - Vérifie la présence des colonnes clés
    - Renomme les colonnes
    - Applique le split sur les champs multiples (compétences, secteurs, etc.)
    - Nettoie les types (dates, string)
    - Supprime les doublons selon `job_url`
    """
    print("🧼 Nettoyage des données...")
    print(f"Les colonnes detectées sont: {df.columns}")
    # Champs obligatoires
    required = ["job_url", "titre", "via", "publication_date"]
    for field in required:
        df = df.filter(col(field).isNotNull() & (col(field) != ""))

    # Renommage et nettoyage
    df = (
        df.dropDuplicates(["job_url"])
        .withColumnRenamed("companie", "compagnie")
        .withColumnRenamed("publication_date", "date")
    )

    # Cas conditionnel : soft_skills ou hard_skills peut être absente
    if "soft_skills" in df.columns:
        df = df.withColumn("soft_skills", split(col("soft_skills"), ",\\s*"))
    else:
        print("⚠️ Colonne 'soft_skills' absente — elle sera ignorée.")

    if "hard_skills" in df.columns:
        df = df.withColumn("hard_skills", split(col("hard_skills"), ",\\s*"))
    else:
        print("⚠️ Colonne 'hard_skills' absente — elle sera ignorée.")
    # Modification des colonnes pour plus de clareté/format
    df = (
        # formattage des colonnes secteur, etudes et experiences
        df.withColumn("secteur", split(col("secteur"), ",\\s*"))
        .withColumn("niveau_etudes", trim(col("niveau_etudes").cast(StringType())))
        .withColumn(
            "niveau_experience", trim(col("niveau_experience").cast(StringType()))
        )
        .withColumnRenamed("via", "source")
        # normalisation du format de date
        .withColumn("date", normalize_date_udf(col("date")))
        .withColumnRenamed("date", "date_publication")
        # changement des skills vers le format postgres
        .withColumn("new_skills", flatten_skills_udf(col("skills")))
        .drop(col("skills"))
        .withColumnRenamed("new_skills", "skills")
    )
    df = df.fillna("Unspecified")
    print(f"✅ Nettoyage terminé. Dataframe a {df.count()} lignes")
    return df


# -----------------------------------------------------------------------------------
# ÉCRITURE / SAUVEGARDE
# -----------------------------------------------------------------------------------


def generate_output_filename():
    """
    Génère un nom de fichier unique basé sur la date et un UUID.
    Exemple : processed_jobs_20250619_ab12cd34.json
    """
    file_id = str(uuid.uuid4())[:8]
    today = datetime.now().strftime("%d_%m_%Y")

    return f"processed_jobs_{today}_{file_id}.json"


def save_locally(df: DataFrame, path="/tmp"):
    """
    Sauvegarde le DataFrame nettoyé localement dans un fichier JSON unique (liste d'objets).
    """
    print(f"💾 Sauvegarde locale dans {path}")

    # Convertir chaque ligne du DataFrame Spark en JSON (chaîne), puis parser en dict Python
    rows = df.toJSON().collect()

    parsed_data = [json.loads(row) for row in rows]

    # S'assurer que le dossier parent existe
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Sauvegarde dans un fichier JSON unique
    with open(os.path.join(path, "cleaned_output.json"), "w", encoding="utf-8") as f:
        json.dump(parsed_data, f, ensure_ascii=False, indent=2)

    return path


def save_locally_2(df: DataFrame, path="/tmp/cleaned_output"):
    """
    Sauvegarde le DataFrame nettoyé localement en JSON (écrasement du dossier).
    """
    print(f"💾 Sauvegarde locale dans {path}")
    df.coalesce(1).write.mode("overwrite").json(path)
    return path


def find_json_in_folder(folder):
    """
    Cherche le premier fichier .json dans un dossier donné.
    Retourne None si le chemin n'est pas un dossier ou si aucun fichier .json n'est trouvé.
    """
    if not os.path.isdir(folder):
        print(f"❌ Le chemin fourni n'est pas un dossier valide : {folder}")
        return None

    for f in os.listdir(folder):
        if f.endswith(".json"):
            return os.path.join(folder, f)

    print(f"⚠️ Aucun fichier JSON trouvé dans : {folder}")
    return None


def upload_to_minio(local_path, filename, bucket="traitement"):
    """
    Upload le fichier JSON local vers le bucket MinIO spécifié.
    """
    print("📤 Upload vers MinIO...")
    client = Minio(
        os.getenv("MINIO_API"),
        access_key=os.getenv("MINIO_ROOT_USER"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
        secure=False,
    )
    json_file = find_json_in_folder(local_path)
    if json_file:
        client.fput_object(bucket, filename, json_file, content_type="application/json")
        print(f"🚀 Upload terminé : {bucket}/{filename}")
    else:
        print("❌ Aucun fichier JSON à uploader.")


# -----------------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------------


def main():
    """
    Pipeline complet :
    1. Initialise Spark et MinIO
    2. Charge les données JSON valides
    3. Nettoie les données
    4. Sauvegarde localement
    5. Upload vers MinIO
    """
    print("🚀 DÉMARRAGE DU SCRIPT SPARK")
    try:
        spark = create_spark_session()
        configure_minio(spark)

        df_raw = read_all_json_from_minio(spark)
        if df_raw is None or df_raw.count() == 0:
            print("🛑 Fin du script : aucun fichier JSON à traiter.")
            return

        df_cleaned = clean_data(df_raw)
        filename = generate_output_filename()
        local_path = "/tmp"

        save_locally(df_cleaned, local_path)
        upload_to_minio(local_path, filename)

        print("✅ PIPELINE TERMINÉ AVEC SUCCÈS")
    except Exception as e:
        print("❌ ERREUR DANS LE SCRIPT :", e)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
    print("🔥 Fichier mis à jour !")

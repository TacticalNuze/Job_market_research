import psycopg2
from psycopg2 import sql
from datetime import datetime
import json
import logging

from __init__ import *

# Configuration du log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def connect():
    """
    Établit une connexion PostgreSQL.
    """
    return psycopg2.connect(
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST"),
        database=os.environ.get("POSTGRES_DB", "postgres"),
        port=os.environ.get("POSTGRES_PORT", 5432
    )


def close(conn):
    """
    Ferme proprement la connexion à PostgreSQL.
    """
    if conn:
        conn.close()

def get_or_create_dimension(cur, table, unique_col, value, extra_cols=None):
    """
    Insère ou récupère l'id d'une dimension selon une valeur unique.
    Pour dim_date, calcule jour, mois, trimestre, annee, jour_semaine.
    """
    if value is None:
        return None
    extra_cols = extra_cols or {}

    if table == 'dim_date':
        try:
            dt = datetime.strptime(value, "%Y-%m-%d").date()
        except Exception as e:
            logging.error(f"Erreur parsing date '{value}' pour {table}: {e}")
            return None
        extra_cols = {
            "jour": dt.day,
            "mois": dt.month,
            "trimestre": (dt.month - 1) // 3 + 1,
            "annee": dt.year,
            "jour_semaine": dt.isoweekday()
        }

    cols = [unique_col] + list(extra_cols.keys())
    vals = [value] + list(extra_cols.values())

    insert_cols = sql.SQL(', ').join(map(sql.Identifier, cols))
    placeholders = sql.SQL(', ').join(sql.Placeholder() * len(cols))
    conflict_col = sql.Identifier(unique_col)

    # Mapping explicite nom table -> nom colonne id
    id_col_mapping = {
        'dim_date': 'id_date',
        'dim_source': 'id_source',
        'dim_contrat': 'id_contrat',
        'dim_titre': 'id_titre',
        'dim_compagnie': 'id_compagnie',
        'dim_niveau_etudes': 'id_niveau_etudes',
        'dim_niveau_experience': 'id_niveau_experience',
        'dim_skill': 'id_skill',
    }
    id_col_name = id_col_mapping.get(table)
    if not id_col_name:
        logging.error(f"Pas de colonne id mappée pour la table {table}")
        return None
    id_col = sql.Identifier(id_col_name)

    query = sql.SQL("""
        INSERT INTO {table} ({cols})
        VALUES ({vals})
        ON CONFLICT ({conflict_col}) DO UPDATE SET {unique_col} = EXCLUDED.{unique_col}
        RETURNING {id_col}
    """).format(
        table=sql.Identifier(table),
        cols=insert_cols,
        vals=placeholders,
        conflict_col=conflict_col,
        unique_col=conflict_col,
        id_col=id_col
    )

    cur.execute(query, vals)
    return cur.fetchone()[0]

def insert_offer(conn, offer):
    """
    Insère une offre d'emploi dans la base en respectant toutes les dimensions et la table de liaison M:N.
    """
    with conn.cursor() as cur:
        # Vérifie doublon via job_url
        cur.execute("SELECT id_offer FROM fact_offre WHERE job_url = %s", (offer.get("job_url"),))
        if cur.fetchone():
            return -1

        # Prétraitement champs optionnels
        date_pub = offer.get("date_publication")
        id_date = get_or_create_dimension(cur, "dim_date", "full_date", date_pub)
        id_source = get_or_create_dimension(cur, "dim_source", "via", offer.get("source"))
        id_contrat = get_or_create_dimension(cur, "dim_contrat", "contrat", offer.get("contrat"))
        id_titre = get_or_create_dimension(cur, "dim_titre", "titre", offer.get("titre"))
        id_compagnie = get_or_create_dimension(
            cur, "dim_compagnie", "compagnie", offer.get("compagnie"),
            {"secteur": offer.get("secteur")}
        )
        id_etudes = get_or_create_dimension(cur, "dim_niveau_etudes", "niveau_etudes", offer.get("niveau_etudes"))
        id_experience = get_or_create_dimension(cur, "dim_niveau_experience", "niveau_experience", offer.get("niveau_experience"))

        competences_txt = ', '.join(
            skill.get("nom") for skill in offer.get("skills", []) if skill.get("nom")
        ) or None

        # Insertion dans fact_offre
        cur.execute("""
            INSERT INTO fact_offre (
                job_url, id_date_publication, id_source, id_contrat, id_titre,
                id_compagnie, id_niveau_etudes, id_niveau_experience,
                description, competences, secteur
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id_offer
        """, (
            offer.get("job_url"), id_date, id_source, id_contrat, id_titre,
            id_compagnie, id_etudes, id_experience,
            offer.get("description"), competences_txt, offer.get("secteur")
        ))

        id_offer = cur.fetchone()[0]

        # Liaison compétences
        for skill in offer.get("skills", []):
            nom = skill.get("nom")
            if not nom:
                continue
            id_skill = get_or_create_dimension(
                cur, "dim_skill", "nom", nom,
                {"type_skill": skill.get("type_skill")}
            )
            cur.execute("""
                INSERT INTO offre_skill (id_offer, id_skill)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
            """, (id_offer, id_skill))

        conn.commit()
        return id_offer


def load_offers_from_file(filepath):
    """
    Lit un fichier JSON et insère chaque offre dans la base.
    Gère les doublons, les erreurs et les logs.
    """
    inserted, skipped, errors = 0, 0, 0
    conn = None

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            raw = f.read().strip()
            offers = json.loads(raw) if raw.startswith('[') else [json.loads(line) for line in raw.splitlines() if line.strip()]
        logging.info(f"{len(offers)} offres trouvées dans {filepath}")
    except Exception as e:
        logging.error(f"Erreur lecture JSON: {e}")
        return

    try:
        conn = connect()
        for i, offer in enumerate(offers, 1):
            try:
                # Fallback des champs optionnels
                offer.setdefault("date_publication", offer.get("publication_date") or offer.get("date"))
                offer.setdefault("source", "Inconnue")
                offer.setdefault("contrat", "Non spécifié")
                offer.setdefault("titre", offer.get("title") or "Non spécifié")
                offer.setdefault("compagnie", "Non spécifiée")
                offer.setdefault("niveau_etudes", "Non spécifié")
                offer.setdefault("niveau_experience", None)

                res = insert_offer(conn, offer)
                if res == -1:
                    skipped += 1
                    logging.info(f"[{i}] Offre déjà existante: {offer.get('job_url')}")
                else:
                    inserted += 1
                    logging.info(f"[{i}] Offre insérée ID={res}")
            except Exception as e:
                errors += 1
                logging.error(f"[{i}] Erreur insertion offre: {e}")
                if conn:
                    conn.rollback()

    except Exception as e:
        logging.error(f"Erreur connexion ou transaction: {e}")
    finally:
        if conn:
            close(conn)

    logging.info(f"Import terminé ✅ — {inserted} insérées, {skipped} ignorées, {errors} erreurs.")



#test 


def load_offers(offers):
    """
    Insère une liste d'offres (déjà parsées) dans PostgreSQL.
    """
    inserted, skipped, errors = 0, 0, 0
    conn = None

    try:
        conn = connect()
        for i, offer in enumerate(offers, 1):
            try:
                # Fallback des champs optionnels
                offer.setdefault("date_publication", offer.get("publication_date") or offer.get("date"))
                offer.setdefault("source", "Inconnue")
                offer.setdefault("contrat", "Non spécifié")
                offer.setdefault("titre", offer.get("title") or "Non spécifié")
                offer.setdefault("compagnie", "Non spécifiée")
                offer.setdefault("niveau_etudes", "Non spécifié")
                offer.setdefault("niveau_experience", None)

                res = insert_offer(conn, offer)
                if res == -1:
                    skipped += 1
                    logging.info(f"[{i}] Offre déjà existante: {offer.get('job_url')}")
                else:
                    inserted += 1
                    logging.info(f"[{i}] Offre insérée ID={res}")
            except Exception as e:
                errors += 1
                logging.error(f"[{i}] Erreur insertion offre: {e}")
                conn.rollback()
    except Exception as e:
        logging.error(f"Erreur connexion ou transaction: {e}")
    finally:
        if conn:
            close(conn)

    logging.info(f"✅ Chargement terminé — {inserted} insérées, {skipped} ignorées, {errors} erreurs.")



def load_offers_from_minio(bucket_name="traitement"):
    """
    Charge toutes les offres JSON depuis un bucket MinIO (en mémoire),
    puis les insère dans PostgreSQL via load_offers.
    """
    logging.info(f"📦 Connexion à MinIO et lecture du bucket : {bucket_name}")

    try:
        offers = read_all_from_bucket_memory(bucket_name=bucket_name)

        if not offers:
            logging.warning("⚠️ Aucune offre trouvée dans le bucket MinIO.")
            return

        logging.info(f"📄 {len(offers)} offres récupérées depuis MinIO")
        
        # Ici on appelle ta fonction d'insertion dans PostgreSQL
        load_offers(offers)
        logging.info("✅ Chargement des offres dans PostgreSQL terminé.")

    except Exception as e:
        logging.error(f"❌ Erreur lors du chargement depuis MinIO : {e}")

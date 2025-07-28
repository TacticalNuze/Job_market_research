import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Fonction : Connexion PostgreSQL
def connect_db(dbname="offers"):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user="root",
            password="123456",
            host="postgres",
            port=5432
        )
        print(f"✅ Connexion réussie à la base '{dbname}'")
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion à la base '{dbname}' :", repr(e))
        return None

# Fonction : Créer la base 'prediction' si elle n'existe pas
def create_database():
    conn = connect_db("postgres")
    if conn is None:
        return
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'prediction'")
    exists = cur.fetchone()
    if not exists:
        cur.execute("CREATE DATABASE prediction")
        print("✅ Base 'prediction' créée.")
    else:
        print("ℹ️ Base 'prediction' existe déjà.")
    cur.close()
    conn.close()

# Fonction : Créer les tables dans 'prediction'
def create_prediction_tables():
    conn = connect_db("prediction")
    if conn is None:
        return
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ts_offres (
            id SERIAL PRIMARY KEY,
            ds DATE NOT NULL,
            y INTEGER NOT NULL,
            id_titre INTEGER,
            id_skill INTEGER,
            granularity TEXT,
            source TEXT,
            UNIQUE (ds, id_titre, id_skill)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast_offres (
            id SERIAL PRIMARY KEY,
            ds DATE NOT NULL,
            yhat FLOAT,
            yhat_lower FLOAT,
            yhat_upper FLOAT,
            id_titre INTEGER,
            id_skill INTEGER,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model_version TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS model_run_log (
            id SERIAL PRIMARY KEY,
            model_target TEXT,
            id_target INTEGER,
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            horizon INTEGER,
            status TEXT,
            message TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tables créées dans la base 'prediction'.")

# Fonction : Remplir ts_offres par titre
def fill_ts_offres_from_fact_offre():
    print("⏳ Remplissage de ts_offres depuis offers.fact_offre...")

    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    # Extraction : Nombre d’offres par jour et titre
    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            f.id_titre,
            COUNT(*) AS y
        FROM 
            fact_offre f
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        GROUP BY 
            d.full_date, f.id_titre
        ORDER BY 
            d.full_date;
    """)
    rows = src_cur.fetchall()

    # Insertion dans ts_offres
    for ds, id_titre, y in rows:
        dest_cur.execute("""
            INSERT INTO ts_offres (ds, y, id_titre, id_skill, granularity, source)
            VALUES (%s, %s, %s, NULL, %s, %s)
            ON CONFLICT (ds, id_titre, id_skill) DO NOTHING;
        """, (ds, y, id_titre, 'jour', 'offers'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes insérées dans ts_offres.")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# ▶️ Point d’entrée
if __name__ == "__main__":
    create_database()
    create_prediction_tables()
    fill_ts_offres_from_fact_offre()

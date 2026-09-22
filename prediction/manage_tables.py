import os

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from datetime import datetime, timedelta
import pandas as pd

# 📦 Connexion PostgreSQL
# dbname reste un paramètre : cette fonction est appelée sur trois bases
# différentes ('postgres' pour le CREATE DATABASE, 'offers' en source,
# 'prediction' en destination).
def connect_db(dbname="offers"):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=os.getenv("POSTGRES_USER", "root"),
            password=os.getenv("POSTGRES_PASSWORD", "123456"),
            host=os.getenv("DB_HOST", "postgres"),
            port=int(os.getenv("DB_PORT", 5432))
        )
        print(f"✅ Connexion réussie à la base '{dbname}'")
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion à la base '{dbname}' :", repr(e))
        return None

# 🏗 Créer base 'prediction' si non existante
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
        print("ℹ Base 'prediction' existe déjà.")
    cur.close()
    conn.close()

# 🧱 Créer les tables optimisées pour Prophet
def create_prediction_tables():
    conn = connect_db("prediction")
    if conn is None:
        return
    cur = conn.cursor()

    # Table principale des séries temporelles (format Prophet: ds, y)
    cur.execute("""
        DROP TABLE IF EXISTS ts_prophet_data CASCADE;
        CREATE TABLE ts_prophet_data (
            id SERIAL PRIMARY KEY,
            ds DATE NOT NULL,
            y INTEGER NOT NULL,
            segment_type VARCHAR(20) NOT NULL CHECK (segment_type IN ('global', 'secteur', 'titre', 'skill', 'contrat', 'source')),
            segment_id INTEGER,
            segment_name TEXT,
            granularity VARCHAR(10) DEFAULT 'daily' CHECK (granularity IN ('daily', 'weekly', 'monthly')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (ds, segment_type, segment_id, granularity)
        );
    """)

    # Table des régresseurs externes pour Prophet
    cur.execute("""
        DROP TABLE IF EXISTS ts_regressors CASCADE;
        CREATE TABLE ts_regressors (
            id SERIAL PRIMARY KEY,
            ds DATE NOT NULL,
            is_weekend INTEGER DEFAULT 0,
            is_holiday INTEGER DEFAULT 0,
            month_num INTEGER,
            day_of_week INTEGER,
            quarter INTEGER,
            rentree_scolaire INTEGER DEFAULT 0,
            fin_annee INTEGER DEFAULT 0,
            debut_annee INTEGER DEFAULT 0,
            vacances_ete INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (ds)
        );
    """)

    # Table des prédictions Prophet
    cur.execute("""
        DROP TABLE IF EXISTS prophet_forecasts CASCADE;
        CREATE TABLE prophet_forecasts (
            id SERIAL PRIMARY KEY,
            ds DATE NOT NULL,
            yhat FLOAT NOT NULL,
            yhat_lower FLOAT,
            yhat_upper FLOAT,
            trend FLOAT,
            trend_lower FLOAT,
            trend_upper FLOAT,
            segment_type VARCHAR(20) NOT NULL,
            segment_id INTEGER,
            segment_name TEXT,
            forecast_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            model_version TEXT DEFAULT '1.0',
            horizon_days INTEGER,
            UNIQUE (ds, segment_type, segment_id, forecast_date)
        );
    """)

    # Table des métriques de performance des modèles
    cur.execute("""
        DROP TABLE IF EXISTS model_performance CASCADE;
        CREATE TABLE model_performance (
            id SERIAL PRIMARY KEY,
            segment_type VARCHAR(20) NOT NULL,
            segment_id INTEGER,
            segment_name TEXT,
            mae FLOAT,
            mape FLOAT,
            rmse FLOAT,
            r2_score FLOAT,
            training_start_date DATE,
            training_end_date DATE,
            test_start_date DATE,
            test_end_date DATE,
            model_version TEXT DEFAULT '1.0',
            trained_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Table des logs d'exécution
    cur.execute("""
        DROP TABLE IF EXISTS prophet_run_logs CASCADE;
        CREATE TABLE prophet_run_logs (
            id SERIAL PRIMARY KEY,
            run_type VARCHAR(50) NOT NULL,
            segment_type VARCHAR(20),
            segment_id INTEGER,
            status VARCHAR(20) CHECK (status IN ('success', 'error', 'warning')),
            message TEXT,
            execution_time_seconds FLOAT,
            records_processed INTEGER,
            started_at TIMESTAMP,
            completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # Index pour les performances
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ts_prophet_ds ON ts_prophet_data(ds);
        CREATE INDEX IF NOT EXISTS idx_ts_prophet_segment ON ts_prophet_data(segment_type, segment_id);
        CREATE INDEX IF NOT EXISTS idx_forecasts_ds ON prophet_forecasts(ds);
        CREATE INDEX IF NOT EXISTS idx_forecasts_segment ON prophet_forecasts(segment_type, segment_id);
        CREATE INDEX IF NOT EXISTS idx_regressors_ds ON ts_regressors(ds);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Tables Prophet créées dans la base 'prediction'.")

# 📊 Remplir les données globales (toutes offres)
def fill_global_timeseries():
    print("⏳ Création série temporelle globale...")
    
    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    # Données quotidiennes globales
    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            COUNT(*) AS y
        FROM 
            fact_offre f
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        GROUP BY 
            d.full_date
        ORDER BY 
            d.full_date;
    """)
    
    rows = src_cur.fetchall()
    
    for ds, y in rows:
        dest_cur.execute("""
            INSERT INTO ts_prophet_data (ds, y, segment_type, segment_id, segment_name, granularity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds, segment_type, segment_id, granularity) 
            DO UPDATE SET y = EXCLUDED.y;
        """, (ds, y, 'global', 0, 'Global', 'daily'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes globales insérées.")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# 🏢 Remplir par secteur
def fill_secteur_timeseries():
    print("⏳ Création séries temporelles par secteur...")
    
    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            c.secteur,
            COUNT(*) AS y
        FROM 
            fact_offre f
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        JOIN 
            dim_compagnie c ON f.id_compagnie = c.id_compagnie
        WHERE 
            c.secteur IS NOT NULL AND c.secteur != ''
        GROUP BY 
            d.full_date, c.secteur
        ORDER BY 
            d.full_date, c.secteur;
    """)
    
    rows = src_cur.fetchall()
    
    # Créer un mapping secteur -> ID
    secteurs = {}
    secteur_id = 1
    
    for ds, secteur, y in rows:
        if secteur not in secteurs:
            secteurs[secteur] = secteur_id
            secteur_id += 1
        
        dest_cur.execute("""
            INSERT INTO ts_prophet_data (ds, y, segment_type, segment_id, segment_name, granularity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds, segment_type, segment_id, granularity) 
            DO UPDATE SET y = EXCLUDED.y;
        """, (ds, y, 'secteur', secteurs[secteur], secteur, 'daily'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes par secteur insérées ({len(secteurs)} secteurs).")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# 💼 Remplir par titre de poste
def fill_titre_timeseries():
    print("⏳ Création séries temporelles par titre...")
    
    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            f.id_titre,
            t.titre,
            COUNT(*) AS y
        FROM 
            fact_offre f
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        JOIN 
            dim_titre t ON f.id_titre = t.id_titre
        GROUP BY 
            d.full_date, f.id_titre, t.titre
        HAVING COUNT(*) >= 1  -- Filtrer les titres avec peu d'offres
        ORDER BY 
            d.full_date, f.id_titre;
    """)
    
    rows = src_cur.fetchall()
    
    for ds, id_titre, titre, y in rows:
        dest_cur.execute("""
            INSERT INTO ts_prophet_data (ds, y, segment_type, segment_id, segment_name, granularity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds, segment_type, segment_id, granularity) 
            DO UPDATE SET y = EXCLUDED.y;
        """, (ds, y, 'titre', id_titre, titre, 'daily'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes par titre insérées.")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# 🧠 Remplir par compétence
def fill_skill_timeseries():
    print("⏳ Création séries temporelles par compétence...")
    
    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            os.id_skill,
            s.nom,
            s.type_skill,
            COUNT(*) AS y
        FROM 
            offre_skill os
        JOIN 
            fact_offre f ON os.id_offer = f.id_offer
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        JOIN 
            dim_skill s ON os.id_skill = s.id_skill
        GROUP BY 
            d.full_date, os.id_skill, s.nom, s.type_skill
        HAVING COUNT(*) >= 2  -- Filtrer les skills rares
        ORDER BY 
            d.full_date, os.id_skill;
    """)
    
    rows = src_cur.fetchall()
    
    for ds, id_skill, nom_skill, type_skill, y in rows:
        segment_name = f"{nom_skill} ({type_skill})"
        dest_cur.execute("""
            INSERT INTO ts_prophet_data (ds, y, segment_type, segment_id, segment_name, granularity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds, segment_type, segment_id, granularity) 
            DO UPDATE SET y = EXCLUDED.y;
        """, (ds, y, 'skill', id_skill, segment_name, 'daily'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes par compétence insérées.")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# 📝 Remplir par type de contrat
def fill_contrat_timeseries():
    print("⏳ Création séries temporelles par contrat...")
    
    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            f.id_contrat,
            c.contrat,
            COUNT(*) AS y
        FROM 
            fact_offre f
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        JOIN 
            dim_contrat c ON f.id_contrat = c.id_contrat
        GROUP BY 
            d.full_date, f.id_contrat, c.contrat
        ORDER BY 
            d.full_date, f.id_contrat;
    """)
    
    rows = src_cur.fetchall()
    
    for ds, id_contrat, contrat, y in rows:
        dest_cur.execute("""
            INSERT INTO ts_prophet_data (ds, y, segment_type, segment_id, segment_name, granularity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds, segment_type, segment_id, granularity) 
            DO UPDATE SET y = EXCLUDED.y;
        """, (ds, y, 'contrat', id_contrat, contrat, 'daily'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes par contrat insérées.")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# 📡 Remplir par source
def fill_source_timeseries():
    print("⏳ Création séries temporelles par source...")
    
    src_conn = connect_db("offers")
    dest_conn = connect_db("prediction")
    if not src_conn or not dest_conn:
        return

    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()

    src_cur.execute("""
        SELECT 
            d.full_date AS ds,
            f.id_source,
            s.via,
            COUNT(*) AS y
        FROM 
            fact_offre f
        JOIN 
            dim_date d ON f.id_date_publication = d.id_date
        JOIN 
            dim_source s ON f.id_source = s.id_source
        GROUP BY 
            d.full_date, f.id_source, s.via
        ORDER BY 
            d.full_date, f.id_source;
    """)
    
    rows = src_cur.fetchall()
    
    for ds, id_source, via, y in rows:
        dest_cur.execute("""
            INSERT INTO ts_prophet_data (ds, y, segment_type, segment_id, segment_name, granularity)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds, segment_type, segment_id, granularity) 
            DO UPDATE SET y = EXCLUDED.y;
        """, (ds, y, 'source', id_source, via, 'daily'))

    dest_conn.commit()
    print(f"✅ {len(rows)} lignes par source insérées.")

    src_cur.close()
    src_conn.close()
    dest_cur.close()
    dest_conn.close()

# 📅 Remplir les régresseurs externes
def fill_regressors():
    print("⏳ Création des régresseurs externes...")
    
    conn = connect_db("prediction")
    if conn is None:
        return

    cur = conn.cursor()

    # Obtenir la plage de dates des données
    cur.execute("""
        SELECT MIN(ds) as min_date, MAX(ds) as max_date 
        FROM ts_prophet_data;
    """)
    
    result = cur.fetchone()
    if not result or not result[0]:
        print("❌ Aucune donnée trouvée dans ts_prophet_data")
        return
    
    min_date, max_date = result
    
    # Générer les régresseurs pour chaque jour
    current_date = min_date
    while current_date <= max_date:
        # Calculs des indicateurs
        is_weekend = 1 if current_date.weekday() >= 5 else 0
        month_num = current_date.month
        day_of_week = current_date.weekday()
        quarter = (current_date.month - 1) // 3 + 1
        
        # Saisons de recrutement (adaptez selon votre marché)
        rentree_scolaire = 1 if month_num in [9, 10] else 0
        fin_annee = 1 if month_num in [11, 12] else 0
        debut_annee = 1 if month_num in [1, 2] else 0
        vacances_ete = 1 if month_num in [7, 8] else 0
        
        # Jours fériés français (exemple basique)
        is_holiday = 0
        if (month_num == 1 and current_date.day == 1) or \
           (month_num == 5 and current_date.day == 1) or \
           (month_num == 5 and current_date.day == 8) or \
           (month_num == 7 and current_date.day == 14) or \
           (month_num == 8 and current_date.day == 15) or \
           (month_num == 11 and current_date.day == 1) or \
           (month_num == 11 and current_date.day == 11) or \
           (month_num == 12 and current_date.day == 25):
            is_holiday = 1
        
        cur.execute("""
            INSERT INTO ts_regressors 
            (ds, is_weekend, is_holiday, month_num, day_of_week, quarter, 
             rentree_scolaire, fin_annee, debut_annee, vacances_ete)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ds) DO UPDATE SET
                is_weekend = EXCLUDED.is_weekend,
                is_holiday = EXCLUDED.is_holiday,
                month_num = EXCLUDED.month_num,
                day_of_week = EXCLUDED.day_of_week,
                quarter = EXCLUDED.quarter,
                rentree_scolaire = EXCLUDED.rentree_scolaire,
                fin_annee = EXCLUDED.fin_annee,
                debut_annee = EXCLUDED.debut_annee,
                vacances_ete = EXCLUDED.vacances_ete;
        """, (current_date, is_weekend, is_holiday, month_num, day_of_week, quarter,
              rentree_scolaire, fin_annee, debut_annee, vacances_ete))
        
        current_date += timedelta(days=1)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Régresseurs externes créés.")

# 📈 Statistiques des données créées
def show_statistics():
    print("\n📊 Statistiques des données créées:")
    
    conn = connect_db("prediction")
    if conn is None:
        return

    cur = conn.cursor()

    # Stats par type de segment
    cur.execute("""
        SELECT 
            segment_type,
            COUNT(DISTINCT segment_id) as nb_segments,
            COUNT(*) as nb_records,
            MIN(ds) as date_debut,
            MAX(ds) as date_fin,
            AVG(y) as moyenne_quotidienne
        FROM ts_prophet_data
        GROUP BY segment_type
        ORDER BY segment_type;
    """)
    
    results = cur.fetchall()
    
    for segment_type, nb_segments, nb_records, date_debut, date_fin, moyenne in results:
        print(f"📌 {segment_type.upper()}:")
        print(f"   - {nb_segments} segments")
        print(f"   - {nb_records} enregistrements")
        print(f"   - Période: {date_debut} → {date_fin}")
        print(f"   - Moyenne: {moyenne:.1f} offres/jour")
        print()

    # Top segments par volume
    print("🏆 Top 10 segments par volume total:")
    cur.execute("""
        SELECT 
            segment_type,
            segment_name,
            SUM(y) as total_offres,
            COUNT(*) as nb_jours
        FROM ts_prophet_data
        WHERE segment_type != 'global'
        GROUP BY segment_type, segment_name
        ORDER BY total_offres DESC
        LIMIT 10;
    """)
    
    top_segments = cur.fetchall()
    for i, (seg_type, seg_name, total, nb_jours) in enumerate(top_segments, 1):
        print(f"{i:2d}. {seg_type} '{seg_name}': {total} offres ({nb_jours} jours)")

    cur.close()
    conn.close()

# 🔄 Fonction principale d'orchestration
def main():
    print("🚀 Démarrage de la création des séries temporelles Prophet\n")
    
    start_time = datetime.now()
    
    try:
        # 1. Créer la base et les tables
        create_database()
        create_prediction_tables()
        
        # 2. Remplir les différentes séries temporelles
        fill_global_timeseries()
        fill_secteur_timeseries()
        fill_titre_timeseries()
        fill_skill_timeseries()
        fill_contrat_timeseries()
        fill_source_timeseries()
        
        # 3. Créer les régresseurs externes
        fill_regressors()
        
        # 4. Afficher les statistiques
        show_statistics()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n✅ Processus terminé avec succès en {duration:.1f} secondes")
        print("🎯 Base 'prediction' prête pour Prophet!")
        
    except Exception as e:
        print(f"\n❌ Erreur lors du processus: {e}")
        import traceback
        traceback.print_exc()

# ▶ Point d'entrée
if __name__ == "__main__":
    main()
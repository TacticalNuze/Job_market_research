import pandas as pd
import psycopg2
import os
from datetime import datetime

print(f"✅ Script lancé depuis : {__file__}")

EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

def connect_db():
    print("🔌 Connexion vers localhost en cours...")
    conn = psycopg2.connect(
        dbname="prediction",
        user="root",
        password="123456",
        host="localhost",
        port=5432
    )
    conn.set_client_encoding('UTF8')
    return conn

def export_table_to_csv(conn, table_name):
    print(f"📥 Lecture de la table : {table_name}")
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT * FROM {table_name};')
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        df = pd.DataFrame.from_records(rows, columns=colnames)

        date_str = datetime.today().strftime("%Y-%m-%d")
        filename = f"{table_name}_{date_str}.csv"
        filepath = os.path.join(EXPORT_DIR, filename)

        df.to_csv(filepath, index=False, encoding="utf-8")
        print(f"✅ Données exportées depuis '{table_name}' vers '{filepath}'")
    except Exception as e:
        print(f"❌ Erreur lors de l'export de {table_name} :", e)
    finally:
        cur.close()

def main():
    try:
        conn = connect_db()
        export_table_to_csv(conn, "ts_offres")
        export_table_to_csv(conn, "forecast_offres")
        export_table_to_csv(conn, "model_run_log")
        conn.close()
        print("🎉 Tous les fichiers ont été exportés avec succès.")
    except Exception as e:
        print("❌ Une erreur est survenue :", e)

if __name__ == "__main__":
    main()

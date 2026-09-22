def main():
    print("Démarrage du script de prédiction...")

    # === Ton ancien code init.py ici ===
    import os

    import pandas as pd
    from prophet import Prophet
    import psycopg2
    import matplotlib.pyplot as plt
    import seaborn as sns
    from datetime import datetime, timedelta
    import warnings
    warnings.filterwarnings('ignore')

    def connect_db(dbname="prediction"):
        """
        Connexion PostgreSQL
        """
        try:
            conn = psycopg2.connect(
                dbname=dbname,
                user=os.getenv("POSTGRES_USER", "root"),
                password=os.getenv("POSTGRES_PASSWORD", "123456"),
                host=os.getenv("DB_HOST", "postgres"),
                port=os.getenv("DB_PORT", "5432")
            )
            print("Connexion à la base réussie ✅")
            return conn
        except Exception as e:
            print(f"Erreur de connexion : {e}")
            return None

    # Connexion à la base
    conn = connect_db()
    if conn is None:
        print("Impossible de continuer sans connexion à la base.")
        return

    # Exemple de requête
    query = "SELECT * FROM ta_table LIMIT 5;"
    df = pd.read_sql(query, conn)
    print(df)

    conn.close()
    print("Prédiction terminée ✅")


if __name__ == "__main__":
    main()

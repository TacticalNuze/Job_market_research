import pandas as pd
from prophet import Prophet
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# 🔌 Connexion PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="prediction",
            user="root",
            password="123456",
            host="postgres",
            port=5432
        )
        print("✅ Connexion réussie à la base 'prediction'")
        return conn
    except Exception as e:
        print("❌ Erreur de connexion à la base 'prediction' :", repr(e))
        return None

# 📥 Charger les séries par id_titre
def get_ts_data_by_title(conn):
    query = """
        SELECT ds, y, id_titre 
        FROM ts_offres 
        WHERE id_titre IS NOT NULL
        ORDER BY ds;
    """
    return pd.read_sql(query, conn)

# 🔮 Lancer Prophet et insérer les prévisions
def forecast_and_save(conn, df, id_titre, horizon=30):
    # Format pour Prophet
    df_prophet = df.rename(columns={"ds": "ds", "y": "y"})[["ds", "y"]]
    if len(df_prophet) < 10:
        print(f"⚠️ Pas assez de données pour id_titre={id_titre}")
        return

    model = Prophet()
    model.fit(df_prophet)

    future = model.make_future_dataframe(periods=horizon)
    forecast = model.predict(future)

    forecast_rows = []
    for _, row in forecast.tail(horizon).iterrows():
        forecast_rows.append((
            row['ds'].date(),              # ds
            row['yhat'],                   # yhat
            row['yhat_lower'],             # yhat_lower
            row['yhat_upper'],             # yhat_upper
            id_titre,                      # id_titre
            None,                          # id_skill
            datetime.now(),                # generated_at
            "v1"                           # model_version
        ))

    insert_query = """
        INSERT INTO forecast_offres (
            ds, yhat, yhat_lower, yhat_upper, 
            id_titre, id_skill, generated_at, model_version
        )
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        execute_values(cur, insert_query, forecast_rows)
    conn.commit()
    print(f"✅ Prédiction insérée pour id_titre={id_titre}")

# ▶️ Point d'entrée
def main():
    conn = connect_db()
    if conn is None:
        return

    all_data = get_ts_data_by_title(conn)

    if all_data.empty:
        print("❌ Aucune donnée trouvée dans ts_offres.")
        conn.close()
        return

    for id_titre, group in all_data.groupby("id_titre"):
        forecast_and_save(conn, group, id_titre)

    conn.close()
    print("✅ Prédictions terminées.")

if __name__ == "__main__":
    main()

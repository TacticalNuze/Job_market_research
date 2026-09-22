import argparse
import os

import pandas as pd
import psycopg2
from prophet import Prophet

# Paramètres de connexion à ta DB
DB_PARAMS = {
    "dbname": os.getenv("PREDICTION_DB", "prediction"),
    "user": os.getenv("POSTGRES_USER", "root"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

# Valeurs autorisées par la contrainte CHECK de ts_prophet_data (manage_tables.py)
SEGMENT_TYPES = ("global", "secteur", "titre", "skill", "contrat", "source")


def load_training_data(segment_type, segment_id, granularity="daily"):
    """Charge une série temporelle au format Prophet (ds, y) depuis ts_prophet_data.

    Les séries sont stockées en format long : une série par couple
    (segment_type, segment_id). Il n'y a pas de colonne id_titre — une série par
    titre correspond à segment_type='titre' et segment_id=<id_titre>.

    Le filtre sur segment_type est indispensable : seul le quadruplet
    (ds, segment_type, segment_id, granularity) est unique, donc un id_titre 123
    et un id_skill 123 peuvent coexister.
    """
    conn = psycopg2.connect(**DB_PARAMS)
    query = """
        SELECT ds, y
        FROM ts_prophet_data
        WHERE segment_type = %s
          AND segment_id = %s
          AND granularity = %s
        ORDER BY ds
    """
    df = pd.read_sql(query, conn, params=(segment_type, segment_id, granularity))
    conn.close()
    return df


def train_prophet_model(df):
    model = Prophet()
    model.fit(df)
    return model


def parse_args():
    parser = argparse.ArgumentParser(
        description="Entraîne un modèle Prophet sur une série de ts_prophet_data."
    )
    parser.add_argument(
        "--segment-type",
        default="global",
        choices=SEGMENT_TYPES,
        help="Type de segment à prédire (défaut: global).",
    )
    parser.add_argument(
        "--segment-id",
        type=int,
        default=0,
        help="Identifiant du segment. Pour 'global' c'est toujours 0 (défaut: 0).",
    )
    parser.add_argument(
        "--granularity",
        default="daily",
        choices=("daily", "weekly", "monthly"),
        help="Granularité de la série (défaut: daily).",
    )
    parser.add_argument(
        "--periods",
        type=int,
        default=30,
        help="Horizon de prévision en jours (défaut: 30).",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print(
        f"Chargement des données pour segment_type={args.segment_type}, "
        f"segment_id={args.segment_id}, granularity={args.granularity}..."
    )
    df = load_training_data(args.segment_type, args.segment_id, args.granularity)

    # Prophet exige au moins deux observations non nulles.
    if len(df) < 2:
        print(
            f"❌ Série insuffisante pour l'entraînement : {len(df)} ligne(s) trouvée(s), "
            "il en faut au moins 2. Vérifiez que manage_tables.py a bien été exécuté."
        )
        return

    print(f"Données chargées: {len(df)} lignes")

    print("Entraînement du modèle Prophet...")
    model = train_prophet_model(df)

    print("Modèle entraîné avec succès !")

    # Prévision sur l'horizon demandé
    future = model.make_future_dataframe(periods=args.periods)
    forecast = model.predict(future)
    print(forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail())


if __name__ == "__main__":
    main()

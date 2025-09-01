import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine
from datetime import datetime
import plotly.express as px
import warnings
import os

warnings.filterwarnings("ignore")


class ProphetJobPredictor:
    """
    Classe pour charger des séries temporelles depuis une base de données,
    entraîner des modèles Prophet par segment, générer des prédictions,
    exporter les résultats en CSV et visualiser les prédictions.
    """

    def __init__(self, db_uri="postgresql+psycopg2://root:123456@postgres:5432/prediction"):
        self.engine = create_engine(db_uri)
        self.models = {}      # Dictionnaire des modèles Prophet par segment
        self.forecasts = {}   # Dictionnaire des prévisions par segment
        print("✅ ProphetJobPredictor initialisé")

    def load_timeseries_data(self, segment_type='global', min_data_points=8, exclude_segments=[]):
        """
        Charge les séries temporelles depuis la table 'ts_prophet_data' pour un type de segment.
        """
        print(f"📊 Chargement des données pour segment_type='{segment_type}'")
        query = f"SELECT * FROM ts_prophet_data WHERE segment_type='{segment_type}'"
        df = pd.read_sql(query, self.engine)

        if df.empty:
            print("❌ Aucune donnée trouvée")
            return {}

        # Filtrer les segments exclus
        df = df[~df['segment_id'].isin(exclude_segments)]
        segment_dict = {}

        for segment_id, seg_df in df.groupby('segment_id'):
            seg_df = seg_df[['ds', 'y']].dropna()
            seg_df['ds'] = pd.to_datetime(seg_df['ds'])
            if len(seg_df) >= min_data_points:
                segment_dict[segment_id] = seg_df
            else:
                print(f"⚠️ Segment {segment_id} ignoré (trop peu de points)")

        return segment_dict

    def train_prophet_models(self, segment_data):
        """
        Entraîne un modèle Prophet pour chaque segment.
        """
        for segment_id, df in segment_data.items():
            model = Prophet(daily_seasonality=True)
            model.fit(df)
            self.models[segment_id] = model
            print(f"✅ Modèle {segment_id} entraîné")

    def make_predictions(self, horizon_days=14):
        """
        Génère des prédictions pour chaque modèle entraîné sur un horizon donné.
        """
        for segment_id, model in self.models.items():
            future = model.make_future_dataframe(periods=horizon_days)
            forecast = model.predict(future)
            self.forecasts[segment_id] = forecast
            print(f"🔮 Prédictions générées: {segment_id} ({len(forecast)} points)")

    def export_predictions_csv(self):
        """
        Exporte toutes les prédictions en un fichier CSV.
        """
        all_predictions = []
        for segment_id, forecast in self.forecasts.items():
            for _, row in forecast.iterrows():
                all_predictions.append({
                    'segment': segment_id,
                    'date': row['ds'].date(),
                    'prediction': row['yhat'],
                    'lower_bound': row['yhat_lower'],
                    'upper_bound': row['yhat_upper']
                })

        if all_predictions:
            df_export = pd.DataFrame(all_predictions)
            project_dir = r"C:\Users\user\Desktop\DXC\Job_market_research\prediction"
            os.makedirs(project_dir, exist_ok=True)
            filename = os.path.join(
                project_dir, f"predictions_prophet_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            )
            df_export.to_csv(filename, index=False)
            print(f"✅ {len(all_predictions)} prédictions exportées vers {filename}")
            return df_export
        else:
            print("❌ Aucune prédiction à exporter")
            return None

    def plot_predictions(self, df_export):
        """
        Affiche un graphique des prédictions par segment.
        """
        if df_export is None or df_export.empty:
            print("❌ Rien à afficher")
            return

        fig = px.line(
            df_export, x='date', y='prediction', color='segment',
            error_y=df_export['upper_bound'] - df_export['prediction'],
            title="🔮 Prédictions Prophet par segment"
        )
        fig.show()


# === SCRIPT PRINCIPAL ===
if __name__ == "__main__":
    predictor = ProphetJobPredictor()

    # Segments à exclure
    exclude_segments = ['secteur_5', 'contrat_99']

    # Charger les données globales
    segment_data = predictor.load_timeseries_data(segment_type='global', exclude_segments=exclude_segments)

    # Charger d'autres types de segments
    for stype in ['secteur', 'contrat', 'source']:
        data = predictor.load_timeseries_data(segment_type=stype, exclude_segments=exclude_segments)
        segment_data.update(data)

    # Entraînement et prédictions
    predictor.train_prophet_models(segment_data)
    predictor.make_predictions(horizon_days=14)

    # Export CSV et affichage graphique
    df_export = predictor.export_predictions_csv()
    predictor.plot_predictions(df_export)



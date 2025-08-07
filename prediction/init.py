# import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# # 📦 Connexion PostgreSQL
# def connect_db(dbname="offers"):
#     try:
#         conn = psycopg2.connect(
#             dbname=dbname,
#             user="root",
#             password="123456",
#             host="postgres",
#             port=5432
#         )
#         print(f"✅ Connexion réussie à la base '{dbname}'")
#         return conn
#     except Exception as e:
#         print(f"❌ Erreur de connexion à la base '{dbname}' :", repr(e))
#         return None

# # 🏗️ Créer base 'prediction' si non existante
# def create_database():
#     conn = connect_db("postgres")
#     if conn is None:
#         return
#     conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
#     cur = conn.cursor()
#     cur.execute("SELECT 1 FROM pg_database WHERE datname = 'prediction'")
#     exists = cur.fetchone()
#     if not exists:
#         cur.execute("CREATE DATABASE prediction")
#         print("✅ Base 'prediction' créée.")
#     else:
#         print("ℹ️ Base 'prediction' existe déjà.")
#     cur.close()
#     conn.close()

# # 🧱 Créer les tables
# def create_prediction_tables():
#     conn = connect_db("prediction")
#     if conn is None:
#         return
#     cur = conn.cursor()

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS ts_offres (
#             id SERIAL PRIMARY KEY,
#             ds DATE NOT NULL,
#             y INTEGER NOT NULL,
#             id_titre INTEGER,
#             id_skill INTEGER,
#             granularity TEXT,
#             source TEXT,
#             UNIQUE (ds, id_titre, id_skill)
#         );
#     """)

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS forecast_offres (
#             id SERIAL PRIMARY KEY,
#             ds DATE NOT NULL,
#             yhat FLOAT,
#             yhat_lower FLOAT,
#             yhat_upper FLOAT,
#             id_titre INTEGER,
#             id_skill INTEGER,
#             generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             model_version TEXT
#         );
#     """)

#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS model_run_log (
#             id SERIAL PRIMARY KEY,
#             model_target TEXT,
#             id_target INTEGER,
#             generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             horizon INTEGER,
#             status TEXT,
#             message TEXT
#         );
#     """)

#     conn.commit()
#     cur.close()
#     conn.close()
#     print("✅ Tables créées dans la base 'prediction'.")

# # 📊 Remplir ts_offres par id_titre
# def fill_ts_offres_from_fact_offre():
#     print("⏳ Remplissage par titre depuis offers.fact_offre...")

#     src_conn = connect_db("offers")
#     dest_conn = connect_db("prediction")
#     if not src_conn or not dest_conn:
#         return

#     src_cur = src_conn.cursor()
#     dest_cur = dest_conn.cursor()

#     src_cur.execute("""
#         SELECT 
#             d.full_date AS ds,
#             f.id_titre,
#             COUNT(*) AS y
#         FROM 
#             fact_offre f
#         JOIN 
#             dim_date d ON f.id_date_publication = d.id_date
#         GROUP BY 
#             d.full_date, f.id_titre
#         ORDER BY 
#             d.full_date;
#     """)
#     rows = src_cur.fetchall()

#     for ds, id_titre, y in rows:
#         dest_cur.execute("""
#             INSERT INTO ts_offres (ds, y, id_titre, id_skill, granularity, source)
#             VALUES (%s, %s, %s, NULL, %s, %s)
#             ON CONFLICT (ds, id_titre, id_skill) DO NOTHING;
#         """, (ds, y, id_titre, 'jour', 'offers'))

#     dest_conn.commit()
#     print(f"✅ {len(rows)} lignes insérées (par titre) dans ts_offres.")

#     src_cur.close()
#     src_conn.close()
#     dest_cur.close()
#     dest_conn.close()

# # 🧠 Remplir ts_offres par id_skill
# def fill_ts_offres_from_skills():
#     print("⏳ Remplissage par compétence depuis offre_skill...")

#     src_conn = connect_db("offers")
#     dest_conn = connect_db("prediction")
#     if not src_conn or not dest_conn:
#         return

#     src_cur = src_conn.cursor()
#     dest_cur = dest_conn.cursor()

#     src_cur.execute("""
#     SELECT 
#         d.full_date AS ds,
#         os.id_skill,
#         COUNT(*) AS y
#     FROM 
#         offre_skill os
#     JOIN 
#         fact_offre f ON os.id_offer = f.id_offer
#     JOIN 
#         dim_date d ON f.id_date_publication = d.id_date
#     GROUP BY 
#         d.full_date, os.id_skill
#     ORDER BY 
#         d.full_date;
# """)


#     rows = src_cur.fetchall()

#     for ds, id_skill, y in rows:
#         dest_cur.execute("""
#             INSERT INTO ts_offres (ds, y, id_titre, id_skill, granularity, source)
#             VALUES (%s, %s, NULL, %s, %s, %s)
#             ON CONFLICT (ds, id_titre, id_skill) DO NOTHING;
#         """, (ds, y, id_skill, 'jour', 'offers'))

#     dest_conn.commit()
#     print(f"✅ {len(rows)} lignes insérées (par skill) dans ts_offres.")

#     src_cur.close()
#     src_conn.close()
#     dest_cur.close()
#     dest_conn.close()

# # ▶️ Point d’entrée
# if __name__ == "__main__":
#     create_database()
#     create_prediction_tables()
#     fill_ts_offres_from_fact_offre()
#     fill_ts_offres_from_skills()

import pandas as pd
import numpy as np
from prophet import Prophet
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import warnings
warnings.filterwarnings('ignore')

def connect():
    """
    Établit une connexion PostgreSQL.
    """
    return psycopg2.connect(
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
        host=os.environ.get("POSTGRES_HOST"),
        database=os.environ.get("POSTGRES_DB", "postgres"),
        port=os.environ.get("POSTGRES_PORT", 5432)
    )

class JobOfferPredictor:
    def __init__(self):
        """
        Initialise le prédicteur - utilise la fonction connect() existante
        """
        self.models = {}
        self.predictions = {}
        
    def connect_db(self):
        """Connexion à la base PostgreSQL"""
        return connect()
    
    def extract_time_series_data(self, granularity='daily', filters=None):
        """
        Extrait les données temporelles de la base
        
        granularity: 'daily', 'weekly', 'monthly'
        filters: dict avec secteur, contrat, titre, etc.
        """
        
        # Construction de la requête selon la granularité
        if granularity == 'daily':
            date_group = "d.full_date"
            date_select = "d.full_date as ds"
        elif granularity == 'weekly':
            date_group = "DATE_TRUNC('week', d.full_date)"
            date_select = "DATE_TRUNC('week', d.full_date) as ds"
        elif granularity == 'monthly':
            date_group = "DATE_TRUNC('month', d.full_date)"
            date_select = "DATE_TRUNC('month', d.full_date) as ds"
        
        base_query = f"""
        SELECT 
            {date_select},
            COUNT(f.id_offer) as y,
            c.secteur,
            cont.contrat,
            t.titre,
            s.via as source
        FROM fact_offre f
        JOIN dim_date d ON f.id_date_publication = d.id_date
        JOIN dim_compagnie c ON f.id_compagnie = c.id_compagnie
        JOIN dim_contrat cont ON f.id_contrat = cont.id_contrat
        JOIN dim_titre t ON f.id_titre = t.id_titre
        JOIN dim_source s ON f.id_source = s.id_source
        """
        
        # Ajout des filtres
        where_conditions = []
        if filters:
            if 'secteur' in filters:
                where_conditions.append(f"c.secteur = '{filters['secteur']}'")
            if 'contrat' in filters:
                where_conditions.append(f"cont.contrat = '{filters['contrat']}'")
            if 'titre' in filters:
                where_conditions.append(f"t.titre = '{filters['titre']}'")
            if 'date_min' in filters:
                where_conditions.append(f"d.full_date >= '{filters['date_min']}'")
        
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        base_query += f" GROUP BY {date_group}, c.secteur, cont.contrat, t.titre, s.via ORDER BY ds"
        
        conn = self.connect_db()
        df = pd.read_sql(base_query, conn)
        conn.close()
        
        return df
    
    def prepare_prophet_data(self, df, segment_by=None):
        """
        Prépare les données pour Prophet
        
        segment_by: colonne pour segmenter les prédictions ('secteur', 'contrat', etc.)
        """
        if segment_by:
            segments = df[segment_by].unique()
            data_segments = {}
            
            for segment in segments:
                segment_data = df[df[segment_by] == segment].groupby('ds')['y'].sum().reset_index()
                segment_data['ds'] = pd.to_datetime(segment_data['ds'])
                data_segments[segment] = segment_data
            
            return data_segments
        else:
            # Données globales
            global_data = df.groupby('ds')['y'].sum().reset_index()
            global_data['ds'] = pd.to_datetime(global_data['ds'])
            return global_data
    
    def add_external_regressors(self, df):
        """
        Ajoute des régresseurs externes (optionnel)
        - Indicateurs économiques
        - Jours fériés
        - Événements spéciaux
        """
        # Exemple: ajouter des indicateurs cycliques
        df['month'] = df['ds'].dt.month
        df['day_of_week'] = df['ds'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        
        # Saisons de recrutement (exemple français)
        df['rentree_scolaire'] = ((df['month'] == 9) | (df['month'] == 10)).astype(int)
        df['fin_annee'] = ((df['month'] == 11) | (df['month'] == 12)).astype(int)
        df['debut_annee'] = ((df['month'] == 1) | (df['month'] == 2)).astype(int)
        
        return df
    
    def train_prophet_models(self, data, add_regressors=True, custom_seasonalities=True):
        """
        Entraîne les modèles Prophet
        """
        if isinstance(data, dict):
            # Modèles segmentés
            for segment_name, segment_data in data.items():
                print(f"Entraînement du modèle pour: {segment_name}")
                
                model = Prophet(
                    yearly_seasonality=True,
                    weekly_seasonality=True,
                    daily_seasonality=False,  # Généralement trop de bruit pour les offres d'emploi
                    changepoint_prior_scale=0.05,  # Flexibilité pour les changements de tendance
                    seasonality_prior_scale=10.0
                )
                
                # Ajout de saisonnalités personnalisées
                if custom_seasonalities:
                    model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
                    model.add_seasonality(name='quarterly', period=91.25, fourier_order=8)
                
                # Préparation des données avec régresseurs
                if add_regressors:
                    segment_data = self.add_external_regressors(segment_data)
                    model.add_regressor('is_weekend')
                    model.add_regressor('rentree_scolaire')
                    model.add_regressor('fin_annee')
                    model.add_regressor('debut_annee')
                
                model.fit(segment_data)
                self.models[segment_name] = model
                
        else:
            # Modèle global
            print("Entraînement du modèle global")
            
            model = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                changepoint_prior_scale=0.05,
                seasonality_prior_scale=10.0
            )
            
            if custom_seasonalities:
                model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
                model.add_seasonality(name='quarterly', period=91.25, fourier_order=8)
            
            if add_regressors:
                data = self.add_external_regressors(data)
                model.add_regressor('is_weekend')
                model.add_regressor('rentree_scolaire')
                model.add_regressor('fin_annee')
                model.add_regressor('debut_annee')
            
            model.fit(data)
            self.models['global'] = model
    
    def make_predictions(self, periods=90, freq='D'):
        """
        Génère les prédictions
        
        periods: nombre de périodes à prédire
        freq: fréquence ('D' daily, 'W' weekly, 'M' monthly)
        """
        for model_name, model in self.models.items():
            print(f"Génération des prédictions pour: {model_name}")
            
            # Création du dataframe futur
            future = model.make_future_dataframe(periods=periods, freq=freq)
            
            # Ajout des régresseurs pour les données futures
            if any(regressor in model.extra_regressors for regressor in ['is_weekend', 'rentree_scolaire']):
                future = self.add_external_regressors(future)
            
            # Prédiction
            forecast = model.predict(future)
            
            self.predictions[model_name] = {
                'forecast': forecast,
                'model': model
            }
    
    def plot_predictions(self, model_name='global', save_path=None):
        """
        Visualise les prédictions
        """
        if model_name not in self.predictions:
            print(f"Aucune prédiction trouvée pour {model_name}")
            return
        
        model = self.predictions[model_name]['model']
        forecast = self.predictions[model_name]['forecast']
        
        # Graphique principal
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'Prédictions Prophet - {model_name}', fontsize=16)
        
        # Graphique 1: Série temporelle complète
        ax1 = axes[0, 0]
        model.plot(forecast, ax=ax1)
        ax1.set_title('Prédiction complète')
        ax1.set_ylabel('Nombre d\'offres')
        
        # Graphique 2: Composants
        ax2 = axes[0, 1]
        model.plot_components(forecast, ax=ax2)
        
        # Graphique 3: Zoom sur les prédictions futures
        ax3 = axes[1, 0]
        last_date = forecast['ds'].iloc[-periods-30]  # 30 jours avant les prédictions
        future_data = forecast[forecast['ds'] >= last_date]
        
        ax3.plot(future_data['ds'], future_data['yhat'], 'b-', label='Prédiction')
        ax3.fill_between(future_data['ds'], 
                        future_data['yhat_lower'], 
                        future_data['yhat_upper'], 
                        alpha=0.3, color='blue', label='Intervalle de confiance')
        ax3.axvline(x=forecast['ds'].iloc[-periods], color='red', linestyle='--', label='Début prédictions')
        ax3.set_title('Zoom sur les prédictions futures')
        ax3.legend()
        ax3.tick_params(axis='x', rotation=45)
        
        # Graphique 4: Métriques de performance
        ax4 = axes[1, 1]
        if 'y' in forecast.columns:  # Si on a les vraies valeurs
            residuals = forecast['y'] - forecast['yhat']
            ax4.hist(residuals.dropna(), bins=30, alpha=0.7)
            ax4.set_title('Distribution des résidus')
            ax4.set_xlabel('Résidus')
        else:
            # Sinon, afficher les tendances saisonnières
            seasonal_data = forecast.groupby(forecast['ds'].dt.month)['yhat'].mean()
            ax4.bar(range(1, 13), seasonal_data)
            ax4.set_title('Tendance saisonnière mensuelle')
            ax4.set_xlabel('Mois')
            ax4.set_xticks(range(1, 13))
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        plt.show()
    
    def get_insights(self, model_name='global'):
        """
        Extrait des insights des prédictions
        """
        if model_name not in self.predictions:
            return None
        
        forecast = self.predictions[model_name]['forecast']
        
        insights = {
            'croissance_prevue': {
                'prochains_30_jours': forecast.tail(30)['yhat'].mean(),
                'prochains_90_jours': forecast.tail(90)['yhat'].mean(),
            },
            'tendance': {
                'direction': 'hausse' if forecast['trend'].iloc[-1] > forecast['trend'].iloc[-30] else 'baisse',
                'intensite': abs(forecast['trend'].iloc[-1] - forecast['trend'].iloc[-30])
            },
            'saisonnalite': {
                'pic_mensuel': forecast.groupby(forecast['ds'].dt.month)['yhat'].mean().idxmax(),
                'creux_mensuel': forecast.groupby(forecast['ds'].dt.month)['yhat'].mean().idxmin()
            },
            'volatilite': forecast['yhat'].std()
        }
        
        return insights

# Exemple d'utilisation
if __name__ == "__main__":
    # Configuration de la base de données
    db_config = {
        'host': 'localhost',
        'database': 'job_offers',
        'user': 'your_username',
        'password': 'your_password',
        'port': 5432
    }
    
    # Initialisation du prédicteur
    predictor = JobOfferPredictor(db_config)
    
    # Extraction des données (exemple: prédiction par secteur)
    print("Extraction des données...")
    raw_data = predictor.extract_time_series_data(
        granularity='daily',
        filters={'date_min': '2023-01-01'}
    )
    
    # Préparation des données segmentées par secteur
    print("Préparation des données...")
    segmented_data = predictor.prepare_prophet_data(raw_data, segment_by='secteur')
    
    # Entraînement des modèles
    print("Entraînement des modèles Prophet...")
    predictor.train_prophet_models(segmented_data, add_regressors=True)
    
    # Génération des prédictions (90 jours)
    print("Génération des prédictions...")
    predictor.make_predictions(periods=90, freq='D')
    
    # Visualisation
    for sector in segmented_data.keys():
        predictor.plot_predictions(sector)
        insights = predictor.get_insights(sector)
        print(f"\nInsights pour {sector}:")
        print(f"- Croissance prévue (30j): {insights['croissance_prevue']['prochains_30_jours']:.1f} offres/jour")
        print(f"- Tendance: {insights['tendance']['direction']}")
        print(f"- Pic mensuel: mois {insights['saisonnalite']['pic_mensuel']}")
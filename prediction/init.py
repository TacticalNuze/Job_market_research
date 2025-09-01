import pandas as pd
import numpy as np
from prophet import Prophet
import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
import sys

warnings.filterwarnings('ignore')

def connect_db(dbname="offers"):
    """
    Connexion PostgreSQL utilisant votre configuration
    """
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user="root",
            password="123456",
            host="postgres",  # change en "postgres" si tu es dans Docker
            port=5432
        )
        print(f"✅ Connexion réussie à la base '{dbname}'")
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion à la base '{dbname}' :", repr(e))
        return None

class JobOfferPredictor:
    def __init__(self):
        """
        Initialise le prédicteur utilisant votre architecture PostgreSQL
        """
        self.models = {}
        self.predictions = {}
        
    def extract_titre_data(self, id_titre=None, date_min=None):
        conn = connect_db("prediction")
        if conn is None:
            return pd.DataFrame()
        
        query = """
        SELECT ds, y, id_titre
        FROM ts_offres 
        WHERE id_titre IS NOT NULL
        """
        
        params = []
        if id_titre:
            query += " AND id_titre = %s"
            params.append(id_titre)
        
        if date_min:
            query += " AND ds >= %s"
            params.append(date_min)
        
        query += " ORDER BY ds"
        
        df = pd.read_sql(query, conn, params=params if params else None)
        conn.close()
        
        if not df.empty:
            df['ds'] = pd.to_datetime(df['ds'])
        
        return df
    
    def extract_skill_data(self, id_skill=None, date_min=None):
        conn = connect_db("prediction")
        if conn is None:
            return pd.DataFrame()
        
        query = """
        SELECT ds, y, id_skill
        FROM ts_offres 
        WHERE id_skill IS NOT NULL
        """
        
        params = []
        if id_skill:
            query += " AND id_skill = %s"
            params.append(id_skill)
        
        if date_min:
            query += " AND ds >= %s"
            params.append(date_min)
        
        query += " ORDER BY ds"
        
        df = pd.read_sql(query, conn, params=params if params else None)
        conn.close()
        
        if not df.empty:
            df['ds'] = pd.to_datetime(df['ds'])
        
        return df
    
    def get_top_titres(self, limit=10):
        conn = connect_db("prediction")
        if conn is None:
            return []
        
        query = """
        SELECT 
            t.id_titre,
            dt.titre,
            SUM(t.y) as total_offres
        FROM ts_offres t
        JOIN offers.dim_titre dt ON t.id_titre = dt.id_titre
        WHERE t.id_titre IS NOT NULL
        GROUP BY t.id_titre, dt.titre
        ORDER BY total_offres DESC
        LIMIT %s
        """
        
        df = pd.read_sql(query, conn, params=[limit])
        conn.close()
        
        return df.to_dict('records')
    
    def get_top_skills(self, limit=10):
        conn = connect_db("prediction")
        if conn is None:
            return []
        
        query = """
        SELECT 
            s.id_skill,
            ds.nom as skill_name,
            ds.type_skill,
            SUM(s.y) as total_offres
        FROM ts_offres s
        JOIN offers.dim_skill ds ON s.id_skill = ds.id_skill
        WHERE s.id_skill IS NOT NULL
        GROUP BY s.id_skill, ds.nom, ds.type_skill
        ORDER BY total_offres DESC
        LIMIT %s
        """
        
        df = pd.read_sql(query, conn, params=[limit])
        conn.close()
        
        return df.to_dict('records')
    
    def prepare_prophet_data(self, df, segment_column=None):
        if segment_column and segment_column in df.columns:
            segments = df[segment_column].unique()
            data_segments = {}
            
            for segment in segments:
                segment_data = df[df[segment_column] == segment][['ds', 'y']].copy()
                segment_data = segment_data.groupby('ds')['y'].sum().reset_index()
                segment_data['ds'] = pd.to_datetime(segment_data['ds'])
                
                if len(segment_data) >= 30:
                    data_segments[segment] = segment_data
            
            return data_segments
        else:
            global_data = df.groupby('ds')['y'].sum().reset_index()
            global_data['ds'] = pd.to_datetime(global_data['ds'])
            return global_data
    
    def add_business_regressors(self, df):
        df = df.copy()
        df['month'] = df['ds'].dt.month
        df['day_of_week'] = df['ds'].dt.dayofweek
        df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
        df['is_monday'] = (df['day_of_week'] == 0).astype(int)
        
        df['rentree_septembre'] = (df['month'] == 9).astype(int)
        df['fin_annee'] = ((df['month'] == 11) | (df['month'] == 12)).astype(int)
        df['debut_annee'] = ((df['month'] == 1) | (df['month'] == 2)).astype(int)
        df['printemps'] = ((df['month'] >= 3) & (df['month'] <= 5)).astype(int)
        
        return df
    
    def train_prophet_model(self, data, model_name, add_regressors=True):
        print(f"🤖 Entraînement du modèle: {model_name}")
        
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
            seasonality_prior_scale=10.0,
            interval_width=0.95
        )
        
        model.add_seasonality(name='monthly', period=30.5, fourier_order=5)
        model.add_seasonality(name='quarterly', period=91.25, fourier_order=8)
        
        if add_regressors:
            data = self.add_business_regressors(data)
            regressors = ['is_weekend', 'is_monday', 'rentree_septembre', 
                         'fin_annee', 'debut_annee', 'printemps']
            for regressor in regressors:
                model.add_regressor(regressor)
        
        model.fit(data)
        self.models[model_name] = model
        
        return model
    
    def make_predictions(self, model_name, periods=30, freq='D'):
        if model_name not in self.models:
            print(f"❌ Modèle '{model_name}' non trouvé")
            return None
        
        model = self.models[model_name]
        future = model.make_future_dataframe(periods=periods, freq=freq)
        
        # Ajout des régresseurs dans futur
        future = self.add_business_regressors(future)
        
        forecast = model.predict(future)
        
        self.predictions[model_name] = {
            'forecast': forecast,
            'model': model
        }
        
        return forecast
    
    def save_predictions_to_db(self, model_name, id_titre=None, id_skill=None):
        if model_name not in self.predictions:
            print(f"❌ Aucune prédiction pour '{model_name}'")
            return
        
        forecast = self.predictions[model_name]['forecast']
        last_actual_date = forecast[forecast['y'].notna()]['ds'].max()
        future_predictions = forecast[forecast['ds'] > last_actual_date].copy()
        
        conn = connect_db("prediction")
        if conn is None:
            return
        
        cur = conn.cursor()
        
        if id_titre:
            cur.execute("DELETE FROM forecast_offres WHERE id_titre = %s", (id_titre,))
        elif id_skill:
            cur.execute("DELETE FROM forecast_offres WHERE id_skill = %s", (id_skill,))
        
        for _, row in future_predictions.iterrows():
            cur.execute("""
                INSERT INTO forecast_offres 
                (ds, yhat, yhat_lower, yhat_upper, id_titre, id_skill, model_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                row['ds'].date(),
                float(row['yhat']),
                float(row['yhat_lower']),
                float(row['yhat_upper']),
                id_titre,
                id_skill,
                f"prophet_{datetime.now().strftime('%Y%m%d')}"
            ))
        
        conn.commit()
        cur.close()
        conn.close()
        
        print(f"✅ {len(future_predictions)} prédictions sauvées pour {model_name}")
    
    def log_model_run(self, model_target, id_target, horizon, status, message=""):
        conn = connect_db("prediction")
        if conn is None:
            return
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO model_run_log (model_target, id_target, horizon, status, message)
            VALUES (%s, %s, %s, %s, %s)
        """, (model_target, id_target, horizon, status, message))
        
        conn.commit()
        cur.close()
        conn.close()
    
    def plot_predictions(self, model_name, save_path=None):
        if model_name not in self.predictions:
            print(f"❌ Aucune prédiction pour '{model_name}'")
            return
        
        model = self.predictions[model_name]['model']
        forecast = self.predictions[model_name]['forecast']
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle(f'Prédictions Prophet - {model_name}', fontsize=16)
        
        ax1 = axes[0, 0]
        model.plot(forecast, ax=ax1)
        ax1.set_title('Prédiction complète')
        ax1.set_ylabel('Nombre d\'offres')
        
        ax2 = axes[0, 1]
        comp_fig = model.plot_components(forecast)
        ax2.text(0.5, 0.5, 'Voir graphique\ndes composants\nci-dessous', 
                ha='center', va='center', transform=ax2.transAxes)
        ax2.set_title('Composants (voir fenêtre séparée)')
        
        ax3 = axes[1, 0]
        last_actual = forecast[forecast['y'].notna()]['ds'].max()
        recent_data = forecast[forecast['ds'] >= last_actual - timedelta(days=30)]
        
        ax3.plot(recent_data['ds'], recent_data['yhat'], 'b-', label='Prédiction')
        ax3.fill_between(recent_data['ds'], 
                        recent_data['yhat_lower'], 
                        recent_data['yhat_upper'], 
                        alpha=0.3, color='blue', label='Intervalle de confiance')
        
        if 'y' in recent_data.columns:
            actual_data = recent_data[recent_data['y'].notna()]
            ax3.scatter(actual_data['ds'], actual_data['y'], 
                       color='red', s=20, label='Valeurs réelles')
        
        ax3.axvline(x=last_actual, color='red', linestyle='--', label='Début prédictions')
        ax3.set_title('Zoom sur les prédictions futures')
        ax3.legend()
        ax3.tick_params(axis='x', rotation=45)
        
        ax4 = axes[1, 1]
        future_data = forecast[forecast['ds'] > last_actual]
        monthly_pred = future_data.groupby(future_data['ds'].dt.month)['yhat'].mean()
        
        if len(monthly_pred) > 0:
            months = list(monthly_pred.index)
            values = list(monthly_pred.values)
            ax4.bar(months, values, alpha=0.7)
            ax4.set_title('Prédictions moyennes par mois')
            ax4.set_xlabel('Mois')
            ax4.set_ylabel('Offres/jour')
        else:
            ax4.text(0.5, 0.5, 'Pas assez de\nprédictions futures\npour l\'analyse', 
                    ha='center', va='center', transform=ax4.transAxes)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        
        plt.show()
        
        model.plot_components(forecast)
        plt.show()

def predict_top_titres(limit=5, periods=30):
    predictor = JobOfferPredictor()
    
    top_titres = predictor.get_top_titres(limit=limit)
    print(f"📊 Top {limit} titres identifiés")
    
    for titre_info in top_titres:
        id_titre = titre_info['id_titre']
        titre_name = titre_info['titre']
        total_offres = titre_info['total_offres']
        
        print(f"\n🎯 Traitement: {titre_name} ({total_offres} offres)")
        
        try:
            data = predictor.extract_titre_data(
                id_titre=id_titre, 
                date_min='2023-01-01'
            )
            
            if len(data) < 30:
                print(f"⚠️ Pas assez de données ({len(data)} points)")
                predictor.log_model_run('titre', id_titre, periods, 'SKIP', 'Données insuffisantes')
                continue
            
            model_name = f"titre_{id_titre}"
            predictor.train_prophet_model(data, model_name)
            forecast = predictor.make_predictions(model_name, periods=periods)
            predictor.save_predictions_to_db(model_name, id_titre=id_titre)
            predictor.plot_predictions(model_name)
            predictor.log_model_run('titre', id_titre, periods, 'SUCCESS')
            
        except Exception as e:
            print(f"❌ Erreur pour {titre_name}: {e}")
            predictor.log_model_run('titre', id_titre, periods, 'ERROR', str(e))

def predict_top_skills(limit=5, periods=30):
    predictor = JobOfferPredictor()
    
    top_skills = predictor.get_top_skills(limit=limit)
    print(f"🧠 Top {limit} compétences identifiées")
    
    for skill_info in top_skills:
        id_skill = skill_info['id_skill']
        skill_name = skill_info['skill_name']
        skill_type = skill_info['type_skill']
        total_offres = skill_info['total_offres']
        
        print(f"\n💡 Traitement: {skill_name} ({skill_type}) - {total_offres} offres")
        
        try:
            data = predictor.extract_skill_data(
                id_skill=id_skill, 
                date_min='2023-01-01'
            )
            
            if len(data) < 30:
                print(f"⚠️ Pas assez de données ({len(data)} points)")
                predictor.log_model_run('skill', id_skill, periods, 'SKIP', 'Données insuffisantes')
                continue
            
            model_name = f"skill_{id_skill}"
            predictor.train_prophet_model(data, model_name)
            forecast = predictor.make_predictions(model_name, periods=periods)
            predictor.save_predictions_to_db(model_name, id_skill=id_skill)
            predictor.plot_predictions(model_name)
            predictor.log_model_run('skill', id_skill, periods, 'SUCCESS')
            
        except Exception as e:
            print(f"❌ Erreur pour {skill_name}: {e}")
            predictor.log_model_run('skill', id_skill, periods, 'ERROR', str(e))

if __name__ == "__main__":
    print("🔮 Démarrage des prédictions Prophet")
    
    conn_offers = connect_db("offers")
    conn_prediction = connect_db("prediction")
    
    if not conn_offers or not conn_prediction:
        print("❌ Problème de connexion aux bases de données")
        exit(1)
    
    conn_offers.close()
    conn_prediction.close()
    
    print("✅ Connexions OK")
    
    if len(sys.argv) > 1:
        choice = sys.argv[1]
        print(f"Choix reçu en argument: {choice}")
    else:
        try:
            choice = input("Votre choix (1/2/3): ").strip()
        except EOFError:
            print("❌ Pas d'entrée disponible, choix par défaut = 3 (les deux)")
            choice = "3"
    
    if choice == "1":
        predict_top_titres(limit=5)
    elif choice == "2":
        predict_top_skills(limit=5)
    elif choice == "3":
        predict_top_titres(limit=5)
        predict_top_skills(limit=5)
    else:
        print("❌ Choix invalide, fin du programme.")

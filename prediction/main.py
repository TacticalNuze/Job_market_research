#!/usr/bin/env python3
"""
Solution Prophet Finale - Corrigée
Résoud tous les problèmes d'attributs et optimisée pour les petits datasets
"""

import pandas as pd
import numpy as np
from prophet import Prophet
import psycopg2
import matplotlib
matplotlib.use('Agg')  # Backend non-interactif
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import traceback
import sys
import os

warnings.filterwarnings('ignore')

def connect_db(dbname="prediction"):
    """Connexion à la base PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user="root",
            password="123456",
            host="postgres",
            port=5432
        )
        return conn
    except Exception as e:
        print(f"❌ Erreur de connexion à la base '{dbname}' :", repr(e))
        return None

class ProphetJobPredictor:
    def __init__(self):
        """Initialise le prédicteur Prophet - FIX: Initialisation explicite"""
        # Initialisation explicite de tous les attributs
        self.models = {}
        self.forecasts = {}
        self.performance_metrics = {}
        print("✅ ProphetJobPredictor initialisé avec tous les attributs")
        
    def load_timeseries_data(self, segment_type='global', segment_ids=None, min_data_points=10):
        """
        Charge les données - ADAPTÉ pour petits datasets
        
        Args:
            segment_type: 'global', 'secteur', 'titre', 'skill', 'contrat', 'source'
            segment_ids: Liste des IDs de segments à charger (None = tous)
            min_data_points: RÉDUIT à 10 pour vos données limitées
        """
        print(f"📊 Chargement des données pour segment_type='{segment_type}' (min {min_data_points} points)")
        
        conn = connect_db("prediction")
        if not conn:
            return {}
        
        try:
            # Requête simplifiée sans régresseurs pour commencer
            base_query = """
            SELECT 
                ts.ds,
                ts.y,
                ts.segment_id,
                ts.segment_name
            FROM ts_prophet_data ts
            WHERE ts.segment_type = %s
            """
            
            params = [segment_type]
            
            if segment_ids:
                placeholders = ','.join(['%s'] * len(segment_ids))
                base_query += f" AND ts.segment_id IN ({placeholders})"
                params.extend(segment_ids)
            
            base_query += " ORDER BY ts.segment_id, ts.ds"
            
            # Exécuter la requête
            df = pd.read_sql(base_query, conn, params=params)
            conn.close()
            
            if df.empty:
                print(f"❌ Aucune donnée trouvée pour {segment_type}")
                return {}
            
            print(f"✅ {len(df)} lignes chargées")
            
            # Convertir les dates
            df['ds'] = pd.to_datetime(df['ds'])
            
            # Grouper par segment
            segments_data = {}
            
            if segment_type == 'global':
                # Données globales
                if len(df) >= min_data_points:
                    segments_data['global'] = df[['ds', 'y']].copy()
                    print(f"✅ Données globales: {len(df)} points")
                else:
                    print(f"⚠️ Données globales insuffisantes: {len(df)} points (min {min_data_points})")
            else:
                # Données segmentées
                for segment_id in df['segment_id'].unique():
                    segment_df = df[df['segment_id'] == segment_id].copy()
                    
                    if len(segment_df) >= min_data_points:
                        segment_name = segment_df['segment_name'].iloc[0]
                        # Nettoyer le nom du segment
                        clean_name = f"{segment_type}_{segment_id}"
                        segments_data[clean_name] = segment_df[['ds', 'y']].copy()
                        print(f"✅ Segment {clean_name}: {len(segment_df)} points")
                    else:
                        segment_name = segment_df['segment_name'].iloc[0]
                        print(f"⚠️ Segment '{segment_name}' ignoré: {len(segment_df)} points")
            
            print(f"✅ {len(segments_data)} segments valides préparés")
            return segments_data
            
        except Exception as e:
            print(f"❌ Erreur lors du chargement: {e}")
            traceback.print_exc()
            if conn:
                conn.close()
            return {}
    
    def train_prophet_models(self, segments_data, use_regressors=False, test_split=0.7):
        """
        Entraîne les modèles Prophet - SIMPLIFIÉ pour petits datasets
        
        Args:
            segments_data: Dict des données par segment
            use_regressors: Désactivé par défaut pour simplifier
            test_split: RÉDUIT à 0.7 pour garder plus de données de test
        """
        
        if not segments_data:
            print("❌ Aucune donnée fournie pour l'entraînement")
            return
        
        # Vérifier que self.models existe
        if not hasattr(self, 'models'):
            self.models = {}
            self.performance_metrics = {}
            print("🔧 Réinitialisation des attributs models et performance_metrics")
        
        for segment_name, data in segments_data.items():
            print(f"🤖 Entraînement du modèle: {segment_name}")
            
            try:
                # Vérifier les données
                if len(data) < 8:  # Minimum absolu pour Prophet
                    print(f"   ⚠️ Pas assez de données: {len(data)} points (min 8)")
                    continue
                
                # Pour les très petits datasets, pas de division train/test
                if len(data) < 15:
                    print(f"   📊 Dataset petit ({len(data)} points) - Pas de division train/test")
                    train_data = data.copy()
                    test_data = pd.DataFrame()
                else:
                    # Diviser en train/test seulement si assez de données
                    split_point = int(len(data) * test_split)
                    train_data = data.iloc[:split_point].copy()
                    test_data = data.iloc[split_point:].copy()
                    print(f"   📊 Division: {len(train_data)} train, {len(test_data)} test")
                
                # Initialiser Prophet avec paramètres adaptés aux petits datasets
                model = Prophet(
                    yearly_seasonality=False,  # Désactivé pour petits datasets
                    weekly_seasonality=True if len(data) > 14 else False,
                    daily_seasonality=False,
                    changepoint_prior_scale=0.01,  # Plus conservateur
                    seasonality_prior_scale=1.0,   # Plus conservateur
                    interval_width=0.8,            # Intervalle plus serré
                    n_changepoints=min(3, len(data)//10)  # Adapter aux données disponibles
                )
                
                # Entraîner le modèle (données de base seulement)
                model.fit(train_data[['ds', 'y']])
                
                # Test sur données de validation si disponibles
                performance_metrics = {}
                if len(test_data) > 0:
                    try:
                        test_forecast = model.predict(test_data[['ds']])
                        
                        # Calculer métriques de performance
                        y_true = test_data['y'].values
                        y_pred = test_forecast['yhat'].values
                        
                        # Vérifier qu'on a les mêmes tailles
                        min_len = min(len(y_true), len(y_pred))
                        y_true = y_true[:min_len]
                        y_pred = y_pred[:min_len]
                        
                        if min_len > 0:
                            mae = mean_absolute_error(y_true, y_pred)
                            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
                            
                            # MAPE sécurisé
                            mape = np.mean(np.abs((y_true - y_pred) / np.maximum(y_true, 0.1))) * 100
                            
                            # R² sécurisé
                            if np.std(y_true) > 0:
                                r2 = r2_score(y_true, y_pred)
                            else:
                                r2 = 0.0
                            
                            performance_metrics = {
                                'mae': mae,
                                'rmse': rmse,
                                'mape': min(mape, 1000),  # Plafonner MAPE
                                'r2': r2,
                                'train_size': len(train_data),
                                'test_size': len(test_data)
                            }
                            
                            print(f"   📊 MAE: {mae:.2f}, RMSE: {rmse:.2f}, MAPE: {min(mape, 1000):.1f}%, R²: {r2:.3f}")
                    except Exception as e:
                        print(f"   ⚠️ Erreur calcul métriques: {e}")
                
                # Stocker le modèle
                self.models[segment_name] = {
                    'model': model,
                    'train_data': train_data,
                    'test_data': test_data,
                    'use_regressors': use_regressors,
                    'regressors': []
                }
                
                if performance_metrics:
                    self.performance_metrics[segment_name] = performance_metrics
                
                print(f"   ✅ Modèle entraîné avec succès")
                
            except Exception as e:
                print(f"   ❌ Erreur d'entraînement: {e}")
                traceback.print_exc()
                continue
        
        print(f"🎯 {len(self.models)} modèles entraînés au total")
    
    def make_predictions(self, horizon_days=14, save_to_db=False):
        """
        Génère les prédictions - RÉDUIT l'horizon pour plus de fiabilité
        
        Args:
            horizon_days: RÉDUIT à 14 jours par défaut
            save_to_db: Sauvegarder en base de données
        """
        
        if not hasattr(self, 'models') or not self.models:
            print("❌ Aucun modèle entraîné. Appelez d'abord train_prophet_models()")
            return
        
        # Vérifier que self.forecasts existe
        if not hasattr(self, 'forecasts'):
            self.forecasts = {}
            print("🔧 Réinitialisation de l'attribut forecasts")
        
        for segment_name, model_data in self.models.items():
            print(f"🔮 Prédiction pour: {segment_name}")
            
            try:
                model = model_data['model']
                
                # Créer le dataframe futur avec horizon réduit
                future = model.make_future_dataframe(periods=horizon_days, freq='D')
                
                # Faire la prédiction
                forecast = model.predict(future)
                
                # Stocker la prédiction
                self.forecasts[segment_name] = {
                    'forecast': forecast,
                    'horizon_days': horizon_days,
                    'generated_at': datetime.now()
                }
                
                # Statistiques de base
                future_forecast = forecast.tail(horizon_days)
                avg_pred = future_forecast['yhat'].mean()
                print(f"   ✅ {len(forecast)} prédictions générées (moyenne future: {avg_pred:.1f})")
                
            except Exception as e:
                print(f"   ❌ Erreur de prédiction pour {segment_name}: {e}")
                traceback.print_exc()
                continue
        
        print(f"🎯 {len(self.forecasts)} ensembles de prédictions générés")
    
    def show_predictions_summary(self, days_ahead=7, top_n=10):
        """
        Affiche un résumé des prédictions - Version sécurisée et corrigée
        """
        
        if not hasattr(self, 'forecasts') or not self.forecasts:
            print("❌ Aucune prédiction disponible")
            print("💡 Exécutez d'abord make_predictions()")
            return []
        
        print(f"\n📈 RÉSUMÉ DES PRÉDICTIONS - {days_ahead} prochains jours")
        print("=" * 70)
        
        predictions_summary = []
        
        for segment_name, forecast_data in self.forecasts.items():
            try:
                forecast = forecast_data['forecast']
                
                # Filtrer les prédictions futures - FIX: Utiliser toutes les dates futures
                today = datetime.now().date()
                future_predictions = forecast[forecast['ds'].dt.date > today]
                
                # Si pas de données futures, prendre les dernières prédictions
                if len(future_predictions) == 0:
                    future_predictions = forecast.tail(days_ahead)
                    print(f"⚠️ {segment_name}: Utilisation des dernières prédictions")
                else:
                    # Limiter au nombre de jours demandés
                    future_predictions = future_predictions.head(days_ahead)
                
                if len(future_predictions) > 0:
                    avg_prediction = future_predictions['yhat'].mean()
                    total_prediction = future_predictions['yhat'].sum()
                    min_prediction = future_predictions['yhat'].min()
                    max_prediction = future_predictions['yhat'].max()
                    
                    # Tendance simple
                    if len(future_predictions) > 1:
                        trend = 'hausse' if future_predictions['yhat'].iloc[-1] > future_predictions['yhat'].iloc[0] else 'baisse'
                    else:
                        trend = 'stable'
                    
                    predictions_summary.append({
                        'segment': segment_name,
                        'moyenne_quotidienne': avg_prediction,
                        'total_periode': total_prediction,
                        'min_pred': min_prediction,
                        'max_pred': max_prediction,
                        'tendance': trend,
                        'nb_points': len(future_predictions)
                    })
                    
                    print(f"✅ {segment_name}: {len(future_predictions)} prédictions, moyenne {avg_prediction:.1f}")
                    
            except Exception as e:
                print(f"⚠️ Erreur traitement {segment_name}: {e}")
                continue
        
        if not predictions_summary:
            print("❌ Aucune prédiction valide trouvée")
            print("💡 Vérifiez les données de forecast et les dates")
            
            # Debug: Afficher les informations de forecast disponibles
            print("\n🔍 DEBUG - Informations des forecasts:")
            for segment_name, forecast_data in self.forecasts.items():
                forecast = forecast_data['forecast']
                print(f"   {segment_name}: {len(forecast)} points, "
                      f"du {forecast['ds'].min()} au {forecast['ds'].max()}")
            
            return []
        
        # Trier par prédiction moyenne
        predictions_summary.sort(key=lambda x: x['moyenne_quotidienne'], reverse=True)
        
        # Afficher le top N
        print(f"\n🏆 TOP {min(top_n, len(predictions_summary))} SEGMENTS:")
        print("-" * 70)
        
        for i, pred in enumerate(predictions_summary[:top_n], 1):
            segment_clean = pred['segment'].replace('_', ' ')
            print(f"{i:2d}. {segment_clean:<20} | "
                  f"Moy: {pred['moyenne_quotidienne']:6.1f} | "
                  f"Total: {pred['total_periode']:7.1f} | "
                  f"Range: {pred['min_pred']:4.1f}-{pred['max_pred']:4.1f} | "
                  f"{pred['tendance']} ({pred['nb_points']}j)")
        
        # Statistiques globales
        if predictions_summary:
            total_avg = sum(p['moyenne_quotidienne'] for p in predictions_summary)
            print(f"\n📊 TOTAL PRÉVU: {total_avg:.1f} offres/jour sur tous segments")
            print(f"📊 SEGMENTS ANALYSÉS: {len(predictions_summary)}")
            
            # Tendances
            hausse_count = sum(1 for p in predictions_summary if p['tendance'] == 'hausse')
            baisse_count = sum(1 for p in predictions_summary if p['tendance'] == 'baisse')
            stable_count = len(predictions_summary) - hausse_count - baisse_count
            
            print(f"📈 TENDANCES: {hausse_count} hausse, {baisse_count} baisse, {stable_count} stable")
            
            # Insights supplémentaires
            best_segment = predictions_summary[0]
            print(f"\n🎯 MEILLEUR SEGMENT: {best_segment['segment']} "
                  f"({best_segment['moyenne_quotidienne']:.1f} offres/jour)")
        
        return predictions_summary[:top_n]
    
    def show_detailed_predictions(self, segment_name=None, days_ahead=14):
        """
        Affiche les prédictions détaillées jour par jour
        """
        
        if not hasattr(self, 'forecasts') or not self.forecasts:
            print("❌ Aucune prédiction disponible")
            return
        
        # Si pas de segment spécifié, prendre le premier
        if segment_name is None:
            segment_name = list(self.forecasts.keys())[0]
        
        if segment_name not in self.forecasts:
            print(f"❌ Segment '{segment_name}' non trouvé")
            available = list(self.forecasts.keys())
            print(f"💡 Segments disponibles: {available}")
            return
        
        forecast_data = self.forecasts[segment_name]
        forecast = forecast_data['forecast']
        
        print(f"\n📅 PRÉDICTIONS DÉTAILLÉES - {segment_name}")
        print("=" * 60)
        
        # Prendre les dernières prédictions (incluant historique récent)
        recent_and_future = forecast.tail(days_ahead + 7)  # 7 jours historiques + prédictions
        
        today = datetime.now().date()
        
        print("Date       | Prédiction | Intervalle de confiance | Type")
        print("-" * 60)
        
        for _, row in recent_and_future.iterrows():
            date = row['ds'].date()
            pred = row['yhat']
            lower = row['yhat_lower']
            upper = row['yhat_upper']
            
            # Déterminer le type
            if date < today:
                type_pred = "Historique"
                marker = "📊"
            elif date == today:
                type_pred = "Aujourd'hui"
                marker = "📍"
            else:
                type_pred = "Futur"
                marker = "🔮"
            
            print(f"{date} | {marker} {pred:6.1f}   | {lower:5.1f} - {upper:5.1f}     | {type_pred}")
        
        # Statistiques de résumé
        future_data = recent_and_future[recent_and_future['ds'].dt.date > today]
        if len(future_data) > 0:
            avg_future = future_data['yhat'].mean()
            total_future = future_data['yhat'].sum()
            print(f"\n📈 RÉSUMÉ FUTUR:")
            print(f"   Moyenne quotidienne: {avg_future:.1f} offres")
            print(f"   Total période: {total_future:.1f} offres")
            print(f"   Période: {len(future_data)} jours")
    
    def export_predictions_csv(self, filename=None):
        """
        Exporte toutes les prédictions vers un fichier CSV
        """
        
        if not hasattr(self, 'forecasts') or not self.forecasts:
            print("❌ Aucune prédiction à exporter")
            return
        
        if filename is None:
            filename = f"predictions_prophet_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        
        all_predictions = []
        
        for segment_name, forecast_data in self.forecasts.items():
            forecast = forecast_data['forecast']
            horizon_days = forecast_data['horizon_days']
            
            # Ajouter les prédictions futures seulement
            today = datetime.now().date()
            future_predictions = forecast[forecast['ds'].dt.date > today]
            
            for _, row in future_predictions.iterrows():
                all_predictions.append({
                    'segment': segment_name,
                    'date': row['ds'].date(),
                    'prediction': row['yhat'],
                    'lower_bound': row['yhat_lower'],
                    'upper_bound': row['yhat_upper'],
                    'trend': row.get('trend', None),
                    'horizon_days': horizon_days,
                    'generated_at': forecast_data['generated_at']
                })
        
        if all_predictions:
            df_export = pd.DataFrame(all_predictions)
            df_export.to_csv(filename, index=False)
            print(f"✅ {len(all_predictions)} prédictions exportées vers {filename}")
            
            # Statistiques de l'export
            nb_segments = df_export['segment'].nunique()
            date_range = f"{df_export['date'].min()} → {df_export['date'].max()}"
            print(f"📊 Export: {nb_segments} segments, période {date_range}")
        else:
            print("❌ Aucune prédiction future à exporter")
    
    def show_performance_summary(self):
        """
        Affiche un résumé des performances - Version sécurisée
        """
        
        if not hasattr(self, 'performance_metrics') or not self.performance_metrics:
            print("❌ Aucune métrique de performance disponible")
            return
        
        print(f"\n🎯 PERFORMANCES DES MODÈLES")
        print("=" * 60)
        
        metrics_list = []
        for segment_name, metrics in self.performance_metrics.items():
            segment_clean = segment_name.replace('_', ' ')
            metrics_list.append({
                'Segment': segment_clean,
                'MAE': metrics['mae'],
                'RMSE': metrics['rmse'],
                'MAPE': metrics['mape'],
                'R²': metrics['r2'],
                'Train': metrics['train_size'],
                'Test': metrics['test_size']
            })
        
        if metrics_list:
            # Trier par R²
            metrics_list.sort(key=lambda x: x['R²'], reverse=True)
            
            print("📊 MÉTRIQUES DÉTAILLÉES:")
            print("-" * 60)
            
            for metric in metrics_list[:10]:  # Top 10
                print(f"{metric['Segment']:<20} | "
                      f"MAE: {metric['MAE']:6.2f} | "
                      f"R²: {metric['R²']:6.3f} | "
                      f"Data: {metric['Train']}+{metric['Test']}")
            
            # Statistiques moyennes
            avg_mae = np.mean([m['MAE'] for m in metrics_list])
            avg_r2 = np.mean([m['R²'] for m in metrics_list])
            
            print(f"\n📈 MOYENNES: MAE={avg_mae:.2f}, R²={avg_r2:.3f}")
            
            # Qualité des modèles
            good_models = len([m for m in metrics_list if m['R²'] > 0.3])
            ok_models = len([m for m in metrics_list if 0.0 <= m['R²'] <= 0.3])
            poor_models = len([m for m in metrics_list if m['R²'] < 0.0])
            
            print(f"🏅 QUALITÉ: {good_models} bons, {ok_models} corrects, {poor_models} faibles")

def check_data_availability():
    """Vérifie la disponibilité des données - Version détaillée"""
    print("🔍 Vérification des données disponibles...")
    
    conn = connect_db("prediction")
    if not conn:
        return False
    
    try:
        # Vérifier les données dans ts_prophet_data
        data_query = """
        SELECT 
            segment_type,
            COUNT(*) as nb_records,
            COUNT(DISTINCT segment_id) as nb_segments,
            MIN(ds) as date_min,
            MAX(ds) as date_max,
            AVG(y) as moyenne_y
        FROM ts_prophet_data 
        GROUP BY segment_type
        ORDER BY nb_records DESC;
        """
        
        data_df = pd.read_sql(data_query, conn)
        conn.close()
        
        if data_df.empty:
            print("❌ Aucune donnée trouvée dans ts_prophet_data")
            print("💡 Exécutez d'abord le script manage_tables.py")
            return False
        
        print("✅ Données disponibles:")
        print("-" * 70)
        for _, row in data_df.iterrows():
            print(f"📊 {row['segment_type'].upper():<12} | "
                  f"{row['nb_segments']:3d} segments | "
                  f"{row['nb_records']:5d} records | "
                  f"Moy: {row['moyenne_y']:5.1f} | "
                  f"{row['date_min']} → {row['date_max']}")
        
        # Recommandations basées sur les données
        total_records = data_df['nb_records'].sum()
        if total_records < 100:
            print(f"\n⚠️ Dataset petit ({total_records} records total)")
            print("💡 Recommandations:")
            print("   - Utilisez min_data_points=5-10")
            print("   - Horizon de prédiction ≤ 14 jours")
            print("   - Désactivez les régresseurs externes")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur vérification données: {e}")
        if conn:
            conn.close()
        return False

def main():
    """
    Fonction principale - OPTIMISÉE pour petits datasets
    """
    
    print("🚀 PRÉDICTION PROPHET - VERSION OPTIMISÉE PETITS DATASETS")
    print("=" * 65)
    
    start_time = datetime.now()
    
    try:
        # 1. Vérification des données
        print("\n1️⃣ VÉRIFICATION DES DONNÉES")
        print("-" * 40)
        if not check_data_availability():
            print("❌ Données insuffisantes - Arrêt du processus")
            return
        
        # 2. Initialisation du prédicteur
        print("\n2️⃣ INITIALISATION DU PRÉDICTEUR")
        print("-" * 40)
        predictor = ProphetJobPredictor()
        
        total_models_trained = 0
        
        # 3. Prédictions globales (priorité)
        print("\n3️⃣ PRÉDICTIONS GLOBALES")
        print("-" * 40)
        
        global_data = predictor.load_timeseries_data('global', min_data_points=8)
        
        if global_data:
            predictor.train_prophet_models(global_data, use_regressors=False)
            predictor.make_predictions(horizon_days=14, save_to_db=False)
            total_models_trained += len(predictor.models)
        
        # 4. Essayer d'autres segments si pas assez de données globales
        for segment_type in ['contrat', 'source', 'secteur']:
            if total_models_trained >= 5:  # Limite pour éviter la surcharge
                break
                
            print(f"\n4️⃣ PRÉDICTIONS {segment_type.upper()}")
            print("-" * 40)
            
            segment_data = predictor.load_timeseries_data(segment_type, min_data_points=8)
            
            if segment_data:
                # Limiter aux 3 meilleurs segments
                limited_data = dict(list(segment_data.items())[:3])
                predictor.train_prophet_models(limited_data, use_regressors=False)
                predictor.make_predictions(horizon_days=14, save_to_db=False)
                total_models_trained += len(limited_data)
        
        # 5. Analyse des résultats
        print("\n5️⃣ ANALYSE DES RÉSULTATS")
        print("-" * 40)
        
        if total_models_trained > 0:
            top_predictions = predictor.show_predictions_summary(days_ahead=7, top_n=10)
            predictor.show_performance_summary()
            
            # Affichage détaillé pour le meilleur segment
            if top_predictions:
                best_segment = top_predictions[0]['segment']
                print(f"\n📅 DÉTAIL DU MEILLEUR SEGMENT:")
                predictor.show_detailed_predictions(best_segment, days_ahead=14)
            
            # Export CSV des prédictions
            print(f"\n💾 EXPORT DES PRÉDICTIONS:")
            predictor.export_predictions_csv()
        else:
            print("❌ Aucun modèle n'a pu être entraîné")
            print("💡 Vérifiez que vous avez au moins 8 points de données par segment")
        
        # 6. Statistiques finales
        print("\n6️⃣ STATISTIQUES FINALES")
        print("-" * 40)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        total_forecasts = len(predictor.forecasts) if hasattr(predictor, 'forecasts') else 0
        total_metrics = len(predictor.performance_metrics) if hasattr(predictor, 'performance_metrics') else 0
        
        print(f"⏱️  Durée totale: {duration:.1f} secondes")
        print(f"🤖 Modèles entraînés: {total_models_trained}")
        print(f"🔮 Prédictions générées: {total_forecasts}")
        print(f"📊 Métriques calculées: {total_metrics}")
        
        if total_models_trained > 0:
            print(f"\n✅ PROCESSUS TERMINÉ AVEC SUCCÈS!")
            print(f"🎯 Prophet opérationnel avec {total_models_trained} modèles")
            
            if total_forecasts > 0:
                print(f"📈 Prédictions disponibles pour analyse")
        else:
            print(f"\n⚠️ PROCESSUS PARTIELLEMENT RÉUSSI")
            print(f"💡 Collectez plus de données pour de meilleurs résultats")
        
    except Exception as e:
        print(f"\n❌ ERREUR CRITIQUE: {e}")
        traceback.print_exc()
        return False
    
    return True

def quick_test():
    """Test rapide adapté aux petites données"""
    print("🧪 TEST RAPIDE PROPHET - PETITES DONNÉES")
    print("-" * 45)
    
    predictor = ProphetJobPredictor()
    
    # Test avec seuil très bas
    global_data = predictor.load_timeseries_data('global', min_data_points=5)
    
    if global_data:
        predictor.train_prophet_models(global_data, use_regressors=False)
        predictor.make_predictions(horizon_days=7, save_to_db=False)
        predictor.show_predictions_summary(days_ahead=7, top_n=3)
        print("✅ Test rapide réussi!")
        return True
    else:
        print("❌ Test rapide échoué - pas assez de données")
        # Essayer avec d'autres types
        for segment_type in ['contrat', 'source']:
            test_data = predictor.load_timeseries_data(segment_type, min_data_points=5)
            if test_data:
                print(f"✅ Données trouvées dans {segment_type}")
                return True
        return False

if __name__ == "__main__":
    """Point d'entrée principal"""
    
    import sys
    
    # Vérifier les arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            quick_test()
        elif sys.argv[1] == "check":
            check_data_availability()
        else:
            print("Usage: python prophet_fixed.py [test|check]")
            print("  test  - Test rapide adapté aux petites données")
            print("  check - Vérification des données")
            print("  (aucun argument) - Exécution complète optimisée")
    else:
        # Exécution complète
        main()
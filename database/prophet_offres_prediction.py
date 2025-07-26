import pandas as pd
from prophet import Prophet
import matplotlib.pyplot as plt
import os

# Récupérer le chemin vers le dossier Downloads de l'utilisateur
user_folder = os.path.expanduser("~")
downloads_folder = os.path.join(user_folder, "Downloads")

# Charger les fichiers CSV depuis Downloads
df_skills = pd.read_csv(os.path.join(downloads_folder, "sql.csv"))
df_offres = pd.read_csv(os.path.join(downloads_folder, "sql (1).csv"))

# Convertir les colonnes de dates en datetime
df_skills['ds'] = pd.to_datetime(df_skills['ds'])
df_offres['ds'] = pd.to_datetime(df_offres['ds'])

# Grouper les compétences par mois (somme de y)
df_skills_monthly = (
    df_skills
    .groupby(pd.Grouper(key='ds', freq='ME'))
    .agg(y=('y', 'sum'))
    .reset_index()
)

# Grouper les offres par mois (somme de y)
df_offres_monthly = (
    df_offres
    .groupby(pd.Grouper(key='ds', freq='ME'))
    .agg(y=('y', 'sum'))
    .reset_index()
)

# Modèle et prédiction pour compétences
model_skills = Prophet()
model_skills.fit(df_skills_monthly)
future_skills = model_skills.make_future_dataframe(periods=6, freq='ME')
forecast_skills = model_skills.predict(future_skills)

# Modèle et prédiction pour offres
model_offres = Prophet()
model_offres.fit(df_offres_monthly)
future_offres = model_offres.make_future_dataframe(periods=6, freq='ME')
forecast_offres = model_offres.predict(future_offres)

# Affichage et sauvegarde des graphiques
fig_skills = model_skills.plot(forecast_skills)
plt.title("Prévision du nombre de compétences")
plt.xlabel("Date")
plt.ylabel("Nombre de compétences")
plt.tight_layout()
fig_skills.savefig("forecast_skills.png")

fig_offres = model_offres.plot(forecast_offres)
plt.title("Prévision du nombre d'offres")
plt.xlabel("Date")
plt.ylabel("Nombre d'offres")
plt.tight_layout()
fig_offres.savefig("forecast_offres.png")

# Affichage des prévisions dans la console (6 derniers mois prévus)
print("Prévisions compétences (6 prochains mois) :")
print(forecast_skills[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(6))
print("\nPrévisions offres (6 prochains mois) :")
print(forecast_offres[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail(6))


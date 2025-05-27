import json
import pandas as pd

# Charger le JSON enrichi
with open('C:\Users\houss\Desktop\DXC\Job_market_research\one_shot_enriched.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Convertir en DataFrame
df = pd.DataFrame(data)

# Enregistrer au format Excel
output_path = 'enriched_offers.xlsx'
df.to_excel(output_path, index=False)

output_path

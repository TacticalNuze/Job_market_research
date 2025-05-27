import os
import json
import sys
from datetime import datetime
import pandas as pd
import re

# Répertoires possibles pour scraping_output/
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CANDIDATE_DIRS = [
    os.path.join(BASE_DIR, "scraping_output"),
    os.path.join(BASE_DIR, "..", "scraping_output"),
    os.path.join(BASE_DIR, "..", "..", "scraping_output"),
]

# Trouve le premier dossier existant
SCRAPING_DIR = next((d for d in CANDIDATE_DIRS if os.path.isdir(os.path.normpath(d))), None)
if SCRAPING_DIR is None:
    print(f"[ERROR] Aucun dossier scraping_output/ trouvé dans {CANDIDATE_DIRS}")
    sys.exit(1)

# Dossier de sortie
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def infer_source_from_filename(fname):
    m = re.match(r"offres_emploi_(.+)\.json", fname, re.IGNORECASE)
    return m.group(1).capitalize() if m else fname.replace(".json", "")

def load_and_annotate():
    all_offers = []
    json_files = sorted(f for f in os.listdir(SCRAPING_DIR) if f.lower().endswith(".json"))
    if not json_files:
        print(f"[WARN] Aucun JSON dans {SCRAPING_DIR}")
        return all_offers

    for fname in json_files:
        full = os.path.join(SCRAPING_DIR, fname)
        via = infer_source_from_filename(fname)
        try:
            with open(full, "r", encoding="utf-8") as f:
                offers = json.load(f)
        except Exception as e:
            print(f"[ERROR] Impossible de charger {fname}: {e}")
            continue

        if isinstance(offers, list):
            for off in offers:
                off["via"] = via
            all_offers.extend(offers)
            print(f"[OK] {len(offers)} offres depuis {fname} (via={via})")
        else:
            print(f"[WARN] Contenu non-list dans {fname}")

    print(f"[INFO] Total offres chargées : {len(all_offers)}")
    return all_offers

def save_backup_and_excel(all_offers):
    if not all_offers:
        print("Aucune offre à sauvegarder.")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Backup JSON
    json_path = os.path.join(OUTPUT_DIR, f"backup_offers_{ts}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_offers, f, ensure_ascii=False, indent=2)
    print(f"[OK] Backup JSON: {json_path}")

    # DataFrame et Excel
    df = pd.DataFrame(all_offers)
    for col in ("collect_date", "publication_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        else:
            print(f"[WARN] Colonne manquante : {col}")

    excel_path = os.path.join(OUTPUT_DIR, f"offers_report_{ts}.xlsx")
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        if "collect_date" in df.columns:
            for date, group in df.groupby("collect_date"):
                sheet = f"collect_{date.strftime('%Y%m%d')}"
                group.to_excel(writer, sheet_name=sheet, index=False)
        if "publication_date" in df.columns:
            for date, group in df.groupby("publication_date"):
                sheet = f"pub_{date.strftime('%Y%m%d')}"
                group.to_excel(writer, sheet_name=sheet, index=False)

    print(f"[OK] Rapport Excel: {excel_path}")

if __name__ == "__main__":
    print(f"Utilisation de scraping_output : {SCRAPING_DIR}")
    offers = load_and_annotate()
    save_backup_and_excel(offers)

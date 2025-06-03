import os
import json
import logging
import re
import time
from datetime import datetime
import requests
from dotenv import load_dotenv

# Configuration des logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('traitement_groq.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Chargement des variables d'environnement
load_dotenv()
API_KEY = os.getenv("GROQ_API_KEY")
GROQ_URL = os.getenv('GROQ_API_URL', 'https://api.groq.com/openai/v1/chat/completions')
if not API_KEY or not API_KEY.startswith("gsk_"):
    logging.error("Clé API Groq non configurée - Vérifiez .env")
    exit(1)

# Fichiers d'entrée et de sortie
INPUT_FILE = r,input_buckup
OUTPUT_FILE = "processed_jobs_groq.json"

# Pré-prompt et prompt système pour GROQ
PRE_PROMPT = (
    "Tu vas recevoir un batch d'offres d'emploi structuré. "
    "Pour chaque offre, analyse et enrichis les informations techniques liées aux données."
)
SYSTEM_PROMPT = (
    "TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :\n"
    "1. Détermine si le poste est lié aux données (1) ou non (0)\n"
    "2. Si data_profile=1, catégorise en BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST\n"
    "3. Normalise le niveau d'études (0-5)\n"
    "4. Normalise l'expérience (années → valeur numérique) et déduit la séniorité (Junior, Senior, Expert)\n"
    "RENVOIE UNIQUEMENT UN JSON ARRAY d'objets avec les clés : "
    "is_data_profile, profile, niveau_etudes, niveau_experience, competences_techniques"
)

# Chargement JSON
def load_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Erreur lecture {file_path} : {e}")
        exit(1)

# Préparation d'une offre
def prepare_offer(o):
    secteurs = [s.strip() for s in re.split(r'[;,]', o.get('secteur','')) if s.strip()]
    comp_list = [c.strip() for c in re.split(r'[;,]', o.get('intro','').lower()) if c.strip()]
    return {
        'title': o.get('titre',''),
        'description': o.get('intro',''),
        'secteur': secteurs,
        'competences': comp_list,
        'job_url': o.get('job_url','')
    }

# Nettoyage de la réponse de l'API
def clean_response(response_text):
    match = re.search(r"\[.*\]", response_text, re.DOTALL)
    if not match:
        logging.error("Aucun JSON valide trouvé dans la réponse")
        return []
    try:
        arr = json.loads(match.group(0))
    except json.JSONDecodeError as e:
        logging.error(f"Erreur JSONDecode : {e}")
        return []
    valid = []
    for entry in arr:
        for k in ('is_data_profile','profile','niveau_etudes','niveau_experience','competences_techniques'):
            entry.setdefault(k, 0 if 'niveau' in k or k=='is_data_profile' else 'NONE')
        # conversion
        entry['is_data_profile'] = safe_int(entry['is_data_profile'])
        entry['niveau_etudes'] = safe_int(entry['niveau_etudes'])
        entry['niveau_experience'] = safe_int(entry['niveau_experience'])
        entry['competences_techniques'] = [c.strip().capitalize() for c in entry.get('competences_techniques',[]) if isinstance(entry.get('competences_techniques',[]), list)]
        entry['profile'] = (entry.get('profile') or 'NONE').upper()
        valid.append(entry)
    return valid

# Conversion sécurisée en entier
def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

# Requête GROQ avec gestion des erreurs et des quotas
def process_with_groq(batch):
    max_retries = 3
    system_msg = PRE_PROMPT + "\n" + SYSTEM_PROMPT
    user_msg = "\n---\n".join([
        f"Titre: {o['title']}\nDescription: {o['description'][:1000]}\nURL: {o['job_url']}" for o in batch
    ])
    payload = {
    'model': 'llama3-70b-8192',  # modèle Groq encore actif
    'temperature': 0.1,
    'messages': [
        {'role': 'system', 'content': system_msg},
        {'role': 'user', 'content': user_msg}
    ]
}


    for attempt in range(max_retries):
        try:
            resp = requests.post(
                GROQ_URL,
                headers={
                    'Authorization': f'Bearer {API_KEY}',
                    'Content-Type': 'application/json'
                },
                json=payload,
                timeout=60
            )

            if resp.status_code == 429:
                logging.warning("Quota dépassé, attente de 60 secondes.")
                time.sleep(60)
                continue

            if not resp.ok:
                logging.error(f"Erreur API {resp.status_code} : {resp.text}")
                return []

            content = resp.json().get("choices", [])[0]["message"]["content"]
            return clean_response(content)

        except Exception as e:
            logging.error(f"Erreur lors de la requête API : {e}")
            time.sleep(10)
    logging.error("Échec après plusieurs tentatives, abandon du batch")
    return []

# Pipeline principal
def main():
    data = load_json(INPUT_FILE)
    batch_size = 10
    batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
    results = []
    for idx, batch in enumerate(batches,1):
        logging.info(f"Lot {idx}/{len(batches)}: {len(batch)} offres")
        processed = process_with_groq([prepare_offer(o) for o in batch])
        if processed:
            for orig, upd in zip(batch, processed):
                orig.update(upd)
                results.append(orig)
        # Pause fixe de 30s après chaque requête pour éviter tout 429
        logging.info("Pause fixe de 30s entre chaque requête")
        time.sleep(30)
    with open(OUTPUT_FILE,'w',encoding='utf-8') as f:
        json.dump(results,f,ensure_ascii=False,indent=2)
    logging.info(f"Enregistré {len(results)} offres dans {OUTPUT_FILE}")

if __name__=='__main__':
    main()

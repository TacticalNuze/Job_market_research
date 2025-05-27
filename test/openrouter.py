#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, io, os, json, time, logging, random, re
from logging.handlers import RotatingFileHandler
import requests
import pandas as pd
from dotenv import load_dotenv

# UTF-8 console sous Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Logger
logger = logging.getLogger("openrouter_pipeline")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO)
fh = RotatingFileHandler("traitement_openrouter.log", encoding="utf-8", maxBytes=5*1024*1024, backupCount=3); fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(ch); logger.addHandler(fh)

# Load env vars
load_dotenv()
API_KEY = os.getenv("OPENROUTER_API_KEY")
API_URL = os.getenv("OPENROUTER_API_URL", "https://openrouter.ai/api/v1/chat/completions")
if not API_KEY:
    logger.error("Missing OPENROUTER_API_KEY")
    sys.exit(1)
HEADERS = {'Authorization': f'Bearer {API_KEY}', 'Content-Type': 'application/json'}
MODEL = 'deepseek/deepseek-r1:free:extended'

# Files
INPUT_FILE = r"C:\Users\houss\Desktop\DXC\Job_market_research\Data_extraction\Traitement\output\backup_offers_20250512_193230.json"
OUT_JSON = "one_shot_enriched.json"
OUT_XLSX = "one_shot_enriched.xlsx"

# Prompts
PRE_PROMPT = "Avant traitement, homogénéise toutes les catégories : minuscules, sans accents."
SYSTEM_PROMPT = (
    "TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :\n"
    "1. Détermine si le poste est lié aux données (1) ou non (0)\n"
    "2. Si data_profile=1, catégorise en BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST\n"
    "3. Normalise le niveau d'études (0-5)\n"
    "4. Normalise l'expérience (années → numérique) et déduit la séniorité (Junior, Senior, Expert)\n"
    "5. Identifie les compétences techniques et les soft skills clés\n"
    "RENVOIE UNIQUEMENT UN JSON ARRAY avec les clés : "
    "is_data_profile, profile, niveau_etudes, niveau_experience, competences_techniques, soft_skills"
)

# Utilities

def clean_and_extract_array(raw: str):
    s = re.sub(r",\s*([}\]])", r"\1", raw)
    s = re.sub(r'"niveau_etude"\s*:', '"niveau_etudes":', s)
    m = re.search(r"\[.*\]", s, re.DOTALL)
    if not m:
        logger.error("No JSON array found")
        return []
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError as e:
        logger.error("JSON decode error: %s", e)
        return []


def safe_int(v):
    try: return int(v)
    except: return 0

# Prepare offer

def prepare_offer(o):
    title = o.get('titre') or o.get('title') or ''
    description = o.get('intro') or o.get('description') or ''
    secteurs_raw = o.get('domaine') or o.get('secteur') or ''
    secteurs = [s.strip().lower() for s in re.split(r"[;,/\\-]", secteurs_raw) if s.strip()]
    comp_field = o.get('competences') or ''
    comps = [c.strip().lower() for c in re.split(r"[;,/\\-]", comp_field) if c.strip()]
    url = o.get('job_url','')
    source = o.get('via','other').lower()
    return {
        'title': title,
        'description': description,
        'secteur': secteurs,
        'competences': comps,
        'job_url': url,
        'source': source
    }

# Call API

def call_openrouter(batch, retries=3):
    payload = {
        'model': MODEL,
        'messages': [
            {'role':'system','content': PRE_PROMPT + "\n" + SYSTEM_PROMPT},
            {'role':'user','content': json.dumps(batch, ensure_ascii=False)}
        ],
        'transforms': ['middle-out'],
        'temperature': 0.1
    }
    for attempt in range(retries):
        r = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        if r.status_code == 429:
            logger.warning("Rate limit 429, waiting 60s")
            time.sleep(60)
            continue
        if not r.ok:
            logger.error(f"API Error {r.status_code}: {r.text}")
            return []
        content = r.json().get('choices', [])[0].get('message', {}).get('content', '')
        return clean_and_extract_array(content)
    logger.error("Failed after retries")
    return []

# Main

def main():
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)
    batch_size = 10
    results = []
    total = len(data)
    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        logger.info(f"Traitement batch {i//batch_size+1}/{(total+batch_size-1)//batch_size}")
        enriched = call_openrouter([prepare_offer(o) for o in batch])
        for orig, upd in zip(batch, enriched):
            upd['is_data_profile'] = safe_int(upd.get('is_data_profile'))
            upd['niveau_etudes'] = safe_int(upd.get('niveau_etudes'))
            upd['niveau_experience'] = safe_int(upd.get('niveau_experience'))
            exp = upd['niveau_experience']
            if exp <= 2: upd['seniority'] = 'Junior'
            elif exp <= 5: upd['seniority'] = 'Senior'
            else: upd['seniority'] = 'Expert'
            upd['profile'] = (upd.get('profile') or 'NONE').upper()
            upd['competences_techniques'] = [c.capitalize() for c in upd.get('competences_techniques', [])]
            upd['soft_skills'] = [s.capitalize() for s in upd.get('soft_skills', [])]
            orig.update(upd)
            results.append(orig)
        time.sleep(1)

    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(results)} offers to {OUT_JSON}")
    pd.DataFrame(results).to_excel(OUT_XLSX, index=False)
    logger.info(f"Excel generated: {OUT_XLSX}")

if __name__ == '__main__':
    main()

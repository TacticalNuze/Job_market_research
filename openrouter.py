#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
one_shot_openrouter_full_debug.py

Ce script :
1. Charge un fichier JSON d’offres d'emploi (liste).  
2. Découpe cette liste en lots de taille configurable.  
3. Pour chaque lot, envoie une requête "one-shot" à OpenRouter
   ("middle-out" + modèle extended gratuit) pour analyser le lot.  
4. Gère automatiquement les erreurs réseau (retry, backoff, jitter).  
5. Extrait et parse le JSON ARRAY renvoyé, fusionne avec les offres initiales.
6. Concatène tous les résultats et les sauvegarde dans un fichier final JSON.

Prérequis :
    pip install --upgrade requests python-dotenv
    export OPENROUTER_API_KEY="votre_clef_openrouter"
"""

import sys
import io
import os
import json
import time
import logging
import random
from logging.handlers import RotatingFileHandler
import requests
from dotenv import load_dotenv

# ─── 0. UTF-8 console sous Windows ─────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─── 1. Logger ─────────────────────────────────────────────────────────────────
logger = logging.getLogger("one_shot_pipeline_debug")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
fh = RotatingFileHandler("one_shot_pipeline_debug.log", encoding="utf-8", maxBytes=5*1024*1024, backupCount=3)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt)
fh.setFormatter(fmt)
logger.addHandler(ch)
logger.addHandler(fh)

# ─── 2. Prompt système ───────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :\n"
    "1. Détermine si chaque poste est lié aux données (1) ou non (0)\n"
    "2. Si data_profile=1, catégorise en BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST\n"
    "3. Normalise le niveau d'études (0-5)\n"
    "4. Normalise l'expérience (années → valeur numérique) et déduit la séniorité (Junior, Senior, Expert)\n"
    "RENVOIE UNIQUEMENT un JSON ARRAY, commençant par [ et finissant par ]."
)

# ─── 3. Extraction JSON ARRAY ─────────────────────────────────────────────────
def extract_json_array(text: str) -> str:
    try:
        start = text.index('[')
        end = text.rindex(']') + 1
        return text[start:end]
    except ValueError:
        return None

# ─── 4. Config API OpenRouter ─────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("OPENROUTER_API_KEY")
API_URL = os.getenv(
    'OPENROUTER_API_URL',
    'https://openrouter.ai/api/v1/chat/completions'
)
if not API_KEY:
    logger.error("Variable OPENROUTER_API_KEY non définie. Vérifiez .env")
    sys.exit(1)
MODEL = 'deepseek/deepseek-r1:free:extended'
HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

# ─── 5. Fonction d'appel OpenRouter avec retry/backoff ─────────────────────────
def call_openrouter(payload: dict, max_retries: int = 3) -> dict:
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
        except requests.RequestException as e:
            wait = attempt * 2 + random.uniform(0, 1)
            logger.warning(f"Erreur réseau ({e}), retry dans {wait:.1f}s (t{attempt})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = attempt * 5 + random.uniform(0, 2)
            logger.warning(f"Rate limit 429, pause {wait:.1f}s (t{attempt})")
            time.sleep(wait)
            continue
        if not resp.ok:
            logger.error(f"API error {resp.status_code}: {resp.text}")
            return {}
        try:
            return resp.json()
        except ValueError:
            logger.error("Réponse non-JSON reçue")
            return {}
    logger.error("Échec des tentatives auprès de l'API après %d essais", max_retries)
    return {}

# ─── 6. Main ─────────────────────────────────────────────────────────────────
def main():
    # Fichier source
    INPUT_FILE = r"C:\Users\houss\Desktop\DXC\Job_market_research\Data_extraction\Traitement\output\backup_offers_20250512_193230.json"
    try:
        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            offers_list = json.load(f)
        logger.info("Chargé %d offres depuis %s", len(offers_list), INPUT_FILE)
    except Exception as e:
        logger.exception("Échec chargement JSON: %s", e)
        return

    batch_size = 10
    all_enriched = []
    total_batches = (len(offers_list) + batch_size - 1) // batch_size

    for idx in range(total_batches):
        batch = offers_list[idx*batch_size:(idx+1)*batch_size]
        raw = json.dumps(batch, ensure_ascii=False)
        logger.info("Traitement batch %d/%d: %d offres", idx+1, total_batches, len(batch))

        payload = {
            "model": MODEL,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": raw}
            ],
            "transforms": ["middle-out"],
            "temperature": 0.0
        }
        start = time.perf_counter()
        result = call_openrouter(payload)
        elapsed = time.perf_counter() - start

        if not result or "choices" not in result:
            logger.error("Pas de retour API pour batch %d", idx+1)
            continue

        content_text = result["choices"][0]["message"]["content"]
        logger.info("Réponse batch %d reçue en %.2fs", idx+1, elapsed)

        json_array_str = extract_json_array(content_text)
        if not json_array_str:
            logger.error("Aucun JSON array pour batch %d", idx+1)
            with open(f"debug_batch_{idx+1}.txt", "w", encoding="utf-8") as dbg:
                dbg.write(content_text)
            continue
        try:
            arr = json.loads(json_array_str)
        except json.JSONDecodeError:
            logger.error("JSON invalide pour batch %d", idx+1)
            continue

        for orig, cls in zip(batch, arr):
            all_enriched.append({**orig, **cls})
        time.sleep(1)

    out_file = "one_shot_enriched.json"
    try:
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(all_enriched, f, ensure_ascii=False, indent=2)
        logger.info("Sauvegardé %d offres enrichies dans %s", len(all_enriched), out_file)
    except Exception as e:
        logger.exception("Échec sauvegarde: %s", e)

if __name__ == "__main__":
    main()

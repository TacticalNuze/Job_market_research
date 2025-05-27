#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import io
import os
import json
import time
import logging
import random
import re
from logging.handlers import RotatingFileHandler

import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai import errors

# ─── UTF-8 console sous Windows ─────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ─── Logger ───────────────────────────────────────────────────────────────────────
logger = logging.getLogger("one_shot_gemini_stream")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO)
fh = RotatingFileHandler("one_shot_gemini_stream.log", encoding="utf-8", maxBytes=5*1024*1024, backupCount=3)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(ch); logger.addHandler(fh)

# ─── Charger la clé API depuis .env ───────────────────────────────────────────────
load_dotenv()
API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    logger.error("Variable GEMINI_API_KEY manquante dans .env")
    sys.exit(1)

# ─── Instanciation du client Gemini ───────────────────────────────────────────────
client = genai.Client(api_key=API_KEY)

# ─── Configuration ────────────────────────────────────────────────────────────────
MODEL = "gemini-2.5-flash-preview-04-17"
INPUT_FILE = "Data_extraction/Traitement/output/backup_offers_20250514_214052.json"
BATCH_SIZE = 10  # nombre d'offres par batch
RETRIES = 3

# ─── Prompts ──────────────────────────────────────────────────────────────────────
PRE_PROMPT = (
    "Avant traitement, homogénéise toutes les catégories de l'offre : minuscules, sans accents."
)
SYSTEM_PROMPT = (
    "TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :\n"
    "1. Détermine si le poste est lié aux données (1) ou non (0)\n"
    "2. Si data_profile=1, catégorise en BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST\n"
    "3. Normalise le niveau d'études (0-5)\n"
    "4. Normalise l'expérience (années → numérique) et déduit la séniorité (Junior, Senior, Expert)\n"
    "5. Extrait les soft skills (e.g., communication, esprit d'équipe...) sous forme de liste\n"
    "6. Extrait les hard skills (e.g., Python, SQL, Hadoop...) sous forme de liste\n"
    "RENVOIE UNIQUEMENT un JSON ARRAY d'objets avec les clés : "
    "is_data_profile, profile, niveau_etudes, niveau_experience, competences_techniques, soft_skills, hard_skills"
)


def clean_and_extract(raw: str):
    """Nettoie et extrait le premier JSON array de la réponse brute."""
    preview = raw[:200].replace("\n", " ")
    logger.debug("clean_and_extract preview: %s", preview)

    # Supprime les virgules terminales et corrige la clé niveau_etudes
    s = re.sub(r",\s*([}\]])", r"\1", raw)
    s = re.sub(r'"niveau_etude"\s*:', '"niveau_etudes":', s)

    # Recherche non-gourmande d'un JSON array
    m = re.search(r"\[\s*?(?:\{.*?\}\s*,?)+\]", s, re.DOTALL)
    if m:
        fragment = m.group(0)
        try:
            return json.loads(fragment)
        except json.JSONDecodeError as e:
            logger.warning("JSONDecodeError sur fragment: %s", e)
            # tentative de correction pour objets collés sans virgule
            fixed_s = re.sub(r"}\s*{", "},{", s)
            m2 = re.search(r"\[\s*?(?:\{.*?\}\s*,?)+\]", fixed_s, re.DOTALL)
            if m2:
                fragment2 = m2.group(0)
                try:
                    return json.loads(fragment2)
                except json.JSONDecodeError as e2:
                    logger.warning("Échec parsing après insertion de virgules: %s", e2)

    # Reconstruction progressive en cas d'échec
    buf, depth = "", 0
    for ch in s:
        if ch == '[':
            depth += 1
        if depth > 0:
            buf += ch
        if ch == ']' and depth > 0:
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(buf)
                except json.JSONDecodeError:
                    buf = ""

    logger.error("Impossible d’extraire un JSON array valide")
    return []


def call_gemini_stream(batch_data: list):
    """Envoie un batch en streaming et assemble la réponse complète."""
    user_input = json.dumps(batch_data, ensure_ascii=False)
    contents = [
        types.Content(
            role="model",
            parts=[types.Part.from_text(text=PRE_PROMPT + "\n" + SYSTEM_PROMPT)]
        ),
        types.Content(
            role="user",
            parts=[types.Part.from_text(text=user_input)]
        ),
    ]
    config = types.GenerateContentConfig(response_mime_type="text/plain")

    full_response = ""
    try:
        for chunk in client.models.generate_content_stream(
            model=MODEL,
            contents=contents,
            config=config,
        ):
            if chunk.text:
                full_response += chunk.text
    except errors.ServerError as e:
        logger.warning("ServerError during streaming: %s", e)
        return []

    return clean_and_extract(full_response)


def main():
    # Lecture du JSON d’entrée
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            all_offers = json.load(f)
    except Exception as e:
        logger.exception("Lecture du fichier d’entrée impossible: %s", e)
        sys.exit(1)

    total = len(all_offers)
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    results = []

    # Traitement en batchs
    for i in range(batches):
        start = i * BATCH_SIZE
        batch = all_offers[start:start + BATCH_SIZE]
        logger.info("Traitement batch %d/%d (offres %d–%d)", i+1, batches, start+1, start+len(batch))

        for attempt in range(1, RETRIES + 1):
            enriched = call_gemini_stream(batch)
            if enriched:
                break
            wait = attempt * 5 + random.random() * 2
            logger.warning("Retry %d pour batch %d après %.1fs", attempt, i+1, wait)
            time.sleep(wait)
        else:
            logger.error("Abandon du batch %d après %d tentatives", i+1, RETRIES)
            enriched = [{}] * len(batch)

        for orig, upd in zip(batch, enriched):
            orig.update(upd)
            results.append(orig)

        time.sleep(1)

    # Sauvegarde JSON
    out_json = "one_shot_enriched_gemini3.json"
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info("Sauvegardé %d offres dans %s", len(results), out_json)

    # Sauvegarde Excel
    df = pd.DataFrame(results)
    out_excel = "one_shot_enriched_gemini3.xlsx"
    df.to_excel(out_excel, index=False)
    logger.info("Excel généré : %s", out_excel)

    print(f"Traitement terminé: {len(results)} offres enrichies.")


if __name__ == "__main__":
    main()

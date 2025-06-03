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

# UTF-8 console sous Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Logger setup
logger = logging.getLogger("one_shot_gemini_stream")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
fh = RotatingFileHandler(
    "one_shot_gemini_stream.log",
    encoding="utf-8",
    maxBytes=5 * 1024 * 1024,
    backupCount=3,
)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt)
fh.setFormatter(fmt)
logger.addHandler(ch)
logger.addHandler(fh)

# Load .env variables
load_dotenv()
API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    logger.error("Variable GEMINI_API_KEY manquante dans .env")
    sys.exit(1)

# Instantiate GenAI client
client = genai.Client(api_key=API_KEY)

# Configuration
MODEL = "gemini-2.0-flash-lite"
INPUT_FILE = "Data_extraction/Traitement/output/backup_offers_20250514_214052.json"
BATCH_SIZE = 10
RETRIES = 3
BACKOFF_BASE = 5

# Normalize profile mapping
PROFILE_MAP = {
    'bi': 'DATA ANALYST',
    'data engineering': 'DATA ENGINEERING',
    'data science': 'DATA SCIENCE',
    'data analyst': 'DATA ANALYST',
    'data scientist': 'DATA SCIENCE',
    'bi developer': 'DATA ENGINEERING',
}

def normalize_profile(raw_profile):
    if not raw_profile or not isinstance(raw_profile, str):
        return 'OTHER'
    key = raw_profile.strip().lower()
    if key in PROFILE_MAP:
        return PROFILE_MAP[key]
    normalized = re.sub(r"[^a-z0-9]+", "_", key)
    return normalized.upper()

# Prompts
PRE_PROMPT = (
    "Avant traitement, homogénéise toutes les catégories de l'offre : minuscules, sans accents."
)
SYSTEM_PROMPT = (
    "TU ES UN EXPERT EN ANALYSE DE POSTES DE DONNÉES. TA MISSION :\n"
    "1. Détermine si le poste est lié aux données (1) ou non (0)\n"
    "2. Si data_profile=1, catégorise en BI / DATA ENGINEERING / DATA SCIENCE / DATA ANALYST\n"
    "3. Normalise le niveau d'études (0-5)\n"
    "4. Normalise l'expérience (années → numérique) et déduit la séniorité (Junior, Senior, Expert)\n"
    "5. Extrait les soft skills sous forme de liste\n"
    "6. Extrait les hard skills sous forme de liste\n"
    "RENVOIE UNIQUEMENT un JSON ARRAY d'objets avec les clés : "
    "is_data_profile, profile, niveau_etudes, niveau_experience, competences_techniques, soft_skills, hard_skills"
)

def clean_and_extract(raw: str):
    logger.debug("Response preview: %s", raw[:200].replace("\n", " "))
    # Primary slice
    start, end = raw.find('['), raw.rfind(']')
    if start != -1 and end != -1:
        frag = raw[start : end + 1]
        try:
            parsed = json.loads(frag)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
    # Regex fallback
    s = re.sub(r",\s*([}\]])", r"\1", raw)
    s = re.sub(r'"niveau_etude"\s*:', '"niveau_etudes":', s)
    m = re.search(r"\[.*?\]", s, re.DOTALL)
    if m:
        frag = m.group(0)
        for candidate in (frag, re.sub(r"}\s*{", "},{", frag)):
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                continue
    # Progressive build
    buf, depth = "", 0
    for ch in raw:
        if ch == '[':
            depth += 1
        if depth > 0:
            buf += ch
        if ch == ']' and depth > 0:
            depth -= 1
            if depth == 0:
                try:
                    parsed = json.loads(buf)
                    if isinstance(parsed, list):
                        return parsed
                except json.JSONDecodeError:
                    buf = ""
    logger.error("Failed to extract JSON array")
    return []

def call_gemini_stream(batch_data: list):
    user_input = json.dumps(batch_data, ensure_ascii=False)
    contents = [
        types.Content(role="model", parts=[types.Part.from_text(text=PRE_PROMPT + "\n" + SYSTEM_PROMPT)]),
        types.Content(role="user", parts=[types.Part.from_text(text=user_input)]),
    ]
    config = types.GenerateContentConfig(response_mime_type="text/plain")

    for attempt in range(1, RETRIES + 1):
        full = ""
        try:
            for chunk in client.models.generate_content_stream(
                model=MODEL, contents=contents, config=config
            ):
                if chunk.text:
                    full += chunk.text
            parsed = clean_and_extract(full)
            if parsed:
                return parsed
        except errors.ClientError as e:
            if e.status_code == 429:
                # Handle quota exhausted
                retry_delay = None
                for detail in e.response_json.get('error', {}).get('details', []):
                    if detail.get('@type', '').endswith('RetryInfo'):
                        delay = detail.get('retryDelay', '')
                        m = re.match(r"(\d+)s", delay)
                        if m:
                            retry_delay = int(m.group(1))
                wait = retry_delay or (BACKOFF_BASE * attempt)
                logger.warning(f"Quota épuisé, attente {wait}s (tentative {attempt})")
                time.sleep(wait)
                continue
            logger.warning(f"ClientError {e.status_code}, retry {attempt}")
            time.sleep(BACKOFF_BASE * attempt)
            continue
        except errors.ServerError as e:
            # Model overloaded or internal error
            wait = BACKOFF_BASE * attempt
            logger.warning(f"ServerError {e.status_code}, retrying in {wait}s")
            time.sleep(wait)
            continue
    # Fallback empty
    logger.error("All retries failed for batch of size %d", len(batch_data))
    return [{}] * len(batch_data)

def main():
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            offers = json.load(f)
    except Exception as e:
        logger.exception("Cannot read input file: %s", e)
        sys.exit(1)

    total, results = len(offers), []
    batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(batches):
        batch = offers[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        logger.info("Batch %d/%d (%d-%d)", i + 1, batches, i * BATCH_SIZE + 1, i * BATCH_SIZE + len(batch))
        enriched = call_gemini_stream(batch)
        for orig, upd in zip(batch, enriched):
            if isinstance(upd, dict):
                orig.update(upd)
            orig['profile'] = normalize_profile(orig.get('profile'))
            results.append(orig)
        time.sleep(1)

    out_json = "one_shot_enriched_gemini2.json"
    with open(out_json, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    pd.DataFrame(results).to_excel("one_shot_enriched_gemini2.xlsx", index=False)
    logger.info("Processing complete: %d enriched offers", len(results))

if __name__ == "__main__":
    main()

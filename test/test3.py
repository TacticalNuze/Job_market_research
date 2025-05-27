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
from google.genai import types, errors
import unicodedata
import jsonschema
from jsonschema import validate

# UTF-8 console sous Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Logger setup
logger = logging.getLogger("ONE_SHOT_GEMINI_STREAM")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO)
fh = RotatingFileHandler(
    "ONE_SHOT_GEMINI_STREAM.LOG", encoding="utf-8", maxBytes=5*1024*1024, backupCount=3
)
fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(ch); logger.addHandler(fh)

# Load API key
load_dotenv()
API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    logger.error("GEMINI_API_KEY MISSING")
    sys.exit(1)

# Instantiate client
client = genai.Client(api_key=API_KEY)

# Config
MODEL = "models/gemini-1.5-flash"
INPUT_FILE = "Data_extraction/Traitement/output/backup_offers_20250514_214052.json"
BATCH_SIZE = 30
RETRIES = 3
BACKOFF_BASE = 5

# JSON schema for input validation
INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "job_url": {"type": "string"},
        "titre": {"type": "string"},
        "via": {"type": "string"},
        "publication_date": {"type": "string", "format": "date"}
    },
    "required": ["job_url", "titre", "via", "publication_date"]
}

# Profile normalization map
PROFILE_MAP = {
    'bi': 'business intelligence',
    'business intelligence': 'business intelligence',
    'data engineering': 'data engineering',
    'data science': 'data science',
    'data analyst': 'data analyst',
    'data scientist': 'data science',
}

def normalize_text(text: str) -> str:
    # Remove accents, lowercase, strip
    n = unicodedata.normalize('NFKD', text)
    return unicodedata.normalize('NFKC', n.encode('ASCII', 'ignore').decode()).lower().strip()


def normalize_profile(raw_profile):
    if not raw_profile or not isinstance(raw_profile, str):
        return 'unknown'
    key = normalize_text(raw_profile)
    return PROFILE_MAP.get(key, key)

# Prompts etc. (unchanged)
PRE_PROMPT = (
    "BEFORE PROCESSING EACH JOB OFFER, NORMALIZE ALL TEXT FIELDS: REMOVE ACCENTS, CONVERT TO LOWERCASE, TRIM WHITESPACE."
)
SYSTEM_PROMPT = (
    "YOU ARE A SENIOR EXPERT IN JOB ANALYSIS FOR DATA ROLES. "
    "YOU WILL RECEIVE A LIST OF RAW JOB OFFERS AS JSON OBJECTS. "
    "FOR EACH OFFER, RETURN A STRUCTURED JSON OBJECT WITH THE FOLLOWING FIELDS:"
    "\n- job_url (string): THE ORIGINAL JOB URL."
    "\n- titre (string): THE JOB TITLE."
    "\n- via (string): THE PUBLICATION SOURCE (e.g., LinkedIn, Indeed)."
    "\n- publication_date (string): THE PUBLICATION DATE (format YYYY-MM-DD)."
    "\n- is_data_profile (integer): 1 IF THE JOB IS DATA-RELATED, ELSE 0."
    "\n- profile (string): IF is_data_profile = 1, CLASSIFY INTO: 'business intelligence', 'data engineering', 'data science', or 'data analyst'."
    "\n- education_level (integer): NORMALIZED EDUCATION LEVEL: 0=None, 1=High School, 2=Bachelor, 3=Master, 4=Post-Master, 5=Doctorate."
    "\n- experience_years (integer): ESTIMATED YEARS OF EXPERIENCE REQUIRED."
    "\n- seniority (string): 'junior' if <=2 years, 'mid' if 2–5, 'senior' if >5."
    "\n- soft_skills (list of strings): EXTRACTED SOFT SKILLS."
    "\n- hard_skills (list of strings): EXTRACTED TECHNICAL SKILLS."
    "\n- location (string or null): CITY AND COUNTRY IF AVAILABLE."
    "\n- salary_range (string or null): EXTRACTED RANGE (e.g., '40k-50k EUR') OR NULL."
    "\n\n8. RETURN ONLY A JSON ARRAY WITH THESE EXACT KEYS IN LOWERCASE."
)

def clean_and_extract(raw: str) -> list:
    # unchanged
    start, end = raw.find('['), raw.rfind(']')
    if 0 <= start < end:
        frag = raw[start:end+1]
        try:
            return json.loads(frag)
        except:
            frag = re.sub(r"}\s*{", ",{", frag)
            try: return json.loads(frag)
            except: pass
    s = re.sub(r",\s*([}\]])", r"\1", raw)
    m = re.search(r"\[.*?\]", s, re.DOTALL)
    if m:
        frag = m.group(0)
        try: return json.loads(frag)
        except:
            frag = re.sub(r"}\s*{", ",{", frag)
            try: return json.loads(frag)
            except: pass
    buf, depth = "", 0
    for ch in raw:
        if ch == '[': depth += 1
        if depth > 0: buf += ch
        if ch == ']' and depth > 0:
            depth -= 1
            if depth == 0:
                try: return json.loads(buf)
                except: buf = ""
    logger.error("UNABLE TO EXTRACT VALID JSON ARRAY")
    return []

def call_gemini_stream(batch: list) -> list:
    # unchanged
    payload = json.dumps(batch, ensure_ascii=False)
    contents = [
        types.Content(role="model", parts=[types.Part.from_text(text=PRE_PROMPT+"\n"+SYSTEM_PROMPT)]),
        types.Content(role="user", parts=[types.Part.from_text(text=payload)])
    ]
    cfg = types.GenerateContentConfig(response_mime_type="text/plain")
    for attempt in range(1, RETRIES+1):
        try:
            full = ""
            for chunk in client.models.generate_content_stream(model=MODEL, contents=contents, config=cfg):
                if chunk.text: full += chunk.text
            parsed = clean_and_extract(full)
            if parsed:
                return parsed
        except errors.ClientError as e:
            if e.status_code == 429:
                time.sleep(BACKOFF_BASE * attempt)
                continue
            time.sleep(BACKOFF_BASE * attempt)
        except errors.ServerError:
            time.sleep(BACKOFF_BASE * attempt)
    logger.error("GIVING UP AFTER %d ATTEMPTS", RETRIES)
    return [{}] * len(batch)

def main():
    try:
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            offers = json.load(f)
    except Exception as e:
        logger.exception("READ ERROR: %s", e)
        sys.exit(1)

    # Validate and normalize input
    clean_offers = []
    for o in offers:
        try:
            validate(instance=o, schema=INPUT_SCHEMA)
            # normalize textual fields
            o['titre'] = normalize_text(o.get('titre', ''))
            o['via'] = normalize_text(o.get('via', ''))
            o['job_url'] = o['job_url'].strip()
            # parse date
            try:
                pd.to_datetime(o['publication_date'], format='%Y-%m-%d')
            except:
                o['publication_date'] = None
            clean_offers.append(o)
        except jsonschema.ValidationError as ve:
            logger.warning(f"Offer schema invalid, skipping: {ve.message}")
    results = []
    for i in range(0, len(clean_offers), BATCH_SIZE):
        batch = clean_offers[i:i+BATCH_SIZE]
        logger.info(f"PROCESSING BATCH {i+1}-{i+len(batch)}")
        enriched = call_gemini_stream(batch)
        for orig, upd in zip(batch, enriched):
            if isinstance(upd, dict):
                orig.update(upd)
            orig['profile'] = normalize_profile(orig.get('profile'))
            results.append(orig)
        time.sleep(1)

    with open('one_shot_enriched_gemini3.json','w',encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    df = pd.DataFrame(results)
    try:
        df.to_excel('one_shot_enriched_gemini3.xlsx', index=False, engine='openpyxl')
    except PermissionError:
        logger.error("PERMISSION DENIED WRITING EXCEL. CLOSE THE FILE AND RETRY.")
        sys.exit(1)

if __name__ == '__main__':
    main()

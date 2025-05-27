#!/usr/bin/env python3
"""
Process SourceSignal job offers: load JSON/NDJSON, normalize, enrich via Gemini,
write enriched data profiles to JSON and Excel, following original script outputs and formats.
"""
import sys
import io
import os
import json
import time
import logging
import re
import unicodedata
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import pandas as pd
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai import types
import argparse

# --- UTF-8 console output for Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# --- Logger Configuration
def setup_logger():
    logger = logging.getLogger("PROCESS_GEMINI")
    logger.setLevel(logging.DEBUG)

    # Console handler (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler (DEBUG+)
    fh = RotatingFileHandler("process_gemini.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

logger = setup_logger()

# --- Configuration Constants
MODEL = "gemini-1.5-flash-latest"
BATCH_SIZE = 10
RETRIES = 3
BACKOFF = 5  # seconds initial backoff

# --- Initialize Gemini API
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    logger.critical("GEMINI_API_KEY missing. Please set it in .env or as environment variable.")
    sys.exit(1)
genai.configure(api_key=api_key)
logger.info(f"Gemini API configured. Model: {MODEL}")
client = genai.GenerativeModel(MODEL)

# --- Normalization Helpers
MONTHS_FR = {
    'janvier':1,'février':2,'mars':3,'avril':4,'mai':5,'juin':6,
    'juillet':7,'août':8,'septembre':9,'octobre':10,'novembre':11,'décembre':12
}
MONTHS_EN = {
    'january':1,'february':2,'march':3,'april':4,'may':5,'june':6,
    'july':7,'august':8,'september':9,'october':10,'november':11,'december':12
}
MONTHS = {**MONTHS_FR, **MONTHS_EN}
MONTHS.update({k[:3]:v for k,v in MONTHS.items()})

def normalize_text(s):
    """
    Strip accents, lowercase, trim whitespace.
    """
    if not s:
        return ""
    n = unicodedata.normalize('NFKD', s).encode('ASCII','ignore').decode()
    return unicodedata.normalize('NFKC', n).lower().strip()


def normalize_date(s):
    """
    Normalize various date formats to YYYY-MM-DD.
    Handles 'today', 'yesterday', 'X days/weeks/months ago',
    timestamps 'YYYY-MM-DD HH:MM:SS', and common formats.
    """
    if not s or not isinstance(s, str):
        return None
    key = s.strip().lower()
    today = datetime.now()
    # relative
    if 'today' in key or 'aujourd' in key:
        return today.strftime('%Y-%m-%d')
    if 'yesterday' in key or 'hier' in key:
        return (today - timedelta(days=1)).strftime('%Y-%m-%d')
    m = re.search(r"(\d+)\s+(day|days|jour|jours|week|weeks|semaine|semaines|month|months|mois)\s+ago", key)
    if m:
        num,unit = int(m.group(1)), m.group(2)
        if 'day' in unit or 'jour' in unit:
            return (today - timedelta(days=num)).strftime('%Y-%m-%d')
        if 'week' in unit or 'semaine' in unit:
            return (today - timedelta(weeks=num)).strftime('%Y-%m-%d')
        return (today - timedelta(days=30*num)).strftime('%Y-%m-%d')
    # fixed formats
    formats = [
        '%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y',
        '%Y/%m/%d', '%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y'
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            continue
    # day + month name
    m2 = re.match(r"(\d{1,2})\s+([A-Za-z]+)", s)
    if m2:
        d = int(m2.group(1))
        mn = m2.group(2).lower()
        if mn in MONTHS:
            try:
                return datetime(today.year, MONTHS[mn], d).strftime('%Y-%m-%d')
            except ValueError:
                pass
    logger.warning(f"Could not parse date: {s}")
    return None


def parse_location(s):
    """
    Parse location string into city, region, country, remote.
    """
    out = {'city':None, 'region':None, 'country':None, 'remote':False}
    if not s:
        return out
    txt = normalize_text(s)
    if 'remote' in txt or 'télétravail' in txt or 'à distance' in txt:
        out['remote'] = True
        return out
    parts = [p.strip() for p in s.split(',') if p.strip()]
    if parts:
        out['city'] = normalize_text(parts[0])
        if len(parts) > 1:
            last = normalize_text(parts[-1])
            if len(last) <= 4 or last in MONTHS_EN:
                out['country'] = last
            else:
                out['region'] = last
        if len(parts) > 2:
            out['region'] = normalize_text(parts[1])
    return out

# --- Loader for SourceSignal input

def load_sorsignal_input(path):
    """
    Load JSON array or NDJSON file and map to normalized schema.
    """
    items = []
    with open(path, 'r', encoding='utf-8') as f:
        try:
            raw = json.load(f)
            items = raw if isinstance(raw, list) else [raw]
        except json.JSONDecodeError:
            f.seek(0)
            for line in f:
                line=line.strip()
                if not line:
                    continue
                try:
                    items.append(json.loads(line))
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON line: {line}")
    mapped = []
    for it in items:
        mapped.append({
            'job_url': it.get('url'),
            'titre': normalize_text(it.get('title')),
            'via': None,
            'contrat': normalize_text(it.get('employment_type')),
            'type_travail': None,
            'publication_date': normalize_date(it.get('created') or it.get('time_posted')),
            'location': parse_location(it.get('location')),
            'description': normalize_text(it.get('description')),
            'company_name': normalize_text(it.get('company_name'))
        })
    return mapped

# --- Gemini prompts
PRE_PROMPT = (
    "BEFORE PROCESSING, NORMALIZE ALL FIELDS with these rules:\n"
    "- REMOVE ACCENTS: strip diacritics from text fields.\n"
    "- LOWERCASE: convert all letters to lowercase.\n"
    "- TRIM: remove leading/trailing whitespace.\n"
    "- DATES: handle:\n"
    "    * 'today', 'yesterday' (relative to current date)\n"
    "    * ISO format: YYYY-MM-DD\n"
    "    * DD-MM-YYYY and DD/MM/YYYY\n"
    "    * 'DD MonthName-HH:MM' (e.g., '12 May-16:35'), assume current year.\n"
    "    * 'X days/weeks/months ago'\n"
    "    * Always output in YYYY-MM-DD format.\n"
    "- LOCATION: parse 'lieu' into {city:string|null, region:string|null, country:string|null, remote:boolean}. 'remote' should be a boolean. If only city is given, infer region/country if possible or leave null. If no location found but 'remote' keyword exists, set remote to true and city/region/country to null.\n"
    "- CONTRACT: normalize 'contrat' examples: 'cdi', 'cdd', 'freelance', 'stage', 'alternance'.\n"
    "- WORK MODE: normalize 'type_travail' examples: 'on-site', 'remote', 'hybrid'.\n"
)

SYSTEM_PROMPT = (
    "YOU ARE A SENIOR DATA JOB ANALYST. STRICTLY FOLLOW THESE RULES FOR EACH INPUT OFFER:\n"
    "1) PRESERVE ORIGINAL FIELDS at top: job_url (string), titre (string|null), via (string|null), contrat (string|null), type_travail (string|null).\n"
    "2) DERIVED FIELDS in this exact order, with correct types:\n"
    "   a) is_data_profile: boolean (true if role is data-related).\n"
    "   b) profile: string, one of [\n"
    "      'data analyst', 'data scientist', 'data engineer', 'business intelligence analyst',\n"
    "      'machine learning engineer', 'data architect', 'data product manager', 'data visualization specialist',\n"
    "      'data governance analyst', 'quantitative analyst', 'MLOps engineer', 'AI engineer', 'database administrator',\n"
    "      'research scientist', 'data strategist', 'analytics engineer', 'IoT data specialist', 'data quality analyst',\n"
    "      'Big Data engineer', 'cloud data engineer', 'data ethicist', 'data privacy officer', 'data security analyst',\n"
    "      'NLP engineer', 'computer vision engineer', 'bioinformatics data scientist', 'data consultant',\n"
    "      'fraud analyst', 'risk analyst', 'marketing analyst', 'financial data analyst', 'supply chain analyst',\n"
    "      'operations analyst', 'database developer', 'CRM analyst', 'ERP specialist', 'actuarial analyst',\n"
    "      'geospatial data scientist', 'clinical data manager', 'biostatistician', 'data migration specialist',\n"
    "      'business systems analyst', 'web analytics specialist', 'customer insights analyst', 'pricing analyst',\n"
    "      'UX data analyst', 'site reliability engineer (SRE) - data', 'technical account manager - data',\n"
    "      'solution architect - data', 'sales engineer - data', 'pre-sales engineer - data', 'data evangelist',\n"
    "      'growth analyst', 'e-commerce analyst', 'media analyst', 'content analyst', 'network data analyst',\n"
    "      'telecom data analyst', 'energy data analyst', 'environmental data analyst', 'healthcare data analyst',\n"
    "      'genomics data scientist', 'clinical research data analyst', 'epidemiology data analyst',\n"
    "      'financial quantitative analyst', 'algorithmic trading analyst', 'credit risk analyst', 'market risk analyst',\n"
    "      'anti-money laundering (AML) analyst', 'compliance data analyst', 'cybersecurity data analyst',\n"
    "      'threat intelligence analyst', 'forensic data analyst', 'devops engineer - data',\n"
    "      'unspecified', 'none'\n" # Added 'unspecified' and 'none' explicitly for clarity
    "      ]. Use 'unspecified' if it's clearly a data role but doesn't fit a specific category from this list, or 'none' if it's not a data role.\n"
    "      - Examples: 'data scientist', 'machine learning engineer', 'data governance analyst', 'data consultant'.\n"
    "   c) education_level: integer 0–5 (0=none,1=high school,2=bachelor,3=master,4=phd,5=postdoc).\n"
    "   d) experience_years: integer or null (meaningful years, e.g., 2, 5).\n"
    "   e) seniority: string 'junior','mid','senior' based on experience_years.\n"
    "   f) hard_skills: array of strings (at least 3 technical skills).\n"
    "      - Format: ['python', 'sql', 'spark']. No duplicates, lowercase.\n"
    "   g) soft_skills: array of strings (at least 3 behavioral skills).\n"
    "      - Format: ['communication', 'teamwork', 'adaptability']. No duplicates.\n"
    "   h) company_name: string (exact hiring company name).\n"
    "   i) sector: array of strings (primary sectors).\n"
    "   j) location: object {city:string|null, region:string|null, country:string|null, remote:boolean}.\n"
    "   k) salary_range: object {min:number|null, max:number|null, currency:string|null, period:string|null}.\n"
    "   l) publication_date: string: YYYY-MM-DD.\n"
    "3) NO ADDITIONAL FIELDS. RETURN ONLY THE JSON ARRAY OF OBJECTS."
)

# --- Extract JSON array from API response

def clean_and_extract(raw_text):
    start, end = raw_text.find('['), raw_text.rfind(']')
    if 0 <= start < end:
        frag = raw_text[start:end+1]
        try:
            return json.loads(frag)
        except json.JSONDecodeError:
            pass
    return []

# --- Post-process parsed results

def post_process_gemini_output(parsed, size):
    out = []
    for i in range(size):
        itm = parsed[i] if i < len(parsed) else {}
        itm['is_data_profile'] = bool(itm.get('is_data_profile'))
        out.append(itm)
    return out

# --- Call Gemini API with retry/backoff

def call_gemini(batch):
    contents = [{
        'role': 'user',
        'parts': [{'text': PRE_PROMPT + SYSTEM_PROMPT + json.dumps(batch, ensure_ascii=False)}]
    }]
    cfg = types.GenerationConfig(response_mime_type='text/plain', temperature=0.7, top_p=0.95, top_k=40)
    for attempt in range(RETRIES):
        try:
            full = ''
            for chunk in client.generate_content(contents=contents, generation_config=cfg, stream=True):
                if hasattr(chunk, 'text') and chunk.text:
                    full += chunk.text
            parsed = clean_and_extract(full)
            return post_process_gemini_output(parsed, len(batch))
        except Exception as e:
            logger.warning(f"Gemini call failed (attempt {attempt+1}): {e}")
            time.sleep(BACKOFF * (2 ** attempt))
    return [{}] * len(batch)

# --- Main Pipeline

def main():
    parser = argparse.ArgumentParser(description='Process SourceSignal offers via Gemini')
    parser.add_argument('input_file', help='Path to JSON/NDJSON file')
    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        logger.critical(f"Input file not found: {args.input_file}")
        sys.exit(1)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_dir = 'output'
    os.makedirs(out_dir, exist_ok=True)
    json_path = os.path.join(out_dir, f'enriched_data_profiles_{timestamp}.json')
    xlsx_path = os.path.join(out_dir, f'enriched_data_profiles_{timestamp}.xlsx')

    offers = load_sorsignal_input(args.input_file)
    logger.info(f"Loaded {len(offers)} offers from {args.input_file}")

    profiles = []
    with open(json_path, 'w', encoding='utf-8') as jf:
        jf.write('[\n')
        first = False
        for i in range(0, len(offers), BATCH_SIZE):
            batch = offers[i:i+BATCH_SIZE]
            logger.info(f"Processing batch {i+1}-{i+len(batch)}")
            enriched = call_gemini(batch)
            for orig, enr in zip(batch, enriched):
                merged = {**orig, **(enr or {})}
                if merged.get('is_data_profile'):
                    if first:
                        jf.write(',\n')
                    json.dump(merged, jf, ensure_ascii=False, indent=2)
                    profiles.append(merged)
                    first = True
            time.sleep(1)
        jf.write('\n]\n')
    logger.info(f"Written {len(profiles)} profiles to {json_path}")

    if profiles:
        df = pd.json_normalize(profiles, sep='_')
        df.to_excel(xlsx_path, index=False)
        logger.info(f"Excel saved to {xlsx_path}")
    else:
        logger.warning("No data profiles found; Excel not created.")

if __name__ == '__main__':
    main()

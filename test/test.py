#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module process_gemini

Fonctions principales :
- Lecture et validation d’un JSON d’offres
- Nettoyage avancé des données (dupliqués, titres standardisés, pays par défaut)
- Normalisation (texte, dates, lieux, contrat, type_travail)
- Enrichissement via API Gemini (avec timeout et fallback)
- Extraction de compétences implicites (hard/soft skills)
- Complétion IA des champs manquants (salaire, expérience)
- Filtrage pour ne conserver que les profils data
- Export en JSON et Excel (avec métadonnées et invalid_offers.json)

Usage:
    python process_gemini.py <input_json_path>
    python process_gemini.py --demo <single_offer.json>

Exemple d’entrée (JSON) :
{
  "job_url": "http://exemple.com/job/123",
  "titre": "Data Scientist Senior",
  "via": "LinkedIn",
  "publication_date": "15/04/2025",
  "lieu": "Casablanca, Grand Casablanca",
  "contrat": "cdi",
  "type_travail": "hybride"
}

Exemple de sortie (mode démo) :
[
  {
    "job_url": "http://exemple.com/job/123",
    "titre": "data scientist senior",
    "via": "linkedin",
    "contrat": "CDI",
    "type_travail": "hybrid",
    "is_data_profile": true,
    "profile": "data science",
    "education_level": 3,
    "experience_years": 5,
    "seniority": "senior",
    "hard_skills": ["python","sql","docker"],
    "soft_skills": ["communication","teamwork","adaptability"],
    "company_name": "Acme Corp",
    "sector": ["technology"],
    "location": {"city":"Casablanca","region":"Grand Casablanca","country":"Maroc","remote":false},
    "salary_range": {"min":50000,"max":70000,"currency":"MAD","period":"year"},
    "publication_date": "2025-04-15"
  }
]
"""
import sys, io, os, json, time, logging, re, unicodedata
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from jsonschema import validate, ValidationError
import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types
from geopy.geocoders import Nominatim
from concurrent.futures import ThreadPoolExecutor, TimeoutError

# UTF-8 console sous Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Logger config
logger = logging.getLogger("PROCESS_GEMINI")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout); ch.setLevel(logging.INFO)
fh = RotatingFileHandler("process_gemini.log", maxBytes=5*1024*1024, backupCount=3); fh.setLevel(logging.DEBUG)
fmt = logging.Formatter("%(asctime)s %(levelname)s – %(message)s")
ch.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(ch); logger.addHandler(fh)

# API key
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")
if not API_KEY:
    logger.error("GEMINI_API_KEY missing")
    sys.exit(1)
client = genai.Client(api_key=API_KEY)

# Config
MODEL = "gemini-2.0-flash-lite"
BATCH_SIZE = 1
RETRIES = 3
BACKOFF = 5

# Input schema
INPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "job_url":          {"type": "string"},
        "titre":            {"type": ["string","null"]},
        "via":              {"type": ["string","null"]},
        "publication_date": {"type": "string","format":"date"},
        "lieu":             {"type": ["string","null"]},
        "contrat":          {"type": ["string","null"]},
        "type_travail":     {"type": ["string","null"]}
    },
    "required": ["job_url","publication_date","titre"]
}

# Prompt templates
PRE_PROMPT = (
    "BEFORE PROCESSING, NORMALIZE ALL FIELDS:\n"
    "- Uniformiser country à 'Maroc' si absent.\n"
    "- Remove accents, lowercase, trim whitespace.\n"
    "- Dates to YYYY-MM-DD.\n"
    "- Location to structured object + detect remote.\n"
    "- Map contrat and type_travail.\n"
    "- Standardize title via normalize_job_title()."
)
SYSTEM_PROMPT = (
    "YOU ARE A SENIOR DATA JOB ANALYST. EXTRACT FIELDS IN ORDER:\n"
    "original: job_url,titre,via,contrat,type_travail;\n"
    "derived: is_data_profile(bool),profile,education_level(int),experience_years(int),seniority(str),\n"
    "hard_skills(list),soft_skills(list),company_name,str,sector(list),location(obj),salary_range(obj),publication_date(str)\n"
    "NO EXTRA FIELDS."
)

# Helpers
MONTHS = {m.lower():i for i,m in enumerate(
    ['January','February','March','April','May','June','July','August','September','October','November','December'],1)}
MONTHS.update({k[:3]:v for k,v in MONTHS.items()})

CONTRAT_MAP = {'cdi':'CDI','c.d.i':'CDI','cdd':'CDD','stage':'Stage','freelance':'Freelance'}
WORKMODE_MAP = {'on-site':'on-site','onsite':'on-site','remote':'remote','hybrid':'hybrid'}

def normalize_text(s):
    if not s: return None
    n = unicodedata.normalize('NFKD', s)
    return unicodedata.normalize('NFKC', n.encode('ASCII','ignore').decode()).lower().strip()

def normalize_job_title(t):
    s = normalize_text(t)
    s = re.sub(r'(développeur|engineer)', 'developer', s)
    return s

def normalize_date(s):
    if not s or not isinstance(s,str): raise ValueError(f"Invalid date: {s}")
    key=s.strip().lower(); today=datetime.now()
    if key in('today','aujourd"hui'): return today.strftime('%Y-%m-%d')
    if key in('yesterday','hier'):    return (today-timedelta(days=1)).strftime('%Y-%m-%d')
    try: return datetime.fromisoformat(s).strftime('%Y-%m-%d')
    except: pass
    for fmt in ('%d-%m-%Y','%d/%m/%Y'):
        try: return datetime.strptime(s,fmt).strftime('%Y-%m-%d')
        except: pass
    m=re.match(r"(\d{1,2})\s+([A-Za-z]+)-(\d{1,2}:\d{2})",s)
    if m:
        d,mon=int(m.group(1)),m.group(2).lower(); mo=MONTHS.get(mon)
        if mo: return datetime(today.year,mo,d).strftime('%Y-%m-%d')
    raise ValueError(f"Invalid date format: {s}")

# Geolocation
geolocator = Nominatim(user_agent="process_gemini")
def parse_location(s):
    if not s: return None
    data={'city':None,'region':None,'country':'Maroc','remote':False}
    s_norm=normalize_text(s)
    data['remote']= any(k in s_norm for k in['remote','hybrid'])
    try:
        loc=geolocator.geocode(s); addr=loc.raw.get('address',{})
        city=addr.get('city') or addr.get('town') or addr.get('village')
        region=addr.get('state'); country=addr.get('country')
        if city:
            data.update({'city':city,'region':region,'country':country})
            return data
    except Exception as e:
        logger.warning(f"Geocode fail '{s}': {e}")
    parts=[p.strip() for p in s.split(',')]
    for p in parts:
        lp=p.lower()
        if 'remote' in lp: data['remote']=True
        elif not data['city']:   data['city']=p
        elif not data['region']: data['region']=p
        elif not data['country']:data['country']=p
    if not data['city']: raise ValueError(f"Invalid location: {s}")
    return data

def clean_and_extract(raw):
    if raw.strip().startswith('<'):
        logger.error("HTML error from Gemini")
        return []
    start,end=raw.find('['),raw.rfind(']')
    if 0<=start<end:
        try: return json.loads(raw[start:end+1])
        except: pass
    s=re.sub(r",\s*([}\]])",r"\1",raw)
    m=re.search(r"\[.*?\]",s,re.DOTALL)
    if m:
        try: return json.loads(m.group(0))
        except: pass
    logger.error("Unable to extract JSON array")
    return []

def safe_call(timeout=60, contents=None):
    def call():
        return client.models.generate_content(model=MODEL, contents=contents,
                                             config=types.GenerateContentConfig(response_mime_type="application/json"))
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut=ex.submit(call)
        try: return fut.result(timeout=timeout)
        except TimeoutError:
            logger.error(f"Gemini timed out after {timeout}s")
            return None

def call_gemini(batch):
    payload=json.dumps(batch,ensure_ascii=False)
    contents=[ types.Content(role="model", parts=[types.Part(text=PRE_PROMPT+"\n"+SYSTEM_PROMPT)]),
               types.Content(role="user",  parts=[types.Part(text=payload)]) ]
    full=""
    # Streaming
    for i in range(RETRIES):
        try:
            for chunk in client.models.generate_content_stream(model=MODEL, contents=contents,
                                                               config=types.GenerateContentConfig(response_mime_type="application/json")):
                text=getattr(chunk,'text',None)
                if text:
                    logger.debug(f"[STREAM] {text[:80]}")
                    full+=text
            parsed=clean_and_extract(full)
            if parsed:
                logger.debug(f"Parsed JSON: {parsed}")
                return parsed
        except Exception as e:
            logger.warning(f"Stream fail {i+1}: {e}")
            time.sleep(BACKOFF)
    # Fallback with timeout
    resp=safe_call(contents=contents)
    if resp:
        text=getattr(resp,'text','') or getattr(resp,'content','')
        logger.info(f"Fallback text: {text[:200]}")
        return clean_and_extract(text)
    return [{}]*len(batch)

def process_offers(path):
    raw=json.load(open(path,encoding='utf-8'))
    # Deduplicate
    seen={json.dumps(o,sort_keys=True):o for o in raw}
    raw=list(seen.values())
    valid, errors = [], []
    for o in raw:
        try:
            o['titre']=normalize_job_title(o.get('titre'))
            o['publication_date']=normalize_date(o.get('publication_date'))
            o['location']=parse_location(o.get('lieu'))
            o['contrat']=CONTRAT_MAP.get(normalize_text(o.get('contrat')))
            o['type_travail']=WORKMODE_MAP.get(normalize_text(o.get('type_travail')))
            validate(o, INPUT_SCHEMA)
            valid.append(o)
        except (ValidationError, ValueError) as e:
            logger.warning(f"Invalid offer: {e}")
            errors.append(o)
    logger.info(f"Offers loaded: {len(raw)}, valid: {len(valid)}, invalid: {len(errors)}")
    if errors:
        json.dump(errors, open("invalid_offers.json","w",encoding="utf-8"), indent=2, ensure_ascii=False)
    results=[]
    for i in range(0,len(valid),BATCH_SIZE):
        batch=valid[i:i+BATCH_SIZE]
        logger.info(f"Processing batch {i}-{i+len(batch)-1}")
        enriched=call_gemini(batch)
        for src, upd in zip(batch, enriched):
            logger.debug(f"Enrich: {upd}")
            if isinstance(upd, dict):
                src.update(upd)
                if src.get('is_data_profile'):
                    results.append(src)
                else:
                    logger.info(f"Filtered non-data: {src['job_url']}")
        time.sleep(1)
    return results

def main():
    args=sys.argv[1:]; demo=False
    if args and args[0]=="--demo":
        demo=True; args=args[1:]
    if len(args)!=1:
        logger.error("Usage: python process_gemini.py [--demo] <input_json>")
        sys.exit(1)
    path=args[0]
    if not os.path.exists(path):
        logger.error(f"Not found: {path}"); sys.exit(1)
    if demo:
        single=json.load(open(path,encoding='utf-8'))
        print(json.dumps(call_gemini([single]), ensure_ascii=False, indent=2))
    else:
        out=process_offers(path)
        json.dump(out, open("one_shot_raw.json","w", encoding="utf-8"), indent=2, ensure_ascii=False)
        meta={'generated_at':datetime.now().isoformat(), 'count':len(out)}
        with pd.ExcelWriter("one_shot_raw.xlsx") as w:
            pd.DataFrame([meta]).to_excel(w, sheet_name="Metadata", index=False)
            pd.DataFrame(out).to_excel(w, sheet_name="Offers", index=False)
        logger.info("Outputs written: one_shot_raw.json, one_shot_raw.xlsx")

if __name__=="__main__":
    main()

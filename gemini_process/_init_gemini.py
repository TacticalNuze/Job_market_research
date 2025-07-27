import io
import os
import re
import sys
import json
import time
import unicodedata
import logging
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import google.generativeai as genai
from dotenv import load_dotenv
from google.generativeai import types

# ─── Logger configuration ─────────────────────────────────────────────
logger = logging.getLogger("GEMINI_INIT")
logger.setLevel(logging.DEBUG)

console = logging.StreamHandler(sys.stdout)
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console)

file_handler = RotatingFileHandler("gemini_pipeline.log", maxBytes=5*1024*1024, backupCount=2)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
file_handler.setLevel(logging.DEBUG)
logger.addHandler(file_handler)

# ─── Load Gemini API and model ─────────────────────────────────────────
MODEL = "gemini-1.5-flash-latest"

load_dotenv()
GEMINI_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_KEY:
    logger.critical("❌ GEMINI_API_KEY manquant dans le .env")
    sys.exit(1)

genai.configure(api_key=GEMINI_KEY)
client = genai.GenerativeModel(MODEL)
logger.info(f"✅ Gemini model initialisé : {MODEL}")

# ─── Text Normalization ───────────────────────────────────────────────
def normalize_text(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode()
    return unicodedata.normalize("NFKC", s).lower().strip()


# ─── Date Normalization ───────────────────────────────────────────────
MONTHS = {
    **{
        "janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
        "juillet": 7, "août": 8, "septembre": 9, "octobre": 10,
        "novembre": 11, "décembre": 12,
    },
    **{
        "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
        "july": 7, "august": 8, "september": 9, "october": 10,
        "november": 11, "december": 12,
    }
}
MONTHS.update({k[:3]: v for k, v in MONTHS.items()})

def normalize_date(s: str | None) -> str | None:
    if not s or not isinstance(s, str):
        return None
    today = datetime.now()
    key = s.lower().strip()

    if "aujourd" in key or "today" in key:
        return today.strftime("%Y-%m-%d")
    if "hier" in key or "yesterday" in key:
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")

    rel = re.search(r"(\d+)\s+(jour|day|semaine|week|mois|month)s?", key)
    if rel:
        num, unit = int(rel.group(1)), rel.group(2)
        if "jour" in unit or "day" in unit:
            return (today - timedelta(days=num)).strftime("%Y-%m-%d")
        if "semaine" in unit or "week" in unit:
            return (today - timedelta(weeks=num)).strftime("%Y-%m-%d")
        if "mois" in unit or "month" in unit:
            return (today - timedelta(days=30*num)).strftime("%Y-%m-%d")

    date_formats = [
        "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d",
        "%d %B %Y", "%d %b %Y", "%b %d, %Y", "%B %d, %Y"
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    match = re.match(r"(\d{1,2})\s+([a-zA-Zéû]+)", s)
    if match:
        day, month_str = int(match.group(1)), match.group(2).lower()
        month = MONTHS.get(month_str[:3])
        if month:
            try:
                return datetime(today.year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

    logger.warning(f"❗ Date non reconnue : {s}")
    return None


# ─── Location Parsing ───────────────────────────────────────────────
def parse_location(s: str | None) -> dict:
    location = {"city": None, "region": None, "country": None, "remote": False}
    if not s:
        return location

    s_clean = normalize_text(s)
    if any(k in s_clean for k in ["remote", "télétravail", "distance"]):
        location["remote"] = True
        parts = [p for p in s_clean.split(",") if "remote" not in p]
        if parts:
            location["city"] = parts[0]
    else:
        parts = [normalize_text(p) for p in s.split(",")]
        if parts:
            location["city"] = parts[0]
            if len(parts) > 1:
                location["region"] = parts[1]
            if len(parts) > 2:
                location["country"] = parts[-1]

    return location


# ─── Clean JSON Output from Gemini ──────────────────────────────────
def clean_and_extract(raw_text: str) -> list[dict]:
    try:
        start, end = raw_text.find("["), raw_text.rfind("]")
        return json.loads(raw_text[start:end+1])
    except Exception:
        logger.warning("⛔ Extraction JSON directe échouée. Fallback regex...")
    
    matches = re.findall(r"\{.*?\}", raw_text, re.DOTALL)
    results = []
    for m in matches:
        try:
            data = json.loads(m)
            if isinstance(data, dict):
                results.append(data)
        except Exception:
            continue
    return results


# ─── Prompt Gemini ───────────────────────────────────────────────────
PRE_PROMPT = (
    "Tu es un assistant expert en recrutement dans les domaines de la Data et de l'IA. "
    "Ta tâche est d'analyser les offres d'emploi et de détecter si elles sont liées à ces domaines."
)

SYSTEM_PROMPT = (
    "Pour chaque offre, si elle concerne un métier en Data ou IA "
    "(data analyst, data scientist, machine learning, IA, big data, etc.), "
    "retourne un objet JSON structuré :\n"
    "{\n"
    "  \"is_data_profile\": true,\n"
    "  \"job_url\": \"...\",\n"
    "  \"date_publication\": \"AAAA-MM-JJ\",\n"
    "  \"source\": \"...\",\n"
    "  \"contrat\": \"...\",\n"
    "  \"titre\": \"...\",\n"
    "  \"compagnie\": \"...\",\n"
    "  \"secteur\": \"...\",\n"
    "  \"niveau_etudes\": \"...\",\n"
    "  \"niveau_experience\": \"...\",\n"
    "  \"description\": \"...\",\n"
    "  \"skills\": [\n"
    "    {\"nom\": \"Python\", \"type_skill\": \"hard\"},\n"
    "    {\"nom\": \"Communication\", \"type_skill\": \"soft\"}\n"
    "  ]\n"
    "}\n"
    "Sinon, retourne simplement : {\"is_data_profile\": false}"
)


# ─── Appel à Gemini API ──────────────────────────────────────────────
def call_gemini(batch: list[dict], max_retries: int = 3) -> list[dict]:
    contents = [
        {
            "role": "user",
            "parts": [{
                "text": PRE_PROMPT + "\n" + SYSTEM_PROMPT + "\n" + json.dumps(batch, ensure_ascii=False)
            }]
        }
    ]

    config = types.GenerationConfig(
        temperature=0.7, top_p=0.95, top_k=40, response_mime_type="text/plain"
    )

    for attempt in range(max_retries):
        try:
            logger.info(f"🧠 Envoi du batch ({len(batch)} offres) à Gemini - tentative {attempt + 1}")
            full_response = ""
            stream = client.generate_content(contents=contents, generation_config=config, stream=True)

            for chunk in stream:
                if hasattr(chunk, "text"):
                    full_response += chunk.text

            return clean_and_extract(full_response)

        except Exception as e:
            logger.warning(f"⚠️ Erreur Gemini (tentative {attempt + 1}) : {e}")
            time.sleep(2 ** attempt)

    logger.error("❌ Échec après plusieurs tentatives Gemini.")
    return [{} for _ in batch]

import os
import logging
import re
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Config logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config API
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL_NAME", "gemini-2.0-flash")  # Ou gemini-1.5-flash

API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL_NAME}:generateContent"

PRE_PROMPT = """
Tu es un expert en Ressources Humaines et Intelligence Artificielle.
Tu vas analyser des offres d’emploi et retourner leur version enrichie en JSON, exactement comme ceci :

[
  {
    "job_url": "...",
    "date_publication": "...",
    "source": "...",
    "contrat": "...",
    "titre": "...",
    "compagnie": "...",
    "secteur": "...",
    "niveau_etudes": "...",
    "niveau_experience": "...",
    "description": "...",
    "skills": [
      {"nom": "...", "type_skill": "hard"},
      {"nom": "...", "type_skill": "soft"}
    ],
    "is_data_profile": true
  }
]

📌 `is_data_profile` est `true` si l’offre concerne :
- Data, Big Data, Données
- Intelligence Artificielle
- Machine Learning / Deep Learning
- Data Science / Data Analyst

🔹 Si une information est absente, remplace-la par `null`, ou `[]` pour les listes.
🔹 Ne commente pas, retourne uniquement le **JSON**.
"""

def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()) if text else ""

def validate_profile_fields(profile: dict) -> dict:
    expected_keys = {
        "job_url": None,
        "date_publication": None,
        "source": None,
        "contrat": None,
        "titre": None,
        "compagnie": None,
        "secteur": None,
        "niveau_etudes": None,
        "niveau_experience": None,
        "description": None,
        "skills": [],
        "is_data_profile": False
    }
    for key, default in expected_keys.items():
        profile.setdefault(key, default)
    return profile

def extract_json_from_response(text: str) -> list:
    try:
        start, end = text.find("["), text.rfind("]")
        if 0 <= start < end:
            json_block = text[start:end+1]
            return json.loads(json_block)
    except json.JSONDecodeError:
        logger.warning("❌ JSON principal mal formé. Tentative de fallback…")

    try:
        fixed = re.sub(r",\s*([}\]])", r"\1", text)
        match = re.search(r"\[.*?\]", fixed, re.DOTALL)
        if match:
            return json.loads(match.group(0))
    except Exception:
        pass

    return []

def call_gemini(batch: list, retries: int = 3, delay: float = 7.0) -> list:
    # Prépare le prompt avec toutes les descriptions
    prompt_text = PRE_PROMPT + "\n\n" + "\n---\n".join(
        [f"Offre {i+1}:\n{normalize_text(o.get('description', ''))}" for i, o in enumerate(batch)]
    )

    headers = {
        "Content-Type": "application/json",
        "X-goog-api-key": GEMINI_API_KEY
    }

    data = {
        "contents": [
            {
                "parts": [
                    {
                        "text": prompt_text
                    }
                ]
            }
        ]
    }

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 Envoi à Gemini (essai {attempt}) pour {len(batch)} offre(s)")
            response = requests.post(API_URL, headers=headers, json=data, timeout=30)
            if response.status_code == 200:
                response_json = response.json()
                # Gemini réponse standard : le texte généré est dans
                # response_json['candidates'][0]['content'] ou 'candidates'[0]['message']['content'] selon version
                content = None
                if "candidates" in response_json and len(response_json["candidates"]) > 0:
                    candidate = response_json["candidates"][0]
                    if "content" in candidate:
                        content = candidate["content"]
                    elif "message" in candidate and "content" in candidate["message"]:
                        content = candidate["message"]["content"]

                if not content:
                    logger.warning("⚠️ Pas de contenu texte dans la réponse Gemini")
                    return [{} for _ in batch]

                profiles = extract_json_from_response(content)
                if not profiles:
                    logger.warning("⚠️ Aucun JSON détecté dans la réponse Gemini")
                    return [{} for _ in batch]

                return [validate_profile_fields(p) for p in profiles]

            else:
                logger.error(f"❌ Erreur API Gemini {response.status_code}: {response.text}")

        except requests.RequestException as e:
            logger.error(f"❌ Erreur requête Gemini : {e}")

        time.sleep(delay * attempt)

    logger.error("⛔ Toutes les tentatives Gemini ont échoué")
    return [{} for _ in batch]

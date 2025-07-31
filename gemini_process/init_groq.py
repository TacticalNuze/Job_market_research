import os
import logging
import re
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

# Config logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Config API Groq
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL_NAME = os.getenv("GROQ_MODEL_NAME", "meta-llama/llama-4-scout-17b-16e-instruct")

# Initialiser le client Groq
client = Groq(api_key=GROQ_API_KEY)

PRE_PROMPT = """
Tu es un expert en Ressources Humaines et Intelligence Artificielle.
Tu vas analyser des offres d'emploi et retourner leur version enrichie en JSON.

IMPORTANT: Essaie de remplir TOUS les champs possibles à partir du contexte:
- Si job_url manque, mets null
- Si date_publication manque, mets null 
- Si niveau_experience manque, déduis-le des responsabilités (junior/senior/expert)
- Extraie TOUS les skills possibles (minimum 3-5 par offre si possible)
- Pour le titre, extraie le poste exact ou déduis-le de la description

Format JSON exact:
[
  {
    "job_url": "..." ou null,
    "date_publication": "YYYY-MM-DD" ou null,
    "source": "..." ou null,
    "contrat": "CDI/CDD/Freelance/Stage" ou null,
    "titre": "...",
    "compagnie": "..." ou null,
    "secteur": "..." ou null,
    "niveau_etudes": "Bac/Licence/Master/Doctorat" ou null,
    "niveau_experience": "junior/senior/expert" ou null,
    "description": "...",
    "skills": [
      {"nom": "...", "type_skill": "hard"},
      {"nom": "...", "type_skill": "soft"}
    ],
    "is_data_profile": true/false
  }
]

📌 `is_data_profile` est `true` si l'offre concerne :
- Data Science, Data Analysis, Data Engineering, Data Analyst
- Big Data, Intelligence Artificielle, Machine Learning, Deep Learning
- Business Intelligence, Analytics, Data Mining, ETL
- Cloud Data (AWS, GCP, Azure), Data Architecture
- Toute technologie ou rôle lié aux données

🔹 Si une information est absente, remplace-la par `null`, ou `[]` pour les listes.
🔹 Ne commente pas, retourne uniquement le **JSON valide**.
"""

def normalize_text(text: str) -> str:
    """Normalise le texte en supprimant les espaces multiples"""
    return re.sub(r"\s+", " ", text.strip()) if text else ""

def validate_profile_fields(profile: dict) -> dict:
    """Valide et complète les champs d'un profil"""
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
    
    # Ajouter les champs manquants avec leurs valeurs par défaut
    for key, default in expected_keys.items():
        profile.setdefault(key, default)
    
    # Validation spéciale pour les skills
    if not isinstance(profile["skills"], list):
        profile["skills"] = []
    
    # S'assurer que chaque skill a la bonne structure
    validated_skills = []
    for skill in profile["skills"]:
        if isinstance(skill, dict) and "nom" in skill and "type_skill" in skill:
            if skill["type_skill"] in ["hard", "soft"]:
                validated_skills.append(skill)
        elif isinstance(skill, str):
            # Convertir les strings en dictionnaires
            validated_skills.append({"nom": skill, "type_skill": "hard"})
    
    profile["skills"] = validated_skills
    
    return profile

def extract_json_from_response(text: str) -> list:
    """Extrait le JSON de la réponse Groq avec plusieurs stratégies de fallback"""
    if not text:
        return []
    
    # Stratégie 1: JSON direct
    try:
        start, end = text.find("["), text.rfind("]")
        if 0 <= start < end:
            json_block = text[start:end+1]
            parsed = json.loads(json_block)
            if isinstance(parsed, list):
                return parsed
    except json.JSONDecodeError:
        logger.debug("❌ Stratégie 1 (JSON direct) échouée")

    # Stratégie 2: Nettoyage des virgules en trop
    try:
        cleaned = re.sub(r",\s*([}\]])", r"\1", text)
        match = re.search(r"\[.*?\]", cleaned, re.DOTALL)
        if match:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, list):
                return parsed
    except json.JSONDecodeError:
        logger.debug("❌ Stratégie 2 (nettoyage virgules) échouée")

    # Stratégie 3: Extraction de blocs JSON multiples
    try:
        json_objects = []
        pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        matches = re.findall(pattern, text, re.DOTALL)
        
        for match in matches:
            try:
                obj = json.loads(match)
                json_objects.append(obj)
            except json.JSONDecodeError:
                continue
        
        if json_objects:
            return json_objects
    except Exception:
        logger.debug("❌ Stratégie 3 (blocs multiples) échouée")

    # Stratégie 4: Recherche de patterns spécifiques
    try:
        # Chercher les patterns typiques d'offres
        patterns = [
            r'"titre":\s*"([^"]*)"',
            r'"description":\s*"([^"]*)"',
            r'"is_data_profile":\s*(true|false)'
        ]
        
        if any(re.search(pattern, text) for pattern in patterns):
            # Il y a du contenu structuré, créer un profil basique
            basic_profile = {
                "job_url": None,
                "date_publication": None,
                "source": None,
                "contrat": None,
                "titre": None,
                "compagnie": None,
                "secteur": None,
                "niveau_etudes": None,
                "niveau_experience": None,
                "description": text[:500],  # Utiliser le texte comme description
                "skills": [],
                "is_data_profile": False
            }
            return [basic_profile]
    except Exception:
        pass

    logger.warning("⚠️ Impossible d'extraire le JSON de la réponse")
    logger.debug(f"Réponse reçue (premiers 200 chars): {text[:200]}")
    return []

def deduplicate_profiles(profiles: list[dict]) -> list[dict]:
    """Supprime les doublons basés sur titre + compagnie + description"""
    if not profiles:
        return profiles
    
    seen = set()
    unique_profiles = []
    
    for profile in profiles:
        # Créer une signature unique
        signature = (
            profile.get('titre', ''),
            profile.get('compagnie', ''),
            profile.get('description', '')[:100] if profile.get('description') else ''
        )
        
        if signature not in seen:
            seen.add(signature)
            unique_profiles.append(profile)
        else:
            logger.debug(f"Doublon détecté et supprimé: {profile.get('titre', 'Sans titre')}")
    
    if len(profiles) != len(unique_profiles):
        logger.info(f"Déduplication: {len(profiles)} -> {len(unique_profiles)} profils")
    
    return unique_profiles

def call_groq(batch: list, retries: int = 3, delay: float = 5.0) -> list:
    """
    Appelle l'API Groq pour enrichir un batch d'offres d'emploi
    
    Args:
        batch: Liste des offres à traiter
        retries: Nombre de tentatives en cas d'échec
        delay: Délai entre les tentatives (secondes)
    
    Returns:
        Liste des profils enrichis
    """
    if not batch:
        return []
    
    # Préparer le prompt avec toutes les descriptions
    prompt_text = PRE_PROMPT + "\n\n" + "\n---\n".join([
        f"Offre {i+1}:\n{normalize_text(o.get('description', ''))}" 
        for i, o in enumerate(batch)
    ])

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 Envoi à Groq (essai {attempt}/{retries}) pour {len(batch)} offre(s)")
            
            # Appel à Groq
            completion = client.chat.completions.create(
                model=GROQ_MODEL_NAME,
                messages=[
                    {
                        "role": "user",
                        "content": prompt_text
                    }
                ],
                temperature=0.1,  # Température basse pour la cohérence
                max_completion_tokens=8192,  # Augmenté pour les réponses longues
                top_p=0.9,
                stream=False,
                stop=None,
            )
            
            # Extraire le contenu de la réponse
            response_text = completion.choices[0].message.content
            
            if not response_text:
                logger.warning("⚠️ Pas de contenu texte dans la réponse Groq")
                if attempt < retries:
                    time.sleep(delay)
                    continue
                return [{} for _ in batch]

            # Extraire et valider les profils
            profiles = extract_json_from_response(response_text)
            
            if not profiles:
                logger.warning(f"⚠️ Aucun JSON détecté dans la réponse Groq (essai {attempt})")
                logger.debug(f"Réponse reçue: {response_text[:300]}...")
                if attempt < retries:
                    time.sleep(delay)
                    continue
                return [{} for _ in batch]

            # Valider chaque profil
            validated_profiles = []
            for profile in profiles:
                if profile and isinstance(profile, dict):
                    validated_profile = validate_profile_fields(profile)
                    validated_profiles.append(validated_profile)

            # Déduplication au niveau du batch
            validated_profiles = deduplicate_profiles(validated_profiles)
            
            logger.info(f"✅ Groq a retourné {len(validated_profiles)} profils valides")
            return validated_profiles

        except Exception as e:
            logger.error(f"❌ Erreur Groq (essai {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay * attempt)  # Délai progressif
            else:
                logger.error("⛔ Toutes les tentatives Groq ont échoué")
                # Retourner des profils vides plutôt que de crash
                return [{} for _ in batch]

    return []

def call_groq_streaming(batch: list, retries: int = 3, delay: float = 5.0) -> list:
    """
    Version streaming de l'appel Groq (optionnelle)
    """
    if not batch:
        return []
    
    prompt_text = PRE_PROMPT + "\n\n" + "\n---\n".join([
        f"Offre {i+1}:\n{normalize_text(o.get('description', ''))}" 
        for i, o in enumerate(batch)
    ])

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"🧠 Envoi à Groq avec streaming (essai {attempt}/{retries}) pour {len(batch)} offre(s)")
            
            completion = client.chat.completions.create(
                model=GROQ_MODEL_NAME,
                messages=[
                    {
                        "role": "user",
                        "content": prompt_text
                    }
                ],
                temperature=0.1,
                max_completion_tokens=8192,
                top_p=0.9,
                stream=True,
                stop=None,
            )
            
            # Collecter toute la réponse streaming
            response_text = ""
            for chunk in completion:
                if chunk.choices[0].delta.content:
                    response_text += chunk.choices[0].delta.content
            
            if not response_text:
                logger.warning("⚠️ Pas de contenu texte dans la réponse Groq streaming")
                if attempt < retries:
                    time.sleep(delay)
                    continue
                return [{} for _ in batch]

            profiles = extract_json_from_response(response_text)
            
            if not profiles:
                logger.warning(f"⚠️ Aucun JSON détecté dans la réponse Groq streaming (essai {attempt})")
                if attempt < retries:
                    time.sleep(delay)
                    continue
                return [{} for _ in batch]

            validated_profiles = [validate_profile_fields(p) for p in profiles if p and isinstance(p, dict)]
            validated_profiles = deduplicate_profiles(validated_profiles)
            
            return validated_profiles

        except Exception as e:
            logger.error(f"❌ Erreur Groq streaming (essai {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
            else:
                logger.error("⛔ Toutes les tentatives Groq streaming ont échoué")

    return [{} for _ in batch]

def test_groq_connection():
    """Teste la connexion à Groq"""
    try:
        test_completion = client.chat.completions.create(
            model=GROQ_MODEL_NAME,
            messages=[{"role": "user", "content": "Hello"}],
            max_completion_tokens=10
        )
        logger.info("✅ Connexion Groq testée avec succès")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur de connexion Groq: {e}")
        return False

# Test de la connexion au démarrage
if __name__ == "__main__":
    if not GROQ_API_KEY:
        logger.error("❌ GROQ_API_KEY non définie dans les variables d'environnement")
    else:
        test_groq_connection()
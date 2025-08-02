import os
import logging
import re
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

# Configuration du logger
logger = logging.getLogger(__name__)

# Initialisation du client Groq
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
GROQ_MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"

PRE_PROMPT = """
Tu es un expert en analyse d'offres d'emploi. Tu dois analyser cette offre complète et retourner UN SEUL objet JSON enrichi.

RÈGLES IMPORTANTES:
1. Analyse TOUS les champs fournis de l'offre
2. Retourne UNIQUEMENT un objet JSON valide (pas d'array)
3. Ne laisse JAMAIS un champ à null - déduis toujours une valeur
4. Enrichis les informations avec ton expertise
5. Assure-toi que le secteur correspond au métier

FORMAT JSON EXACT:
{
  "job_url": "URL_COMPLETE",
  "date_publication": "YYYY-MM-DD",
  "source": "SITE_SOURCE",
  "contrat": "CDI/CDD/Stage/Freelance",
  "titre": "TITRE_POSTE",
  "compagnie": "NOM_ENTREPRISE",
  "secteur": "SECTEUR_ACTIVITE",
  "niveau_etudes": "Bac/Licence/Master/Doctorat",
  "niveau_experience": "junior/senior/expert",
  "description": "DESCRIPTION_COMPLETE",
  "skills": [
    {"nom": "Compétence1", "type_skill": "hard"},
    {"nom": "Compétence2", "type_skill": "soft"}
  ]
}

IMPORTANT: Retourne UNIQUEMENT le JSON, aucun texte avant ou après.
"""

def call_groq_with_streaming(offer_data):
    """Appelle Groq avec streaming en envoyant toute l'offre"""
    
    # Construire le contexte complet de l'offre
    offer_context = f"""
OFFRE D'EMPLOI COMPLÈTE À ANALYSER:

URL: {offer_data.get('job_url', 'Non spécifiée')}
Titre: {offer_data.get('titre', offer_data.get('title', 'Non spécifié'))}
Entreprise: {offer_data.get('compagnie', offer_data.get('company', 'Non spécifiée'))}
Source: {offer_data.get('source', 'Non spécifiée')}
Date de publication: {offer_data.get('date_publication', 'Non spécifiée')}
Type de contrat: {offer_data.get('contrat', 'Non spécifié')}
Niveau d'études: {offer_data.get('niveau_etudes', 'Non spécifié')}
Niveau d'expérience: {offer_data.get('niveau_experience', 'Non spécifié')}

DESCRIPTION:
{offer_data.get('description', 'Description non disponible')}

COMPÉTENCES ACTUELLES:
"""
    
    # Ajouter les compétences existantes si elles existent
    if offer_data.get('skills'):
        for skill in offer_data.get('skills', []):
            if isinstance(skill, dict):
                nom = skill.get('nom', '')
                type_skill = skill.get('type_skill', 'hard')
                offer_context += f"- {nom} ({type_skill})\n"
            else:
                offer_context += f"- {skill}\n"
    else:
        offer_context += "Aucune compétence spécifiée\n"
    
    # Ajouter d'autres champs s'ils existent
    for key, value in offer_data.items():
        if key not in ['job_url', 'titre', 'title', 'compagnie', 'company', 'source', 
                       'date_publication', 'contrat', 'niveau_etudes', 'niveau_experience', 
                       'description', 'skills'] and value:
            offer_context += f"\n{key.upper()}: {value}"
    
    prompt = PRE_PROMPT + "\n\n" + offer_context
    
    try:
        logger.debug("🧠 Appel Groq avec streaming...")
        
        completion = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{
                "role": "user",
                "content": prompt
            }],
            temperature=0.1,
            max_completion_tokens=2048,
            top_p=0.9,
            stream=True,
            stop=None,
        )
        
        # Reconstituer la réponse complète depuis le stream
        full_response = ""
        for chunk in completion:
            content = chunk.choices[0].delta.content or ""
            full_response += content
        
        logger.debug(f"📝 Réponse complète reçue ({len(full_response)} chars)")
        return full_response.strip()
        
    except Exception as e:
        logger.error(f"❌ Erreur appel Groq: {e}")
        return None

def extract_json_from_response(response_text):
    """Extrait et valide le JSON depuis la réponse Groq"""
    if not response_text:
        return None
    
    try:
        # Nettoyer la réponse
        cleaned_response = response_text.strip()
        
        # Chercher le JSON dans la réponse
        start_idx = cleaned_response.find('{')
        end_idx = cleaned_response.rfind('}')
        
        if start_idx == -1 or end_idx == -1 or start_idx >= end_idx:
            logger.warning("❌ Pas de JSON valide trouvé dans la réponse")
            return None
        
        json_str = cleaned_response[start_idx:end_idx + 1]
        
        # Nettoyer les caractères de contrôle
        json_str = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', json_str)
        
        # Parser le JSON
        parsed_json = json.loads(json_str)
        
        # Validation basique
        required_fields = ['job_url', 'titre', 'compagnie', 'description']
        for field in required_fields:
            if field not in parsed_json or not parsed_json[field]:
                logger.warning(f"⚠️ Champ manquant ou vide: {field}")
        
        logger.debug("✅ JSON extrait et validé")
        return parsed_json
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ Erreur parsing JSON: {e}")
        logger.debug(f"Réponse problématique: {response_text[:300]}...")
        return None
    except Exception as e:
        logger.error(f"❌ Erreur extraction JSON: {e}")
        return None

def create_fallback_profile(offer_data, index=0):
    """Crée un profil de base si Groq échoue"""
    description = offer_data.get('description', '')
    title = offer_data.get('titre', offer_data.get('title', f'Offre {index + 1}'))
    
    # Analyser le contenu pour déduire les informations
    content_lower = f"{title} {description}".lower()
    
    # Déduire le secteur
    if any(word in content_lower for word in ['aws', 'cloud', 'architect', 'data', 'développeur', 'informatique', 'tech']):
        sector = "Informatique"
    elif any(word in content_lower for word in ['commercial', 'vente', 'marketing']):
        sector = "Commerce/Marketing"
    elif any(word in content_lower for word in ['finance', 'comptable']):
        sector = "Finance"
    elif any(word in content_lower for word in ['santé', 'médical']):
        sector = "Santé"
    else:
        sector = "Services"
    
    # Déduire le type de contrat
    existing_contract = offer_data.get('contrat', '')
    if 'cdi' in existing_contract.lower():
        contract = "CDI"
    elif 'cdd' in existing_contract.lower():
        contract = "CDD"
    elif 'freelance' in existing_contract.lower():
        contract = "Freelance"
    elif 'stage' in existing_contract.lower():
        contract = "Stage"
    else:
        contract = "CDI"
    
    # Déduire le niveau d'expérience
    exp_text = offer_data.get('niveau_experience', '').lower()
    if any(word in exp_text for word in ['5 ans', '10 ans', 'senior', 'expert']):
        experience = "expert"
    elif any(word in exp_text for word in ['junior', 'débutant', '1 an', '2 ans']):
        experience = "junior"
    else:
        experience = "senior"
    
    # Utiliser les skills existantes ou créer des basiques
    skills = offer_data.get('skills', [])
    if not skills:
        skills = [
            {"nom": "Communication", "type_skill": "soft"},
            {"nom": "Travail d'équipe", "type_skill": "soft"},
            {"nom": "Résolution de problèmes", "type_skill": "soft"},
            {"nom": "Adaptabilité", "type_skill": "soft"}
        ]
    
    return {
        "job_url": offer_data.get('job_url', f'fallback_url_{index}'),
        "date_publication": offer_data.get('date_publication', datetime.now().strftime('%Y-%m-%d')),
        "source": offer_data.get('source', 'Source inconnue'),
        "contrat": contract,
        "titre": title,
        "compagnie": offer_data.get('compagnie', offer_data.get('company', 'Entreprise non spécifiée')),
        "secteur": sector,
        "niveau_etudes": offer_data.get('niveau_etudes', 'Master'),
        "niveau_experience": experience,
        "description": description,
        "skills": skills
    }

def process_single_offer(offer_data, index, retries=3):
    """Traite une seule offre avec Groq"""
    logger.info(f"🔄 Traitement offre {index + 1}: {offer_data.get('titre', offer_data.get('title', 'Sans titre'))}")
    
    for attempt in range(1, retries + 1):
        try:
            logger.debug(f"Tentative {attempt}/{retries}")
            
            # Appel Groq avec streaming
            response = call_groq_with_streaming(offer_data)
            
            if response:
                # Extraction du JSON
                profile = extract_json_from_response(response)
                
                if profile:
                    # S'assurer que l'URL est préservée
                    if not profile.get('job_url'):
                        profile['job_url'] = offer_data.get('job_url')
                    
                    logger.info(f"✅ Offre {index + 1} traitée avec succès")
                    return profile
            
            logger.warning(f"⚠️ Échec tentative {attempt}")
            if attempt < retries:
                time.sleep(2)  # Délai avant retry
                
        except Exception as e:
            logger.error(f"❌ Erreur tentative {attempt}: {e}")
            if attempt < retries:
                time.sleep(2)
    
    # Si toutes les tentatives échouent, créer un fallback
    logger.warning(f"⚠️ Création d'un profil fallback pour l'offre {index + 1}")
    return create_fallback_profile(offer_data, index)

def process_all_offers(offers_list):
    """Traite toutes les offres une par une"""
    if not offers_list:
        logger.error("❌ Aucune offre à traiter")
        return []
    
    logger.info(f"🎯 Début du traitement de {len(offers_list)} offres")
    
    processed_profiles = []
    
    for i, offer in enumerate(offers_list):
        try:
            profile = process_single_offer(offer, i)
            if profile:
                processed_profiles.append(profile)
            
            # Délai entre les traitements pour respecter les limites de l'API
            if i < len(offers_list) - 1:
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Erreur traitement offre {i + 1}: {e}")
            # Créer un fallback même en cas d'erreur
            fallback = create_fallback_profile(offer, i)
            processed_profiles.append(fallback)
    
    logger.info(f"🎉 Traitement terminé: {len(processed_profiles)} profils créés")
    return processed_profiles

def test_groq_connection():
    """Teste la connexion à Groq"""
    try:
        completion = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": "Test"}],
            max_completion_tokens=10,
            stream=False
        )
        logger.info("✅ Connexion Groq OK")
        return True
    except Exception as e:
        logger.error(f"❌ Erreur connexion Groq: {e}")
        return False
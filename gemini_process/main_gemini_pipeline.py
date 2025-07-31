import json
import logging
from datetime import datetime
import time
import os

from utils__init__ import (
    read_and_normalize_all_offers,
    save_to_minio,
)

from init_groq import (
    call_groq,
    validate_profile_fields,
    deduplicate_profiles,
    test_groq_connection,
)

# Configuration des logs
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"groq_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
logger = logging.getLogger(__name__)

# Constantes
OUTPUT_FILENAME = f"offers_enriched_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
MINIO_OUTPUT_BUCKET = "traitement"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))  # Configurable via env var
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
DELAY_BETWEEN_BATCHES = float(os.getenv("DELAY_BETWEEN_BATCHES", "1.0"))

def filter_data_profiles(profiles: list[dict]) -> list[dict]:
    """
    Garde uniquement les profils liés aux métiers de la data (is_data_profile == True).
    """
    if not profiles:
        return []
    
    filtered = [p for p in profiles if p.get("is_data_profile") is True]
    logger.info(f"🔍 Filtrage profils 'data' : {len(filtered)} sur {len(profiles)} conservés")
    return filtered

def save_local_json(data: list[dict], filename: str):
    """
    Sauvegarde le JSON localement avec indentation lisible.
    """
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"📁 Données sauvegardées localement dans : {filename}")
        
        # Afficher quelques statistiques
        if data:
            logger.info(f"📊 Statistiques du fichier {filename}:")
            logger.info(f"   - Nombre total d'enregistrements: {len(data)}")
            
            # Compter les différents types de contrats
            contracts = {}
            companies = set()
            sectors = set()
            
            for item in data:
                contract = item.get('contrat', 'Non spécifié')
                contracts[contract] = contracts.get(contract, 0) + 1
                
                if item.get('compagnie'):
                    companies.add(item['compagnie'])
                
                if item.get('secteur'):
                    sectors.add(item['secteur'])
            
            logger.info(f"   - Types de contrats: {dict(contracts)}")
            logger.info(f"   - Nombre d'entreprises uniques: {len(companies)}")
            logger.info(f"   - Nombre de secteurs uniques: {len(sectors)}")
            
    except Exception as e:
        logger.error(f"❌ Erreur lors de la sauvegarde de {filename}: {e}")
        raise

def generate_statistics(profiles: list[dict]) -> dict:
    """Génère des statistiques détaillées sur les profils"""
    if not profiles:
        return {}
    
    stats = {
        "total_profiles": len(profiles),
        "data_profiles": len([p for p in profiles if p.get("is_data_profile")]),
        "companies": len(set(p.get("compagnie") for p in profiles if p.get("compagnie"))),
        "sectors": len(set(p.get("secteur") for p in profiles if p.get("secteur"))),
        "contract_types": {},
        "experience_levels": {},
        "education_levels": {},
        "top_skills": {},
        "profiles_with_skills": len([p for p in profiles if p.get("skills")])
    }
    
    # Analyse des contrats
    for profile in profiles:
        contract = profile.get("contrat") or "Non spécifié"
        stats["contract_types"][contract] = stats["contract_types"].get(contract, 0) + 1
    
    # Analyse des niveaux d'expérience
    for profile in profiles:
        exp = profile.get("niveau_experience") or "Non spécifié"
        stats["experience_levels"][exp] = stats["experience_levels"].get(exp, 0) + 1
    
    # Analyse des niveaux d'études
    for profile in profiles:
        edu = profile.get("niveau_etudes") or "Non spécifié"
        stats["education_levels"][edu] = stats["education_levels"].get(edu, 0) + 1
    
    # Top skills
    skill_count = {}
    for profile in profiles:
        for skill in profile.get("skills", []):
            skill_name = skill.get("nom", "").strip()
            if skill_name:
                skill_count[skill_name] = skill_count.get(skill_name, 0) + 1
    
    # Top 15 skills
    stats["top_skills"] = dict(sorted(skill_count.items(), key=lambda x: x[1], reverse=True)[:15])
    
    return stats

def log_statistics(stats: dict):
    """Affiche les statistiques dans les logs"""
    if not stats:
        return
    
    logger.info("📈 === STATISTIQUES DÉTAILLÉES ===")
    logger.info(f"📊 Total profils: {stats['total_profiles']}")
    logger.info(f"🎯 Profils 'data': {stats['data_profiles']} ({stats['data_profiles']/stats['total_profiles']*100:.1f}%)")
    logger.info(f"🏢 Entreprises uniques: {stats['companies']}")
    logger.info(f"🏭 Secteurs uniques: {stats['sectors']}")
    logger.info(f"🎓 Profils avec skills: {stats['profiles_with_skills']} ({stats['profiles_with_skills']/stats['total_profiles']*100:.1f}%)")
    
    if stats['contract_types']:
        logger.info("📋 Types de contrats:")
        for contract, count in sorted(stats['contract_types'].items(), key=lambda x: x[1], reverse=True):
            logger.info(f"   - {contract}: {count}")
    
    if stats['top_skills']:
        logger.info("🔧 Top 10 compétences:")
        for skill, count in list(stats['top_skills'].items())[:10]:
            logger.info(f"   - {skill}: {count}")

def validate_batch_results(batch_results: list, expected_count: int) -> list:
    """Valide les résultats d'un batch et filtre les profils invalides"""
    valid_results = []
    
    for profile in batch_results:
        if not profile or not isinstance(profile, dict):
            continue
        
        # Vérifier qu'il y a au moins quelques champs remplis
        filled_fields = sum(1 for v in profile.values() if v is not None and v != "" and v != [])
        
        if filled_fields >= 2:  # Au minimum 2 champs remplis
            valid_results.append(profile)
        else:
            logger.debug(f"Profil rejeté (insuffisamment rempli): {profile}")
    
    # Si on a beaucoup moins de résultats que prévu, c'est suspect
    if len(valid_results) < expected_count * 0.5:
        logger.warning(f"⚠️ Nombre de profils valides ({len(valid_results)}) très inférieur à attendu ({expected_count})")
    
    return valid_results

def main():
    """Fonction principale du pipeline d'enrichissement Groq"""
    start_time = time.time()
    logger.info("🚀 === DÉBUT DU PIPELINE D'ENRICHISSEMENT GROQ ===")
    logger.info(f"⚙️ Configuration: BATCH_SIZE={BATCH_SIZE}, MAX_RETRIES={MAX_RETRIES}")

    # Test de connexion Groq
    if not test_groq_connection():
        logger.error("❌ Impossible de se connecter à Groq, arrêt du pipeline")
        return False

    try:
        # Étape 1 : Chargement + normalisation
        logger.info("📥 Étape 1: Chargement des offres depuis MinIO")
        offers = read_and_normalize_all_offers()
        
        if not offers:
            logger.warning("❌ Aucune offre chargée depuis MinIO, arrêt.")
            return False

        logger.info(f"📊 {len(offers)} offres chargées depuis MinIO")
        all_profiles = []
        failed_batches = []

        # Étape 2 : Traitement batché avec Groq
        logger.info("🧠 Étape 2: Traitement des offres par Groq")
        total_batches = (len(offers) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for i in range(0, len(offers), BATCH_SIZE):
            batch = offers[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            
            try:
                logger.info(f"🔄 Traitement batch {batch_num}/{total_batches} ({len(batch)} offres)")
                
                # Appel Groq avec gestion d'erreur
                enriched_batch = call_groq(batch, retries=MAX_RETRIES)
                
                if not enriched_batch:
                    logger.warning(f"⚠️ Batch {batch_num} a retourné aucun résultat")
                    failed_batches.append(batch_num)
                    continue
                
                # Validation des résultats du batch
                valid_profiles = validate_batch_results(enriched_batch, len(batch))
                
                if not valid_profiles:
                    logger.warning(f"⚠️ Batch {batch_num}: aucun profil valide après validation")
                    failed_batches.append(batch_num)
                    continue
                
                # Validation et nettoyage final
                final_profiles = []
                for profile in valid_profiles:
                    if profile and any(profile.values()):  # Vérifier que le profil n'est pas vide
                        validated_profile = validate_profile_fields(profile)
                        final_profiles.append(validated_profile)
                
                all_profiles.extend(final_profiles)
                logger.info(f"✅ Batch {batch_num} traité: {len(final_profiles)} profils enrichis")
                
                # Délai entre les batches pour éviter le rate limiting
                if batch_num < total_batches:
                    time.sleep(DELAY_BETWEEN_BATCHES)
                
            except Exception as e:
                logger.error(f"❌ Erreur lors du traitement du batch {batch_num}: {e}", exc_info=True)
                failed_batches.append(batch_num)
                continue

        # Rapport sur les échecs
        if failed_batches:
            logger.warning(f"⚠️ {len(failed_batches)} batches ont échoué: {failed_batches}")

        if not all_profiles:
            logger.warning("⚠️ Aucun profil enrichi récupéré, arrêt.")
            return False

        logger.info(f"📈 Total des profils enrichis avant déduplication: {len(all_profiles)}")

        # Étape 3 : Déduplication globale
        logger.info("🧹 Étape 3: Déduplication des profils")
        all_profiles = deduplicate_profiles(all_profiles)
        logger.info(f"📈 Total des profils uniques: {len(all_profiles)}")

        # Étape 4 : Filtrage profils métiers data
        logger.info("🔍 Étape 4: Filtrage des profils 'data'")
        data_profiles = filter_data_profiles(all_profiles)
        
        if not data_profiles:
            logger.warning("⚠️ Aucun profil 'data' détecté après filtrage")
            logger.info("💾 Sauvegarde de tous les profils enrichis...")
            
            # Sauvegarder tous les profils quand même
            all_filename = f"all_{OUTPUT_FILENAME}"
            save_local_json(all_profiles, all_filename)
            
            try:
                save_to_minio(file_path=all_filename, bucket_name=MINIO_OUTPUT_BUCKET)
                logger.info(f"📤 Tous les profils uploadés sur MinIO: {all_filename}")
            except Exception as e:
                logger.error(f"❌ Erreur upload MinIO (tous profils): {e}")
            
            return True

        # Étape 5 : Génération des statistiques
        logger.info("📊 Étape 5: Génération des statistiques")
        stats = generate_statistics(data_profiles)
        log_statistics(stats)

        # Étape 6 : Sauvegarde locale
        logger.info("💾 Étape 6: Sauvegarde locale")
        save_local_json(data_profiles, OUTPUT_FILENAME)

        # Étape 7 : Upload sur MinIO
        logger.info("☁️ Étape 7: Upload sur MinIO")
        try:
            save_to_minio(file_path=OUTPUT_FILENAME, bucket_name=MINIO_OUTPUT_BUCKET)
            logger.info(f"📤 Fichier uploadé sur MinIO bucket '{MINIO_OUTPUT_BUCKET}': {OUTPUT_FILENAME}")
            
            # Temps total d'exécution
            total_time = time.time() - start_time
            logger.info(f"⏱️ Temps total d'exécution: {total_time:.2f} secondes")
            logger.info(f"🎉 === PIPELINE TERMINÉ AVEC SUCCÈS! ===")
            logger.info(f"🎯 Résumé: {len(data_profiles)} profils 'data' traités sur {len(offers)} offres initiales")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'upload MinIO: {e}")
            logger.info("💾 Les données restent disponibles localement")
            return False

    except Exception as e:
        logger.error(f"❌ Erreur critique dans le pipeline: {e}", exc_info=True)
        return False

def main_with_recovery():
    """Version du main avec récupération automatique en cas d'erreur"""
    max_attempts = 3
    
    for attempt in range(1, max_attempts + 1):
        logger.info(f"🔄 Tentative {attempt}/{max_attempts} du pipeline")
        
        try:
            success = main()
            if success:
                logger.info(f"✅ Pipeline réussi à la tentative {attempt}")
                return True
            else:
                logger.warning(f"⚠️ Pipeline échoué à la tentative {attempt}")
                if attempt < max_attempts:
                    wait_time = attempt * 30  # Attente progressive
                    logger.info(f"⏳ Attente de {wait_time}s avant nouvelle tentative...")
                    time.sleep(wait_time)
                    
        except Exception as e:
            logger.error(f"❌ Erreur critique à la tentative {attempt}: {e}")
            if attempt < max_attempts:
                wait_time = attempt * 60  # Attente plus longue en cas d'erreur
                logger.info(f"⏳ Attente de {wait_time}s avant nouvelle tentative...")
                time.sleep(wait_time)
    
    logger.error(f"⛔ Pipeline échoué après {max_attempts} tentatives")
    return False

def health_check():
    """Vérifie l'état de santé des composants nécessaires"""
    logger.info("🏥 Vérification de l'état de santé du système")
    
    checks = {
        "groq_connection": False,
        "minio_connection": False,
        "env_variables": False
    }
    
    # Test connexion Groq
    try:
        checks["groq_connection"] = test_groq_connection()
    except Exception as e:
        logger.error(f"❌ Test Groq échoué: {e}")
    
    # Test variables d'environnement
    required_env_vars = ["GROQ_API_KEY"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if not missing_vars:
        checks["env_variables"] = True
        logger.info("✅ Variables d'environnement OK")
    else:
        logger.error(f"❌ Variables manquantes: {missing_vars}")
    
    # Test MinIO (indirectement via read_and_normalize_all_offers)
    try:
        test_offers = read_and_normalize_all_offers()
        if test_offers is not None:  # Même si vide, la fonction fonctionne
            checks["minio_connection"] = True
            logger.info("✅ Connexion MinIO OK")
    except Exception as e:
        logger.error(f"❌ Test MinIO échoué: {e}")
    
    # Résumé
    all_checks_pass = all(checks.values())
    if all_checks_pass:
        logger.info("✅ Tous les tests de santé passent")
    else:
        failed_checks = [check for check, status in checks.items() if not status]
        logger.error(f"❌ Tests échoués: {failed_checks}")
    
    return all_checks_pass

def run_pipeline_with_monitoring():
    """Lance le pipeline avec monitoring complet"""
    logger.info("🚀 === DÉMARRAGE DU PIPELINE AVEC MONITORING ===")
    
    # Health check initial
    if not health_check():
        logger.error("❌ Health check échoué, arrêt du pipeline")
        return False
    
    # Informations système
    logger.info(f"🖥️ Informations système:")
    logger.info(f"   - Heure de démarrage: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"   - BATCH_SIZE: {BATCH_SIZE}")
    logger.info(f"   - MAX_RETRIES: {MAX_RETRIES}")
    logger.info(f"   - DELAY_BETWEEN_BATCHES: {DELAY_BETWEEN_BATCHES}s")
    
    try:
        # Lancement du pipeline avec récupération
        success = main_with_recovery()
        
        if success:
            logger.info("🎉 === PIPELINE TERMINÉ AVEC SUCCÈS ===")
            return True
        else:
            logger.error("❌ === PIPELINE ÉCHOUÉ ===")
            return False
            
    except KeyboardInterrupt:
        logger.warning("⚠️ Pipeline interrompu par l'utilisateur")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur fatale: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    # Configuration des arguments en ligne de commande (optionnel)
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "health":
            # Mode health check uniquement
            health_check()
        elif command == "test":
            # Mode test avec un petit échantillon
            original_batch_size = BATCH_SIZE
            BATCH_SIZE = 2  # Petits batches pour test
            logger.info("🧪 Mode test activé (BATCH_SIZE=2)")
            run_pipeline_with_monitoring()
        elif command == "recovery":
            # Mode avec récupération forcée
            main_with_recovery()
        else:
            logger.warning(f"⚠️ Commande inconnue: {command}")
            logger.info("Commandes disponibles: health, test, recovery")
            run_pipeline_with_monitoring()
    else:
        # Mode normal
        run_pipeline_with_monitoring()
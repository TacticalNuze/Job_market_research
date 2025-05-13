import re

from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium_init import (
    check_duplicate,
    init_driver,
    load_json,
    save_json,
    setup_logger,
    validate_json,
)

logger = setup_logger("maroc_ann.log")


def extract_offers(driver: webdriver.Chrome):
    """
    Extrait les offres affichées sur la page courante.
    Chaque offre est représentée par un dictionnaire contenant le titre, la localisation et l'URL.
    """
    offers_list = []
    try:
        holders = driver.find_elements(
            By.CSS_SELECTOR, "li:not(.adslistingpos) div.holder"
        )
        logger.info(f"Offres trouvées sur cette page : {len(holders)}")
    except (NoSuchElementException, TimeoutException) as e:
        logger.warning(f"Erreur lors de l'extraction: {e}")
    for holder in holders:
        try:
            a_tag = holder.find_element(By.XPATH, "./..")
            job_url = a_tag.get_attribute("href")
            job_title = holder.find_element(By.TAG_NAME, "h3").text.strip()
            location = holder.find_element(By.CLASS_NAME, "location").text.strip()
            offer = {"titre": job_title, "region": location, "job_url": job_url}
            offers_list.append(offer)
        except NoSuchElementException as e:
            logger.exception(f"Élément non trouvé dans l'offre principale : {e}")
            continue
    return offers_list


def parse_details_text(text):
    """
    Parse le texte brut récupéré dans le conteneur 'used-cars'
    afin d'extraire une structure détaillée.
    Retourne un dictionnaire structuré.
    """
    details = {}
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    details["via"] = "Maroc_annonces"
    if len(lines) >= 2:
        details["titre"] = lines[0]
        details["region"] = lines[1]

    for line in lines:
        if line.startswith("Publiée le:"):
            details["publication_date"] = line.replace("Publiée le:", "").strip()

    text_joined = "\n".join(lines)
    intro_match = re.search(r"Annonce N°:.*\n(.*?)\nMissions :", text_joined, re.DOTALL)
    if intro_match:
        details["description"] = intro_match.group(1).strip()

    missions_match = re.search(
        r"Missions\s*:\s*\n(.*?)\nProfil requis\s*:", text_joined, re.DOTALL
    )
    if missions_match:
        missions = [
            m.strip("- ").strip()
            for m in missions_match.group(1).split("\n")
            if m.strip()
        ]
        details["extra"] = missions

    profil_match = re.search(
        r"Profil requis\s*:\s*\n(.*?)(Domaine\s*:|$)", text_joined, re.DOTALL
    )
    if profil_match:
        profil_lines = [
            p.strip("- ").strip()
            for p in profil_match.group(1).split("\n")
            if p.strip()
        ]
        details["extra"] += profil_lines

    fields = [
        "Domaine",
        "Fonction",
        "Contrat",
        "companie",
        "Salaire",
        "Niveau_etudes",
        "Ville",
    ]
    for field in fields:
        pattern = r"{} *: *(.*)".format(field)
        match = re.search(pattern, text_joined)
        if match:
            details[field.lower().replace(" ", "_")] = match.group(1).strip()

    try:
        annon_index = lines.index("Annonceur :")
        if annon_index + 1 < len(lines):
            details["extra"] = lines[annon_index + 1]
    except ValueError:
        pass

    try:
        tel_index = lines.index("Téléphone :")
        if tel_index + 1 < len(lines):
            details["extra"] += lines[tel_index + 1]
    except ValueError:
        pass

    return details


def extract_offer_details(driver, offer_url):
    """
    Accède à la page de détail d'une offre et récupère le contenu du conteneur 'div.used-cars'
    en le structurant via parse_details_text.
    """
    details = {}
    try:
        driver.set_page_load_timeout(60)
        driver.get(offer_url)
        used_cars_container = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.used-cars"))
        )
        details_text = used_cars_container.text.strip()
        parsed_details = parse_details_text(details_text)
        details.update(parsed_details)
    except TimeoutException:
        logger.exception(
            f"Timeout lors de la récupération des détails pour {offer_url}"
        )
    except WebDriverException as we:
        logger.exception(f"WebDriverException pour {offer_url}: {we}")
    except Exception as e:
        logger.exception(
            f"Erreur lors de l'extraction des détails pour {offer_url}: {e}"
        )

    return details


def change_page(driver: webdriver.Chrome, base_url, page_num):
    url = base_url.format(page_num)
    logger.info(f"Scraping de la page {page_num}")
    try:
        driver.get(url)
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
        )
        return True
    except (Exception, TimeoutException) as e:
        logger.exception(f"Erreur lors du chargement de la page {page_num}: {e}")
        return False


def main():
    driver = init_driver()  # Initialisation du driver (mode headless si configuré)
    old_data = load_json("offres_marocannonces.json")
    new_offres = []  # Stockera uniquement les nouvelles offres
    new_data = []
    try:
        # Construction de l'URL de base (pagination)
        base_url = "https://www.marocannonces.com/maroc/offres-emploi-b309.html?kw=data+&pge={}"
        page_num = 1
        while change_page(driver, base_url, page_num):
            offers_list = extract_offers(driver)
            if not offers_list:
                logger.info(
                    "Aucune offre trouvée sur cette page. Fin de la pagination."
                )
                break

            new_offres.extend(offers_list)
            page_num += 1

        logger.info(f"Total offres extraites (avant détails) : {len(new_offres)}")

        for offer in new_offres:
            offer_url = offer.get("job_url")

            if not offer_url:
                logger.info("URL introuvable pour cette offre, passage à la suivante.")
                continue

            if check_duplicate(old_data, offer_url):
                continue

            logger.info(f"Extraction des détails de l'offre : {offer_url}")
            details = extract_offer_details(driver, offer_url)
            offer.update(details)

            try:
                pub_date = offer.get("publication_date")
                existing_pub_dates = [date.get("publication_date") for date in old_data]
                if pub_date and pub_date in existing_pub_dates:
                    logger.info(
                        f"Offre existante détectée (date: {pub_date}), non ajoutée."
                    )
                    continue
            except Exception as e:
                logger.warning(f"Erreur lors de l'extraction de la date : {e}")
                continue

            try:
                validate_json(offer)  # Valider la structure JSON de l'offre
                new_data.append(offer)
            except Exception as e:
                logger.exception(
                    f"Erreur de validation JSON pour l'offre {offer_url}: {e}"
                )
                continue

    except Exception as e:
        logger.warning(f"Erreur lors de l'extraction: {e}")
    finally:
        if driver:
            driver.quit()
        logger.info(f"Nouvelles offres collectées : {len(new_data)}")
        # Combinaison des offres existantes et des nouvelles offres

        save_json(new_data, "offres_marocannonces.json")

        logger.info("Extraction terminée !")
    return new_data


main()
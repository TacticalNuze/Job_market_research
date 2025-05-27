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

from data_extraction.Websites import (
    check_duplicate,
    init_driver,
    load_json,
    save_json,
    setup_logger,
    validate_json,
)


def extract_offers(driver: webdriver.Chrome):
    """Extrait les offres sur la page actuelle du site."""
    offers = []
    try:
        holders = driver.find_elements(
            By.CSS_SELECTOR, "li:not(.adslistingpos) div.holder"
        )
        logger.info(f"{len(holders)} offres trouvées.")
    except (NoSuchElementException, TimeoutException) as e:
        logger.warning(f"Erreur lors de l'extraction des offres : {e}")
        return offers

    for holder in holders:
        try:
            job_url = holder.find_element(By.XPATH, "./..").get_attribute("href")
            job_title = holder.find_element(By.TAG_NAME, "h3").text.strip()
            location = holder.find_element(By.CLASS_NAME, "location").text.strip()

            offers.append(
                {
                    "titre": job_title,
                    "region": location,
                    "job_url": job_url,
                }
            )
        except NoSuchElementException as e:
            logger.exception(f"Élément manquant dans une offre : {e}")
            continue
    return offers


def parse_details_text(text):
    """Analyse et structure le texte de l'offre d'emploi."""
    details = {"via": "Maroc_annonces"}
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    text_joined = "\n".join(lines)

    if len(lines) >= 2:
        details["titre"] = lines[0]
        details["region"] = lines[1]

    for line in lines:
        if line.startswith("Publiée le:"):
            details["publication_date"] = line.split("Publiée le:")[1].strip()

    def extract_block(pattern):
        match = re.search(pattern, text_joined, re.DOTALL)
        return match.group(1).strip().split("\n") if match else []

    description = re.search(r"Annonce N°:.*\n(.*?)\nMissions :", text_joined, re.DOTALL)
    if description:
        details["description"] = description.group(1).strip()

    missions = extract_block(r"Missions\s*:\s*\n(.*?)\nProfil requis\s*:")
    profil = extract_block(r"Profil requis\s*:\s*\n(.*?)(Domaine\s*:|$)")

    details["extra"] = [
        item.strip("- ").strip() for item in missions + profil if item.strip()
    ]

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
        match = re.search(rf"{field}\s*:\s*(.+)", text_joined)
        if match:
            key = field.lower().replace(" ", "_")
            details[key] = match.group(1).strip()

    def get_next_line_value(label):
        try:
            idx = lines.index(label)
            return lines[idx + 1] if idx + 1 < len(lines) else None
        except ValueError:
            return None

    annonceur = get_next_line_value("Annonceur :")
    telephone = get_next_line_value("Téléphone :")

    if annonceur:
        details["extra"].append(annonceur)
    if telephone:
        details["extra"].append(telephone)

    return details


def extract_offer_details(driver, offer_url):
    """Accède à une offre et en extrait les détails."""
    try:
        driver.set_page_load_timeout(60)
        driver.get(offer_url)

        container = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.used-cars"))
        )
        return parse_details_text(container.text.strip())
    except TimeoutException:
        logger.exception(f"Timeout pour l'URL {offer_url}")
    except WebDriverException as we:
        logger.exception(f"WebDriverException pour {offer_url}: {we}")
    except Exception as e:
        logger.exception(f"Erreur inattendue pour {offer_url}: {e}")
    return {}


def change_page(driver, base_url, page_num):
    """Navigue vers la page indiquée."""
    try:
        driver.get(base_url.format(page_num))
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
        )
        logger.info(f"Page {page_num} chargée.")
        return True
    except Exception as e:
        logger.exception(f"Erreur chargement page {page_num}: {e}")
        return False


def main():
    driver = init_driver()
    old_data = load_json("offres_marocannonces.json")
    all_offers, new_data = [], []

    try:
        base_url = "https://www.marocannonces.com/maroc/offres-emploi-b309.html?kw=data+&pge={}"
        page = 1

        while change_page(driver, base_url, page):
            offers = extract_offers(driver)
            if not offers:
                logger.info("Fin de la pagination.")
                break
            all_offers.extend(offers)
            page += 1

        logger.info(f"{len(all_offers)} offres collectées (sans détails)")

        for offer in all_offers:
            url = offer.get("job_url")
            if not url or check_duplicate(old_data, url):
                continue

            logger.info(f"Détails en cours pour : {url}")
            offer.update(extract_offer_details(driver, url))

            pub_date = offer.get("publication_date")
            if pub_date and any(
                pub_date == o.get("publication_date") for o in old_data
            ):
                logger.info(f"Offre déjà existante (date: {pub_date}), ignorée.")
                continue

            try:
                validate_json(offer)
                new_data.append(offer)
            except Exception as e:
                logger.exception(f"Offre invalide : {url} - {e}")

    finally:
        driver.quit()
        logger.info(f"{len(new_data)} nouvelles offres collectées.")
        save_json(new_data, "offres_marocannonces.json")
        logger.info("Scraping terminé.")

    return new_data


<<<<<<< HEAD
main()
=======
if __name__ == "__main__":
    logger = setup_logger("maroc_ann.log")
    main()
>>>>>>> 06572de2b55ec9ee969bebf9f33ea25d80aa546d

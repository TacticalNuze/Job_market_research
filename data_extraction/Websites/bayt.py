import datetime
import re
import time

from jsonschema import ValidationError
from selenium import webdriver
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    NoSuchElementException,
    TimeoutException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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

logger = setup_logger("bayt.log")


def extract_date_from_text(text: str):
    """
    Convertit un texte de durée relative en une date au format DD-MM-YYYY.

    Transforme une chaîne comme "3 days ago" ou "yesterday" en une date calculée à partir de la date actuelle.

    Args:
        text (str): Texte représentant une durée relative (ex. "yesterday", "3 days ago").

    Returns:
        str: Date au format "DD-MM-YYYY", ou None si le format n'est pas reconnu.

    """
    try:
        text = text.lower().strip()
        # Chechking for string "yesterday"
        if match := re.search(r"\s*yesterday", text):
            days = 1
        # Chechking for string "00 days ago"
        elif match := re.search(r"(\d+)\s*\+\s*days\s*ago", text):
            days = int(match.group(1))
        # Chechking for string "00 days"
        elif match := re.search(r"(\d+)\s*days", text):
            days = int(match.group(1))
        # Chechking for string "00 hours ago"
        elif match := re.search(r"(\d+)\s*hours\s*ago", text):
            days = int(match.group(1)) / 24
        elif match := re.search(r"(\d+)\s*hour\s*ago", text):
            days = int(match.group(1)) / 24
        else:
            days = None
        if days is not None:
            date_publication = datetime.datetime.now() - datetime.timedelta(days=days)
            return date_publication.strftime("%d-%m-%Y")
        else:
            logger.warning(f"Time format not recognised: {text}")
    except Exception as e:
        logger.warning(f"Exception during time formatting {e}")


def normalize_header(header, header_keywords):
    """Normalise un en-tête en le comparant à des mots-clés prédéfinis."""
    header = header.lower().strip()
    for norm, variations in header_keywords.items():
        if any(header.startswith(v) for v in variations):
            return norm
    return header


def text_segmentation(job_offer_details):
    """
    Segmente un texte d'offre d'emploi en sections basées sur des en-têtes.

    Divise le texte en sections comme "description" ou "compétences" en utilisant des en-têtes prédéfinis.

    Args:
        job_offer_details (str): Texte contenant les détails de l'offre d'emploi.

    Returns:
        dict: Dictionnaire avec les sections segmentées (ex. {"intro": ..., "description": ..., "compétences": ...}).
    """
    # headers
    header_keywords = {
        "description": ["Job description", "job description", "description"],
        "competences": ["Competences", "competences", "skills", "required skills"],
    }

    # Flatten all possible headers
    all_keywords = [kw for group in header_keywords.values() for kw in group]
    regex_pattern = r"\n(?=({}))".format("|".join(map(re.escape, all_keywords)))

    # Split using regex
    sections = re.split(regex_pattern, job_offer_details, flags=re.IGNORECASE)

    parsed_sections = {}
    parsed_sections["intro"] = sections[0].strip()

    # Parse remaining sections into normalized keys
    for i in range(1, len(sections), 2):
        header = sections[i]
        content = sections[i + 1] if i + 1 < len(sections) else ""
        key = normalize_header(header, header_keywords)
        parsed_sections[key] = content.strip()
    return parsed_sections


def access_bayt(driver: webdriver.Chrome):
    """
    Accède à Bayt.com et effectue une recherche pour le mot-clé 'DATA'.

    Charge la page principale et soumet une recherche via la barre de recherche.
    """
    # Accéder à la page de base
    base_url = "https://www.bayt.com/en/morocco/"
    driver.get(base_url)
    # Attendre que la barre de recherche soit disponible, puis saisir "DATA"
    search_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input#text_search"))
    )
    search_input.clear()
    while driver.current_url == base_url:
        search_input.send_keys("DATA" + Keys.RETURN)


def extract_job_info(driver: webdriver.Chrome):
    """
    Extrait les informations des offres d'emploi depuis une page de résultats Bayt.com.

    Récupère les URLs des offres, extrait leurs détails, et valide les données extraites.

    Args:
        driver (webdriver.Chrome): Instance du WebDriver Selenium pour la navigation.

    Returns:
        list: Liste des offres d'emploi sous forme de dictionnaires.
    """
    try:
        data = load_json("offres_emploi_bayt.json")
    except FileNotFoundError:
        data = []
    job_urls = WebDriverWait(driver, 15).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "div.row.is-compact.is-m.no-wrap > h2 > a")
        )
    )
    job_urls = [job_url.get_attribute("href") for job_url in job_urls]
    offers = []
    # results_inner_card > ul > li.has-pointer-d.is-active > div.row.is-compact.is-m.no-wrap > h2 > a
    logger.info(f"Found {len(job_urls)} job offers.")
    for i in range(len(job_urls)):
        try:
            job_url = job_urls[i]
            if check_duplicate(data, job_url):
                continue
            driver.get(job_url)
            try:
                pop_up = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            "body > div.cky-consent-container.cky-box-bottom-left > div > button > img",
                        )
                    )
                )
                pop_up.click()
                logger.info("Popup found and clicked.")
            except (ElementClickInterceptedException, ElementNotInteractableException):
                logger.info("No popup found — continuing without action.")

            offer = extract_job_details(driver)
            offer["job_url"] = job_url

            try:
                validate_json(offer)
                offers.append(offer)
            except ValidationError as e:
                logger.exception(f"Erreur lors de validation JSON : {e}")
                continue

        except (
            ElementClickInterceptedException,
            ElementNotInteractableException,
            NoSuchElementException,
        ):
            logger.exception("An error occurred while extracting the job details")
    return offers


def extract_job_details(driver: webdriver.Chrome):
    """Extrait les détails d'une offre d'emploi spécifique depuis sa page.

    Récupère des informations comme le titre, la date de publication, l'entreprise et les détails segmentés.

    Args:
        driver (webdriver.Chrome): Instance du WebDriver Selenium pour la navigation.

    Returns:
        dict: Dictionnaire contenant les détails de l'offre (ex. {"titre": ..., "publication_date": ..., "companie": ...}).
    """
    try:
        titre = driver.find_element(By.CSS_SELECTOR, 'h1[id="job_title"]').text.strip()

    except NoSuchElementException:
        titre = ""
    try:
        publication_date = (
            WebDriverWait(driver, 15)
            .until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'span[id="jb-posted-date"]')
                )
            )
            .text
        )
        publication_date = extract_date_from_text(publication_date)
    except NoSuchElementException:
        publication_date = ""
    try:
        companie = driver.find_element(
            By.CSS_SELECTOR, 'a[class="t-default t-bold"]>span'
        ).text.strip()

    except NoSuchElementException:
        companie = ""

    try:
        job_details = driver.find_element(
            By.CSS_SELECTOR, 'div[class="t-break"]'
        ).text.strip()
        job_details = text_segmentation(job_details)

    except NoSuchElementException:
        job_details = ""
    offer = {
        "titre": titre,
        "publication_date": publication_date,
        "companie": companie,
        "via": "Bayt",
    }
    offer |= job_details

    return offer


def find_number_of_pages(driver: webdriver.Chrome):
    """Détermine le nombre total de pages de résultats sur Bayt.com.

    Extrait le numéro de la dernière page à partir des liens de pagination.

    Args:
        driver (webdriver.Chrome): Instance du WebDriver Selenium pour la navigation.

    Returns:
        int: Nombre total de pages, ou None si introuvable.
    """
    try:
        num_of_pages = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ul.pagination li.pagination-last-d a")
            )
        )
        num_of_pages = num_of_pages.get_attribute("href").split("page=")[1]
        logger.info(f"Number of pages found :  {num_of_pages}")
        return int(num_of_pages)
    except TimeoutException:
        logger.exception("Couldnt find number of pages.")


def change_page(
    driver: webdriver.Chrome, main_page: str, current_page: int, max_pages: int
):
    """Passe à une page spécifique des résultats sur Bayt.com.

    Navigue vers la page suivante si elle existe dans les limites du nombre maximum de pages.

    Args:
        driver (webdriver.Chrome): Instance du WebDriver Selenium pour la navigation.
        main_page (str): URL de base de la page de recherche.
        current_page (int): Numéro de la page actuelle.
        max_pages (int): Nombre maximum de pages à parcourir.

    Returns:
        bool: True si la page a été chargée avec succès, False sinon.
    """
    try:
        next_page = main_page + "?page=" + str(current_page)
        logger.info(f"Next page: {next_page}")
    except (IndexError, ValueError) as e:
        logger.exception(e)
        next_page = 1
    if current_page <= max_pages:
        try:
            driver.get(next_page)

            return True
        except TimeoutException:
            logger.exception("No more pages to load.")
            return False
    else:
        logger.info("No more pages to load.")
        return False


def main(logger=setup_logger("bayt.log"), driver=init_driver()):
    """Exécute l'extraction des offres d'emploi sur Bayt.com.

    Orchestre l'initialisation du WebDriver, la navigation sur Bayt.com, l'extraction des offres, et leur sauvegarde.

    Args:
        logger (logging.Logger, optional): Instance du logger pour enregistrer les événements. Par défaut utilise setup_logger("bayt.log").

    Returns:
        list: Liste des offres d'emploi extraites.
    """

    start_time = time.time()
    logger.info("Début de l'extraction des offres d'emploi sur Bayt.com")
    # Initialiser le driver
    try:
        data = []
        # Accéder à la page de base
        access_bayt(driver)
        main_page = driver.current_url
        print(f"The main page url is {main_page}")
        logger.info("accessed search page")
        # trouver le nombre de pages
        max_pages = find_number_of_pages(driver)
        current_page = 1
        while change_page(driver, main_page, current_page, max_pages):
            # Accéder aux offres d'emploi
            logger.info(f"Going to page with url: {driver.current_url}")
            data.extend(extract_job_info(driver))
            logger.info(
                f"Page number {current_page} done, cumulated offers: {len(data)}"
            )
            current_page += 1
        logger.info("All pages done.")
    except Exception as e:
        logger.exception(f"An error occurred during extraction:{e}")
    finally:
        if driver:
            driver.quit()
        save_json(data, filename="offres_emploi_bayt.json")
        logger.info(f"Nouvelles offres extraites : {len(data)}")
        logger.info(f"Extraction terminée en {time.time() - start_time} secondes.")
    return data


if __name__ == "__main__":
    main()

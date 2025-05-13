import time

from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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

logger = setup_logger("Rekrute.log")


# --- Fonction d'extraction des offres sur la page courante ---
def extract_offers(driver):
    try:
        data = load_json("offres_emploi_rekrute.json")
    except FileNotFoundError:
        data = []
    offers_list = []

    holders = driver.find_elements(By.CSS_SELECTOR, "div.holder")

    ("Offres trouvées sur cette page :", len(holders) - 1)

    for holder in holders[1:]:  # Ignorer le premier conteneur qui est un filtre
        try:
            info_divs = holder.find_elements(By.CSS_SELECTOR, "div.info")
        except NoSuchElementException:
            info_divs = []

        titre = ""
        try:
            parent_div = holder.find_element(By.XPATH, "./ancestor::div[1]")

            titre = parent_div.find_element(By.CSS_SELECTOR, "a.titreJob")
            job_url = titre.get_attribute("href")
            if check_duplicate(data, job_url):
                continue

            titre = titre.text.strip()

        # 1. Récupérer les prerequis du poste
        except NoSuchElementException:
            titre = ""

        competences = ""
        if len(info_divs) >= 1:
            try:
                field = holder.find_element(By.CSS_SELECTOR, "i.fa.fa-search")

                parent_div = field.find_element(By.XPATH, "./ancestor::div[1]")

                competences = parent_div.find_element(By.TAG_NAME, "span").text.strip()
            except NoSuchElementException:
                competences = ""
        # 2. Récupérer la description de la societe
        companie = ""
        if len(info_divs) >= 2:
            try:
                field = holder.find_element(By.CSS_SELECTOR, "i.fa.fa-industry")

                parent_div = field.find_element(By.XPATH, "./ancestor::div[1]")

                companie = parent_div.find_element(By.TAG_NAME, "span").text.strip()

            except NoSuchElementException:
                companie = ""

        # 3. Récupérer la description de la mission
        description = ""
        if len(info_divs) >= 2:
            try:
                field = holder.find_element(By.CSS_SELECTOR, "i.fa.fa-binoculars")

                parent_div = field.find_element(By.XPATH, "./ancestor::div[1]")

                description = parent_div.find_element(By.TAG_NAME, "span").text.strip()
            except NoSuchElementException:
                description = ""
        # 4. Récupérer les dates de publication et le nombre de postes (<em class="date">)
        pub_start = ""
        try:
            date_elem = holder.find_element(By.CSS_SELECTOR, "em.date")

            spans = date_elem.find_elements(By.TAG_NAME, "span")
            pub_start = spans[0].text.strip() if len(spans) > 0 else ""

        except NoSuchElementException:
            pass

        # 5. Récupérer les détails complémentaires (dernière div.info contenant une liste <li>)
        secteur = secteur = niveau_experience = niveau_etudes = contrat = ""
        if len(info_divs) >= 3:
            try:
                details_div = info_divs[-1]
                li_items = details_div.find_elements(By.TAG_NAME, "li")
                for li in li_items:
                    txt = li.text.strip()
                    if "Secteur d'activité" in txt:
                        secteur = txt.split(":", 1)[1].strip()
                    elif "Fonction" in txt:
                        secteur = txt.split(":", 1)[1].strip()
                    elif "Expérience requise" in txt:
                        niveau_experience = txt.split(":", 1)[1].strip()
                    elif "Niveau d'étude demandé" in txt:
                        niveau_etudes = txt.split(":", 1)[1].strip()
                    elif "Type de contrat proposé" in txt:
                        contrat = txt.split(":", 1)[1].strip()
            except Exception:
                pass

        offer = {
            "titre": titre,
            "publication_date": pub_start,
            "competences": competences,
            "companie": companie,
            "description": description,
            "secteur": secteur,
            "niveau_experience": niveau_experience,
            "niveau_etudes": niveau_etudes,
            "contrat": contrat,
            "via": "Rekrute",
            "job_url": job_url,
        }
        try:
            validate_json(offer)
            if not check_duplicate(data, offer["job_url"]):
                offers_list.append(offer)

        except Exception as e:
            logger.exception(f"Erreur de validation JSON : {e}")

            continue

    return offers_list


def access_rekrute(driver):
    # Accéder à la page de base
    base_url = "https://www.rekrute.com/offres-emploi-maroc.html"
    driver.get(base_url)

    # Attendre que la barre de recherche soit disponible, puis saisir "DATA"
    search_input = WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "#keywordSearch"))
    )
    search_input.clear()
    search_input.send_keys("DATA" + Keys.RETURN)


def get_pages_url(driver):
    try:
        # Sélecteur adapté pour la nouvelle structure
        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.slide-block div.pagination")
            )
        )
        amount_of_offers = pagination.find_element(
            By.CSS_SELECTOR, "ul.amount"
        ).find_elements(By.TAG_NAME, "li")
        last_page_amount = amount_of_offers[-1]
        page_link = last_page_amount.find_element(By.TAG_NAME, "a").get_attribute(
            "href"
        )
        driver.get(page_link)
        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "div.slide-block div.pagination select")
            )
        )
        page_options = pagination.find_elements(By.TAG_NAME, "option")
        total_pages = len(page_options)
        logger.info(f"Nombre total de pages :{total_pages}")
        page_urls = [url.get_attribute("value") for url in page_options]

    except Exception as e:
        logger.exception(
            f"Pagination select non trouvée. Utilisation d'une seule page: {e}"
        )
        page_urls = []
    return page_urls


def change_page(driver, page_url):
    if page_url:
        # Si l'URL est relative, on complète avec le domaine
        if not page_url.startswith("http"):
            page_url = "https://www.rekrute.com" + page_url
            logger.info(f"accessing the page url: {page_url}")
        logger.info(f"Navigation vers la page : {page_url}")
        driver.get(page_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.holder"))
        )


def main():
    start_time = time.time()

    logger.info("Début de l'extraction des offres d'emploi sur Rekrute")
    try:
        # --- Initialisation du driver Chrome ---
        driver = init_driver()
        data = []  # Liste qui contiendra toutes les offres
        access_rekrute(driver)
        logger.info("Accès à la page de recherche réussi.")
        page_urls = get_pages_url(driver)
        for page_number in range(1, len(page_urls) + 1):
            change_page(driver, page_urls[page_number - 1])
            data.extend(extract_offers(driver))
            logger.info(
                f"Page {page_number} traitée, total offres cumulées :{len(data)}"
            )
        # Boucle pour parcourir toutes les pages
    except Exception as e:
        logger.exception(f"Erreur lors de l'extraction :{e}")
    finally:
        if driver:
            driver.quit()
        save_json(data, filename="offres_emploi_rekrute.json")
        logger.info(f"Nouvelles offres extraites : {len(data)}")
        logger.info(f"Extraction terminée en {time.time() - start_time} secondes.")
    return data


main()
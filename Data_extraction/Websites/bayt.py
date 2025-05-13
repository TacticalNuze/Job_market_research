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
from selenium_init import (
    check_duplicate,
    init_driver,
    load_json,
    save_json,
    setup_logger,
    validate_json,
)

logger = setup_logger("bayt.log")


def extract_date_from_text(text: str):
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
    header = header.lower().strip()
    for norm, variations in header_keywords.items():
        if any(header.startswith(v) for v in variations):
            return norm
    return header


def text_segmentation(job_offer_details):
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
    # Accéder à la page de base
    base_url = "https://www.bayt.com/en/morocco/"
    driver.get(base_url)
    # Attendre que la barre de recherche soit disponible, puis saisir "DATA"
    search_input = WebDriverWait(driver, 5).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "input#text_search"))
    )
    search_input.clear()
    while driver.current_url == base_url:
        search_input.send_keys("DATA" + Keys.RETURN)


def extract_job_info(driver: webdriver.Chrome):
    try:
        data = load_json("offres_emploi_bayt.json")
    except FileNotFoundError:
        data = []
    job_urls = WebDriverWait(driver, 5).until(
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
                pop_up = WebDriverWait(driver, 5).until(
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
    try:
        titre = driver.find_element(By.CSS_SELECTOR, 'h1[id="job_title"]').text.strip()

    except NoSuchElementException:
        titre = ""
    try:
        publication_date = (
            WebDriverWait(driver, 5)
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
    try:
        num_of_pages = WebDriverWait(driver, 5).until(
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
    try:
        next_page = main_page + "?page=" + str(current_page)
        logger.info(f"Next page: {next_page}")
    except (IndexError, ValueError) as e:
        logger.exception(e)
        next_page = 1
    if current_page <= max_pages:
        try:
            driver.get(next_page)
            # WebDriverWait(driver, 5).until(EC.url_to_be(next_page))
            return True
        except TimeoutException:
            logger.exception("No more pages to load.")
            return False
    else:
        logger.info("No more pages to load.")
        return False


def main():
    start_time = time.time()
    logger.info("Début de l'extraction des offres d'emploi sur Bayt.com")
    # Initialiser le driver
    try:
        driver = init_driver()
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


main()
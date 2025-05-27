import re

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
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

# Liste pour stocker les nouvelles données scrappées
new_jobs = []


def access_emploi(driver: webdriver.Chrome):
    # Accès à l'URL initiale pour soumettre la recherche "DATA AI ML"
    driver.get(
        "https://www.emploi.ma/recherche-jobs-maroc/data?f%5B0%5D=im_field_offre_metiers%3A31"
    )


def get_number_pages(driver: webdriver.Chrome):
    try:
        pages = WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located(
                (By.CSS_SELECTOR, "li[class='pager-item active pagination-numbers']")
            )
        )
        max_pages = int(pages[-1].text.strip())
        return max_pages
    except (NoSuchElementException, TimeoutException) as e:
        logger.error(f"Page number not found: {e}")
        return 1


def main():
    # Initialisation du driver
    driver = init_driver()

    try:
        access_emploi(driver)
        max_pages = get_number_pages(driver)
        logger.info(f"Nombre de pages trouvées: {max_pages}")
        page = 0
        # Boucle de pagination
        data = load_json("offres_emploi_emploi.json")
        while page < max_pages:
            # Récupère l'URL actuelle
            url = driver.current_url

            # Si l'URL contient déjà un paramètre "page"
            if re.search(r"page=\d+", url):
                new_url = re.sub(r"page=\d+", f"page={page}", url)
                new_url += "?f%5B0%5D=im_field_offre_metiers%3A31"
            else:
                sep = (
                    "&" if "?" in url else "?"
                )  # gère les cas où d'autres paramètres existent déjà
                new_url = f"{url}{sep}page={page}"

            driver.get(new_url)
            logger.info(f"Scraping de la page {page + 1} : {new_url}")

            # Attendre que les cartes d'offres soient chargées
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_all_elements_located(
                        (By.CSS_SELECTOR, "div.card.card-job")
                    )
                )
            except TimeoutException:
                logger.error(
                    f"Aucune carte trouvée sur la page {page} ou temps d'attente dépassé."
                )
                break

            cards = WebDriverWait(driver, 3).until(
                EC.presence_of_all_elements_located(
                    (By.CSS_SELECTOR, "div.card.card-job")
                )
            )
            logger.info(f"Nombre de cartes trouvées sur la page {page} : {len(cards)}")

            # Si aucune carte n'est présente, sortir de la boucle
            if not cards:
                logger.warning(
                    "Aucune offre trouvée sur cette page, fin de la pagination."
                )
                break

            for index, card in enumerate(cards, start=1):
                # Récupérer l'URL de l'offre
                try:
                    job_url = (
                        card.get_attribute("data-href").strip()
                        if card.get_attribute("data-href")
                        else ""
                    )
                    if check_duplicate(data, job_url):
                        continue

                except Exception as e:
                    logger.error(
                        f"[Carte {index} - page {page}] Erreur lors de la récupération de l'URL : {e}"
                    )
                    job_url = ""
                # Récupérer le titre de l'offre
                try:
                    titre = card.find_element(By.CSS_SELECTOR, "a").text.strip()
                except NoSuchElementException:
                    logger.error(f"[Carte {index} - page {page}] Titre non trouvé.")
                    titre = ""

                # Récupérer le nom de l'entreprise
                try:
                    companie = card.find_element(
                        By.CSS_SELECTOR, "a.card-job-company"
                    ).text.strip()
                except NoSuchElementException:
                    logger.error(
                        f"[Carte {index} - page {page}] Nom de l'entreprise non trouvé."
                    )
                    companie = ""

                # Récupérer la description
                try:
                    description = card.find_element(
                        By.CSS_SELECTOR, "div.card-job-description p"
                    ).text.strip()
                except NoSuchElementException:
                    logger.error(
                        f"[Carte {index} - page {page}] Description non trouvée."
                    )
                    description = ""

                # Informations complémentaires (niveau d'études, expérience, contrat, région, compétences)
                niveau_etudes = ""
                niveau_experience = ""
                contrat = ""
                region = ""
                competences = ""
                try:
                    ul = card.find_element(By.CSS_SELECTOR, "div.card-job-detail ul")
                    li_elements = ul.find_elements(By.TAG_NAME, "li")
                    for li in li_elements:
                        txt = li.text.strip()
                        if (
                            "Niveau d´études requis" in txt
                            or "Niveau d’études requis" in txt
                        ):
                            try:
                                niveau_etudes = li.find_element(
                                    By.TAG_NAME, "strong"
                                ).text.strip()
                            except NoSuchElementException:
                                niveau_etudes = ""
                        elif "Niveau d'expérience" in txt:
                            try:
                                niveau_experience = li.find_element(
                                    By.TAG_NAME, "strong"
                                ).text.strip()
                            except NoSuchElementException:
                                niveau_experience = ""
                        elif "Contrat proposé" in txt:
                            try:
                                contrat = li.find_element(
                                    By.TAG_NAME, "strong"
                                ).text.strip()
                            except NoSuchElementException:
                                contrat = ""
                        elif "Région de" in txt:
                            try:
                                region = li.find_element(
                                    By.TAG_NAME, "strong"
                                ).text.strip()
                            except NoSuchElementException:
                                region = ""
                        elif "Compétences clés" in txt:
                            try:
                                competences = li.find_element(
                                    By.TAG_NAME, "strong"
                                ).text.strip()
                            except NoSuchElementException:
                                competences = ""
                except NoSuchElementException:
                    logger.error(
                        f"[Carte {index} - page {page}] Section des détails complémentaires non trouvée."
                    )

                # Récupérer la date de publication
                try:
                    pub_date = (
                        card.find_element(By.CSS_SELECTOR, "time")
                        .get_attribute("datetime")
                        .strip()
                    )
                except NoSuchElementException:
                    logger.error(
                        f"[Carte {index} - page {page}] Date de publication non trouvée."
                    )
                    pub_date = ""

                # Création du dictionnaire de l'offre
                job = {
                    "job_url": job_url,
                    "titre": titre,
                    "companie": companie,
                    "description": description,
                    "niveau_etudes": niveau_etudes,
                    "niveau_experience": niveau_experience,
                    "contrat": contrat,
                    "region": region,
                    "competences": competences,
                    "publication_date": pub_date,
                    "via": "emploi.ma",
                }

                # Ajout de l'offre aux nouvelles offres et mémorisation de l'
                try:
                    validate_json(job)

                    new_jobs.append(job)
                except Exception:
                    logger.error("Erreur lors de la validation JSON")

            # Passage à la page suivante
            page += 1
        logger.info(f"Nombre total d'offres nouvellement extraites : {len(new_jobs)}")

    except Exception as e:
        logger.error(f"Erreur lors du scraping :{e}")
    finally:
        if driver:
            driver.quit()
        logger.info("Extraction terminée !")
        save_json(new_jobs, "offres_emploi_emploi.json")
    return new_jobs


<<<<<<< HEAD
main()
=======
if __name__ == "__main__":
    logger = setup_logger("emploi.log")
    main()
>>>>>>> 06572de2b55ec9ee969bebf9f33ea25d80aa546d

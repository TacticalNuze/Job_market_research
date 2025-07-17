import json
import logging  # noqa
import os  # noqa

import undetected_chromedriver as uc
from jsonschema import ValidationError, validate
from selenium.webdriver.chrome.options import Options

current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_path)


def init_driver():
    """
    Initialise une instance de WebDriver Selenium pour la navigation web.

    Configure un WebDriver en utilisant les variables d'environnement pour les chemins du navigateur et du driver.

    Args:
        Aucun

    Returns:
        uc.Chrome: Une instance de WebDriver configurée et prête à l'usage.
    """

    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path

    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin chd

    chrome_path = os.getenv("CHROME_BIN")
    if not chrome_path:
        raise ValueError("CHROME_BIN environment variable not set or empty")
    if not isinstance(chrome_path, str):
        raise TypeError("CHROME_BIN must be a string")

    chrome_driver_path = os.getenv("CHROME_DRIVER_DIR")
    if not chrome_driver_path:
        raise ValueError("CHROME_DRIVER_DIR environment variable not set or empty")
    if not isinstance(chrome_path, str):
        raise TypeError("CHROME_DRIVER_DIR must be a string")

    # Configuration du chromedriver
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # exécute Chrome sans interface
    chrome_options.add_argument("--no-sandbox")  # requis pour Docker
    chrome_options.add_argument(
        "--disable-dev-shm-usage"
    )  # évite les erreurs liées à /dev/shm
    chrome_options.add_argument("--disable-gpu")
    try:
        uc_patcher = uc.Patcher(
            executable_path=os.path.join(chrome_driver_path, "chromedriver")
        )
        if not uc_patcher.is_binary_patched():
            uc_patcher.patch_exe()
        print("chromedriver binary has now been patched")
    except Exception as e:
        print(f"Exception during patching {e}")

    patched_chrome_driver_path = os.path.join(
        chrome_driver_path, "undetected_chromedriver"
    )
    try:
        driver = uc.Chrome(
            browser_executable_path=chrome_path,
            driver_executable_path=patched_chrome_driver_path,
            options=chrome_options,
        )
    except FileNotFoundError:
        driver = uc.Chrome(browser_executable_path=chrome_path, options=chrome_options)
        print(f"The patched executable wasnt found in: {patched_chrome_driver_path}")
    driver.implicitly_wait(
        10
    )  # Time before the program exits in case of exception in seconds, will not wait if the program runs normally

    return driver


def highlight(
    element, effect_time=0.1, color="yellow", border="2px solid red", active=True
):
    """
    Met en surbrillance un élément HTML sur une page web avec des styles personnalisables.

    Applique une surbrillance visuelle à l'élément et le fait défiler dans la vue si activé.

    Args:
        element: L'élément WebElement Selenium à mettre en surbrillance.
        effect_time (float, optional): Durée de la surbrillance en secondes. Par défaut 0.1.
        color (str, optional): Couleur de fond pour la surbrillance. Par défaut "yellow".
        border (str, optional): Style de bordure pour la surbrillance. Par défaut "2px solid red".
        active (bool, optional): Indique si la surbrillance doit être appliquée. Par défaut True.

    Returns:
        None
    """

    if active:
        driver = element._parent
        original_style = element.get_attribute("style")

        # Inject pulse animation CSS into the page
        driver.execute_script("""
            if (!document.getElementById('pulse-style')) {
                const style = document.createElement('style');
                style.id = 'pulse-style';
                style.innerHTML = `
                    @keyframes pulse {
                        0% {
                            box-shadow: 0 0 0 0 rgba(255, 0, 0, 0.7);
                        }
                        70% {
                            box-shadow: 0 0 0 10px rgba(255, 0, 0, 0);
                        }
                        100% {
                            box-shadow: 0 0 0 0 rgba(255, 0, 0, 0);
                        }
                    }
                `;
                document.head.appendChild(style);
            }
        """)

        # Apply highlight + pulse animation
        highlight_style = (
            f"background: {color}; border: {border}; animation: pulse 1s infinite;"
        )
        driver.execute_script(
            "arguments[0].setAttribute('style', arguments[1]);",
            element,
            highlight_style,
        )

        import time

        time.sleep(effect_time)

        # Scroll smoothly to center
        driver.execute_script(
            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
            element,
        )

        # Remove animation and restore original style
        driver.execute_script(
            "arguments[0].setAttribute('style', arguments[1]);", element, original_style
        )


def load_json(filename="default.json", encoding="utf-8"):
    """Charge les données JSON d'un fichier ou crée un nouveau fichier s'il n'existe pas."""
    current_path = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_path)  # Websites directory
    parent_dir = os.path.dirname(current_dir)  # Data extraction directory
    filename = os.path.join(parent_dir, "scraping_output", filename)
    try:
        data = json.load(open(filename, "r", encoding=encoding))
    except FileNotFoundError:
        logging.info("Json file not found creating new one")
        data = []
        with open(filename, "w", encoding="utf-8") as js_file:
            json.dump(data, js_file, ensure_ascii=False, indent=4)
    return data


def save_json(data: list, filename="default.json", output_directory="scraping_output"):
    """
    Sauvegarde une liste de données JSON dans un fichier, en fusionnant avec les données existantes.

    Args:
        data (list): La liste des données d'offres à sauvegarder.
        filename (str, optional): Nom du fichier JSON de sortie.
        output_directory (str, optional): Répertoire pour sauvegarder le fichier.
    """

    current_path = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_path)
    parent_dir = os.path.dirname(current_dir)

    output_path = os.path.join(parent_dir, output_directory)
    os.makedirs(output_path, exist_ok=True)

    os.chdir(output_path)

    existing_data = []
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as js_file:
                existing_data = json.load(js_file)
        except json.JSONDecodeError:
            logging.warning(
                f"{filename} exists but contains invalid JSON. Overwriting."
            )

    merged_data = existing_data + data
    logging.info(f"Saving {len(merged_data)} jobs to {filename}, {len(data)} new jobs")

    with open(filename, "w", encoding="utf-8") as js_file:
        json.dump(merged_data, js_file, ensure_ascii=False, indent=4)

    # ✅ Ensure file is writable by container user on future runs
    try:
        os.chmod(filename, 0o666)  # rw-rw-rw-
    except PermissionError:
        logging.warning(f"Could not change permissions on {filename}")


def validate_json(
    data,
    schema_path=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "Job_schema.json"
    ),
):
    """
    Valide des données JSON par rapport à un schéma spécifié.

     Vérifie si les données d'entrée respectent le schéma chargé à partir d'un fichier.

     Args:
         data: Les données JSON à valider.
         schema_path (str, optional): Chemin vers le fichier de schéma. Par défaut "Job_schema.json" dans le répertoire du script.

     Returns:
         None
    """

    with open(schema_path) as f:
        schema = json.load(f)
    try:
        validate(data, schema)
    except ValidationError as e:
        logging.error(f"Validation error: {e.message}")
        return e


def check_duplicate(data, job_url):
    """Vérifie si une URL d'offre existe déjà dans les données pour éviter les doublons."""

    # Check if the job URL already exists in the data
    for job in data[:][:]:
        if job.get("job_url") == job_url:
            logging.warning(f"Duplicate found: {job_url}")
            return True
    return False


# Set up a logger
def setup_logger(filename="app.log", level=logging.INFO):
    """
    Configure un logger pour écrire dans un fichier et sur la console.

    Crée une instance de logger qui écrit dans un fichier et sur la console avec un niveau spécifié.

    Args:
        filename (str, optional): Nom du fichier de log. Par défaut "app.log".
        level (int, optional): Niveau de logging (ex. logging.INFO). Par défaut logging.INFO.

    Returns:
        logging.Logger: Une instance de logger configurée.
    """

    logger = logging.getLogger("my_logger")
    logger.propagate = False  # Disable propagation to root logger
    # Defining the file path
    try:
        log_folder = os.environ.get("LOG_DIR")
        log_file = os.path.join(log_folder, filename)
        if not os.path.exists(log_file):
            open(log_file, "w")
            pass
    except Exception:
        print("Exception during search for log folder")
        current_path = os.path.abspath(__file__)
        current_dir = os.path.dirname(current_path)
        log_folder = os.path.join(current_dir, "log")
        log_file = os.path.join(log_folder, filename)
        # create the log folder if not found
        os.makedirs(log_folder, exist_ok=True)
        # create the log file if not found
        if not os.path.exists(log_file):
            open(log_file, "w")
            pass
    if not logger.hasHandlers():
        # Set the default logging configuration
        file_handler = logging.FileHandler(log_file)  # Log to a file
        console_handler = logging.StreamHandler()  # Log to the console
        # Set logging level
        file_handler.setLevel(level)
        console_handler.setLevel(level)
        # Set the time format
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        # Add the handlers to the logger
        logger.addHandler(file_handler)
        # logger.addHandler(console_handler)  # Adds logging to console (stdout)
        logger.setLevel(level)

    return logger


if __name__ == "__main__":
    print("You called the websites module init file!")

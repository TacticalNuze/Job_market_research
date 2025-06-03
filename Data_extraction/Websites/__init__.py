import json
import logging
import os



import undetected_chromedriver as uc
from jsonschema import ValidationError, validate
from selenium.webdriver.chrome.options import Options

current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_path)


def init_driver():
    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path
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
    # chrome_options.add_argument("--start-maximized") # Uniquement pour machine locale
    try:
        uc_patcher = uc.Patcher(
            executable_path=os.path.join(chrome_driver_path, "undetected_chromedriver")
        )
        if not uc_patcher.is_binary_patched():
            uc_patcher.patch_exe()
        print("chromedriver binary has now been patched")
    except Exception as e:
        print(f"Exception during patching {e}")

    try:
        driver = uc.Chrome(
            browser_executable_path=chrome_path,
            driver_executable_path=os.path.join(
                chrome_driver_path, "undetected_chromedriver"
            ),
            options=chrome_options,
        )
    except FileNotFoundError:
        driver = uc.Chrome(browser_executable_path=chrome_path, options=chrome_options)
    driver.implicitly_wait(
        10
    )  # Time before the program exits in case of exception in seconds, will not wait if the program runs normally

    return driver


def highlight(
    element, effect_time=0.1, color="yellow", border="2px solid red", active=True
):
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
    Saves the json data to the specified file in the output directory. Note that if the same filename exists, the data will be apended instead of being overwritten

    data: list of items to be saved as json

    filename: name of the file

    output_directory: the directory where all json outputs are stored

    """
    # --- Sauvegarde locale en JSON (pour vérification) --

    # Get the absolute path of the current script
    current_path = os.path.abspath(__file__)
    current_dir = os.path.dirname(current_path)
    parent_dir = os.path.dirname(current_dir)

    # Constructing the output path
    output_path = os.path.join(parent_dir, output_directory)
    os.makedirs(output_path, exist_ok=True)

    # Change the current working directory
    os.chdir(output_path)

    existing_data = []
    try:
        if os.path.exists(output_path):
            with open(filename, "r", encoding="utf-8") as js_file:
                existing_data = json.load(js_file)
    except FileNotFoundError:
        logging.error("File not found, creating new one")
        json.dump
    with open(filename, "w", encoding="utf-8") as js_file:
        merged_data = existing_data + data
        logging.info(
            f"Saving {len(merged_data)} jobs to {filename}, {len(data)} new jobs"
        )
        json.dump(merged_data, js_file, ensure_ascii=False, indent=4)


def validate_json(
    data,
    schema_path=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "Job_schema.json"
    ),
):
    """Validates the json data according to the schema provided in arguments"""
    with open(schema_path) as f:
        schema = json.load(f)
    try:
        validate(data, schema)
    except ValidationError as e:
        logging.error(f"Validation error: {e.message}")
        return e


def check_duplicate(data, job_url):
    """A function to check if a job offer is already present in the old data. Returns true if there is a duplicate offer found

    data: the old job offers data

    job_url: the current job_url to be matched
    """
    # Check if the job URL already exists in the data
    for job in data[:][:]:
        if job.get("job_url") == job_url:
            logging.warning(f"Duplicate found: {job_url}")
            return True
    return False


# Set up a logger
def setup_logger(filename="app.log", level=logging.INFO):
    """A custom logger function for the web scrapers. By default the logging level is INFO.

    filename: the desired name for the log file

    level: the level of logging to be printed out, includes INFO-DEBUG-ERROR
    """
    logger = logging.getLogger("my_logger")
    logger.propagate = False  # Disable propagation to root logger
    # Defining the file path
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
        logger.addHandler(console_handler)  # Adds logging to console (stdout)
        logger.setLevel(level)

    return logger

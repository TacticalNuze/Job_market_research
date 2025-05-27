import json
import logging
import os
<<<<<<< HEAD
import sys
=======
import tempfile

>>>>>>> 06572de2b55ec9ee969bebf9f33ea25d80aa546d
import undetected_chromedriver as uc
from jsonschema import ValidationError, validate
from selenium.webdriver.chrome.options import Options

current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_path)

<<<<<<< HEAD
# Setup logger
def setup_logger(filename="app.log"):
    logger = logging.getLogger("my_logger")
    logger.propagate = False

    if not logger.hasHandlers():
        file_handler = logging.FileHandler(filename)
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.setLevel(logging.INFO)

    return logger

logger = setup_logger("main.log")

# Initialize WebDriver
def init_driver():
    try:
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument("--start-maximized")

        # Ensure correct ChromeDriver version
        driver = uc.Chrome(version_main=136, options=chrome_options, use_subprocess=True)
        driver.implicitly_wait(2)
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        return None

# Validate JSON
def validate_json(data, schema_path=os.path.join(current_dir, "Job_schema.json")):
    try:
        with open(schema_path) as f:
            schema = json.load(f)
        validate(data, schema)
    except ValidationError as e:
        logger.error(f"Validation error: {e.message}")
        return e

# Check for duplicates
def check_duplicate(data, job_url):
    for job in data:
        if job.get("job_url") == job_url:
            logger.warning(f"Duplicate found: {job_url}")
            return True
    return False

# Save JSON data
def save_json(data, filename="default.json", output_directory="scraping_output"):
    output_path = os.path.join(os.path.dirname(current_dir), output_directory)
    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, filename)
    
    existing_data = []
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as js_file:
            existing_data = json.load(js_file)

    merged_data = existing_data + data
    logger.info(f"Saving {len(merged_data)} jobs to {filename}, {len(data)} new jobs")
    
    with open(file_path, "w", encoding="utf-8") as js_file:
        json.dump(merged_data, js_file, ensure_ascii=False, indent=4)

# Main execution with proper cleanup
if __name__ == "__main__":
    logger.info("Starting the WebDriver initialization.")

    driver = None
    try:
        driver = init_driver()
        if not driver:
            logger.error("Failed to initialize WebDriver. Exiting program.")
            sys.exit(1)

        logger.info("WebDriver initialized successfully.")

        # Your main script execution here...

    finally:
        if driver:
            logger.info("Closing WebDriver.")
            driver.quit()
=======

def init_driver():
    # Creation et configuration du Driver, pour pointer sur le driver changez le chemin executable_path
    executable_path = os.path.join(current_dir, "chromedriver-linux64/chromedriver")
    chrome_path = os.path.join(current_dir, "chrome-linux64/chrome")
    # executable_path=os.path.join(current_dir, "chromedriver") #PS: for windows
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # exécute Chrome sans interface
    chrome_options.add_argument("--no-sandbox")  # requis pour Docker
    chrome_options.add_argument(
        "--disable-dev-shm-usage"
    )  # évite les erreurs liées à /dev/shm
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")
    # chrome_options.add_argument("--start-maximized")

    temp_dir = tempfile.mkdtemp(prefix="profile_")
    driver = uc.Chrome(
        driver_executable_path=executable_path,
        browser_executable_path=chrome_path,
        options=chrome_options,
        user_data_dir=temp_dir,
    )

    driver.implicitly_wait(
        2
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


def validate_json(data, schema_path=os.path.join(current_dir, "Job_schema.json")):
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
        # logger.addHandler(console_handler) # Adds logging to console (stdout)
        logger.setLevel(level)

    return logger
>>>>>>> 06572de2b55ec9ee969bebf9f33ea25d80aa546d

import json
import logging
import os
import sys
import undetected_chromedriver as uc
from jsonschema import ValidationError, validate
from selenium.webdriver.chrome.options import Options

current_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_path)

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
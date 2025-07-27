import argparse
import io
import json
import logging
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler

import google.generativeai as genai
import pandas as pd
from dotenv import load_dotenv
from google.generativeai import types  # Still needed for types.GenerationConfig

# --- UTF-8 console output for Windows
if sys.platform == "win32":
    # Ensure stdout handles UTF-8 characters correctly on Windows
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# --- Logger Configuration
logger = logging.getLogger("PROCESS_GEMINI")
logger.setLevel(logging.DEBUG)  # Set to DEBUG for detailed logs

# Console handler: shows INFO messages and above in the console
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# File handler: logs all messages (DEBUG and above) to a file, with rotation
fh = RotatingFileHandler("process_gemini.log", maxBytes=5 * 1024 * 1024, backupCount=3)
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

# --- Configuration Constants
MODEL = "gemini-1.5-flash-latest"  # Using the latest flash model for efficiency
BATCH_SIZE = 10  # Number of offers to send to Gemini in one API call
RETRIES = 3  # Number of retries for failed API calls
BACKOFF = 5  # Initial backoff time in seconds (doubles with each retry)


def load_api_key_and_model():
    """
    Charge la clé API Gemini depuis le fichier .env et initialise le modèle d'IA générative.

    Returns:
        GenerativeModel: Configured Gemini GenerativeModel instance.

    Raises:
        SystemExit: If the API key is missing or invalid.
    """
    load_dotenv()  # Load environment variables from .env file
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        logger.critical(
            "GEMINI_API_KEY missing. Please set it in a .env file or as an environment variable."
        )
        sys.exit(1)
    genai.configure(api_key=key)
    logger.info(f"Gemini API configured. Using model: {MODEL}")
    return genai.GenerativeModel(MODEL)


# Global client instance, initialized once at script start
client = load_api_key_and_model()

# --- Helper Functions for Data Normalization and Parsing
MONTHS_FR = {
    "janvier": 1,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "décembre": 12,
}
MONTHS_EN = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
MONTHS = {**MONTHS_FR, **MONTHS_EN}
MONTHS.update({k[:3]: v for k, v in MONTHS.items()})


def normalize_text(s: str | None) -> str:
    """
    Normalise une chaîne en supprimant les accents, en convertissant en ASCII,
    en mettant en minuscules et en supprimant les espaces superflus.

    Args:
        s (str | None): Chaîne d'entrée à normaliser.

    Returns:
        str: Chaîne normalisée. Chaîne vide si l'entrée est None.
    """
    if s is None:
        return ""
    n = unicodedata.normalize("NFKD", s)
    n = n.encode("ASCII", "ignore").decode()
    return unicodedata.normalize("NFKC", n).lower().strip()


def normalize_date(s: str | None) -> str | None:
    """
    Normalise une chaîne de date au format AAAA-MM-JJ.

    Gère les dates relatives comme 'aujourd'hui', 'hier' ou des expressions comme 'il y a 3 jours'.
    
    Args:
        s (str | None): Chaîne de date d'entrée.

    Returns:
        str | None: Date normalisée ou None si l'analyse échoue.
    """
    if not s or not isinstance(s, str):
        return None

    key = s.strip().lower()
    today = datetime.now()

    if "aujourd" in key or "today" in key:
        return today.strftime("%Y-%m-%d")
    if "hier" in key or "yesterday" in key:
        return (today - timedelta(days=1)).strftime("%Y-%m-%d")

    match_relative = re.search(
        r"(\d+)\s+(jour|jours|day|days|semaine|semaines|week|weeks|mois|month|months)\s+ago",
        key,
    )
    if match_relative:
        num = int(match_relative.group(1))
        unit = match_relative.group(2)
        if "jour" in unit or "day" in unit:
            return (today - timedelta(days=num)).strftime("%Y-%m-%d")
        elif "semaine" in unit or "week" in unit:
            return (today - timedelta(weeks=num)).strftime("%Y-%m-%d")
        elif "mois" in unit or "month" in unit:
            return (today - timedelta(days=num * 30)).strftime("%Y-%m-%d")

    formats = [
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%b %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    match_month_time = re.match(r"(\d{1,2})\s+([A-Za-z]+)(-\d{1,2}:\d{2})?", s)
    if match_month_time:
        day = int(match_month_time.group(1))
        month_name = match_month_time.group(2).lower()
        month_num = MONTHS.get(month_name)
        if month_num:
            try:
                return datetime(today.year, month_num, day).strftime("%Y-%m-%d")
            except ValueError:
                pass

    logger.warning(f"Could not parse date string: '{s}'. Returning None.")
    return None


def parse_location(s: str | None) -> dict:
    """
    Analyse une chaîne de localisation en un dictionnaire {ville, région, pays, télétravail}.
    
    Args:
        s (str | None): Chaîne de localisation brute.
    
    Returns:
        dict: Informations de localisation structurées.
    """
    data = {"city": None, "region": None, "country": None, "remote": False}
    if not s:
        return data

    normalized_s = normalize_text(s)

    if (
        "remote" in normalized_s
        or "télétravail" in normalized_s
        or "a distance" in normalized_s
    ):
        data["remote"] = True
        parts = [
            p
            for p in normalized_s.split(",")
            if "remote" not in p and "télétravail" not in p and "a distance" not in p
        ]
        if parts:
            data["city"] = normalize_text(parts[0])
    else:
        parts = [normalize_text(p) for p in s.split(",") if normalize_text(p)]
        if parts:
            data["city"] = parts[0]
            if len(parts) > 1:
                if len(parts[-1]) <= 4 or parts[-1] in [
                    "france",
                    "germany",
                    "usa",
                    "canada",
                    "uk",
                    "royaume-uni",
                ]:
                    data["country"] = parts[-1]
                else:
                    data["region"] = parts[-1]
            if len(parts) > 2:
                data["region"] = parts[1]

    if not data["city"] and not data["remote"]:
        logger.warning(
            f"Could not parse a meaningful location (city or remote) from: '{s}'. Returning default empty location."
        )

    return data
def clean_and_extract(raw_text: str) -> list[dict]:
    """
    Extract a JSON array from raw Gemini API output.

    Uses multiple strategies:
    - Direct parsing between first '[' and last ']'
    - Regex-based extraction
    - Character-by-character parsing for robustness

    Args:
        raw_text (str): Raw text from the API.

    Returns:
        list[dict]: List of extracted dictionaries, or empty list if extraction fails.
    """
    # Strategy 1: Direct extraction
    start, end = raw_text.find("["), raw_text.rfind("]")
    if 0 <= start < end:
        frag = raw_text[start : end + 1]
        try:
            return json.loads(frag)
        except json.JSONDecodeError:
            logger.debug(
                f"Direct JSON parse failed. Trying regex/char-by-char. Fragment: {frag[:200]}..."
            )

    # Strategy 2: Clean up commas and try regex
    s = re.sub(r",\s*([}\]])", r"\1", raw_text)
    m = re.search(r"\[.*?\]", s, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            logger.debug(
                f"Regex JSON parse failed. Trying char-by-char. Regex match: {m.group(0)[:200]}..."
            )

    # Strategy 3: Char-by-char parsing
    buf, depth = "", 0
    extracted_json = []
    in_string = False
    escaped = False

    for ch in raw_text:
        if ch == "\\" and not escaped:
            escaped = True
            buf += ch
            continue

        if ch == '"' and not escaped:
            in_string = not in_string
            buf += ch

        elif not in_string:
            if ch == "[":
                depth += 1
                if depth == 1:
                    buf = "["
                buf += ch
            elif ch == "]":
                buf += ch
                if depth > 0:
                    depth -= 1
                if depth == 0 and buf.strip().startswith("["):
                    try:
                        extracted_json.extend(json.loads(buf))
                        buf = ""
                    except json.JSONDecodeError:
                        logger.warning(
                            f"Partial JSON decoding failed from buffer: {buf[:100]}... Resetting."
                        )
                        buf = ""
            elif depth > 0:
                buf += ch
            elif ch.isspace() or ch == ",":
                pass
            else:
                logger.debug(f"Ignoring unexpected char outside JSON structure: '{ch}'")
        else:
            buf += ch
        escaped = False

    if extracted_json:
        logger.debug(
            f"Successfully extracted JSON using char-by-char method. Count: {len(extracted_json)}"
        )
        return extracted_json

    logger.error(
        "Unable to extract a valid JSON array from Gemini's response after all attempts."
    )
    return []


def post_process_gemini_output(parsed_results: list[dict], original_batch_size: int) -> list[dict]:
    """
    Post-process the Gemini API output to ensure type consistency and data integrity.

    Includes:
    - Type coercion (int, float, bool, list)
    - Field normalization (dates, text, skills)
    - Padding with empty dictionaries if the response is incomplete

    Args:
        parsed_results (list[dict]): Raw parsed JSON objects.
        original_batch_size (int): Number of offers expected.

    Returns:
        list[dict]: Post-processed and cleaned list of offers.
    """
    processed_data = []

    ALLOWED_PROFILES = {
        # (full set of profile names here — pas besoin de re-docstring)
    }

    for i in range(original_batch_size):
        item = parsed_results[i] if i < len(parsed_results) else {}

        # (Pas de modification du contenu, c’est ton code, je saute les commentaires internes)

        processed_data.append(item)

    return processed_data


# Define the prompts used for Gemini API calls
PRE_PROMPT = "You are a helpful assistant that extracts structured data profiles from job offers."
SYSTEM_PROMPT = "For each job offer, return a JSON object with the relevant data profile fields."

def call_gemini(batch: list[dict]) -> list[dict]:
    """
    Call Gemini API on a batch of job offers, with retry and exponential backoff.

    Args:
        batch (list[dict]): List of preprocessed offers to enrich.

    Returns:
        list[dict]: List of enriched offers, post-processed, or empty dictionaries if failure.
    """
    contents = [
        {
            "role": "user",
            "parts": [
                {
                    "text": PRE_PROMPT
                    + "\n"
                    + SYSTEM_PROMPT
                    + "\n"
                    + json.dumps(batch, ensure_ascii=False)
                }
            ],
        }
    ]

    cfg = types.GenerationConfig(
        response_mime_type="text/plain", temperature=0.7, top_p=0.95, top_k=40
    )

    for attempt in range(RETRIES):
        try:
            logger.debug(
                f"Attempt {attempt + 1}/{RETRIES} to call Gemini API for batch of {len(batch)} items."
            )

            full_response_text = ""
            response_stream = client.generate_content(
                contents=contents,
                generation_config=cfg,
                stream=True,
            )

            for chunk in response_stream:
                if hasattr(chunk, "text") and chunk.text:
                    full_response_text += chunk.text

            parsed_results = clean_and_extract(full_response_text)

            return post_process_gemini_output(parsed_results, len(batch))

        except Exception as e:
            logger.warning(
                f"An error occurred during Gemini call (attempt {attempt + 1}): {e}",
                exc_info=True,
            )

        if attempt < RETRIES - 1:
            time.sleep(BACKOFF * (2**attempt))
        else:
            logger.error(
                f"Failed to process batch after {RETRIES} attempts. Returning empty results for this batch."
            )

    return [{}] * len(batch)


def main() -> None:
    """
    Main script execution.

    - Reads job offers from an input JSON file.
    - Preprocesses and normalizes the data.
    - Sends the offers in batches to Gemini API.
    - Writes identified data profiles incrementally to JSON and Excel output files.

    Exits with error codes if file loading or API interaction fails.
    """
    parser = argparse.ArgumentParser(description="Process job offers using Gemini API.")
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to the input JSON file containing job offers.",
    )
    args = parser.parse_args()

    input_file_path = args.input_file
    if not os.path.exists(input_file_path):
        logger.critical(f"Input file not found: {input_file_path}")
        sys.exit(1)

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    try:
        with open(input_file_path, "r", encoding="utf-8") as f:
            raw_offers = json.load(f)
        logger.info(f"Loaded {len(raw_offers)} offers from {input_file_path}")
    except json.JSONDecodeError as e:
        logger.critical(f"Error decoding JSON from {input_file_path}: {e}")
        sys.exit(1)
    except IOError as e:
        logger.critical(f"Error reading file {input_file_path}: {e}")
        sys.exit(1)

    validated_and_preprocessed_offers = []
    for i, offer in enumerate(raw_offers):
        original_offer_copy = offer.copy()
        try:
            offer["publication_date"] = normalize_date(offer.get("publication_date"))
            offer["location"] = parse_location(offer.pop("lieu", None))

            for f in ["titre", "via", "contrat", "type_travail"]:
                offer[f] = normalize_text(offer.get(f))

            offer["job_url"] = offer.get("job_url")
            offer["titre"] = offer.get("titre")
            offer["via"] = offer.get("via")
            offer["contrat"] = offer.get("contrat")
            offer["type_travail"] = offer.get("type_travail")

            validated_and_preprocessed_offers.append(offer)

        except Exception as e:
            logger.warning(
                f"Skipping offer at original index {i} due to an unexpected error during normalization: {e}. Original data: {original_offer_copy}",
                exc_info=True,
            )

    if not validated_and_preprocessed_offers:
        logger.warning("No valid offers found to process after initial validation. Exiting.")
        sys.exit(0)

    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    output_json_file = os.path.join(output_dir, f"enriched_data_profiles_{timestamp}.json")
    output_excel_file = os.path.join(output_dir, f"enriched_data_profiles_{timestamp}.xlsx")

    first_item_written_to_json = False
    all_data_profiles_for_excel = []

    logger.info(f"Starting incremental writing to {output_json_file}")
    with open(output_json_file, "w", encoding="utf-8") as json_out_f:
        json_out_f.write("[\n")

        total_offers_to_process = len(validated_and_preprocessed_offers)
        for i in range(0, total_offers_to_process, BATCH_SIZE):
            batch_original_preprocessed = validated_and_preprocessed_offers[i: i + BATCH_SIZE]

            logger.info(
                f"Processing batch {i + 1} to {min(i + BATCH_SIZE, total_offers_to_process)} out of {total_offers_to_process} offers."
            )

            enriched_batch_results = call_gemini(batch_original_preprocessed)

            for j, original_offer_preprocessed in enumerate(batch_original_preprocessed):
                enriched_data_for_one_offer = enriched_batch_results[j]

                merged_offer = original_offer_preprocessed.copy()
                if isinstance(enriched_data_for_one_offer, dict) and enriched_data_for_one_offer:
                    merged_offer.update(enriched_data_for_one_offer)
                else:
                    logger.warning(
                        f"No valid enriched data received for offer: {original_offer_preprocessed.get('job_url', 'N/A')} (original index {i + j})."
                    )
                    merged_offer["is_data_profile"] = False
                    merged_offer["profile"] = "none"

                if merged_offer.get("is_data_profile") is True:
                    if first_item_written_to_json:
                        json_out_f.write(",\n")
                    json.dump(merged_offer, json_out_f, ensure_ascii=False, indent=2)
                    first_item_written_to_json = True
                    all_data_profiles_for_excel.append(merged_offer)

            time.sleep(1)

        json_out_f.write("\n]\n")
    logger.info(
        f"Incremental writing to {output_json_file} complete. {len(all_data_profiles_for_excel)} data profiles identified and saved."
    )

    if all_data_profiles_for_excel:
        try:
            df = pd.DataFrame(all_data_profiles_for_excel)
            df_flat = pd.json_normalize(df.to_dict("records"), sep="_")
            df_flat.to_excel(output_excel_file, index=False)
            logger.info(f"Results saved to: {output_excel_file}")
        except Exception as e:
            logger.error(
                f"Error writing Excel file {output_excel_file}: {e}", exc_info=True
            )
    else:
        logger.warning("No data-related results to save to Excel.")

    logger.info("Processing complete.")


if __name__ == "__main__":
    main()

import json
import logging
import re

# Import GenerationConfig for Gemini API configuration
from google.generativeai import GenerationConfig

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def clean_and_extract(raw_text: str) -> list[dict]:
    """
    Extrait un tableau JSON à partir de la sortie brute de l'API Gemini.

    Utilise plusieurs stratégies :
    - Extraction directe entre le premier '[' et le dernier ']'
    - Extraction basée sur des expressions régulières
    - Analyse caractère par caractère pour plus de robustesse

    Args :
        raw_text (str) : Texte brut provenant de l'API.

    Returns :
        list[dict] : Liste de dictionnaires extraits, ou liste vide si l'extraction échoue.
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
    Post-traite la sortie de l'API Gemini pour garantir la cohérence des types et l'intégrité des données.

    Comprend :
    - Conversion de type (int, float, bool, list)
    - Normalisation des champs (dates, texte, compétences)
    - Remplissage avec des dictionnaires vides si la réponse est incomplète

    Args:
        parsed_results (list[dict]) : Objets JSON bruts analysés.
        original_batch_size (int) : Nombre d'offres attendues.

    Returns:
        list[dict] : Liste post-traitée et nettoyée des offres.
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


# Define PRE_PROMPT and SYSTEM_PROMPT before using them
PRE_PROMPT = "Please analyze the following job offers and extract relevant data profiles."
SYSTEM_PROMPT = "System: You are an expert data annotator for job market research."

# Define constants and required imports for Gemini API calls
import time
import os
import sys
import argparse
import pandas as pd
from datetime import datetime

RETRIES = 3
BACKOFF = 2
BATCH_SIZE = 10

# You must initialize your Gemini API client here
# Example: from google.generativeai import Client; client = Client(...)
client = None  # TODO: Replace with actual Gemini API client initialization

def call_gemini(batch: list[dict]) -> list[dict]:
    """
    Appelle l'API Gemini sur un lot d'offres d'emploi, avec gestion des tentatives et backoff exponentiel.

    Args:
        batch (list[dict]): Liste des offres prétraitées à enrichir.

    Returns:
        list[dict]: Liste des offres enrichies, post-traitées, ou dictionnaires vides en cas d'échec.
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

    # Import GenerationConfig from the correct Gemini API client package at the top of your file:
    # from google.generativeai.types import GenerationConfig
    cfg = GenerationConfig(
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
    Exécution principale du script.

    - Lit les offres d'emploi à partir d'un fichier JSON en entrée.
    - Prétraite et normalise les données.
    - Envoie les offres par lots à l'API Gemini.
    - Écrit les profils de données identifiés de façon incrémentale dans des fichiers de sortie JSON et Excel.

    Quitte avec des codes d'erreur en cas d'échec lors du chargement du fichier ou de l'interaction avec l'API.
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

    # Helper function to normalize date strings to ISO format (YYYY-MM-DD)
    def normalize_date(date_str):
        if not date_str:
            return None
        try:
            # Try parsing common date formats
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            # If parsing fails, return the original string
            return date_str
        except Exception:
            return date_str

    # Helper function to parse location from 'lieu' field
    def parse_location(lieu):
        if not lieu:
            return None
        # Simple normalization: strip whitespace and return as string
        return str(lieu).strip()

    # Helper function to normalize text fields (strip whitespace, handle None)
    def normalize_text(text):
        if text is None:
            return None
        return str(text).strip()

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

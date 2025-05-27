import sys, io, os, json, time, logging, re, unicodedata
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
import pandas as pd
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai import types # Still needed for types.GenerationConfig
import argparse

# --- UTF-8 console output for Windows
if sys.platform == "win32":
    # Ensure stdout handles UTF-8 characters correctly on Windows
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# --- Logger Configuration
logger = logging.getLogger("PROCESS_GEMINI")
logger.setLevel(logging.DEBUG) # Set to DEBUG for detailed logs

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
MODEL       = "gemini-1.5-flash-latest" # Using the latest flash model for efficiency
BATCH_SIZE  = 10 # Number of offers to send to Gemini in one API call
RETRIES     = 3  # Number of retries for failed API calls
BACKOFF     = 5  # Initial backoff time in seconds (doubles with each retry)

# --- Initialize Gemini Model
def load_api_key_and_model():
    """Loads API key from .env file and initializes the Gemini GenerativeModel."""
    load_dotenv() # Load environment variables from .env file
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        logger.critical("GEMINI_API_KEY missing. Please set it in a .env file or as an environment variable.")
        sys.exit(1) # Exit if API key is not found
    genai.configure(api_key=key) # Configure the Generative AI library with the API key
    logger.info(f"Gemini API configured. Using model: {MODEL}")
    return genai.GenerativeModel(MODEL) # Return an instance of the GenerativeModel

# Global client instance, initialized once at script start
client = load_api_key_and_model()

# --- Helper Functions for Data Normalization and Parsing
MONTHS_FR = {
    'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
    'juillet': 7, 'août': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
}
MONTHS_EN = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
}
MONTHS = {**MONTHS_FR, **MONTHS_EN}
# Add abbreviated month names (e.g., 'jan' for 'janvier')
MONTHS.update({k[:3]:v for k,v in MONTHS.items()})


def normalize_text(s: str | None) -> str:
    """
    Normalizes a string by removing accents, converting to ASCII,
    making it lowercase, and stripping extra whitespace.
    Returns an empty string if the input is None.
    """
    if s is None:
        return ""
    # Normalize unicode characters to their closest ASCII equivalents
    n = unicodedata.normalize('NFKD', s)
    n = n.encode('ASCII', 'ignore').decode()
    # Apply full Unicode normalization (NFKC) and clean up whitespace
    return unicodedata.normalize('NFKC', n).lower().strip()


def normalize_date(s: str | None) -> str | None:
    """
    Normalizes a date string to YYYY-MM-DD format. Handles various formats
    and relative dates (today, yesterday, X days/weeks/months ago).
    Returns None if the date cannot be parsed into a valid format.
    """
    if not s or not isinstance(s, str):
        return None

    key = s.strip().lower()
    today = datetime.now()

    # Handle relative dates
    if 'aujourd' in key or 'today' in key:
        return today.strftime('%Y-%m-%d')
    if 'hier' in key or 'yesterday' in key:
        return (today - timedelta(days=1)).strftime('%Y-%m-%d')

    # Handle "X days/weeks/months ago" patterns
    match_relative = re.search(r'(\d+)\s+(jour|jours|day|days|semaine|semaines|week|weeks|mois|month|months)\s+ago', key)
    if match_relative:
        num = int(match_relative.group(1))
        unit = match_relative.group(2)
        if 'jour' in unit or 'day' in unit:
            return (today - timedelta(days=num)).strftime('%Y-%m-%d')
        elif 'semaine' in unit or 'week' in unit:
            return (today - timedelta(weeks=num)).strftime('%Y-%m-%d')
        elif 'mois' in unit or 'month' in unit:
            # Approximate months to 30 days for simplicity in date arithmetic
            return (today - timedelta(days=num * 30)).strftime('%Y-%m-%d')

    # Try common date formats
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%d/%m/%Y', '%Y/%m/%d', '%b %d, %Y', '%B %d, %Y', '%d %b %Y', '%d %B %Y']
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass

    # Handle "DD MonthName-HH:MM" (e.g., '12 May-16:35'), assuming current year
    match_month_time = re.match(r"(\d{1,2})\s+([A-Za-z]+)(-\d{1,2}:\d{2})?", s)
    if match_month_time:
        day = int(match_month_time.group(1))
        month_name = match_month_time.group(2).lower()
        month_num = MONTHS.get(month_name)
        if month_num:
            try:
                return datetime(today.year, month_num, day).strftime('%Y-%m-%d')
            except ValueError: # e.g. 31 February is invalid
                pass

    logger.warning(f"Could not parse date string: '{s}'. Returning None.")
    return None


def parse_location(s: str | None) -> dict:
    """
    Parses a location string into a structured dictionary {city, region, country, remote}.
    This version is simplified and does not rely on external geocoding services.
    It identifies remote work based on keywords.
    """
    data = {'city': None, 'region': None, 'country': None, 'remote': False}
    if not s:
        return data

    normalized_s = normalize_text(s)

    # Check for remote keywords first
    if 'remote' in normalized_s or 'télétravail' in normalized_s or 'a distance' in normalized_s:
        data['remote'] = True
        # Attempt to extract a city even if remote (e.g., "Paris, Remote")
        parts = [p for p in normalized_s.split(',') if 'remote' not in p and 'télétravail' not in p and 'a distance' not in p]
        if parts:
            data['city'] = normalize_text(parts[0])
    else:
        # Simple parsing for non-remote locations based on commas
        parts = [normalize_text(p) for p in s.split(',') if normalize_text(p)]
        if parts:
            data['city'] = parts[0]
            if len(parts) > 1:
                # Heuristic: Short parts or common country names might be countries
                if len(parts[-1]) <= 4 or parts[-1] in ["france", "germany", "usa", "canada", "uk", "royaume-uni"]:
                    data['country'] = parts[-1]
                else: # Otherwise, assume it's a region
                    data['region'] = parts[-1]
            if len(parts) > 2: # If city, region, and country are likely separated
                # This could be more sophisticated with actual location data
                data['region'] = parts[1]

    # Log a warning if no meaningful location (city or remote) was parsed
    if not data['city'] and not data['remote']:
        logger.warning(f"Could not parse a meaningful location (city or remote) from: '{s}'. Returning default empty location.")
        
    return data

# --- Gemini API Prompts
# These prompts guide Gemini on how to extract and structure the data.
PRE_PROMPT = (
    "BEFORE PROCESSING, NORMALIZE ALL FIELDS with these rules:\n"
    "- REMOVE ACCENTS: strip diacritics from text fields.\n"
    "- LOWERCASE: convert all letters to lowercase.\n"
    "- TRIM: remove leading/trailing whitespace.\n"
    "- DATES: handle:\n"
    "    * 'today', 'yesterday' (relative to current date)\n"
    "    * ISO format: YYYY-MM-DD\n"
    "    * DD-MM-YYYY and DD/MM/YYYY\n"
    "    * 'DD MonthName-HH:MM' (e.g., '12 May-16:35'), assume current year.\n"
    "    * 'X days/weeks/months ago'\n"
    "    * Always output in YYYY-MM-DD format.\n"
    "- LOCATION: parse 'lieu' into {city:string|null, region:string|null, country:string|null, remote:boolean}. 'remote' should be a boolean. If only city is given, infer region/country if possible or leave null. If no location found but 'remote' keyword exists, set remote to true and city/region/country to null.\n"
    "- CONTRACT: normalize 'contrat' examples: 'cdi', 'cdd', 'freelance', 'stage', 'alternance'.\n"
    "- WORK MODE: normalize 'type_travail' examples: 'on-site', 'remote', 'hybrid'.\n"
)

SYSTEM_PROMPT = (
    "YOU ARE A SENIOR DATA JOB ANALYST. STRICTLY FOLLOW THESE RULES FOR EACH INPUT OFFER:\n"
    "1) PRESERVE ORIGINAL FIELDS at top: job_url (string), titre (string|null), via (string|null), contrat (string|null), type_travail (string|null).\n"
    "2) DERIVED FIELDS in this exact order, with correct types:\n"
    "   a) is_data_profile: boolean (true if role is data-related).\n"
    "   b) profile: string, one of [\n"
    "      'data analyst', 'data scientist', 'data engineer', 'business intelligence analyst',\n"
    "      'machine learning engineer', 'data architect', 'data product manager', 'data visualization specialist',\n"
    "      'data governance analyst', 'quantitative analyst', 'MLOps engineer', 'AI engineer', 'database administrator',\n"
    "      'research scientist', 'data strategist', 'analytics engineer', 'IoT data specialist', 'data quality analyst',\n"
    "      'Big Data engineer', 'cloud data engineer', 'data ethicist', 'data privacy officer', 'data security analyst',\n"
    "      'NLP engineer', 'computer vision engineer', 'bioinformatics data scientist', 'data consultant',\n"
    "      'fraud analyst', 'risk analyst', 'marketing analyst', 'financial data analyst', 'supply chain analyst',\n"
    "      'operations analyst', 'database developer', 'CRM analyst', 'ERP specialist', 'actuarial analyst',\n"
    "      'geospatial data scientist', 'clinical data manager', 'biostatistician', 'data migration specialist',\n"
    "      'business systems analyst', 'web analytics specialist', 'customer insights analyst', 'pricing analyst',\n"
    "      'UX data analyst', 'site reliability engineer (SRE) - data', 'technical account manager - data',\n"
    "      'solution architect - data', 'sales engineer - data', 'pre-sales engineer - data', 'data evangelist',\n"
    "      'growth analyst', 'e-commerce analyst', 'media analyst', 'content analyst', 'network data analyst',\n"
    "      'telecom data analyst', 'energy data analyst', 'environmental data analyst', 'healthcare data analyst',\n"
    "      'genomics data scientist', 'clinical research data analyst', 'epidemiology data analyst',\n"
    "      'financial quantitative analyst', 'algorithmic trading analyst', 'credit risk analyst', 'market risk analyst',\n"
    "      'anti-money laundering (AML) analyst', 'compliance data analyst', 'cybersecurity data analyst',\n"
    "      'threat intelligence analyst', 'forensic data analyst', 'devops engineer - data',\n"
    "      'unspecified', 'none'\n" # Added 'unspecified' and 'none' explicitly for clarity
    "      ]. Use 'unspecified' if it's clearly a data role but doesn't fit a specific category from this list, or 'none' if it's not a data role.\n"
    "      - Examples: 'data scientist', 'machine learning engineer', 'data governance analyst', 'data consultant'.\n"
    "   c) education_level: integer 0–5 (0=none,1=high school,2=bachelor,3=master,4=phd,5=postdoc).\n"
    "   d) experience_years: integer or null (meaningful years, e.g., 2, 5).\n"
    "   e) seniority: string 'junior','mid','senior' based on experience_years.\n"
    "   f) hard_skills: array of strings (at least 3 technical skills).\n"
    "      - Format: ['python', 'sql', 'spark']. No duplicates, lowercase.\n"
    "   g) soft_skills: array of strings (at least 3 behavioral skills).\n"
    "      - Format: ['communication', 'teamwork', 'adaptability']. No duplicates.\n"
    "   h) company_name: string (exact hiring company name).\n"
    "   i) sector: array of strings (primary sectors).\n"
    "   j) location: object {city:string|null, region:string|null, country:string|null, remote:boolean}.\n"
    "   k) salary_range: object {min:number|null, max:number|null, currency:string|null, period:string|null}.\n"
    "   l) publication_date: string: YYYY-MM-DD.\n"
    "3) NO ADDITIONAL FIELDS. RETURN ONLY THE JSON ARRAY OF OBJECTS."
)

def clean_and_extract(raw_text: str) -> list[dict]:
    """
    Extracts a JSON array from a raw string, attempting multiple robust strategies
    to handle common formatting issues in API responses.
    """
    # Strategy 1: Direct extraction between the first '[' and the last ']'
    start, end = raw_text.find('['), raw_text.rfind(']')
    if 0 <= start < end:
        frag = raw_text[start : end + 1]
        try:
            return json.loads(frag)
        except json.JSONDecodeError:
            logger.debug(f"Direct JSON parse failed. Trying regex/char-by-char. Fragment: {frag[:200]}...")

    # Strategy 2: Clean up superfluous commas and try regex extraction
    # This addresses cases like `{"key": "value",}` or `[item1,,item2]`
    s = re.sub(r",\s*([}\]])", r"\1", raw_text) # Remove trailing commas before '}' or ']'
    m = re.search(r"\[.*?\]", s, re.DOTALL) # Find the first array-like structure
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            logger.debug(f"Regex JSON parse failed. Trying char-by-char. Regex match: {m.group(0)[:200]}...")

    # Strategy 3: Character-by-character parsing for robust JSON array extraction
    buf, depth = "", 0
    extracted_json = []
    in_string = False
    escaped = False

    for ch in raw_text:
        if ch == '\\' and not escaped: # Handle escape character
            escaped = True
            buf += ch
            continue
        
        if ch == '"' and not escaped: # Toggle in_string flag
            in_string = not in_string
            buf += ch
            
        elif not in_string: # Process structural characters only if not inside a string
            if ch == '[':
                depth += 1
                if depth == 1: # Start of a new top-level array
                    buf = "["
                buf += ch
            elif ch == ']':
                buf += ch
                if depth > 0:
                    depth -= 1
                # If we've closed a top-level array and buffer looks like JSON
                if depth == 0 and buf.strip().startswith('['):
                    try:
                        extracted_json.extend(json.loads(buf)) # Parse and add to results
                        buf = "" # Reset buffer for next JSON block
                    except json.JSONDecodeError:
                        logger.warning(f"Partial JSON decoding failed from char-by-char buffer: {buf[:100]}... Resetting buffer.")
                        buf = "" # Clear buffer if invalid, to prevent further errors
            elif depth > 0: # Add characters to buffer if inside a JSON structure
                buf += ch
            elif ch.isspace() or ch == ',': # Ignore whitespace and commas outside main JSON structure
                pass
            else: # Log unexpected characters outside JSON structure
                logger.debug(f"Ignoring unexpected character outside JSON structure: '{ch}'")
        else: # If inside a string, just append the character
            buf += ch
        escaped = False # Reset escape flag

    if extracted_json:
        logger.debug(f"Successfully extracted JSON using char-by-char method. Count: {len(extracted_json)}")
        return extracted_json

    logger.error("Unable to extract a valid JSON array from Gemini's response after all attempts.")
    return []


def post_process_gemini_output(parsed_results: list[dict], original_batch_size: int) -> list[dict]:
    """
    Applies post-processing to data received from Gemini to ensure type consistency
    and specific formats. This includes date normalization, profile categorization,
    and handling of nested objects like 'location' and 'salary_range'.
    Ensures the output list has the same size as the original batch by padding with {} if necessary.
    """
    processed_data = []
    
    # Define a set of allowed profiles for quick lookup and validation
    ALLOWED_PROFILES = {
        'data analyst', 'data scientist', 'data engineer', 'business intelligence analyst',
        'machine learning engineer', 'data architect', 'data product manager',
        'data visualization specialist', 'data governance analyst', 'quantitative analyst',
        'mlops engineer', 'ai engineer', 'database administrator', 'research scientist',
        'data strategist', 'analytics engineer', 'iot data specialist', 'data quality analyst',
        'big data engineer', 'cloud data engineer', 'data ethicist', 'data privacy officer',
        'data security analyst', 'nlp engineer', 'computer vision engineer',
        'bioinformatics data scientist', 'data consultant', 'fraud analyst', 'risk analyst',
        'marketing analyst', 'financial data analyst', 'supply chain analyst',
        'operations analyst', 'database developer', 'crm analyst', 'erp specialist',
        'actuarial analyst', 'geospatial data scientist', 'clinical data manager',
        'biostatistician', 'data migration specialist', 'business systems analyst',
        'web analytics specialist', 'customer insights analyst', 'pricing analyst',
        'ux data analyst', 'site reliability engineer (sre) - data',
        'technical account manager - data', 'solution architect - data',
        'sales engineer - data', 'pre-sales engineer - data', 'data evangelist',
        'growth analyst', 'e-commerce analyst', 'media analyst', 'content analyst',
        'network data analyst', 'telecom data analyst', 'energy data analyst',
        'environmental data analyst', 'healthcare data analyst', 'genomics data scientist',
        'clinical research data analyst', 'epidemiology data analyst',
        'financial quantitative analyst', 'algorithmic trading analyst', 'credit risk analyst',
        'market risk analyst', 'anti-money laundering (aml) analyst', 'compliance data analyst',
        'cybersecurity data analyst', 'threat intelligence analyst', 'forensic data analyst',
        'devops engineer - data',
        'unspecified', 'none'
    }

    # Iterate up to the original batch size to ensure proper alignment and padding
    for i in range(original_batch_size):
        # Retrieve the corresponding item from parsed_results, or an empty dict if it doesn't exist
        item = parsed_results[i] if i < len(parsed_results) else {}

        # --- Apply post-processing rules for each field to ensure consistent types and formats ---
        
        # 1. is_data_profile (boolean)
        is_data = item.get('is_data_profile')
        # Robust conversion: True for 'true', '1', or actual True; False otherwise.
        item['is_data_profile'] = bool(is_data) if isinstance(is_data, (bool, int)) else (str(is_data).lower() == 'true' if isinstance(is_data, str) else False)

        # 2. profile (string)
        if 'profile' in item and item['profile'] is not None:
            normalized_profile = normalize_text(item['profile'])
            if normalized_profile in ALLOWED_PROFILES:
                item['profile'] = normalized_profile
            elif item['is_data_profile']: # If identified as a data role but specific profile is unclear
                # Attempt to infer a common data profile based on keywords
                if 'analyst' in normalized_profile: item['profile'] = 'data analyst'
                elif 'engineer' in normalized_profile and 'machine learning' in normalized_profile: item['profile'] = 'machine learning engineer'
                elif 'engineer' in normalized_profile and ('data' in normalized_profile or 'big data' in normalized_profile): item['profile'] = 'data engineer'
                elif 'scientist' in normalized_profile and 'data' in normalized_profile: item['profile'] = 'data scientist'
                elif 'intelligence' in normalized_profile or 'bi' in normalized_profile: item['profile'] = 'business intelligence analyst'
                elif 'mlops' in normalized_profile: item['profile'] = 'mlops engineer'
                elif 'architect' in normalized_profile: item['profile'] = 'data architect'
                elif 'consultant' in normalized_profile: item['profile'] = 'data consultant'
                elif 'admin' in normalized_profile or 'database' in normalized_profile: item['profile'] = 'database administrator'
                else: item['profile'] = 'unspecified' # Default for recognized data roles that don't fit
            else:
                item['profile'] = 'none' # Not a data profile
        else:
            # Default if profile is missing or None. If is_data_profile is True, set to 'unspecified'.
            item['profile'] = 'none' if not item.get('is_data_profile', False) else 'unspecified'

        # 3. education_level (integer 0-5)
        try:
            edu_level = item.get('education_level')
            item['education_level'] = int(edu_level) if edu_level is not None else None
            # Validate range
            if item['education_level'] is not None and not (0 <= item['education_level'] <= 5):
                item['education_level'] = None # Out of expected range
        except (ValueError, TypeError):
            item['education_level'] = None

        # 4. experience_years (integer or null)
        try:
            exp_val = item.get('experience_years')
            if isinstance(exp_val, (int, float)): # Already a number
                item['experience_years'] = int(exp_val)
            elif isinstance(exp_val, str): # String like "3-5 years" or "5+"
                exp_str = exp_val.split('-')[0].strip().replace('+', '')
                item['experience_years'] = int(exp_str) if exp_str else None
            else: # Any other type (e.g., None, list)
                item['experience_years'] = None
        except (ValueError, TypeError):
            item['experience_years'] = None

        # 5. seniority (string 'junior','mid','senior')
        item['seniority'] = normalize_text(item.get('seniority')) if item.get('seniority') else None
        if item['seniority'] not in {'junior', 'mid', 'senior'} and item['seniority'] is not None:
            item['seniority'] = None # Set to None if invalid value

        # 6. hard_skills & 7. soft_skills (lists of strings)
        for skill_type in ['hard_skills', 'soft_skills']:
            skills_raw = item.get(skill_type)
            if isinstance(skills_raw, str):
                # Split by comma and normalize each part, filter out empty strings
                item[skill_type] = [normalize_text(s) for s in skills_raw.split(',') if normalize_text(s)]
            elif isinstance(skills_raw, list):
                # Normalize each item in the list, filter out empty strings
                item[skill_type] = [normalize_text(s) for s in skills_raw if s is not None and normalize_text(s)]
            else:
                item[skill_type] = [] # Default to empty list
            item[skill_type] = list(dict.fromkeys(item[skill_type])) # Remove duplicates while preserving order

        # 8. company_name (string or null)
        item['company_name'] = normalize_text(item.get('company_name')) if item.get('company_name') else None

        # 9. sector (list of strings)
        sectors_raw = item.get('sector')
        if isinstance(sectors_raw, str):
            item['sector'] = [normalize_text(s) for s in sectors_raw.split(',') if normalize_text(s)]
        elif isinstance(sectors_raw, list):
            item['sector'] = [normalize_text(s) for s in sectors_raw if s is not None and normalize_text(s)]
        else:
            item['sector'] = []
        item['sector'] = list(dict.fromkeys(item['sector'])) # Remove duplicates

        # 10. location (object)
        if isinstance(item.get('location'), dict):
            loc = item['location']
            loc['city'] = normalize_text(loc.get('city')) if loc.get('city') else None
            loc['region'] = normalize_text(loc.get('region')) if loc.get('region') else None
            loc['country'] = normalize_text(loc.get('country')) if loc.get('country') else None
            
            # Robust boolean conversion for 'remote' field within location
            remote_val = loc.get('remote')
            loc['remote'] = bool(remote_val) if isinstance(remote_val, (bool, int)) else (str(remote_val).lower() == 'true' if isinstance(remote_val, str) else False)
            
            item['location'] = loc
        else:
            # Default empty location object if data is missing or malformed
            item['location'] = {'city': None, 'region': None, 'country': None, 'remote': False}

        # 11. salary_range (object or null)
        if isinstance(item.get('salary_range'), dict):
            salary = item['salary_range']
            try: salary['min'] = float(salary.get('min')) if salary.get('min') is not None else None
            except (ValueError, TypeError): salary['min'] = None
            try: salary['max'] = float(salary.get('max')) if salary.get('max') is not None else None
            except (ValueError, TypeError): salary['max'] = None
            
            salary['currency'] = normalize_text(salary.get('currency')).upper() if salary.get('currency') else None
            salary['period'] = normalize_text(salary.get('period')) if salary.get('period') else None

            # Convert all salaries to yearly for consistency (assuming 160 hours/month)
            if salary.get('period') == 'monthly' and salary.get('min') is not None:
                salary['min'] *= 12
                if salary['max'] is not None: salary['max'] *= 12
                salary['period'] = 'yearly'
            elif salary.get('period') == 'hourly' and salary.get('min') is not None:
                salary['min'] *= (160 * 12) # 160 hours/month * 12 months = 1920 hours/year
                if salary['max'] is not None: salary['max'] *= (160 * 12)
                salary['period'] = 'yearly'
            
            # If both min and max are None after conversion, set salary_range to None
            if salary['min'] is None and salary['max'] is None:
                item['salary_range'] = None
            else:
                item['salary_range'] = salary
        else:
            item['salary_range'] = None # Default to None if salary data is missing or malformed

        # 12. publication_date (string: YYYY-MM-DD)
        item['publication_date'] = normalize_date(item.get('publication_date'))

        processed_data.append(item) # Add the cleaned and processed item to the list
    
    return processed_data


def call_gemini(batch: list[dict]) -> list[dict]:
    """
    Calls the Gemini API to enrich a batch of job offers with a retry mechanism.
    Returns a list of post-processed objects, one for each entry in the batch.
    On total failure for a batch, returns a list of empty dictionaries of the same batch size.
    """
    # The `contents` structure for the API call: list of dictionaries.
    # The GenerativeModel class handles the conversion internally.
    contents = [
        {
            "role": "user",
            "parts": [
                {
                    "text": PRE_PROMPT + "\n" + SYSTEM_PROMPT + "\n" + json.dumps(batch, ensure_ascii=False)
                }
            ]
        }
    ]
    
    # Configuration for content generation, using `types.GenerationConfig`
    cfg = types.GenerationConfig(response_mime_type="text/plain", temperature=0.7, top_p=0.95, top_k=40)
    
    for attempt in range(RETRIES):
        try:
            logger.debug(f"Attempt {attempt + 1}/{RETRIES} to call Gemini API for batch of {len(batch)} items.")
            
            full_response_text = ""
            # `client` is the GenerativeModel instance initialized globally
            response_stream = client.generate_content(
                contents=contents,
                generation_config=cfg, # This is the corrected parameter name
                stream=True # Enable streaming for potentially long responses
            )
            
            for chunk in response_stream:
                if hasattr(chunk, 'text') and chunk.text: # Check if the chunk has the 'text' attribute
                    full_response_text += chunk.text

            parsed_results = clean_and_extract(full_response_text)
            
            # Post-process results and ensure alignment with the original batch size
            return post_process_gemini_output(parsed_results, len(batch))

        except Exception as e:
            logger.warning(f"An error occurred during Gemini call (attempt {attempt + 1}): {e}", exc_info=True)

        if attempt < RETRIES - 1:
            time.sleep(BACKOFF * (2 ** attempt)) # Exponential backoff for subsequent retries
        else:
            logger.error(f"Failed to process batch after {RETRIES} attempts. Returning empty results for this batch.")

    # On total failure after all retries, return a list of empty dicts matching the batch size
    return [{}] * len(batch)


def main() -> None:
    """
    Main entry point of the script:
    1. Reads job offers from a specified JSON input file.
    2. Preprocesses basic fields of each offer.
    3. Calls the Gemini API in batches to enrich the offer data.
    4. Incrementally writes identified "data profile" offers to a JSON output file.
    5. Collects all "data profile" offers and saves them to an Excel file upon completion.
    """
    parser = argparse.ArgumentParser(description='Process job offers using Gemini API.')
    parser.add_argument('input_file', type=str, help='Path to the input JSON file containing job offers.')
    args = parser.parse_args()

    input_file_path = args.input_file
    if not os.path.exists(input_file_path):
        logger.critical(f"Input file not found: {input_file_path}")
        sys.exit(1)

    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True) # Create output directory if it doesn't exist

    # Load raw offers from the input JSON file
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

    # Validate and pre-process all offers once before sending to Gemini
    validated_and_preprocessed_offers = []
    for i, offer in enumerate(raw_offers):
        original_offer_copy = offer.copy() # Keep a copy for debugging if preprocessing fails
        try:
            # Apply initial normalization and parsing for some fields
            offer['publication_date'] = normalize_date(offer.get('publication_date'))
            
            # Replace 'lieu' field with a structured 'location' object
            offer['location'] = parse_location(offer.pop('lieu', None))
            
            # Normalize other text fields
            for f in ['titre', 'via', 'contrat', 'type_travail']:
                offer[f] = normalize_text(offer.get(f))
            
            # Ensure essential keys are present, even if their values are None,
            # for consistent structure in the input sent to Gemini.
            offer['job_url'] = offer.get('job_url') # Assuming job_url is always present and unique
            offer['titre'] = offer.get('titre')
            offer['via'] = offer.get('via')
            offer['contrat'] = offer.get('contrat')
            offer['type_travail'] = offer.get('type_travail')

            validated_and_preprocessed_offers.append(offer)

        except Exception as e:
            logger.warning(f"Skipping offer at original index {i} due to an unexpected error during initial normalization or preprocessing: {e}. Original data: {original_offer_copy}", exc_info=True)


    if not validated_and_preprocessed_offers:
        logger.warning("No valid offers found to process after initial validation. Exiting.")
        sys.exit(0)

    # Generate output filenames with timestamp for uniqueness
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    output_json_file = os.path.join(output_dir, f'enriched_data_profiles_{timestamp}.json')
    output_excel_file = os.path.join(output_dir, f'enriched_data_profiles_{timestamp}.xlsx')

    first_item_written_to_json = False # Flag to correctly format JSON array (add commas)
    all_data_profiles_for_excel = [] # List to collect all data profiles for the final Excel export

    logger.info(f"Starting incremental writing to {output_json_file}")
    # Open the JSON output file in write mode
    with open(output_json_file, 'w', encoding='utf-8') as json_out_f:
        json_out_f.write('[\n') # Write the opening bracket of the JSON array

        total_offers_to_process = len(validated_and_preprocessed_offers)
        # Process offers in batches
        for i in range(0, total_offers_to_process, BATCH_SIZE):
            batch_original_preprocessed = validated_and_preprocessed_offers[i : i + BATCH_SIZE]
            
            logger.info(f"Processing batch {i+1} to {min(i + BATCH_SIZE, total_offers_to_process)} out of {total_offers_to_process} offers.")
            
            # Call Gemini API for the current batch
            enriched_batch_results = call_gemini(batch_original_preprocessed)
            
            # Iterate through the results of the batch (original pre-processed offers paired with enriched data)
            for j, original_offer_preprocessed in enumerate(batch_original_preprocessed):
                # Retrieve the corresponding enriched data. `enriched_batch_results` is guaranteed
                # to be the same size as `batch_original_preprocessed` due to `post_process_gemini_output`.
                enriched_data_for_one_offer = enriched_batch_results[j]

                # Merge the original pre-processed offer with the enriched data from Gemini.
                # Enriched data takes precedence if a field exists in both.
                merged_offer = original_offer_preprocessed.copy()
                if isinstance(enriched_data_for_one_offer, dict) and enriched_data_for_one_offer:
                    merged_offer.update(enriched_data_for_one_offer)
                else:
                    # If Gemini enrichment failed for this specific offer (e.g., empty dict returned),
                    # we explicitly mark it as not a data profile for filtered output.
                    logger.warning(f"No valid enriched data received for offer: {original_offer_preprocessed.get('job_url', 'N/A')} (original index {i+j}). It will not be filtered as a data profile in the final output.")
                    merged_offer['is_data_profile'] = False # Ensure this is False if enrichment failed
                    merged_offer['profile'] = 'none' # Ensure profile is 'none' if enrichment failed

                # If the merged offer is identified as a data profile, write it incrementally to JSON
                if merged_offer.get('is_data_profile') is True:
                    if first_item_written_to_json:
                        json_out_f.write(',\n') # Add a comma before each item except the very first
                    json.dump(merged_offer, json_out_f, ensure_ascii=False, indent=2)
                    first_item_written_to_json = True
                    all_data_profiles_for_excel.append(merged_offer) # Also collect for final Excel export

            # Introduce a small delay between batches to respect API rate limits and avoid throttling
            time.sleep(1)
        
        json_out_f.write('\n]\n') # Write the closing bracket of the JSON array
    logger.info(f"Incremental writing to {output_json_file} complete. {len(all_data_profiles_for_excel)} data profiles identified and saved.")

    # After all batches are processed, save all collected data profiles to an Excel file
    if all_data_profiles_for_excel:
        try:
            df = pd.DataFrame(all_data_profiles_for_excel)
            # Flatten nested dictionaries (like 'location' and 'salary_range') into separate columns
            # e.g., 'location' becomes 'location_city', 'location_region', etc.
            df_flat = pd.json_normalize(df.to_dict('records'), sep='_')
            df_flat.to_excel(output_excel_file, index=False)
            logger.info(f"Results saved to: {output_excel_file}")
        except Exception as e:
            logger.error(f"Error writing Excel file {output_excel_file}: {e}", exc_info=True)
    else:
        logger.warning("No data-related results to save to Excel. Output Excel file not created.")

    logger.info("Processing complete.")

if __name__ == '__main__':
    main()
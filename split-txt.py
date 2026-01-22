import os
import re
import unicodedata
from tqdm import tqdm
from datetime import datetime
import pandas as pd

fulltxt_dir = "./data/full-txt"
speech_dir = "./data/speech"
log_file = "./data/speech/extraction_log.txt"
metadata_csv = "./data/speech/speech_metadata.csv"
meeting_metadata_csv = "./data/speech/meeting_metadata.csv"
atlasti_variables_csv = "./data/speech/atlasti_document_variables.csv"

# =============================================================================
# SESSION TO YEAR MAPPING
# =============================================================================
# UN General Assembly sessions start in September of each year
# Session 48 = 1993, Session 79 = 2024
SESSION_YEAR_MAP = {f"session_{s}": 1945 + s for s in range(48, 80)}

# =============================================================================
# COUNTRY TO REGION MAPPING (UN Regional Groups)
# =============================================================================
REGION_MAP = {
    # African Group
    "Algeria": "Africa", "Angola": "Africa", "Benin": "Africa", "Botswana": "Africa",
    "Burkina Faso": "Africa", "Burundi": "Africa", "Cabo Verde": "Africa", "Cape Verde": "Africa",
    "Cameroon": "Africa", "Central African Republic": "Africa", "Chad": "Africa", "Comoros": "Africa",
    "Congo": "Africa", "Côte d'Ivoire": "Africa", "Democratic Republic of the Congo": "Africa",
    "Djibouti": "Africa", "Egypt": "Africa", "Equatorial Guinea": "Africa", "Eritrea": "Africa",
    "Eswatini": "Africa", "Swaziland": "Africa", "Ethiopia": "Africa", "Gabon": "Africa",
    "Gambia": "Africa", "Ghana": "Africa", "Guinea": "Africa", "Guinea-Bissau": "Africa",
    "Kenya": "Africa", "Lesotho": "Africa", "Liberia": "Africa", "Libya": "Africa",
    "Madagascar": "Africa", "Malawi": "Africa", "Mali": "Africa", "Mauritania": "Africa",
    "Mauritius": "Africa", "Morocco": "Africa", "Mozambique": "Africa", "Namibia": "Africa",
    "Niger": "Africa", "Nigeria": "Africa", "Rwanda": "Africa", "Sao Tome and Principe": "Africa",
    "Sao Tomé and Principe": "Africa", "Senegal": "Africa", "Seychelles": "Africa",
    "Sierra Leone": "Africa", "Somalia": "Africa", "South Africa": "Africa", "South Sudan": "Africa",
    "Sudan": "Africa", "Togo": "Africa", "Tunisia": "Africa", "Uganda": "Africa",
    "United Republic of Tanzania": "Africa", "Tanzania": "Africa", "Zambia": "Africa", "Zimbabwe": "Africa",

    # Asia-Pacific Group
    "Afghanistan": "Asia-Pacific", "Bahrain": "Asia-Pacific", "Bangladesh": "Asia-Pacific",
    "Bhutan": "Asia-Pacific", "Brunei Darussalam": "Asia-Pacific", "Cambodia": "Asia-Pacific",
    "China": "Asia-Pacific", "Cyprus": "Asia-Pacific", "Democratic People's Republic of Korea": "Asia-Pacific",
    "Fiji": "Asia-Pacific", "India": "Asia-Pacific", "Indonesia": "Asia-Pacific",
    "Iran": "Asia-Pacific", "Islamic Republic of Iran": "Asia-Pacific", "Iraq": "Asia-Pacific",
    "Japan": "Asia-Pacific", "Jordan": "Asia-Pacific", "Kazakhstan": "Asia-Pacific",
    "Kiribati": "Asia-Pacific", "Kuwait": "Asia-Pacific", "Kyrgyzstan": "Asia-Pacific",
    "Lao People's Democratic Republic": "Asia-Pacific", "Lebanon": "Asia-Pacific",
    "Malaysia": "Asia-Pacific", "Maldives": "Asia-Pacific", "Marshall Islands": "Asia-Pacific",
    "Micronesia": "Asia-Pacific", "Federated States of Micronesia": "Asia-Pacific",
    "Mongolia": "Asia-Pacific", "Myanmar": "Asia-Pacific", "Nauru": "Asia-Pacific",
    "Nepal": "Asia-Pacific", "Oman": "Asia-Pacific", "Pakistan": "Asia-Pacific",
    "Palau": "Asia-Pacific", "Papua New Guinea": "Asia-Pacific", "Philippines": "Asia-Pacific",
    "Qatar": "Asia-Pacific", "Republic of Korea": "Asia-Pacific", "Samoa": "Asia-Pacific",
    "Saudi Arabia": "Asia-Pacific", "Singapore": "Asia-Pacific", "Solomon Islands": "Asia-Pacific",
    "Sri Lanka": "Asia-Pacific", "Syrian Arab Republic": "Asia-Pacific", "Syria": "Asia-Pacific",
    "Tajikistan": "Asia-Pacific", "Thailand": "Asia-Pacific", "Timor-Leste": "Asia-Pacific",
    "Tonga": "Asia-Pacific", "Turkmenistan": "Asia-Pacific", "Tuvalu": "Asia-Pacific",
    "United Arab Emirates": "Asia-Pacific", "Uzbekistan": "Asia-Pacific", "Vanuatu": "Asia-Pacific",
    "Viet Nam": "Asia-Pacific", "Yemen": "Asia-Pacific",

    # Eastern European Group
    "Albania": "Eastern Europe", "Armenia": "Eastern Europe", "Azerbaijan": "Eastern Europe",
    "Belarus": "Eastern Europe", "Bosnia and Herzegovina": "Eastern Europe", "Bulgaria": "Eastern Europe",
    "Croatia": "Eastern Europe", "Czech Republic": "Eastern Europe", "Czechia": "Eastern Europe",
    "Estonia": "Eastern Europe", "Georgia": "Eastern Europe", "Hungary": "Eastern Europe",
    "Latvia": "Eastern Europe", "Lithuania": "Eastern Europe", "Montenegro": "Eastern Europe",
    "North Macedonia": "Eastern Europe", "Poland": "Eastern Europe", "Republic of Moldova": "Eastern Europe",
    "Moldova": "Eastern Europe", "Romania": "Eastern Europe", "Russian Federation": "Eastern Europe",
    "Serbia": "Eastern Europe", "Slovakia": "Eastern Europe", "Slovenia": "Eastern Europe",
    "Ukraine": "Eastern Europe",

    # Latin American and Caribbean Group (GRULAC)
    "Antigua and Barbuda": "GRULAC", "Argentina": "GRULAC", "Bahamas": "GRULAC",
    "Barbados": "GRULAC", "Belize": "GRULAC", "Bolivia": "GRULAC",
    "Plurinational State of Bolivia": "GRULAC", "Brazil": "GRULAC", "Chile": "GRULAC",
    "Colombia": "GRULAC", "Costa Rica": "GRULAC", "Cuba": "GRULAC", "Dominica": "GRULAC",
    "Dominican Republic": "GRULAC", "Ecuador": "GRULAC", "El Salvador": "GRULAC",
    "Grenada": "GRULAC", "Guatemala": "GRULAC", "Guyana": "GRULAC", "Haiti": "GRULAC",
    "Honduras": "GRULAC", "Jamaica": "GRULAC", "Mexico": "GRULAC", "Nicaragua": "GRULAC",
    "Panama": "GRULAC", "Paraguay": "GRULAC", "Peru": "GRULAC",
    "Saint Kitts and Nevis": "GRULAC", "Saint Lucia": "GRULAC",
    "Saint Vincent and the Grenadines": "GRULAC", "St. Vincent and the Grenadines": "GRULAC",
    "Suriname": "GRULAC", "Trinidad and Tobago": "GRULAC", "Uruguay": "GRULAC",
    "Venezuela": "GRULAC", "Bolivarian Republic of Venezuela": "GRULAC",

    # Western European and Others Group (WEOG)
    "Andorra": "WEOG", "Australia": "WEOG", "Austria": "WEOG", "Belgium": "WEOG",
    "Canada": "WEOG", "Denmark": "WEOG", "Finland": "WEOG", "France": "WEOG",
    "Germany": "WEOG", "Greece": "WEOG", "Iceland": "WEOG", "Ireland": "WEOG",
    "Israel": "WEOG", "Italy": "WEOG", "Liechtenstein": "WEOG", "Luxembourg": "WEOG",
    "Malta": "WEOG", "Monaco": "WEOG", "Netherlands": "WEOG", "Kingdom of the Netherlands": "WEOG",
    "New Zealand": "WEOG", "Norway": "WEOG", "Portugal": "WEOG", "San Marino": "WEOG",
    "Spain": "WEOG", "Sweden": "WEOG", "Switzerland": "WEOG", "Turkey": "WEOG",
    "Türkiye": "WEOG", "United Kingdom": "WEOG", "United States": "WEOG", "United States of America": "WEOG",

    # Observers / Special
    "Holy See": "Observer", "State of Palestine": "Observer", "Palestine": "Observer",
}

# =============================================================================
# CHINESE-LED ORGANIZATION MEMBERSHIP
# =============================================================================
# Format: { "Country": year_joined } or None if not a member
# For organizations founded at a specific date, countries joining at founding get that year

# FOCAC - Forum on China-Africa Cooperation (founded October 2000)
# All African states except Eswatini (which recognizes Taiwan)
FOCAC_MEMBERS = {
    "Algeria": 2000, "Angola": 2000, "Benin": 2000, "Botswana": 2000,
    "Burkina Faso": 2000, "Burundi": 2000, "Cabo Verde": 2000, "Cape Verde": 2000,
    "Cameroon": 2000, "Central African Republic": 2000, "Chad": 2000, "Comoros": 2000,
    "Congo": 2000, "Côte d'Ivoire": 2000, "Democratic Republic of the Congo": 2000,
    "Djibouti": 2000, "Egypt": 2000, "Equatorial Guinea": 2000, "Eritrea": 2000,
    "Ethiopia": 2000, "Gabon": 2000, "Gambia": 2016,  # Gambia switched from Taiwan in 2016
    "Ghana": 2000, "Guinea": 2000, "Guinea-Bissau": 2000, "Kenya": 2000,
    "Lesotho": 2000, "Liberia": 2000, "Libya": 2000, "Madagascar": 2000,
    "Malawi": 2008,  # Malawi switched from Taiwan in 2008
    "Mali": 2000, "Mauritania": 2000, "Mauritius": 2000, "Morocco": 2000,
    "Mozambique": 2000, "Namibia": 2000, "Niger": 2000, "Nigeria": 2000,
    "Rwanda": 2000, "Sao Tome and Principe": 2016,  # Switched from Taiwan in 2016
    "Sao Tomé and Principe": 2016, "Senegal": 2000, "Seychelles": 2000,
    "Sierra Leone": 2000, "Somalia": 2000, "South Africa": 2000,
    "South Sudan": 2011,  # South Sudan independence 2011
    "Sudan": 2000, "Togo": 2000, "Tunisia": 2000, "Uganda": 2000,
    "United Republic of Tanzania": 2000, "Tanzania": 2000, "Zambia": 2000, "Zimbabwe": 2000,
    # Eswatini/Swaziland NOT a member (recognizes Taiwan)
}

# CASCF - China-Arab States Cooperation Forum (founded January 2004)
# All Arab League member states
CASCF_MEMBERS = {
    "Algeria": 2004, "Bahrain": 2004, "Comoros": 2004, "Djibouti": 2004,
    "Egypt": 2004, "Iraq": 2004, "Jordan": 2004, "Kuwait": 2004,
    "Lebanon": 2004, "Libya": 2004, "Mauritania": 2004, "Morocco": 2004,
    "Oman": 2004, "State of Palestine": 2004, "Palestine": 2004, "Qatar": 2004,
    "Saudi Arabia": 2004, "Somalia": 2004, "Sudan": 2004,
    "Syrian Arab Republic": 2004, "Syria": 2004, "Tunisia": 2004,
    "United Arab Emirates": 2004, "Yemen": 2004,
}

# SCO - Shanghai Cooperation Organisation
# Founded June 2001 from Shanghai Five (1996)
# Format: year of full membership or observer status
SCO_MEMBERS = {
    # Founding members (2001)
    "China": 2001, "Russian Federation": 2001, "Kazakhstan": 2001,
    "Kyrgyzstan": 2001, "Tajikistan": 2001, "Uzbekistan": 2001,
    # Later full members
    "India": 2017, "Pakistan": 2017,
    "Iran": 2023, "Islamic Republic of Iran": 2023,
    "Belarus": 2024,
}

# SCO Observers (not full members, but participating)
SCO_OBSERVERS = {
    "Mongolia": 2004, "Afghanistan": 2012,
}

# SCO Dialogue Partners
SCO_DIALOGUE_PARTNERS = {
    "Sri Lanka": 2010, "Turkey": 2013, "Türkiye": 2013,
    "Cambodia": 2015, "Azerbaijan": 2016, "Nepal": 2016, "Armenia": 2016,
    "Egypt": 2022, "Qatar": 2022, "Saudi Arabia": 2022,
    "Kuwait": 2023, "Maldives": 2023, "Myanmar": 2023, "United Arab Emirates": 2023,
    "Bahrain": 2023,
}

# BRI - Belt and Road Initiative (announced September 2013)
# Countries with signed MoU with China
# Based on official data and Fudan University Green Finance Center tracking
BRI_MEMBERS = {
    # 2013 - Founding year
    "Belarus": 2013, "Cambodia": 2013, "China": 2013, "Kyrgyzstan": 2013,
    "Moldova": 2013, "North Macedonia": 2013, "Pakistan": 2013,
    # 2014
    "Thailand": 2014,
    # 2015
    "Armenia": 2015, "Azerbaijan": 2015, "Bulgaria": 2015, "Cameroon": 2015,
    "Comoros": 2015, "Czech Republic": 2015, "Czechia": 2015, "Hungary": 2015,
    "Indonesia": 2015, "Iraq": 2015, "Kazakhstan": 2015, "Poland": 2015,
    "Romania": 2015, "Serbia": 2015, "Slovakia": 2015, "Somalia": 2015,
    "South Africa": 2015, "Turkey": 2015, "Türkiye": 2015, "Uzbekistan": 2015,
    # 2016
    "Egypt": 2016, "Georgia": 2016, "Myanmar": 2016, "Papua New Guinea": 2016,
    # 2017
    "Albania": 2017, "Bosnia and Herzegovina": 2017, "Côte d'Ivoire": 2017,
    "Croatia": 2017, "Estonia": 2017, "Kenya": 2017, "Latvia": 2017,
    "Lithuania": 2017, "Madagascar": 2017, "Malaysia": 2017, "Maldives": 2017,
    "Montenegro": 2017, "Morocco": 2017, "Nepal": 2017, "New Zealand": 2017,
    "North Macedonia": 2017, "Panama": 2017, "Philippines": 2017, "Slovenia": 2017,
    "Sri Lanka": 2017, "Timor-Leste": 2017, "Turkmenistan": 2017, "Ukraine": 2017,
    "Viet Nam": 2017,
    # 2018
    "Algeria": 2018, "Angola": 2018, "Antigua and Barbuda": 2018, "Bahrain": 2018,
    "Bangladesh": 2019, "Barbados": 2019, "Benin": 2018, "Bolivia": 2018,
    "Brunei Darussalam": 2018, "Burundi": 2018, "Chad": 2018, "Chile": 2018,
    "Cook Islands": 2018, "Costa Rica": 2018, "Djibouti": 2018, "Dominica": 2018,
    "Ecuador": 2018, "El Salvador": 2018, "Equatorial Guinea": 2019,
    "Eritrea": 2021, "Ethiopia": 2018, "Fiji": 2018, "Gabon": 2018, "Ghana": 2018,
    "Greece": 2018, "Grenada": 2018, "Guyana": 2018, "Iran": 2018,
    "Islamic Republic of Iran": 2018, "Jamaica": 2019, "Kuwait": 2018,
    "Lao People's Democratic Republic": 2018, "Lebanon": 2017, "Lesotho": 2019,
    "Liberia": 2019, "Libya": 2018, "Luxembourg": 2019, "Mauritania": 2018,
    "Micronesia": 2018, "Federated States of Micronesia": 2018, "Mozambique": 2018,
    "Namibia": 2018, "Niger": 2018, "Nigeria": 2018, "Niue": 2018, "Oman": 2018,
    "Peru": 2019, "Portugal": 2018, "Qatar": 2019, "Rwanda": 2018, "Samoa": 2018,
    "Saudi Arabia": 2018, "Senegal": 2018, "Seychelles": 2018, "Sierra Leone": 2018,
    "Singapore": 2018, "Solomon Islands": 2019, "South Sudan": 2018, "Sudan": 2018,
    "Suriname": 2018, "Tajikistan": 2018, "Tanzania": 2018,
    "United Republic of Tanzania": 2018, "Togo": 2018, "Tonga": 2018,
    "Trinidad and Tobago": 2018, "Tunisia": 2018, "Uganda": 2018,
    "United Arab Emirates": 2018, "Uruguay": 2018, "Vanuatu": 2018,
    "Venezuela": 2018, "Bolivarian Republic of Venezuela": 2018, "Yemen": 2018,
    "Zambia": 2018, "Zimbabwe": 2018,
    # 2019
    "Cyprus": 2019, "Cuba": 2019, "Dominican Republic": 2019, "Italy": 2019,
    "Kiribati": 2020, "Mali": 2019,
    # 2020-2024
    "Botswana": 2021, "Central African Republic": 2021, "Democratic Republic of the Congo": 2021,
    "Guinea-Bissau": 2021, "Argentina": 2022, "Malawi": 2022, "Nicaragua": 2022,
    "Syria": 2022, "Syrian Arab Republic": 2022, "Afghanistan": 2023, "Honduras": 2023,
    "Jordan": 2023,
}

# Countries that have exited BRI (for reference)
BRI_EXITED = {
    "Estonia": 2022, "Latvia": 2022, "Lithuania": 2021,
    "Italy": 2023, "Philippines": 2023, "Panama": 2025,
}

# China-CELAC Forum (Community of Latin American and Caribbean States)
# Established July 2014 at Brasilia summit, first ministerial Jan 2015 Beijing
# All 33 CELAC member states are members (all Latin American & Caribbean countries)
CELAC_MEMBERS = {
    # Central America
    "Belize": 2014, "Costa Rica": 2014, "El Salvador": 2014, "Guatemala": 2014,
    "Honduras": 2014, "Mexico": 2014, "Nicaragua": 2014, "Panama": 2014,
    # Caribbean
    "Antigua and Barbuda": 2014, "Bahamas": 2014, "Barbados": 2014,
    "Cuba": 2014, "Dominica": 2014, "Dominican Republic": 2014, "Grenada": 2014,
    "Guyana": 2014, "Haiti": 2014, "Jamaica": 2014,
    "Saint Kitts and Nevis": 2014, "Saint Lucia": 2014,
    "Saint Vincent and the Grenadines": 2014, "Suriname": 2014,
    "Trinidad and Tobago": 2014,
    # South America
    "Argentina": 2014, "Bolivia": 2014, "Bolivarian Republic of Venezuela": 2014,
    "Brazil": 2014, "Chile": 2014, "Colombia": 2014, "Ecuador": 2014,
    "Paraguay": 2014, "Peru": 2014, "Uruguay": 2014, "Venezuela": 2014,
}


def get_org_membership(country):
    """
    Get Chinese-led organization membership for a country.
    Returns dict with org name -> join year (or None if not member).
    """
    return {
        'focac': FOCAC_MEMBERS.get(country),
        'cascf': CASCF_MEMBERS.get(country),
        'sco': SCO_MEMBERS.get(country),
        'sco_observer': SCO_OBSERVERS.get(country),
        'sco_dialogue': SCO_DIALOGUE_PARTNERS.get(country),
        'bri': BRI_MEMBERS.get(country),
        'celac': CELAC_MEMBERS.get(country),
    }


def is_member_at_time(country, org_dict, speech_year, exit_dict=None):
    """
    Check if country was a member of an organization at the time of the speech.
    Returns True if member during speech year (joined <= year and not yet exited).

    Args:
        country: Country name
        org_dict: Dict mapping country -> join year
        speech_year: Year of the speech
        exit_dict: Optional dict mapping country -> exit year
    """
    join_year = org_dict.get(country)
    if join_year is None:
        return False
    if speech_year < join_year:
        return False
    # Check if country has exited
    if exit_dict:
        exit_year = exit_dict.get(country)
        if exit_year is not None and speech_year >= exit_year:
            return False
    return True


def get_region(country):
    """Get UN regional group for a country, or 'Unknown' if not found."""
    return REGION_MAP.get(country, "Unknown")


def get_year(session):
    """Get year from session string (e.g., 'session_48' -> 1993)."""
    return SESSION_YEAR_MAP.get(session, None)


def sanitize_filename(name):
    """
    Sanitize a string for use in a filename.
    - Transliterates accented characters to ASCII (é → e, ü → u, etc.)
    - Removes spaces, quotes, and other problematic characters
    - Produces clean, cross-platform compatible filenames
    """
    # Normalize unicode and remove accents (é → e, ô → o, ü → u, etc.)
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))

    # Remove spaces
    name = name.replace(' ', '')

    # Remove quotes and other problematic characters (don't replace, just omit)
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', "'", ',', '.', ';']:
        name = name.replace(char, '')

    return name

# =============================================================================
# REGEX PATTERNS FOR SPEECH EXTRACTION
# =============================================================================

# Pattern to match the start of a speech by a country delegate.
#
# Structure: \n[Title] [Name] ([Country]) ([Language]):
#
# Examples that SHOULD match:
#   - "Mr. Onkeya (Lao People's Democratic Republic):"
#   - "Mr. Chem Widhya (Cambodia) (spoke in French):"
#   - "Dame Billie Miller (Barbados):"
#   - "His Excellency Mr. Niyazov (Turkmenistan):"
#   - "Sheikh Hasina (Bangladesh):"
#
# Examples that should NOT match:
#   - "The President (spoke in French):" (session chair)
#   - "The Acting President:" (session chair)
#   - "President Wade (spoke in French):" (head of state, not delegate)
#   - "Mr. Olhaye (Djibouti), Vice-President, took the Chair." (procedural)
#
# Capture groups:
#   1. Speaker name with title (required) - e.g., "Mr. Onkeya", "Dame Billie Miller"
#   2. Country (required) - e.g., "Lao People's Democratic Republic", "Cambodia"
#   3. Language note (optional) - e.g., "spoke in French", "spoke in Lao; English text provided"
#
# Valid speaker titles (delegates always use formal titles):
SPEAKER_TITLES = r'''(?:
    Mr\.?|Mrs\.?|Ms\.?|Miss|Mme\.?|    # Common titles (period optional for British style)
    Dame|Sir|Dr\.?|                    # Honorifics
    His[ \t]+Excellency|Her[ \t]+Excellency|               # Diplomatic
    His[ \t]+Royal[ \t]+Highness|Her[ \t]+Royal[ \t]+Highness|   # Royalty
    Baron|Baroness|Lord|Lady|          # Nobility
    Prince|Princess|Sheikh|Dato|Datuk| # Other titles
    Chief|                             # Tribal/traditional leaders
    Commodore|Admiral|                 # Naval ranks
    Major-General|Lieutenant-General|Brigadier-General|General|Colonel|Major|Captain|  # Military ranks
    Archbishop|Cardinal|Bishop|Monsignor|Father|Pastor|   # Religious titles
    U|Daw|                             # Burmese honorifics
    President|                         # Heads of state (e.g., "President Tong (Kiribati):")
    Prime[ \t]+Minister|               # Head of government (when speaking as delegate)
    Minister                           # Ministers
)'''

# Pattern to identify head-of-state/government speeches (for logging/review purposes)
# These are legitimate General Debate speeches but may warrant separate consideration
# Includes:
#   - "President [Name]" - heads of state (e.g., President Tong of Kiribati)
#   - "Prime Minister [Name]" - heads of government (e.g., Prime Minister Schoof)
#   - "Sheikh [Name]" - may be head of government (e.g., Sheikh Hasina, PM of Bangladesh)
HEAD_OF_STATE_PATTERN = re.compile(r'^(?:President|Prime\s+Minister|Sheikh)\s+', re.IGNORECASE)

# Country must start with a capital letter and NOT be a language note or UN title
# This excludes:
#   - Malformed lines like "Mr. X ( spoke in French ):"
#   - UN officials like "Mr. X (Under-Secretary-General...):'"
COUNTRY_PATTERN = r'''
    (?![ \t]*spoke)             # Not a language note (spoke in ...)
    (?![ \t]*interpretation)    # Not a language note (interpretation from ...)
    (?!Under-Secretary)         # Not a UN official title
    (?!Secretary-General)       # Not a UN official title
    (?!Assistant[ \t]+Secretary)  # Not a UN official title
    [A-Z][^)]+                  # Country: starts with capital, any chars until )
'''

SPEECH_PATTERN = re.compile(r'''
    \n[ \t]*                    # Must start at beginning of a new line (with optional indent)
    (                           # Group 1: Speaker name with title
        ''' + SPEAKER_TITLES + r'''
        [^\n(]+                 # Rest of name (any chars except newline or open paren)
    )
    [ \t]+                      # Whitespace between name and country (spaces/tabs only)
    \((''' + COUNTRY_PATTERN + r''')\)   # Group 2: Country in parentheses (required, validated)
    (?:[ \t]+\(([^)]+)\))?      # Group 3: Language note in parentheses (optional)
    [ \t]*:                     # Colon after the intro, possibly with whitespace
''', re.VERBOSE)

# Pattern to find where the "General Debate" agenda item begins.
# This marks the start of the section containing individual country speeches.
# Note: [ \t]* matches only spaces/tabs (not newlines) for trailing whitespace
GENERAL_DEBATE_PATTERN = re.compile(r'\n[ \t]*general debate[ \t]*\n', re.IGNORECASE)

# Pattern to find "The President:" or "The Acting President:" which indicates
# end of a speech and transition to procedural remarks.
# May include optional parentheticals, e.g.:
#   - "The President:"
#   - "The Acting President:"
#   - "The President (spoke in French):"
#   - "The Acting President (spoke in Spanish):"
# The [ \t]* before : handles any whitespace before the colon.
PRESIDENT_PATTERN = re.compile(r'\n[ \t]*The (?:Acting )?President(?:[ \t]*\([^)]+\))?[ \t]*:')

# Pattern to find the end of the meeting.
# Example: "The meeting rose at 1 p.m." or "The meeting rose at 6.05 p.m."
# This marks the absolute end of the document's speech content.
MEETING_END_PATTERN = re.compile(r'\n[ \t]*The meeting rose at[ \t]*')

# Pattern to detect POTENTIALLY malformed speaker lines for manual review.
# These look like speeches but don't match the strict SPEECH_PATTERN.
# Examples:
#   - "Mr. Asselborn ( spoke in French ):" - missing country
#   - "Mr. Shaaban (Under-Secretary-General ...):" - UN official, not delegate
# This uses a looser pattern to catch candidates, then we compare against SPEECH_PATTERN.
POTENTIAL_SPEECH_PATTERN = re.compile(r'''
    \n[ \t]*
    (''' + SPEAKER_TITLES + r'''[^\n(]+)   # Speaker with valid title
    [ \t]+
    \([^)]+\)                               # Any parenthetical
    (?:[ \t]+\([^)]+\))?                    # Optional second parenthetical
    [ \t]*:
''', re.VERBOSE)


def split_texts():
    # Initialize log
    os.makedirs(speech_dir, exist_ok=True)
    flagged_lines = []  # Lines that look like speeches but didn't match
    head_of_state_speeches = []  # Head-of-state speeches for review
    skipped_files = []  # Files skipped (no general debate pattern found)
    all_speeches = []  # All speech metadata for DataFrame export
    all_meetings = []  # Meeting-level metadata for DataFrame export
    speech_counter = 0  # Global counter for unique speech IDs
    for subdir in tqdm(os.listdir(fulltxt_dir), desc="Sessions", position=0):
        subdir_path = os.path.join(fulltxt_dir, subdir)
        if os.path.isdir(subdir_path):
            # Create corresponding output subdirectory
            output_subdir = os.path.join(speech_dir, subdir)
            os.makedirs(output_subdir, exist_ok=True)

            # Process all .txt files in the subdirectory
            for filename in tqdm(os.listdir(subdir_path), desc=f".txt files in {subdir}", position=1, leave=False):
                if filename.lower().endswith(".txt"):
                    txt_path = os.path.join(subdir_path, filename)
                    with open(txt_path, 'r', encoding='utf-8') as f:
                        content = f.read()

                    # Find the "general debate" section (case insensitive)
                    general_debate_match = GENERAL_DEBATE_PATTERN.search(content)
                    if not general_debate_match:
                        skipped_files.append({
                            'session': subdir,
                            'file': filename,
                            'reason': 'No general debate pattern found'
                        })
                        continue

                    # Get content from general debate section onwards
                    section_content = content[general_debate_match.end():]

                    # Initialize meeting-level accumulators
                    meeting_word_count = 0
                    meeting_countries = set()
                    meeting_languages = set()
                    meeting_speech_count = 0
                    meeting_head_of_state_count = 0

                    # Find all speech starts (strict pattern)
                    speeches = []
                    strict_match_positions = set()
                    for match in SPEECH_PATTERN.finditer(section_content):
                        speaker_name = match.group(1).strip()
                        country = match.group(2).strip()
                        language = match.group(3).strip() if match.group(3) else None
                        start_pos = match.end()
                        speeches.append({
                            'speaker': speaker_name,
                            'country': country,
                            'language': language,
                            'start': start_pos,
                            'match_start': match.start()  # Where the intro line begins
                        })
                        strict_match_positions.add(match.start())

                        # Track head-of-state speeches for review
                        if HEAD_OF_STATE_PATTERN.match(speaker_name):
                            head_of_state_speeches.append({
                                'file': filename,
                                'speaker': speaker_name,
                                'country': country
                            })

                    # Find potential speeches that didn't match strict pattern (for manual review)
                    for potential_match in POTENTIAL_SPEECH_PATTERN.finditer(section_content):
                        if potential_match.start() not in strict_match_positions:
                            # This looks like a speech but didn't pass strict validation
                            line = potential_match.group(0).strip()
                            flagged_lines.append({
                                'file': filename,
                                'line': line[:100],
                                'reason': 'Potential speech - failed strict pattern validation'
                            })

                    # Extract speech content (ends at next speech, "The President:", or meeting end)
                    for i, speech in enumerate(speeches):
                        start = speech['start']

                        # Find potential end markers
                        president_match = PRESIDENT_PATTERN.search(section_content[start:])
                        meeting_end_match = MEETING_END_PATTERN.search(section_content[start:])

                        # Next speech starts where the next intro line begins
                        next_speech_start = speeches[i + 1]['match_start'] if i + 1 < len(speeches) else len(section_content)

                        # Determine end position from the earliest end marker
                        end_candidates = [next_speech_start]
                        if president_match:
                            end_candidates.append(start + president_match.start())
                        if meeting_end_match:
                            end_candidates.append(start + meeting_end_match.start())
                        end = min(end_candidates)

                        speech_text = section_content[start:end].strip()

                        # Calculate additional metadata
                        year = get_year(subdir)
                        region = get_region(speech['country'])
                        paragraph_count = len([p for p in speech_text.split('\n\n') if p.strip()])
                        language = speech['language'] if speech['language'] else 'English'

                        # Generate unique speech ID and filename
                        speech_counter += 1
                        speech_id = f"speech_{speech_counter:05d}"
                        base_name = os.path.splitext(filename)[0]
                        safe_country = sanitize_filename(speech['country'])
                        speech_filename = f"{speech_id}_{base_name}_{safe_country}.txt"
                        speech_path = os.path.join(output_subdir, speech_filename)

                        # Write speech file with comprehensive header for ATLAS.ti
                        with open(speech_path, 'w', encoding='utf-8') as out_f:
                            out_f.write(f"[METADATA]\n")
                            out_f.write(f"Speech ID: {speech_id}\n")
                            out_f.write(f"Year: {year}\n")
                            out_f.write(f"Session: {subdir.replace('session_', '')}\n")
                            out_f.write(f"Meeting: {base_name}\n")
                            out_f.write(f"Country: {speech['country']}\n")
                            out_f.write(f"Region: {region}\n")
                            out_f.write(f"Speaker: {speech['speaker']}\n")
                            out_f.write(f"Language: {language}\n")
                            # Chinese-led organization membership (at time of speech)
                            out_f.write(f"FOCAC Member: {is_member_at_time(speech['country'], FOCAC_MEMBERS, year)}\n")
                            out_f.write(f"CASCF Member: {is_member_at_time(speech['country'], CASCF_MEMBERS, year)}\n")
                            out_f.write(f"SCO Member: {is_member_at_time(speech['country'], SCO_MEMBERS, year)}\n")
                            out_f.write(f"BRI Member: {is_member_at_time(speech['country'], BRI_MEMBERS, year, BRI_EXITED)}\n")
                            out_f.write(f"CELAC Member: {is_member_at_time(speech['country'], CELAC_MEMBERS, year)}\n")
                            out_f.write(f"[/METADATA]\n\n")
                            out_f.write(speech_text)

                        # Calculate Chinese org membership
                        country = speech['country']
                        focac_joined = FOCAC_MEMBERS.get(country)
                        cascf_joined = CASCF_MEMBERS.get(country)
                        sco_joined = SCO_MEMBERS.get(country)
                        bri_joined = BRI_MEMBERS.get(country)
                        bri_exited = BRI_EXITED.get(country)
                        celac_joined = CELAC_MEMBERS.get(country)

                        # Boolean: is member (ever joined, regardless of exit)
                        is_focac = focac_joined is not None
                        is_cascf = cascf_joined is not None
                        is_sco = sco_joined is not None
                        is_bri = bri_joined is not None
                        is_celac = celac_joined is not None

                        # Boolean: was member at time of speech (accounts for exits)
                        focac_at_speech = is_member_at_time(country, FOCAC_MEMBERS, year)
                        cascf_at_speech = is_member_at_time(country, CASCF_MEMBERS, year)
                        sco_at_speech = is_member_at_time(country, SCO_MEMBERS, year)
                        bri_at_speech = is_member_at_time(country, BRI_MEMBERS, year, BRI_EXITED)
                        celac_at_speech = is_member_at_time(country, CELAC_MEMBERS, year)

                        # Track metadata for all speeches
                        # meeting_id: filename without extension (e.g., "meeting_48_05")
                        meeting_id = os.path.splitext(filename)[0]
                        is_head_of_state = bool(HEAD_OF_STATE_PATTERN.match(speech['speaker']))
                        word_count = len(speech_text.split())
                        all_speeches.append({
                            'speech_id': speech_id,
                            'meeting_id': meeting_id,
                            'session': subdir,
                            'year': year,
                            'source_file': filename,
                            'output_file': speech_filename,
                            'output_path': speech_path,
                            'speaker': speech['speaker'],
                            'country': speech['country'],
                            'region': region,
                            'language': language,
                            'word_count': word_count,
                            'paragraph_count': paragraph_count,
                            'is_head_of_state': is_head_of_state,
                            # Chinese-led organization membership (boolean: ever member)
                            'is_focac_member': is_focac,
                            'is_cascf_member': is_cascf,
                            'is_sco_member': is_sco,
                            'is_bri_member': is_bri,
                            # Join year (None if not member)
                            'focac_joined': focac_joined,
                            'cascf_joined': cascf_joined,
                            'sco_joined': sco_joined,
                            'bri_joined': bri_joined,
                            # Exit year (None if not exited) - currently only BRI has exits
                            'bri_exited': bri_exited,
                            # Was member at time of speech (accounts for exits)
                            'focac_at_speech': focac_at_speech,
                            'cascf_at_speech': cascf_at_speech,
                            'sco_at_speech': sco_at_speech,
                            'bri_at_speech': bri_at_speech,
                            # China-CELAC Forum membership
                            'is_celac_member': is_celac,
                            'celac_joined': celac_joined,
                            'celac_at_speech': celac_at_speech,
                        })

                        # Accumulate for meeting-level metadata
                        meeting_word_count += word_count
                        meeting_countries.add(speech['country'])
                        meeting_languages.add(language)
                        meeting_speech_count += 1
                        if is_head_of_state:
                            meeting_head_of_state_count += 1

                    # Track meeting-level metadata (aggregated from extracted speeches)
                    meeting_id = os.path.splitext(filename)[0]
                    meeting_flagged_count = sum(1 for f in flagged_lines if f['file'] == filename)
                    all_meetings.append({
                        'meeting_id': meeting_id,
                        'session': subdir,
                        'meeting_file': filename,
                        'speech_count': meeting_speech_count,
                        'country_count': len(meeting_countries),
                        'countries': '; '.join(sorted(meeting_countries)),
                        'languages': '; '.join(sorted(meeting_languages)),
                        'total_word_count': meeting_word_count,
                        'head_of_state_count': meeting_head_of_state_count,
                        'flagged_count': meeting_flagged_count
                    })

    # Create DataFrames and save to CSV
    df = pd.DataFrame(all_speeches)
    df.to_csv(metadata_csv, index=False)

    df_meetings = pd.DataFrame(all_meetings)

    # Create ATLAS.ti-compatible document variables CSV
    # ATLAS.ti expects: Document Name, Variable1, Variable2, ...
    # Document Name must match the filename exactly (without path)
    df_atlasti = df[['output_file', 'speech_id', 'session', 'year', 'country', 'region',
                      'speaker', 'language', 'word_count', 'paragraph_count', 'is_head_of_state',
                      'is_focac_member', 'is_cascf_member', 'is_sco_member', 'is_bri_member', 'is_celac_member',
                      'focac_joined', 'cascf_joined', 'sco_joined', 'bri_joined', 'bri_exited', 'celac_joined',
                      'focac_at_speech', 'cascf_at_speech', 'sco_at_speech', 'bri_at_speech', 'celac_at_speech']].copy()
    df_atlasti = df_atlasti.rename(columns={
        'output_file': 'Document Name',
        'speech_id': 'Speech ID',
        'session': 'Session',
        'year': 'Year',
        'country': 'Country',
        'region': 'UN Region',
        'speaker': 'Speaker',
        'language': 'Language',
        'word_count': 'Word Count',
        'paragraph_count': 'Paragraph Count',
        'is_head_of_state': 'Head of State',
        'is_focac_member': 'FOCAC Member',
        'is_cascf_member': 'CASCF Member',
        'is_sco_member': 'SCO Member',
        'is_bri_member': 'BRI Member',
        'is_celac_member': 'CELAC Member',
        'focac_joined': 'FOCAC Joined',
        'cascf_joined': 'CASCF Joined',
        'sco_joined': 'SCO Joined',
        'bri_joined': 'BRI Joined',
        'bri_exited': 'BRI Exited',
        'celac_joined': 'CELAC Joined',
        'focac_at_speech': 'FOCAC At Speech',
        'cascf_at_speech': 'CASCF At Speech',
        'sco_at_speech': 'SCO At Speech',
        'bri_at_speech': 'BRI At Speech',
        'celac_at_speech': 'CELAC At Speech',
    })
    df_atlasti.to_csv(atlasti_variables_csv, index=False)
    df_meetings.to_csv(meeting_metadata_csv, index=False)

    # Write log file with summary statistics and review items
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"Speech Extraction Log - {datetime.now().isoformat()}\n")
        f.write("=" * 80 + "\n\n")

        # Section 0: Summary Statistics
        f.write("SUMMARY STATISTICS\n")
        f.write("-" * 40 + "\n")
        f.write(f"Total files found: {len(all_meetings) + len(skipped_files)}\n")
        f.write(f"Total meetings processed: {len(all_meetings)}\n")
        f.write(f"Files skipped (no general debate): {len(skipped_files)}\n")
        f.write(f"Total speeches extracted: {len(all_speeches)}\n")
        f.write(f"Total sessions processed: {df['session'].nunique()}\n")
        f.write(f"Total countries represented: {df['country'].nunique()}\n")
        f.write(f"Total word count: {df['word_count'].sum():,}\n")
        f.write(f"Average speech length: {df['word_count'].mean():.0f} words\n")
        f.write(f"Average speeches per meeting: {df_meetings['speech_count'].mean():.1f}\n")
        f.write(f"Head-of-state speeches: {df['is_head_of_state'].sum()}\n")
        f.write(f"\nSpeeches by session:\n")
        session_counts = df.groupby('session').size().sort_index()
        for session, count in session_counts.items():
            f.write(f"  {session}: {count}\n")
        f.write(f"\nTop 10 countries by speech count:\n")
        country_counts = df['country'].value_counts().head(10)
        for country, count in country_counts.items():
            f.write(f"  {country}: {count}\n")
        f.write(f"\nLanguage breakdown:\n")
        lang_counts = df['language'].value_counts()
        for lang, count in lang_counts.items():
            f.write(f"  {lang}: {count}\n")
        f.write("\n" + "=" * 80 + "\n\n")

        # Section 1: Head-of-state speeches
        f.write("HEAD-OF-STATE/GOVERNMENT SPEECHES\n")
        f.write("-" * 40 + "\n")
        f.write("These are speeches by heads of state (titled 'President [Name]') during the\n")
        f.write("General Debate. They are legitimate GA speeches but may warrant separate\n")
        f.write("consideration in your analysis. Discuss with research partners.\n\n")
        if head_of_state_speeches:
            f.write(f"Found {len(head_of_state_speeches)} head-of-state speeches:\n\n")
            for item in head_of_state_speeches:
                f.write(f"  {item['speaker']} ({item['country']}) - {item['file']}\n")
        else:
            f.write("No head-of-state speeches found.\n")
        f.write("\n" + "=" * 80 + "\n\n")

        # Section 2: Skipped files
        f.write("SKIPPED FILES (NO GENERAL DEBATE)\n")
        f.write("-" * 40 + "\n")
        f.write("These files were skipped because they don't contain the 'Agenda item ... General debate'\n")
        f.write("pattern. They may be procedural meetings, elections, or other non-debate sessions.\n\n")
        if skipped_files:
            f.write(f"Skipped {len(skipped_files)} files:\n\n")
            for item in sorted(skipped_files, key=lambda x: (x['session'], x['file'])):
                f.write(f"  {item['session']}/{item['file']}\n")
        else:
            f.write("No files skipped - all files contained general debate section.\n")
        f.write("\n" + "=" * 80 + "\n\n")

        # Section 3: Flagged lines
        f.write("FLAGGED LINES FOR MANUAL REVIEW\n")
        f.write("-" * 40 + "\n")
        f.write("These lines look like delegate speeches but failed strict pattern validation.\n\n")
        if flagged_lines:
            f.write(f"Found {len(flagged_lines)} flagged lines:\n\n")
            for item in flagged_lines:
                f.write(f"File: {item['file']}\n")
                f.write(f"Line: {item['line']}\n")
                f.write(f"Reason: {item['reason']}\n")
                f.write("-" * 40 + "\n")
        else:
            f.write("No flagged lines - all potential speeches matched strict pattern.\n")

    # Print summary
    print(f"\n✅ Extracted {len(all_speeches)} speeches from {len(all_meetings)} meetings across {df['session'].nunique()} sessions")
    print(f"📊 Speech metadata saved to: {metadata_csv}")
    print(f"📊 Meeting metadata saved to: {meeting_metadata_csv}")
    print(f"📊 ATLAS.ti document variables saved to: {atlasti_variables_csv}")
    print(f"📋 Extraction log written to: {log_file}")
    if head_of_state_speeches:
        print(f"   ℹ️  {len(head_of_state_speeches)} head-of-state/government speeches noted for review")
    if skipped_files:
        print(f"   ℹ️  {len(skipped_files)} files skipped (no general debate pattern)")
    if flagged_lines:
        print(f"   ⚠️  {len(flagged_lines)} potential speeches flagged for manual review")
    else:
        print("   ✓ No flagged lines - all potential speeches matched strict pattern")


if __name__ == "__main__":
    split_texts()
